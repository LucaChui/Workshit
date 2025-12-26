# -*- coding: utf-8 -*-
"""
DAM 资产清单导出脚本（按文档版，密码输入回显）
- API_ROOT（默认）: http://66.natic.cn:8090/v1/xasset
- 登录: POST {API_ROOT}/user/login，表单: username=...&password=...，要求响应 {"code":200,"message":"ok","data":"<token>"}
- 资产列表: GET {API_ROOT}/asset?page=&pageCount=&username=
- 资产详情: GET {API_ROOT}/asset/{uuid}
- 结构树: GET {API_ROOT}/asset/structure?uuids=<uuid>
- 导出: Excel(assets_summary / files_detail / suffix_stats)
"""

import argparse
import logging
import os
import sys
import time
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

# 注意：应需求改为回显输入，因此不再使用 getpass
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ========= Session / Retry =========
def build_session(token: Optional[str] = None, retries: int = 3, backoff: float = 0.8) -> requests.Session:
    s = requests.Session()
    if token:
        s.headers["Authorization"] = f"Bearer {token}"
    s.headers["Accept"] = "application/json;charset=utf-8"
    s.headers["Accept-Encoding"] = "gzip"
    retry_cfg = Retry(
        total=retries, connect=retries, read=retries, status=retries,
        backoff_factor=backoff, status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"]),
        respect_retry_after_header=True, raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_cfg)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


# ========= Helpers =========
def _basename_from_furl(furl: Optional[str]) -> Optional[str]:
    if not furl:
        return None
    path = urllib.parse.urlparse(furl).path
    name = os.path.basename(path)
    return name or None


def _normalize_id(v: Any) -> Optional[str]:
    return str(v) if v is not None else None


def _extract_asset_list_page(body: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Optional[int], Optional[int]]:
    items: List[Dict[str, Any]] = []
    pages = body.get("pages")
    page_index = body.get("pageIndex")
    data = body.get("data")
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        inner = data.get("data")
        if isinstance(inner, list):
            items = inner
        pages = data.get("pages", pages)
        page_index = data.get("pageIndex", page_index)
    if not isinstance(items, list):
        items = []
    return items, pages, page_index


# ========= API calls (严格按文档) =========
def login(api_root: str, username: str, password: str, timeout: int, sess: Optional[requests.Session] = None) -> str:
    url = api_root.rstrip("/") + "/user/login"
    s = sess or build_session()
    payload = {"username": username, "password": password}
    logging.debug("Request payload(form): %s", payload)
    logging.info("POST %s (login as %s)", url, username)

    # ① 改为表单提交；不再附带 params
    r = s.post(url, data=payload, timeout=timeout)

    logging.debug("Set-Cookie: %s", r.headers.get("Set-Cookie"))
    logging.debug("Response: %s", r.text)
    if not (200 <= r.status_code < 300):
        raise RuntimeError(f"HTTP {r.status_code}: {r.text}")
    body = r.json()
    code = body.get("code")
    if code not in (200, 0):
        raise RuntimeError(f"登录失败：code={code}, message={body.get('message')}")
    token = body.get("data")
    if not token or not isinstance(token, str):
        raise RuntimeError("登录成功但未返回 token（data 字段为空）")
    return token


def list_assets_by_username(api_root: str, sess: requests.Session, owner_username: str,
                            page_size: int = 50, timeout: int = 30) -> List[Dict[str, Any]]:
    asset_base = api_root.rstrip("/") + "/asset"
    all_assets: List[Dict[str, Any]] = []
    page = 1
    while True:
        params = {"page": page, "pageCount": page_size, "username": owner_username}
        url = asset_base
        logging.info("GET %s params=%s", url, params)
        r = sess.get(url, params=params, timeout=timeout)
        if not (200 <= r.status_code < 300):
            raise RuntimeError(f"HTTP {r.status_code}: {r.text}")
        body = r.json()
        items, pages, page_index = _extract_asset_list_page(body)
        all_assets.extend(items)
        logging.debug("Fetched page %s: %d items (total so far %d)", page, len(items), len(all_assets))
        if pages is None or page_index is None or page_index >= pages:
            break
        page += 1
    return all_assets

def get_structure_filenames(api_root: str, sess: requests.Session, uuid: str,
                            timeout: int = 30) -> List[Dict[str, Any]]:
    url = api_root.rstrip("/") + "/asset/structure"
    params = [("uuids", uuid)]
    logging.info("GET %s params=%s", url, params)
    try:
        r = sess.get(url, params=params, timeout=timeout)
        if not (200 <= r.status_code < 300):
            raise RuntimeError(f"HTTP {r.status_code}: {r.text}")
        payload = r.json().get("data")
        if not payload:
            return []
        nodes: List[Dict[str, Any]] = []
        trees: List[Dict[str, Any]] = payload if isinstance(payload, list) else [payload]

        def dfs(node: Dict[str, Any]):
            for ch in node.get("children") or []:
                ch_type = ch.get("type")
                if ch_type in ("MODEL", "IMAGE"):
                    nodes.append({
                        "id": ch.get("id"),
                        "name": ch.get("name"),
                        "type": ch_type,
                        "modelType": (ch.get("details") or {}).get("modelType"),
                        "url": ch.get("url"),
                    })
                if ch.get("children"):
                    dfs(ch)

        for t in trees:
            dfs(t)
        return nodes
    except Exception as e:
        logging.warning("structure fetch failed for %s: %s", uuid, e)
        return []


def get_asset_detail(api_root: str, sess: requests.Session, uuid: str,
                     timeout: int = 30) -> Optional[Dict[str, Any]]:
    url = api_root.rstrip("/") + f"/asset/{uuid}"
    logging.info("GET %s", url)
    r = sess.get(url, timeout=timeout)
    try:
        if not (200 <= r.status_code < 300):
            raise RuntimeError(f"HTTP {r.status_code}: {r.text}")
    except Exception as e:
        logging.warning("asset detail fetch failed for %s: %s", uuid, e)
        return None
    body = r.json()
    return body.get("data", body)


# ========= Checklist builders =========
def build_checklist_rows(asset: Dict[str, Any], structure_files: List[Dict[str, Any]],
                         detail_asset: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    asset_name = asset.get("name")
    asset_uuid = asset.get("uuid")
    asset_class = asset.get("clasz")
    creator = asset.get("creator")
    creation_date = asset.get("creationDate")

    files = asset.get("files") or []
    rows: List[Dict[str, Any]] = []

    struct_name_by_id: Dict[str, str] = {}
    for sf in structure_files:
        sid = _normalize_id(sf.get("id"))
        if sid and sf.get("name"):
            struct_name_by_id[sid] = sf["name"]

    detail_name_by_id: Dict[str, str] = {}
    if detail_asset:
        for df in (detail_asset.get("files") or []):
            did = _normalize_id(df.get("id"))
            if not did:
                continue
            for k in ("name", "fileName", "originalName", "filename"):
                val = df.get(k)
                if val:
                    detail_name_by_id[did] = val
                    break

    def display_name(f: Dict[str, Any]) -> str:
        fid = _normalize_id(f.get("id"))
        if fid and fid in struct_name_by_id:
            return struct_name_by_id[fid]
        if fid and fid in detail_name_by_id:
            return detail_name_by_id[fid]
        base = _basename_from_furl(f.get("furl"))
        if base:
            return base
        suffix = (f.get("suffix") or "").lower() or None
        if suffix:
            return f"{f.get('type') or 'FILE'}.{suffix}"
        return f.get("type") or "FILE"

    for f in files:
        rows.append({
            "asset_name": asset_name,
            "asset_uuid": asset_uuid,
            "asset_class": asset_class,
            "creator": creator,
            "creationDate": creation_date,
            "file_id": f.get("id"),
            "file_type": f.get("type"),
            "file_suffix": f.get("suffix"),
            "file_modelType": f.get("modelType"),
            "file_size": f.get("specification"),
            "file_url": f.get("furl"),
            "file_name": display_name(f),
        })

    type_counts: Dict[str, int] = {}
    suf_counts: Dict[str, int] = {}
    for r in rows:
        t = (r["file_type"] or "UNKNOWN").upper()
        s = (r["file_suffix"] or "NONE").lower()
        type_counts[t] = type_counts.get(t, 0) + 1
        suf_counts[s] = suf_counts.get(s, 0) + 1

    summary = {
        "asset_name": asset_name,
        "asset_uuid": asset_uuid,
        "asset_class": asset_class,
        "creator": creator,
        "creationDate": creation_date,
        "file_count": len(rows),
        "file_types": ", ".join(f"{k}:{v}" for k, v in sorted(type_counts.items())),
        "file_suffixes": ", ".join(f"{k}:{v}" for k, v in sorted(suf_counts.items())),
    }
    return rows, summary


# ========= Main =========
def main():
    ap = argparse.ArgumentParser(description="按用户导出 DAM 资产清单（Excel），严格按文档调用 API。")
    ap.add_argument("--api-root", default="http://66.natic.cn:21200/v1/xasset",
                    help="API 根路径（不含 /asset /user/login），默认为文档地址 http://66.natic.cn:21200/v1/xasset")
    ap.add_argument("--owner", default=None, help="按此用户名筛选资产（默认=登录用户名）")
    ap.add_argument("--page-size", type=int, default=50)
    ap.add_argument("--timeout", type=int, default=30)
    ap.add_argument("--retries", type=int, default=3)
    ap.add_argument("--backoff", type=float, default=0.8)
    ap.add_argument("--log-level", default="INFO", help="DEBUG/INFO/WARNING/ERROR")
    args = ap.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    api_root = args.api_root.rstrip("/")
    logging.info("API_ROOT=%s", api_root)

    login_user = input("请输入登录用户名：").strip()
    # 改为回显输入
    password = input("请输入密码（回显）：")
    owner = args.owner or login_user

    out_in = input("输出 Excel 文件路径（默认 assets_checklist.xlsx）：").strip()
    if not out_in:
        out = "assets_checklist.xlsx"
    else:
        if out_in.endswith(("\\", "/")) or os.path.isdir(out_in):
            out = os.path.join(out_in, "assets_checklist.xlsx")
        else:
            out = out_in
    out_dir = os.path.dirname(out)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    base_sess = build_session(retries=args.retries, backoff=args.backoff)

    logging.info("[1/4] 登录为 %s", login_user)
    token = login(api_root, login_user, password, timeout=args.timeout, sess=base_sess)

    # ② 继续使用新的鉴权 Session，但额外增加 token 头以兼容网关
    sess = build_session(token=token, retries=args.retries, backoff=args.backoff)
    sess.headers["token"] = token

    logging.info("[2/4] 拉取 %s 的资产列表（pageSize=%d）", owner, args.page_size)
    assets = list_assets_by_username(api_root, sess, owner, page_size=args.page_size, timeout=args.timeout)
    logging.info("资产总数：%d", len(assets))

    logging.info("[3/4] 汇总每个资产的文件明细（结构树精确匹配 + 详情 fallback）")
    all_file_rows: List[Dict[str, Any]] = []
    all_summaries: List[Dict[str, Any]] = []

    for i, a in enumerate(assets, 1):
        uuid = a.get("uuid")
        struct_files: List[Dict[str, Any]] = []
        detail_asset: Optional[Dict[str, Any]] = None
        if uuid:
            struct_files = get_structure_filenames(api_root, sess, uuid, timeout=args.timeout)
            if not struct_files:
                detail_asset = get_asset_detail(api_root, sess, uuid, timeout=args.timeout)

        rows, summary = build_checklist_rows(a, struct_files, detail_asset=detail_asset)
        all_file_rows.extend(rows)
        all_summaries.append(summary)
        if i % 20 == 0:
            time.sleep(0.2)

    logging.info("[4/4] 导出 Excel → %s", out)
    with pd.ExcelWriter(out, engine="openpyxl") as xw:
        df_files = pd.DataFrame(all_file_rows)
        df_assets = pd.DataFrame(all_summaries)
        if not df_assets.empty:
            df_assets = df_assets.sort_values(by=["file_count", "asset_name"], ascending=[False, True])
        df_assets.to_excel(xw, index=False, sheet_name="assets_summary")
        df_files.to_excel(xw, index=False, sheet_name="files_detail")
        if not df_files.empty:
            pivot_suffix = (
                df_files.assign(file_suffix=df_files["file_suffix"].fillna("NONE"))
                .pivot_table(index="file_suffix", values="file_id", aggfunc="count")
                .rename(columns={"file_id": "count"})
                .sort_values("count", ascending=False)
            )
            pivot_suffix.to_excel(xw, sheet_name="suffix_stats")

    logging.info("完成 ✅ 输出文件：%s", out)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception("执行失败：%s", e)
        sys.exit(1)
