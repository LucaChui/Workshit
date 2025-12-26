#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量对比并（可选）修改资产名称(name)与资产类型(clasz)的脚本

功能：
1) 读取本地 Excel（列：uuid，name，可选；clasz，可选）
2) 调用接口按 uuid 获取系统里的资产，拿到 id/name/clasz（旧值）
3) 对比 Excel 与系统的 name/clasz，报告差异
4) 若未启用 dry-run，则尝试更新（多候选端点/载荷，尽量兼容）
5) 导出 CSV 报告：uuid、asset_id、name_old/name_new、clasz_old/clasz_new、status、message

登录：表单（application/x-www-form-urlencoded），拿 token 后走 Authorization: Bearer <token>
查询：GET /v1/xasset/asset/{uuid}
更新：尝试 PUT/PATCH/POST 不同写法，兼容 json/form 载荷
"""

import logging
import sys
import csv
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry

DEFAULT_API_ROOT = "http://66.natic.cn:21200/v1/xasset"


def make_session() -> requests.Session:
    """创建会话；对 429/502/503/504 适度重试；不对 500 重试以免掩盖服务真实错误。"""
    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.4,
        status_forcelist=[429, 502, 503, 504],
        allowed_methods=["GET", "PUT", "POST", "PATCH", "DELETE"],
    )
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    # 如需强制不走系统代理，取消下两行注释
    # s.trust_env = False
    # s.proxies = {"http": None, "https": None}
    return s


def login(api_root: str, sess: requests.Session, username: str, password: str, timeout: int = 30) -> str:
    """表单登录，返回 token 字符串。"""
    url = api_root.rstrip("/") + "/user/login"
    payload = {"username": username, "password": password}
    logging.debug("POST %s form=%s", url, payload)
    r = sess.post(url, data=payload, timeout=timeout)
    logging.debug("resp %s text=%s", r.status_code, r.text)
    if not (200 <= r.status_code < 300):
        raise RuntimeError(f"HTTP {r.status_code}: {r.text}")
    body = r.json()
    code = body.get("code")
    if code not in (200, 0):
        raise RuntimeError(f"登录失败：code={code}, message={body.get('message')}")
    token = body.get("data")
    if not token or not isinstance(token, str):
        raise RuntimeError("登录成功但未返回 token")
    return token


def get_asset_by_uuid(api_root: str, sess: requests.Session, uuid: str, timeout: int = 30) -> Optional[Dict[str, Any]]:
    """按 uuid 获取资产详情，返回 data dict 或 None。"""
    url = api_root.rstrip("/") + f"/asset/{uuid}"
    logging.debug("GET %s", url)
    r = sess.get(url, timeout=timeout)
    if not (200 <= r.status_code < 300):
        return None
    body = r.json()
    return body.get("data")


def try_update_asset(api_root: str, sess: requests.Session, asset_id: str,
                     name: Optional[str], clasz: Optional[str], timeout: int = 30) -> Tuple[bool, str]:
    """
    阶梯式回退更新：多端点 + 多载荷变体（json/form），尽量兼容不同后端实现。
    返回 (成功?, 说明/尝试轨迹摘要)
    """
    base = api_root.rstrip("/")

    # 构造几组载荷变体（包含 clasz / claze、是否携带 id）
    variants: List[Dict[str, Dict[str, Any]]] = []

    def dmerge(**kwargs):
        return {k: v for k, v in kwargs.items() if v is not None}

    # JSON：带 id
    variants.append({"json": dmerge(id=asset_id, name=name, clasz=clasz)})
    variants.append({"json": dmerge(id=asset_id, name=name, claze=clasz)})  # 某些文档/实现写成 claze
    # JSON：不带 id（从 path 取 id）
    variants.append({"json": dmerge(name=name, clasz=clasz)})
    variants.append({"json": dmerge(name=name, claze=clasz)})
    # FORM 版本（表单）
    form1 = dmerge(id=asset_id, name=name, clasz=clasz)
    form2 = dmerge(id=asset_id, name=name, claze=clasz)
    variants.append({"data": form1})
    variants.append({"data": form2})

    candidates: List[Tuple[str, str, Optional[Dict[str, str]], Dict[str, Any]]] = []
    for send in variants:
        candidates.extend([
            ("PUT",   f"{base}/asset/{asset_id}", None, send),
            ("PATCH", f"{base}/asset/{asset_id}", None, send),
            ("PUT",   f"{base}/asset",            None, send),
            ("POST",  f"{base}/asset/update",     None, send),
        ])

    tried: List[str] = []
    for method, url, params, send in candidates:
        try:
            r = sess.request(method, url, params=params, timeout=timeout, **send)
            snippet = r.text[:200].replace("\n", " ")
            tried.append(f"{method} {url} -> {r.status_code} {snippet}")
            if 200 <= r.status_code < 300:
                return True, f"{method} {url} OK"
            # 业务 code 兜底
            try:
                js = r.json()
                code = js.get("code") or js.get("Code")
                if str(code) in ("0", "200"):
                    return True, f"{method} {url} OK(code)"
            except Exception:
                pass
        except Exception as e:
            tried.append(f"{method} {url} -> EXC {repr(e)}")

    # 失败：返回前 4 条轨迹，避免过长
    summary = " | ".join(tried[:4]) + (" ..." if len(tried) > 4 else "")
    return False, summary or "no compatible update endpoint"


def run():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    api_root_in = input(f"请输入 API 根地址（默认 {DEFAULT_API_ROOT}）：").strip()
    api_root = api_root_in or DEFAULT_API_ROOT

    excel = input("请输入 Excel 路径（含 uuid/name/clasz 列）：").strip()
    user = input("请输入登录用户名：").strip()
    pwd = input("请输入密码（回显）：")

    dry_run_in = input("是否开启 dry-run 模式 (y/N)：").strip().lower()
    dry_run = dry_run_in == "y"

    report_csv = "bulk_update_report.csv"

    sess = make_session()
    token = login(api_root, sess, user, pwd)
    sess.headers["Authorization"] = f"Bearer {token}"

    df = pd.read_excel(excel)
    # 列名统一为小写
    df.columns = [str(c).strip().lower() for c in df.columns]

    # 输出字段：包含旧值/新值与差异说明
    fieldnames = [
        "uuid", "asset_id",
        "name_old", "name_new",
        "clasz_old", "clasz_new",
        "status", "message"
    ]

    out_rows: List[Dict[str, Any]] = []
    for i, row in df.iterrows():
        uuid = str(row.get("uuid") or "").strip()
        name_new = str(row.get("name") or "").strip() or None
        clasz_new = str(row.get("clasz") or "").strip() or None
        if not uuid:
            # 跳过空 uuid 行
            continue

        asset = get_asset_by_uuid(api_root, sess, uuid)
        if not asset:
            out_rows.append({
                "uuid": uuid,
                "asset_id": "",
                "name_old": "",
                "name_new": name_new or "",
                "clasz_old": "",
                "clasz_new": clasz_new or "",
                "status": "not_found",
                "message": "系统未找到该UUID"
            })
            continue

        asset_id = str(asset.get("id") or "")
        name_old = asset.get("name")
        clasz_old = asset.get("clasz")

        # 差异说明
        diffs: List[str] = []
        if name_new and name_new != name_old:
            diffs.append(f"name差异: 系统={name_old} | Excel={name_new}")
        if clasz_new and clasz_new != clasz_old:
            diffs.append(f"clasz差异: 系统={clasz_old} | Excel={clasz_new}")
        diff_note = "； ".join(diffs) if diffs else "无差异"

        if dry_run:
            out_rows.append({
                "uuid": uuid,
                "asset_id": asset_id,
                "name_old": name_old or "",
                "name_new": name_new or "",
                "clasz_old": clasz_old or "",
                "clasz_new": clasz_new or "",
                "status": "dry-run",
                "message": diff_note
            })
            continue

        # 只有确实有变化时才尝试更新，减少无效请求
        if (name_new and name_new != name_old) or (clasz_new and clasz_new != clasz_old):
            ok, msg = try_update_asset(api_root, sess, asset_id, name_new, clasz_new)
            out_rows.append({
                "uuid": uuid,
                "asset_id": asset_id,
                "name_old": name_old or "",
                "name_new": name_new or "",
                "clasz_old": clasz_old or "",
                "clasz_new": clasz_new or "",
                "status": "success" if ok else "failed",
                "message": (diff_note + "； " + msg) if diff_note else msg
            })
        else:
            out_rows.append({
                "uuid": uuid,
                "asset_id": asset_id,
                "name_old": name_old or "",
                "name_new": name_new or "",
                "clasz_old": clasz_old or "",
                "clasz_new": clasz_new or "",
                "status": "skipped",
                "message": "无差异，跳过更新"
            })

    # 写报告
    with open(report_csv, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in out_rows:
            writer.writerow(r)
    logging.info("已生成报告：%s", report_csv)


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logging.exception("执行失败：%s", e)
        sys.exit(1)
