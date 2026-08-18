#!/usr/bin/env python3
"""One-shot setup for the Airtable `ReactionPicks` table (Plan 4).

Plan 4 (2026-08-17) 上線後新增的表 — 由 ``pipeline/lib/discord_picks.py``
被動寫入 user 在 ``#每日頭條`` / ``#每日podcast`` / ``#每日reddit`` 按 ✅
的訊息,供 ``pipeline/scripts/weekly_recap.py`` 週二/五 07:00 CEST 拉
過去 3 天清單推回顧。

What it does (idempotent — safe to re-run):
  1. Verify (or create) the ``ReactionPicks`` table in the ``Pipelines``
     base (``appHilorcrC5T0p2u``) with the 12 fields defined in
     ``FIELD_SPEC``(見下方)。
  2. Print the resulting table id + field list so the caller can confirm
     and wire it into:
     - ``pipeline/lib/discord_picks.py``(寫 reaction 用)
     - ``pipeline/scripts/weekly_recap.py``(讀 picks 推 recap)
     - ``cron/jobs.yaml`` 環境變數 ``AIRTABLE_REACTION_PICKS_TABLE_ID``

欄位選擇重點
------------
- ``reaction_date`` 是 dateTime,時區 ``Europe/Berlin``(user 在 Berlin
  時區按 ✅,寫入時直接用 Europe/Berlin 表示,後續 weekly_recap 拉
  「過去 3 天」時轉 Europe/Berlin 的 date 比對,不會被 UTC 偏移搞混)。
- ``message_kind`` 是 singleSelect — channel_id 自動 map 為
  news / podcast / reddit / other,寫進去後 weekly recap 不用再 parse。
- ``reaction_id`` 是 dedup key,格式
  ``f"{user_id}-{channel_id}-{message_id}-{emoji_name}"``,即使 daemon
  retry / 重啟也不會重複寫入(搭配 Airtable GET filterByFormula 先查再
  POST 的 pattern)。

Pre-reqs:
  - ``AIRTABLE_API_KEY`` env var set(PAT 開頭 ``pat...``)。
  - ``Pipelines`` base 已存在(由 ``setup_airtable_processed_content.py``
    或人工建)。

Usage:
  AIRTABLE_API_KEY=pat_xxx \\
    python3 pipeline/scripts/setup_airtable_reaction_picks.py

Exit codes:
  0  — 成功
  2  — token 缺
  3  — base 找不到
  4  — table / field 建立失敗
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib import error, request

API_BASE = "https://api.airtable.com/v0"

# 固定 base id(已存在)— 跟其他 setup script 同 base。
BASE_ID = "appHilorcrC5T0p2u"
TABLE_NAME = "ReactionPicks"


# ---------------------------------------------------------------------------
# Plan 4 spec (2026-08-17) — 12 欄位。
# schema 演進請在這裡改動,並同步通知 discord_picks.py 跟 weekly_recap.py。
# ---------------------------------------------------------------------------
FIELD_SPEC: List[Dict[str, Any]] = [
    {
        "name": "reaction_date",
        "type": "dateTime",
        "options": {
            "dateFormat": {"name": "iso"},
            "timeFormat": {"name": "24hour"},
            "timeZone": "Europe/Berlin",
        },
    },
    {"name": "channel_id", "type": "singleLineText"},
    {"name": "channel_name", "type": "singleLineText"},
    {"name": "message_id", "type": "singleLineText"},
    {"name": "message_url", "type": "url"},
    {
        "name": "message_kind",
        "type": "singleSelect",
        "options": {
            "choices": [
                {"name": "news"},
                {"name": "podcast"},
                {"name": "reddit"},
                {"name": "other"},
            ]
        },
    },
    {"name": "title", "type": "multilineText"},
    {"name": "snippet", "type": "multilineText"},
    {"name": "embed_url", "type": "url"},
    {"name": "embed_image", "type": "url"},
    {"name": "discord_user_id", "type": "singleLineText"},
    {"name": "reaction_id", "type": "singleLineText"},
]


# ---------------------------------------------------------------------------
# HTTP helpers(跟 setup_airtable_daily_digest.py 同一份,獨立 module)
# ---------------------------------------------------------------------------

class AirtableSetupError(RuntimeError):
    pass


def _request(
    method: str,
    path: str,
    *,
    body: Optional[Dict[str, Any]] = None,
    timeout: float = 30.0,
) -> Dict[str, Any]:
    token = os.environ.get("AIRTABLE_API_KEY", "")
    if not token:
        raise AirtableSetupError(
            "AIRTABLE_API_KEY env var not set. Source it from ~/.hermes/.env "
            "before running this script."
        )
    url = f"{API_BASE}{path}"
    data: Optional[bytes] = None
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = request.Request(url, data=data, method=method, headers=headers)
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            payload = resp.read().decode("utf-8")
            return json.loads(payload) if payload else {}
    except error.HTTPError as e:
        body_txt = e.read().decode("utf-8", errors="replace")
        raise AirtableSetupError(
            f"{method} {path} -> {e.code}: {body_txt}"
        ) from e


def list_tables(base_id: str) -> List[Dict[str, Any]]:
    out = _request("GET", f"/meta/bases/{base_id}/tables")
    return out.get("tables", [])


def find_table_by_name(base_id: str, name: str) -> Optional[Dict[str, Any]]:
    for t in list_tables(base_id):
        if t.get("name") == name:
            return t
    return None


def list_fields(base_id: str, table_id: str) -> List[Dict[str, Any]]:
    out = _request("GET", f"/meta/bases/{base_id}/tables/{table_id}/fields")
    return out.get("fields", [])


def create_table(
    base_id: str, name: str, fields: List[Dict[str, Any]]
) -> Dict[str, Any]:
    return _request(
        "POST", f"/meta/bases/{base_id}/tables", body={"name": name, "fields": fields}
    )


# ---------------------------------------------------------------------------
# Setup flow
# ---------------------------------------------------------------------------

def verify_base(base_id: str) -> None:
    """List tables — 如果 base 不存在,API 會回 404。"""
    try:
        list_tables(base_id)
    except AirtableSetupError as e:
        msg = str(e)
        if "404" in msg or "NOT_FOUND" in msg:
            raise AirtableSetupError(
                f"base {base_id} 找不到或 PAT 沒存取權。請先跑 "
                f"setup_airtable_processed_content.py 建立 Pipelines base,"
                f"或到 https://airtable.com 手動建。"
            ) from e
        raise


def ensure_table(base_id: str) -> str:
    """建立或驗證 ReactionPicks 表。

    注意(同 setup_airtable_daily_digest.py):
    Airtable PAT 在這個 base 上無法 ``GET /meta/bases/<id>/tables/<id>/fields``
    (回 404),所以「欄位是否建立成功」只能靠 ``POST`` 建立時的回傳。
    重跑時如果表已存在,只印出 table_id,請到 Airtable UI 確認欄位。
    """
    existing = find_table_by_name(base_id, TABLE_NAME)
    if existing:
        table_id = existing["id"]
        print(f"[table] '{TABLE_NAME}' exists: {table_id}")
        # 列出 base 內全部 table 名,協助確認。
        all_tables = [t.get("name") for t in list_tables(base_id)]
        print(f"[base {base_id}] tables: {all_tables}")
        return table_id

    print(f"[table] creating '{TABLE_NAME}' with {len(FIELD_SPEC)} fields...")
    created = create_table(base_id, TABLE_NAME, FIELD_SPEC)
    table_id = created["id"]
    created_fields = created.get("fields", [])
    print(f"[table] created '{TABLE_NAME}': {table_id}")
    print(f"[verify] POST response reports {len(created_fields)} fields:")
    for f in created_fields:
        opts = f.get("options", {})
        if f["type"] == "singleSelect":
            choices = ", ".join(c["name"] for c in opts.get("choices", []))
            print(f"  - {f['name']:<18} {f['type']:<14} (choices={choices})")
        elif f["type"] == "dateTime":
            tz = opts.get("timeZone", "?")
            print(f"  - {f['name']:<18} {f['type']:<14} (tz={tz})")
        else:
            print(f"  - {f['name']:<18} {f['type']}")
    time.sleep(0.5)
    return table_id


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> int:
    if not os.environ.get("AIRTABLE_API_KEY"):
        print(
            "AIRTABLE_API_KEY not set. Source ~/.hermes/.env before running:\n"
            "  export AIRTABLE_API_KEY=$(grep '^AIRTABLE_API_KEY=' "
            "/root/.hermes/.env | cut -d= -f2-)",
            file=sys.stderr,
        )
        return 2

    try:
        verify_base(BASE_ID)
        table_id = ensure_table(BASE_ID)
    except AirtableSetupError as e:
        msg = str(e).lower()
        if "base" in msg:
            return 3
        return 4

    print()
    print("=" * 60)
    print(f"BASE_ID  = {BASE_ID}")
    print(f"TABLE_ID = {table_id}")
    print()
    print("Wire TABLE_ID into:")
    print("  - pipeline/lib/discord_picks.py  (寫 reaction)")
    print("  - pipeline/scripts/weekly_recap.py (讀 picks 推 recap)")
    print("  - cron/jobs.yaml env: AIRTABLE_REACTION_PICKS_TABLE_ID")
    print()
    print("Field spec reminder (請到 Airtable UI 確認):")
    for f in FIELD_SPEC:
        opts = f.get("options", {})
        if f["type"] == "singleSelect":
            choices = ", ".join(c["name"] for c in opts.get("choices", []))
            print(f"  - {f['name']:<18} {f['type']:<14} ({choices})")
        elif f["type"] == "dateTime":
            tz = opts.get("timeZone", "?")
            print(f"  - {f['name']:<18} {f['type']:<14} (tz={tz})")
        else:
            print(f"  - {f['name']:<18} {f['type']}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
