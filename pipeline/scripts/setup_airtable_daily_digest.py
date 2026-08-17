#!/usr/bin/env python3
"""One-shot setup for the Airtable `DailyDigestPicks` table (Plan 3).

What it does (idempotent — safe to re-run):
  1. Verify (or create) the `DailyDigestPicks` table in the `Pipelines`
     base (``appHilorcrC5T0p2u``) with the 11 fields defined inline
     (see FIELD_SPEC). The fields are stable per the Plan 3 spec, so
     we hard-code them here instead of going through a separate JSON
     file. If the table already exists, only missing fields are added.
  2. Print the resulting table id + field list so the caller can
     confirm and wire it into ``pipeline/scripts/daily_digest.py``.

Pre-reqs:
  - ``AIRTABLE_API_KEY`` env var set (PAT starting with ``pat...``).
  - The ``Pipelines`` base must already exist (created by
    ``setup_airtable_processed_content.py`` or by hand).

Usage:
  AIRTABLE_API_KEY=pat_xxx \\
    python3 pipeline/scripts/setup_airtable_daily_digest.py

Exit codes:
  0  — success
  2  — token missing
  3  — base not found
  4  — table / field creation failed
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

# 固定 base id(已存在)— 跟 setup_airtable_processed_content.py 同 base。
BASE_ID = "appHilorcrC5T0p2u"
TABLE_NAME = "DailyDigestPicks"


# ---------------------------------------------------------------------------
# Plan 3 spec(2026-08-17)— 11 欄位。schema 演進請在這裡改動。
# ---------------------------------------------------------------------------
FIELD_SPEC: List[Dict[str, Any]] = [
    {
        "name": "digest_date",
        "type": "date",
        "options": {"dateFormat": {"name": "iso"}},
    },
    {
        "name": "source_type",
        "type": "singleSelect",
        "options": {
            "choices": [
                {"name": "news"},
                {"name": "youtube"},
                {"name": "reddit"},
            ]
        },
    },
    {"name": "source_name", "type": "singleLineText"},
    {"name": "title", "type": "singleLineText"},
    {"name": "url", "type": "url"},
    {"name": "vault_path", "type": "singleLineText"},
    {
        "name": "picked",
        "type": "singleSelect",
        "options": {
            "choices": [
                {"name": "yes"},
                {"name": "no"},
                {"name": "maybe"},
            ]
        },
    },
    {
        "name": "picked_at",
        "type": "dateTime",
        "options": {
            "dateFormat": {"name": "iso"},
            "timeFormat": {"name": "24hour"},
            "timeZone": "Europe/Berlin",
        },
    },
    {"name": "discord_user_id", "type": "singleLineText"},
    {"name": "message_id", "type": "singleLineText"},
    {
        "name": "content_kind",
        "type": "singleSelect",
        "options": {
            "choices": [
                {"name": "short-summary"},
                {"name": "longform"},
                {"name": "paywall-preview"},
                {"name": "short-paywall-preview"},
                {"name": "full-article"},
            ]
        },
    },
]


# ---------------------------------------------------------------------------
# HTTP helpers
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


def add_field(
    base_id: str, table_id: str, field: Dict[str, Any]
) -> Dict[str, Any]:
    return _request(
        "POST",
        f"/meta/bases/{base_id}/tables/{table_id}/fields",
        body=field,
    )


# ---------------------------------------------------------------------------
# Setup flow
# ---------------------------------------------------------------------------

def verify_base(base_id: str) -> None:
    """List tables — if the base doesn't exist, the API returns 404."""
    try:
        list_tables(base_id)
    except AirtableSetupError as e:
        msg = str(e)
        if "404" in msg or "NOT_FOUND" in msg:
            raise AirtableSetupError(
                f"base {base_id} not found or PAT has no access. "
                "Run setup_airtable_processed_content.py first to create the "
                "Pipelines base, or create it by hand at https://airtable.com."
            ) from e
        raise


def ensure_table(base_id: str) -> str:
    """建立或驗證 DailyDigestPicks 表。

    注意:Airtable PAT(``create`` 權限)可以 ``POST`` 新表 + 欄位,但
    ``GET /meta/bases/<id>/tables/<id>/fields`` 在這個 PAT 上回 404
    (2026-08-17 實測)。因此「驗證欄位是否建立成功」靠 ``POST`` 回傳
    的 ``fields`` 陣列;「補漏欄位」步驟也只在新表建立後跑(舊表 re-run
    視為 schema 已固定)。
    """
    existing = find_table_by_name(base_id, TABLE_NAME)
    if existing:
        table_id = existing["id"]
        print(f"[table] '{TABLE_NAME}' exists: {table_id}")
        # PAT 無法讀 fields → 跳過補漏,請用 Airtable UI 確認欄位。
        return table_id

    print(f"[table] creating '{TABLE_NAME}' with {len(FIELD_SPEC)} fields...")
    created = create_table(base_id, TABLE_NAME, FIELD_SPEC)
    table_id = created["id"]
    created_fields = created.get("fields", [])
    print(f"[table] created '{TABLE_NAME}': {table_id}")
    print(f"[verify] POST response reports {len(created_fields)} fields:")
    for f in created_fields:
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

    # 印出最終狀態。欄位驗證來自 POST response(只在新建時)
    # 既有表時提醒用 UI 確認。
    final_fields = list_fields(BASE_ID, table_id) if False else []
    print()
    print("=" * 60)
    print(f"BASE_ID  = {BASE_ID}")
    print(f"TABLE_ID = {table_id}")
    if final_fields:
        print(f"FIELDS ({len(final_fields)}):")
        for f in final_fields:
            opts = f.get("options", {})
            if f["type"] == "singleSelect":
                choices = ", ".join(c["name"] for c in opts.get("choices", []))
                print(f"  - {f['name']:<18} {f['type']:<14} ({choices})")
            elif f["type"] == "dateTime":
                tz = opts.get("timeZone", "?")
                print(f"  - {f['name']:<18} {f['type']:<14} (tz={tz})")
            else:
                print(f"  - {f['name']:<18} {f['type']}")
    else:
        print("(欄位細節請到 Airtable UI 確認 — PAT 無法 GET fields endpoint)")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
