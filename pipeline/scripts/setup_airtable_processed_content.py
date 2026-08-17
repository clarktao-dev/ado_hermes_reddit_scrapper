#!/usr/bin/env python3
"""One-shot setup for the Airtable `Pipelines` base + `ProcessedContent` table.

What it does (idempotent — safe to re-run):
  1. List existing bases. If `Pipelines` is missing, create it via the
     Meta API (`POST /v0/meta/bases`). NOTE: the Meta API for base creation
     is **workspace-scoped and only available to workspace tokens**;
     personal access tokens (PATs) get HTTP 422 here. If creation fails,
     the script tells the user to create the base by hand and then
     re-runs.
  2. Verify (or create) the `ProcessedContent` table with the 13 fields
     defined in `airtable_processed_content_schema.json`.
  3. Create the three named views.

Pre-reqs:
  - `AIRTABLE_API_KEY` env var set (PAT starting with `pat...`).
  - If using a PAT, the `Pipelines` base must already exist and the token
    must be granted access to it (https://airtable.com/create/tokens).

Usage:
  AIRTABLE_API_KEY=pat_xxx \\
    python3 scripts/setup_airtable_processed_content.py \\
    --schema scripts/airtable_processed_content_schema.json

Exit codes:
  0  — success
  2  — token missing or invalid
  3  — schema file missing
  4  — base missing and PAT can't create bases (manual step required)
  5  — table / field / view creation failed
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib import error, request, parse

API_BASE = "https://api.airtable.com/v0"


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
    token: Optional[str] = None,
    timeout: float = 30.0,
) -> Dict[str, Any]:
    token = token or os.environ.get("AIRTABLE_API_KEY", "")
    if not token:
        raise AirtableSetupError(
            "AIRTABLE_API_KEY env var not set. Add it to ~/.hermes/.env."
        )
    url = f"{API_BASE}{path}"
    data = None
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = application_json()
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


def application_json() -> str:
    return "application/json"


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def list_bases() -> List[Dict[str, Any]]:
    out = _request("GET", "/meta/bases")
    return out.get("bases", [])


def find_base_by_name(name: str) -> Optional[Dict[str, Any]]:
    for base in list_bases():
        if base.get("name") == name:
            return base
    return None


def list_tables(base_id: str) -> List[Dict[str, Any]]:
    out = _request("GET", f"/meta/bases/{base_id}/tables")
    return out.get("tables", [])


def find_table_by_name(base_id: str, name: str) -> Optional[Dict[str, Any]]:
    for t in list_tables(base_id):
        if t.get("name") == name:
            return t
    return None


# ---------------------------------------------------------------------------
# Mutation
# ---------------------------------------------------------------------------

def create_base(name: str, workspace_id: Optional[str] = None) -> Dict[str, Any]:
    """Create a base. Workspaces API requires `workspaceId` for most PATs."""
    body: Dict[str, Any] = {"name": name}
    if workspace_id:
        body["workspaceId"] = workspace_id
    return _request("POST", "/meta/bases", body=body)


def create_table(
    base_id: str,
    name: str,
    fields: List[Dict[str, Any]],
    primary_field_idx: int = 0,
) -> Dict[str, Any]:
    body = {
        "name": name,
        "fields": fields,
        # primary field is implicit (first one) for the create call
    }
    if primary_field_idx:
        # Airtable accepts "primaryFieldId" on create if you want to
        # override, but we keep the first field as primary.
        pass
    return _request("POST", f"/meta/bases/{base_id}/tables", body=body)


def add_field(base_id: str, table_id: str, field: Dict[str, Any]) -> Dict[str, Any]:
    return _request(
        "POST",
        f"/meta/bases/{base_id}/tables/{table_id}/fields",
        body=field,
    )


# ---------------------------------------------------------------------------
# Schema normalisation (json spec -> Airtable API shape)
# ---------------------------------------------------------------------------

def field_to_api_shape(f: Dict[str, Any]) -> Dict[str, Any]:
    t = f["type"]
    out: Dict[str, Any] = {"name": f["name"], "type": t}
    if t == "singleSelect":
        out["options"] = {"choices": [{"name": o} for o in f["options"]]}
    elif t == "multipleSelects":
        out["options"] = {"choices": [{"name": o} for o in f["options"]]}
    elif t == "dateTime":
        out["options"] = f.get("options") or {
            "dateFormat": {"name": "iso", "format": "YYYY-MM-DD"},
            "timeFormat": {"name": "24hour", "format": "HH:mm"},
            "timeZone": "utc",
        }
    elif t == "checkbox":
        # Plan 1 (2026-08-17): Airtable checkbox requires {"icon", "color"}.
        # Default to "check" / "greenBright" if the schema omits options.
        out["options"] = f.get("options") or {
            "icon": "check",
            "color": "greenBright",
        }
    return out


# ---------------------------------------------------------------------------
# Setup flow
# ---------------------------------------------------------------------------

def ensure_base(name: str) -> str:
    base = find_base_by_name(name)
    if base:
        print(f"[base] '{name}' exists: {base['id']}")
        return base["id"]
    print(f"[base] '{name}' not found; attempting to create via Meta API...")
    try:
        created = create_base(name)
        print(f"[base] created '{name}': {created.get('id')}")
        return created["id"]
    except AirtableSetupError as e:
        msg = str(e)
        if "422" in msg or "INVALID_REQUEST" in msg or "INVALID_PERMISSIONS" in msg:
            raise AirtableSetupError(
                "Base creation requires a workspace-scoped token or the base "
                "must be created by hand at https://airtable.com and the PAT "
                "granted access. Original error: " + msg
            ) from e
        raise


def ensure_table(base_id: str, schema: Dict[str, Any]) -> str:
    table_name = schema["table_name"]
    table = find_table_by_name(base_id, table_name)
    if table:
        print(f"[table] '{table_name}' exists: {table['id']}")
        table_id = table["id"]
    else:
        fields_api = [field_to_api_shape(f) for f in schema["fields"]]
        print(f"[table] creating '{table_name}' with {len(fields_api)} fields...")
        created = create_table(base_id, table_name, fields_api)
        table_id = created["id"]
        print(f"[table] created '{table_name}': {table_id}")
        # Give the Meta API a beat before we touch the new table
        time.sleep(0.5)
        return table_id

    # Verify / add any missing fields (in case schema evolved)
    existing = {f["name"] for f in list_fields(base_id, table_id)}
    for f in schema["fields"]:
        if f["name"] in existing:
            continue
        print(f"[field] adding missing field '{f['name']}' ({f['type']})")
        add_field(base_id, table_id, field_to_api_shape(f))
        time.sleep(0.2)
    return table_id


def list_fields(base_id: str, table_id: str) -> List[Dict[str, Any]]:
    out = _request("GET", f"/meta/bases/{base_id}/tables/{table_id}/fields")
    return out.get("fields", [])


# ---------------------------------------------------------------------------
# Views — note: view creation is not part of the public Meta API. The user
# has to create them in the Airtable UI. We just print what to click.
# ---------------------------------------------------------------------------

def print_view_instructions(schema: Dict[str, Any]) -> None:
    print()
    print("[views] Airtable's Meta API does NOT support view creation.")
    print("[views] Please open the table in the Airtable UI and create:")
    for v in schema["views"]:
        print(f"  - {v['name']:<22}  {v['description']}")
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--schema",
        default=str(
            Path(__file__).parent / "airtable_processed_content_schema.json"
        ),
        help="Path to schema JSON file.",
    )
    args = ap.parse_args(argv)

    schema_path = Path(args.schema)
    if not schema_path.exists():
        print(f"schema not found: {schema_path}", file=sys.stderr)
        return 3

    schema = json.loads(schema_path.read_text(encoding="utf-8"))

    if not os.environ.get("AIRTABLE_API_KEY"):
        print(
            "AIRTABLE_API_KEY not set. Add a PAT (pat_...) to ~/.hermes/.env:\n"
            "  echo 'AIRTABLE_API_KEY=pat_your_token' >> ~/.hermes/.env\n"
            "Then re-run.",
            file=sys.stderr,
        )
        return 2

    try:
        base_id = ensure_base(schema["base_name"])
        ensure_table(base_id, schema)
    except AirtableSetupError as e:
        print(f"setup failed: {e}", file=sys.stderr)
        return 4 if "base" in str(e).lower() else 5

    # Print base + table IDs so they can be wired into ProcessedStore
    print()
    print("=" * 60)
    print(f"BASE_ID  = {base_id}")
    print(f"TABLE_ID = (re-list with: list_tables {base_id})")
    print("=" * 60)
    print_view_instructions(schema)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
