#!/usr/bin/env python3
"""One-time migration: Airtable ProcessedContent + ReactionPicks → Firestore.

Reads all records from the legacy Airtable tables and upserts them into
Firestore collections (``processed`` and ``reactions`` by default).

Pre-reqs
--------
- ``AIRTABLE_API_KEY`` — still needed for the export step.
- Firestore credentials configured (see ``setup_firestore.py``).

Usage
-----
  AIRTABLE_API_KEY=pat_xxx \\
  GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json \\
  FIRESTORE_PROJECT_ID=my-project \\
    python3 pipeline/scripts/migrate_airtable_to_firestore.py

  # Dry-run (print counts only):
    python3 pipeline/scripts/migrate_airtable_to_firestore.py --dry-run

  # Migrate only one table:
    python3 pipeline/scripts/migrate_airtable_to_firestore.py --only processed
    python3 pipeline/scripts/migrate_airtable_to_firestore.py --only reactions
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib import error, parse, request

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from pipeline.lib.firestore_client import collection_name_for_table, get_firestore_client
from pipeline.lib.processed_store import DEFAULT_TABLE, make_hash

logger = logging.getLogger(__name__)

API_BASE = "https://api.airtable.com/v0"
DEFAULT_BASE_ID = "appHilorcrC5T0p2u"
DEFAULT_PROCESSED_TABLE = "ProcessedContent"
DEFAULT_REACTIONS_TABLE_ID = "tblUzHUmmL6IwHJch"


def _airtable_get(path: str, api_key: str) -> Dict[str, Any]:
    url = f"{API_BASE}{path}"
    req = request.Request(
        url,
        headers={"Authorization": f"Bearer {api_key}", "Accept": "application/json"},
    )
    with request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_airtable_table(
    base_id: str,
    table: str,
    api_key: str,
) -> List[Dict[str, Any]]:
    """Fetch every record from an Airtable table (name or table id)."""
    records: List[Dict[str, Any]] = []
    offset: Optional[str] = None
    table_q = parse.quote(table, safe="")
    while True:
        path = f"/{base_id}/{table_q}?pageSize=100"
        if offset:
            path += f"&offset={offset}"
        page = _airtable_get(path, api_key)
        records.extend(page.get("records", []))
        offset = page.get("offset")
        if not offset:
            break
    return records


def migrate_processed(
    records: List[Dict[str, Any]],
    *,
    dry_run: bool,
) -> int:
    collection_name = collection_name_for_table(DEFAULT_TABLE)
    if dry_run:
        logger.info("[dry-run] would migrate %d ProcessedContent records → %s",
                    len(records), collection_name)
        return len(records)

    db = get_firestore_client()
    col = db.collection(collection_name)
    written = 0
    for rec in records:
        fields = dict(rec.get("fields") or {})
        source_hash = fields.get("source_hash")
        if not source_hash:
            source_type = fields.get("source_type", "")
            source_id = fields.get("source_id", "")
            if source_type and source_id:
                source_hash = make_hash(source_type, source_id)
                fields["source_hash"] = source_hash
            else:
                logger.warning("skip record %s — no source_hash", rec.get("id"))
                continue
        col.document(source_hash).set(fields, merge=True)
        written += 1
    logger.info("migrated %d ProcessedContent records → %s", written, collection_name)
    return written


def migrate_reactions(
    records: List[Dict[str, Any]],
    *,
    dry_run: bool,
) -> int:
    from pipeline.lib.reaction_store import DEFAULT_TABLE as REACTION_TABLE

    collection_name = collection_name_for_table(REACTION_TABLE)
    if dry_run:
        logger.info("[dry-run] would migrate %d ReactionPicks records → %s",
                    len(records), collection_name)
        return len(records)

    db = get_firestore_client()
    col = db.collection(collection_name)
    written = 0
    for rec in records:
        fields = dict(rec.get("fields") or {})
        reaction_id = fields.get("reaction_id")
        if not reaction_id:
            logger.warning("skip reaction %s — no reaction_id", rec.get("id"))
            continue
        col.document(reaction_id).set(fields, merge=True)
        written += 1
    logger.info("migrated %d ReactionPicks records → %s", written, collection_name)
    return written


def main() -> int:
    parser = argparse.ArgumentParser(description="Migrate Airtable data to Firestore")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--only",
        choices=["processed", "reactions", "all"],
        default="all",
    )
    parser.add_argument(
        "--base-id",
        default=os.environ.get("AIRTABLE_PROCESSED_CONTENT_BASE_ID", DEFAULT_BASE_ID),
    )
    parser.add_argument(
        "--processed-table",
        default=os.environ.get("AIRTABLE_PROCESSED_CONTENT_TABLE", DEFAULT_PROCESSED_TABLE),
    )
    parser.add_argument(
        "--reactions-table",
        default=os.environ.get("AIRTABLE_REACTION_PICKS_TABLE_ID", DEFAULT_REACTIONS_TABLE_ID),
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    api_key = os.environ.get("AIRTABLE_API_KEY", "")
    if not api_key:
        logger.error("AIRTABLE_API_KEY not set — needed to export from Airtable")
        return 2

    total = 0
    if args.only in ("processed", "all"):
        logger.info("fetching Airtable %s...", args.processed_table)
        processed = fetch_airtable_table(args.base_id, args.processed_table, api_key)
        total += migrate_processed(processed, dry_run=args.dry_run)

    if args.only in ("reactions", "all"):
        logger.info("fetching Airtable ReactionPicks (%s)...", args.reactions_table)
        reactions = fetch_airtable_table(args.base_id, args.reactions_table, api_key)
        total += migrate_reactions(reactions, dry_run=args.dry_run)

    print(f"done — {total} records {'would be ' if args.dry_run else ''}migrated")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
