#!/usr/bin/env python3
"""Verify Firestore setup for the pipeline ledger.

Checks credentials, project id, and collection access. Optionally writes
a probe document (then deletes it).

Usage
-----
  GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json \\
  FIRESTORE_PROJECT_ID=my-project \\
    python3 pipeline/scripts/setup_firestore.py

  python3 pipeline/scripts/setup_firestore.py --write-probe
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from pipeline.lib.firestore_client import (
    collection_name_for_table,
    get_database_id,
    get_firestore_client,
    get_project_id,
)
from pipeline.lib.processed_store import DEFAULT_TABLE

logger = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify Firestore pipeline setup")
    parser.add_argument(
        "--write-probe",
        action="store_true",
        help="Write and delete a test document in the processed collection",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    project_id = get_project_id()
    database_id = get_database_id()
    processed_col = collection_name_for_table(DEFAULT_TABLE)
    reactions_col = os.environ.get("FIRESTORE_REACTIONS_COLLECTION", "reactions")

    print("Firestore configuration")
    print(f"  project_id          : {project_id}")
    print(f"  database_id         : {database_id}")
    print(f"  processed collection: {processed_col}")
    print(f"  reactions collection: {reactions_col}")
    print()
    print("Add to ~/.hermes/.env:")
    print(f"  FIRESTORE_PROJECT_ID={project_id}")
    print(f"  GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json")
    print(f"  FIRESTORE_PROCESSED_COLLECTION={processed_col}")
    print(f"  FIRESTORE_REACTIONS_COLLECTION={reactions_col}")

    client = get_firestore_client()
    processed = client.collection(processed_col)
    count = 0
    for _ in processed.limit(1).stream():
        count += 1
    print()
    print(f"processed collection readable (sample count={count})")

    if args.write_probe:
        probe_id = "__firestore_setup_probe__"
        processed.document(probe_id).set({"probe": True})
        processed.document(probe_id).delete()
        print("write-probe: OK (document written and deleted)")

    print()
    print("Next step — migrate existing Airtable data:")
    print("  python3 pipeline/scripts/migrate_airtable_to_firestore.py --dry-run")
    print("  python3 pipeline/scripts/migrate_airtable_to_firestore.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
