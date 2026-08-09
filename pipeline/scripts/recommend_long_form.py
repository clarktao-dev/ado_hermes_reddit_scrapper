#!/usr/bin/env python3
"""Recommend which processed podcast to upgrade into a long-form article.

Workflow (Task 7c):

1. Run daily short-summary pipeline (default behavior).
2. Cron / user calls ``candidates`` to get a list of recently-processed
   podcasts that have NOT yet been promoted to ``article_type="long-form"``.
3. User reviews the candidates, then runs either:
   - ``confirm <source_id>`` — triggers ``youtube_daily.py --video-id <id> --mode long``
   - ``skip <source_id>`` — marks the record as ``article_type="skipped-long-form"``

This bypasses ``ProcessedStore.update_*`` helpers on purpose — they only
cover side-effect columns. Direct Airtable PATCH is the simplest way to
flip ``article_type`` for an existing record, and the existing
``ProcessedStore.mark_processed`` path (idempotent on source_hash) will
not overwrite it because ``mark_processed`` skips records it already has.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

# Make the package importable when run as a script
ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

import requests  # noqa: E402  (project depends on requests)

from pipeline.lib.processed_store import (  # noqa: E402
    ProcessedStore,
    DEFAULT_TABLE,
    make_hash,
)

EXCLUDED_ARTICLE_TYPES = {"long-form", "pending-long-form", "skipped-long-form"}
ARTICLE_TYPE_FIELD = "article_type"


def _get_store() -> ProcessedStore:
    base_id = os.environ.get("AIRTABLE_PROCESSED_CONTENT_BASE_ID")
    api_key = os.environ.get("AIRTABLE_API_KEY")
    if not base_id:
        raise SystemExit("AIRTABLE_PROCESSED_CONTENT_BASE_ID not set")
    return ProcessedStore(base_id=base_id, table_name=DEFAULT_TABLE, api_key=api_key)


def _patch_record(store: ProcessedStore, record_id: str, source_hash: str,
                  fields: Dict[str, Any]) -> str:
    """Wrap the private _patch_record. Caller is responsible for cache coherence."""
    return store._patch_record(record_id, fields, source_hash=source_hash)  # noqa: SLF001


def _list_all(store: ProcessedStore, formula: str) -> List[Dict[str, Any]]:
    """Direct list via _list_all_records (private but stable)."""
    return store._list_all_records(filter_formula=formula)  # noqa: SLF001


# ---------- subcommands ----------------------------------------------------

def cmd_candidates(args: argparse.Namespace) -> int:
    """Print youtube records from the last N days that are still candidates
    for a long-form article (article_type ∉ {long-form, pending-long-form,
    skipped-long-form})."""
    store = _get_store()
    formula = (
        f"AND({{source_type}}='youtube',"
        f"DATETIME_DIFF(NOW(), {{processed_at}}, 'days') <= {args.days_back})"
    )
    records = _list_all(store, formula)
    candidates = []
    for r in records:
        f = r.get("fields", {})
        at = (f.get(ARTICLE_TYPE_FIELD) or "").strip()
        if at in EXCLUDED_ARTICLE_TYPES:
            continue
        candidates.append({"id": r["id"], **f})

    # Sort by duration_sec ASC (shorter podcasts make tighter long-form articles)
    candidates.sort(
        key=lambda c: int(
            ((c.get("metadata") or {}).get("duration_sec"))
            or ((c.get("metadata") or {}).get("duration"))
            or 999999
        )
    )
    candidates = candidates[: args.limit]

    print(f"過去 {args.days_back} 天 youtube candidate ({len(candidates)} 筆):\n")
    if not candidates:
        print("  (沒有 — Pipeline 還沒跑、或全部都做過/跳過了)")
        return 0

    for c in candidates:
        meta = c.get("metadata") or {}
        # metadata is stored as JSON string in Airtable
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}
        duration = meta.get("duration_sec") or meta.get("duration") or "?"
        channel = (c.get("channels") or ["?"])[0] if c.get("channels") else "?"
        print(f"  📹 {c.get('title', '?')}")
        print(f"     channel:    {channel}")
        print(f"     duration:   {duration}s")
        print(f"     published:  {c.get('first_seen_at', '?')}")
        print(f"     source_id:  {c.get('source_id', '?')}")
        print(f"     record_id:  {c['id']}")
        print()

    if args.discord:
        from pipeline.lib.youtube_discord import _send  # local import — keep CLI fast
        body_lines = ["**📚 Podcast 推薦做長文** (過去 {} 天)".format(args.days_back)]
        for c in candidates:
            meta = c.get("metadata") or {}
            if isinstance(meta, str):
                try:
                    meta = json.loads(meta)
                except Exception:
                    meta = {}
            duration = meta.get("duration_sec") or meta.get("duration") or "?"
            body_lines.append(
                f"• **{c.get('title', '?')}** — `{c.get('source_id', '?')}` "
                f"({duration}s)"
            )
        body_lines.append("")
        body_lines.append(
            "確認做長文: `python3 pipeline/scripts/recommend_long_form.py confirm <source_id>`\n"
            "跳過: `python3 pipeline/scripts/recommend_long_form.py skip <source_id>`"
        )
        _send(channel="home", title="📚 Podcast 推薦做長文", content="\n".join(body_lines))
        print(f"✅ Discord 推 {len(candidates)} 部")

    return 0


def _find_record_by_source_id(store: ProcessedStore, source_id: str) -> Optional[Dict[str, Any]]:
    formula = (
        f"AND({{source_type}}='youtube',"
        f"{{source_id}}='{source_id.replace(chr(39), chr(39) + chr(39))}')"
    )
    rows = _list_all(store, formula)
    return rows[0] if rows else None


def cmd_confirm(args: argparse.Namespace) -> int:
    """Mark a record pending-long-form and shell out to youtube_daily.py --mode long."""
    store = _get_store()
    rec = _find_record_by_source_id(store, args.source_id)
    if not rec:
        print(f"❌ Source ID {args.source_id} 找不到 youtube record")
        return 1
    rec_id = rec["id"]
    source_hash = make_hash("youtube", args.source_id)
    _patch_record(store, rec_id, source_hash, {ARTICLE_TYPE_FIELD: "pending-long-form"})
    print(f"✅ {args.source_id} 標記為 pending-long-form")

    cmd = [
        "/root/.hermes/hermes-agent/venv/bin/python",
        str(ROOT / "pipeline" / "youtube_daily.py"),
        "--video-id", args.source_id,
        "--mode", "long",
    ]
    if args.dry_run:
        cmd.append("--dry-run")
    print(f"🚀 跑: {' '.join(cmd)}")
    rc = subprocess.call(cmd, cwd=str(ROOT))
    return rc


def cmd_skip(args: argparse.Namespace) -> int:
    """Mark a record as skipped-long-form so it stops appearing in candidates."""
    store = _get_store()
    rec = _find_record_by_source_id(store, args.source_id)
    if not rec:
        print(f"❌ Source ID {args.source_id} 找不到 youtube record")
        return 1
    rec_id = rec["id"]
    source_hash = make_hash("youtube", args.source_id)
    _patch_record(store, rec_id, source_hash, {ARTICLE_TYPE_FIELD: "skipped-long-form"})
    print(f"✅ {args.source_id} 標記為 skipped-long-form")
    return 0


# ---------- argparse -------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="recommend_long_form.py",
        description="Recommend processed podcasts that are good candidates for long-form articles.",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    p_cand = sub.add_parser("candidates", help="List candidate podcasts (default: past 7 days)")
    p_cand.add_argument("--days-back", type=int, default=7)
    p_cand.add_argument("--limit", type=int, default=5)
    p_cand.add_argument("--discord", action="store_true",
                        help="Also push the candidate list to Discord (default: off)")
    p_cand.set_defaults(func=cmd_candidates)

    p_conf = sub.add_parser("confirm", help="Trigger long-form generation for a video")
    p_conf.add_argument("source_id")
    p_conf.add_argument("--dry-run", action="store_true")
    p_conf.set_defaults(func=cmd_confirm)

    p_skip = sub.add_parser("skip", help="Mark a video as skipped-long-form")
    p_skip.add_argument("source_id")
    p_skip.set_defaults(func=cmd_skip)

    return p


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
