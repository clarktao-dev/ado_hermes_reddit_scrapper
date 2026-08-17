#!/usr/bin/env python3
"""Backfill Reddit vault markdown files into Airtable ProcessedContent.

Walks ``vault/Reddit/YYYY-MM-DD/*.md`` for the past 14 days, parses
``r_<sub>-<title-slug>.md`` filenames + ``**URL**:`` frontmatter, dedups
against ``ProcessedContent`` via ``ProcessedStore.is_processed`` and
``mark_processed`` writes a row per item.

Design notes
------------
- Vault filename format (set by ``reddit_monitor.write_vault``):
    ``r_<subreddit>-<title-slug>.md``
  e.g. ``r_Finanzen-房屋融資預算範例-中產階級家庭.md``.
- ``**URL**:`` is the first ``**URL**: <https-URL>`` line in the frontmatter
  block at the top of each file. Reddit permalinks are passed through
  ``ProcessedStore.normalize_url`` so tracking params don't break dedup.
- The 14-day window is hard-coded; backfills beyond that should use the
  script with an explicit ``DAYS`` env override.
- We **do not** write a new record if ``source_id`` (== ``r_<stem>``) is
  already marked — this lets us re-run the script safely.
- ``ProcessedStore.mark_processed`` is called with ``channels=[f'reddit.r_<sub>']``
  so that downstream ``fetch_candidates`` can group by subreddit and
  ``discord_picks`` can attribute to a channel. The Airtable
  ``multipleSelects`` option ``reddit.r_<sub>`` may not yet exist on the
  field; ``typecast=True`` in the request body lets Airtable add new
  options automatically (already configured in ProcessedStore).

CLI
---
No args. Environment:
    AIRTABLE_API_KEY                  Personal Access Token
    AIRTABLE_PROCESSED_CONTENT_BASE_ID Airtable base id (defaults to appHilorcrC5T0p2u)
    BACKFILL_DAYS                     Override 14-day lookback (optional)

Usage:
    AIRTABLE_API_KEY=... \\
    AIRTABLE_PROCESSED_CONTENT_BASE_ID=appHilorcrC5T0p2u \\
    /root/.hermes/hermes-agent/venv/bin/python3 \\
        pipeline/scripts/backfill_reddit_to_airtable.py
"""
from __future__ import annotations

import os
import pathlib
import re
import sys
from datetime import date, timedelta

_HERE = pathlib.Path(__file__).resolve().parent
_REPO = _HERE.parent.parent
sys.path.insert(0, str(_REPO))
sys.path.insert(0, str(_REPO / "pipeline"))

from pipeline.lib.processed_store import (  # noqa: E402
    ProcessedStore,
    ProcessedStoreError,
)

VAULT_REDDIT = _REPO / "immobilien-kb" / "vault" / "Reddit"
BASE_ID = os.environ.get(
    "AIRTABLE_PROCESSED_CONTENT_BASE_ID", "appHilorcrC5T0p2u"
)

# Filename regex: ``r_<subreddit>-<title-slug>``.
# Title-slug may contain hyphens, CJK, dots, etc. — we anchor on the first
# hyphen only (the one separating subreddit from title).
_FILENAME_RE = re.compile(r"^r_([^-]+)-(.+)$")
_URL_RE = re.compile(r"\*\*URL\*\*:\s*(https?://\S+)")


def _iter_vault_files(days: int):
    """Yield ``(path, day_str)`` for every per-post Reddit file in the window."""
    today = date.today()
    for d in range(days):
        day = (today - timedelta(days=d)).isoformat()
        day_dir = VAULT_REDDIT / day
        if not day_dir.exists():
            continue
        for f in sorted(day_dir.glob("*.md")):
            if f.name == "_index.md":
                continue
            yield f, day


def _extract_url(content: str) -> str | None:
    """Find the first ``**URL**: <URL>`` line in the file body."""
    m = _URL_RE.search(content)
    return m.group(1) if m else None


def main() -> int:
    days = int(os.environ.get("BACKFILL_DAYS", "14"))

    print(f"Backfilling Reddit vault -> ProcessedContent")
    print(f"  base_id  : {BASE_ID}")
    print(f"  vault    : {VAULT_REDDIT}")
    print(f"  lookback : {days} days")
    print()

    ps = ProcessedStore(BASE_ID)

    written = 0
    skipped_already = 0
    skipped_parse = 0
    skipped_no_url = 0
    errors: list[tuple[str, str]] = []

    for path, day in _iter_vault_files(days):
        stem = path.stem
        m = _FILENAME_RE.match(stem)
        if not m:
            print(f"  [skip-parse] {path.name}")
            skipped_parse += 1
            continue
        subreddit = m.group(1)
        title_slug = m.group(2).replace("-", " ").strip()
        if not title_slug:
            print(f"  [skip-empty-title] {path.name}")
            skipped_parse += 1
            continue

        try:
            content = path.read_text(encoding="utf-8")
        except OSError as e:
            print(f"  [error-read] {path.name}: {e}")
            errors.append((path.name, str(e)))
            continue

        url = _extract_url(content)
        if not url:
            # URL is informational only — still record the item, just with
            # url=''. Airtable ``url`` column will tolerate empty.
            print(f"  [warn-no-url] {path.name}")
            skipped_no_url += 1

        # source_id is unique per vault file. Using stem preserves human
        # readability in Airtable and avoids clashing with reddit_monitor
        # which uses the reddit atom id as its source_id.
        source_id = f"vault:{stem}"

        if ps.is_processed(source_type="reddit", source_id=source_id):
            skipped_already += 1
            continue

        channel = f"reddit.r_{subreddit}"
        try:
            ps.mark_processed(
                source_type="reddit",
                source_id=source_id,
                title=title_slug,
                output_path=str(path),
                channels=[channel],
                article_type="short-summary",
                metadata={
                    "subreddit": subreddit,
                    "vault_day": day,
                    "backfilled": True,
                    "url": url or "",
                },
                tags=["backfill"],
            )
            written += 1
            print(f"  [ok] {path.name} -> r/{subreddit}")
        except ProcessedStoreError as e:
            print(f"  [error-airtable] {path.name}: {e}")
            errors.append((path.name, str(e)))
        except Exception as e:  # pragma: no cover — defensive
            print(f"  [error-unexpected] {path.name}: {e}")
            errors.append((path.name, str(e)))

    print()
    print("=" * 60)
    print("Backfill Summary")
    print(f"  written          : {written}")
    print(f"  skipped (already): {skipped_already}")
    print(f"  skipped (parse)  : {skipped_parse}")
    print(f"  warned (no-url)  : {skipped_no_url}")
    print(f"  errors           : {len(errors)}")
    if errors:
        for name, msg in errors:
            print(f"     - {name}: {msg[:120]}")
    print("=" * 60)
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
