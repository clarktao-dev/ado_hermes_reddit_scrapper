#!/usr/bin/env python3
"""End-to-end test driver for news_daily's ProcessedStore integration.

Runs all six PM-mandated verifications (V1-V6) against the real Airtable
``ProcessedContent`` table (Production base, table
``appHilorcrC5T0p2u / tblyJl2IBTgnImkM5``). Every record this script
creates is tagged with ``pipeline_run_id = "qa-task4-<timestamp>"`` so a
post-run cleanup pass can find and delete them all.

Usage::

    /root/.hermes/hermes-agent/venv/bin/python \\
        pipeline/tests/run_news_daily_with_fixtures.py

The script exits 0 on full success and non-zero on any failed assertion.
Cleanup runs even on failure so the test base stays empty.

Verification matrix:

    V1  normalize_url()      — grep news_daily.py for the call site
    V2  is_processed()       — grep news_daily.py for the call site
    V3  get_recent(days=3)   — grep news_daily.py for the call site
    V4  8 old fixtures       — all mark_processed first → filter_processed
                               skips all 8 (exact-match dedup + recent-news gate)
    V5  3-day-old + today    — 3-day-old records block today's batch via
                               exact-match (is_processed=true) AND
                               recent-news gate (days=3 sees them)
    V6  7-day-old + today    — 7-day-old record is OUTSIDE the recent-news
                               window so it should NOT block today's batch;
                               only the exact-match dedup applies. New
                               items pass through.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from typing import Optional
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).parent
REPO_ROOT = HERE.parent.parent
sys.path.insert(0, str(REPO_ROOT))

# Use the same venv Hermes does, fall back to current python
VENV_PY = "/root/.hermes/hermes-agent/venv/bin/python"

from pipeline.lib.processed_store import (  # noqa: E402
    DEFAULT_TABLE,
    ProcessedStore,
    make_hash,
    normalize_url,
)
from pipeline import news_daily  # noqa: E402

PROCESSED_BASE_ID = os.environ.get(
    "AIRTABLE_PROCESSED_CONTENT_BASE_ID", "appHilorcrC5T0p2u",
)
PROCESSED_TABLE = os.environ.get(
    "AIRTABLE_PROCESSED_CONTENT_TABLE", DEFAULT_TABLE,
)


# --------------------------------------------------------------------------- #
# Test fixtures — hardcoded items, no RSS fetches.
# --------------------------------------------------------------------------- #

def _today_utc() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    """Airtable-friendly UTC ISO8601 with millisecond precision."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def make_fixtures(prefix: str, n: int = 8) -> list:
    """Build a list of `n` deterministic news items (different URLs, same prefix)."""
    out = []
    base_dt = _today_utc()
    for i in range(n):
        out.append({
            "url": f"https://example.com/{prefix}/news/{i}?utm_source=test",
            "title": f"{prefix} news {i}",
            "title_zh": f"（{prefix} 新聞 {i}）",
            "source_name": "Test Feed",
            "source_language": "de",
            "pub_date": base_dt.isoformat(),
            "pub_date_epoch": int(base_dt.timestamp()),
            "relevance_to_buyer": 7,
        })
    return out


# --------------------------------------------------------------------------- #
# Test helpers
# --------------------------------------------------------------------------- #

def banner(title: str) -> None:
    print()
    print("=" * 72)
    print(f"  {title}")
    print("=" * 72)


def find_record_by_hash(store: ProcessedStore, source_hash: str):
    """Return the first record with ``source_hash`` (or None)."""
    records = store._list_all_records(
        filter_formula=f"{{source_hash}}='{source_hash}'"
    )
    return records[0] if records else None


def patch_processed_at(store: ProcessedStore, record_id: str, when: datetime) -> None:
    """Backdate ``processed_at`` on an existing record (for V5/V6 fixture setup)."""
    fields = {"processed_at": _iso(when), "first_seen_at": _iso(when)}
    path = f"{store._table_path()}/{record_id}"
    store._request("PATCH", path, body={"typecast": True, "fields": fields})
    # Evict from cache so subsequent lookups re-fetch
    store.clear_cache()


def insert_fixture(
    store: ProcessedStore,
    item: dict,
    run_id: str,
    processed_at: Optional[datetime] = None,
    first_seen_at: Optional[datetime] = None,
) -> str:
    """Insert one fixture item into the ledger; backdate if requested.

    Returns the Airtable record id.
    """
    url_norm = item["url_normalized"] if "url_normalized" in item else normalize_url(item["url"])
    record_id = store.mark_processed(
        source_type="news",
        source_id=url_norm,
        title=item.get("title", ""),
        channels=["news.daily_top3"],
        pipeline_run_id=run_id,
        output_path=None,
        metadata={"epoch": item.get("pub_date_epoch"), "source": "qa-task4"},
        tags=[],
        first_seen_at=first_seen_at,
    )
    if processed_at is not None:
        patch_processed_at(store, record_id, processed_at)
    return record_id


def cleanup_records(store: ProcessedStore, run_id: str) -> int:
    """Delete every record this test driver created (keyed by pipeline_run_id).

    Returns the number of records deleted.
    """
    records = store._list_all_records(
        filter_formula=f"{{pipeline_run_id}}='{run_id}'"
    )
    for r in records:
        rid = r.get("id")
        if not rid:
            continue
        try:
            store._request(
                "DELETE",
                f"{store._table_path()}/{rid}",
            )
        except Exception as e:  # noqa: BLE001
            print(f"  cleanup: failed to delete {rid}: {e}", file=sys.stderr)
    store.clear_cache()
    return len(records)


# --------------------------------------------------------------------------- #
# Verifications
# --------------------------------------------------------------------------- #

def verify_v1_v2_v3() -> tuple[bool, bool, bool]:
    """V1/V2/V3: grep news_daily.py for the three required call sites."""
    banner("V1+V2+V3 — static grep over pipeline/news_daily.py")
    src = (REPO_ROOT / "pipeline" / "news_daily.py").read_text(encoding="utf-8")

    # V1: normalize_url() must be called somewhere (definition import is fine,
    # but we want to confirm it's *applied* — i.e. assigned to a dict).
    v1_calls = re.findall(r"\bnormalize_url\(", src)
    v1_assign = "url_normalized" in src
    v1 = len(v1_calls) > 0 and v1_assign
    print(f"  V1  normalize_url() call sites: {len(v1_calls)}")
    print(f"      url_normalized assigned in code: {v1_assign}")

    # V2: store.is_processed("news", ...) must appear.
    v2_calls = re.findall(r'\.is_processed\(\s*[\'"]news[\'"]', src)
    v2 = len(v2_calls) > 0
    print(f"  V2  store.is_processed(\"news\", ...) call sites: {len(v2_calls)}")

    # V3: get_recent(source_type="news", days=...) must appear (or kw form).
    v3_calls = re.findall(
        r'get_recent\(\s*[^)]*source_type\s*=\s*[\'"]news[\'"][^)]*days\s*=',
        src,
    )
    v3 = len(v3_calls) > 0
    print(f"  V3  get_recent(source_type='news', days=N) call sites: {len(v3_calls)}")
    return v1, v2, v3


def run_v4(store: ProcessedStore, run_id: str) -> tuple[int, int, list, list]:
    """V4: 8 old news fixtures → expect all 8 skipped by filter_processed.

    Returns ``(kept, skipped, kept_record_ids, skipped_record_ids)``.
    """
    banner("V4 — 8 old news fixtures → expect 0 kept, 8 skipped")
    fixtures = make_fixtures("v4-old", n=8)
    # Pre-mark all 8 as if they were processed in the past (within the
    # 3-day window so the recent-news gate also activates).
    backdate = _today_utc() - timedelta(days=1)
    inserted_ids: list = []
    for it in fixtures:
        it["url_normalized"] = normalize_url(it["url"])
        rid = insert_fixture(
            store, it, run_id=run_id,
            processed_at=backdate, first_seen_at=backdate,
        )
        inserted_ids.append(rid)
    print(f"  inserted {len(inserted_ids)} backdated fixtures "
          f"(processed_at={_iso(backdate)})")

    # Now invoke filter_processed on the same fixtures (with the recent-news
    # gate active — get_recent(days=3) WILL find our just-inserted record,
    # but the Airtable DATETIME_DIFF formula operates on whole-day windows
    # so a 1-day-old record is still inside the window).
    kept = news_daily.filter_processed(list(fixtures), store=store, days_threshold=3)
    print(f"  filter_processed → kept={len(kept)}, "
          f"input={len(fixtures)}, skipped={len(fixtures) - len(kept)}")
    return len(kept), len(fixtures) - len(kept), [], inserted_ids


def run_v5(store: ProcessedStore, run_id: str) -> tuple[int, int, list, list]:
    """V5: 3-day-old (backdated) + today mixed → 3-day-old skipped by
    is_processed; today's items also skipped because recent-news gate is open.

    The spec's wording says 3 days = threshold, but a 3-day-old record
    should be exactly at the boundary. Airtable's
    ``DATETIME_DIFF(NOW(), processed_at, 'days') <= 3`` treats that as
    "inside" (3 <= 3). So both the 3-day-old batch and the today batch
    are skipped.
    """
    banner("V5 — 3-day-old + today mixed → expect all skipped (recent gate active)")
    fixtures_old = make_fixtures("v5-3d", n=3)
    fixtures_today = make_fixtures("v5-today", n=2)
    backdate = _today_utc() - timedelta(days=3, hours=1)  # safely inside window
    inserted_ids: list = []
    for it in fixtures_old:
        it["url_normalized"] = normalize_url(it["url"])
        rid = insert_fixture(
            store, it, run_id=run_id,
            processed_at=backdate, first_seen_at=backdate,
        )
        inserted_ids.append(rid)
    print(f"  inserted {len(fixtures_old)} backdated (3d-old) fixtures")
    print(f"  prepared {len(fixtures_today)} today fixtures (NOT inserted)")

    # Run filter_processed on the union (all 5 fixtures)
    all_items = list(fixtures_old) + list(fixtures_today)
    kept = news_daily.filter_processed(all_items, store=store, days_threshold=3)
    # Expectation: every item is skipped because the recent-news gate is
    # wide-open (we just inserted 3 records inside the 3-day window).
    print(f"  filter_processed → kept={len(kept)}, "
          f"input={len(all_items)}, skipped={len(all_items) - len(kept)}")
    return len(kept), len(all_items) - len(kept), [], inserted_ids


def run_v6(store: ProcessedStore, run_id: str) -> tuple[int, int, list, list]:
    """V6: 7-day-old + today mixed → 7-day-old is OUTSIDE recent window.

    The 7-day-old record's URL won't trigger exact-match dedup (since the
    today's items have different URLs), but the recent-news gate also
    doesn't trigger (7 > 3 days). So today's items should pass; the
    7-day-old item would also pass since we don't see its URL in the
    today batch. We only expect today's items to be kept.
    """
    banner("V6 — 7-day-old + today mixed → expect today's items pass "
           "(7-day-old is outside recent-news window)")
    fixtures_old = make_fixtures("v6-7d", n=1)
    fixtures_today = make_fixtures("v6-today", n=4)
    backdate = _today_utc() - timedelta(days=7, hours=1)  # safely OUTSIDE 3-day window
    inserted_ids: list = []
    for it in fixtures_old:
        it["url_normalized"] = normalize_url(it["url"])
        rid = insert_fixture(
            store, it, run_id=run_id,
            processed_at=backdate, first_seen_at=backdate,
        )
        inserted_ids.append(rid)
    print(f"  inserted {len(fixtures_old)} backdated (7d-old) fixtures")
    print(f"  prepared {len(fixtures_today)} today fixtures (NOT inserted)")

    # Run filter_processed on the union
    all_items = list(fixtures_old) + list(fixtures_today)
    kept = news_daily.filter_processed(all_items, store=store, days_threshold=3)
    # Expectation: all 5 pass through. The 7-day-old record is outside
    # the 3-day window so get_recent returns nothing → recent gate is
    # CLOSED. The 7-day-old URL is unique, so is_processed on each of
    # today's URLs is also false. The 7-day-old URL: is_processed returns
    # true (exact match via the inserted record), so the 7-day-old fixture
    # IS skipped by the exact-match dedup.
    print(f"  filter_processed → kept={len(kept)}, "
          f"input={len(all_items)}, skipped={len(all_items) - len(kept)}")
    return len(kept), len(all_items) - len(kept), [], inserted_ids


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", action="store_true",
                    help="Skip the cleanup pass (records stay in Airtable).")
    args = ap.parse_args()

    run_id = f"qa-task4-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
    print(f"Run id: {run_id}")
    print(f"Base:   {PROCESSED_BASE_ID}")
    print(f"Table:  {PROCESSED_TABLE}")

    store = ProcessedStore(PROCESSED_BASE_ID, table_name=PROCESSED_TABLE)
    inserted_records: list = []

    results: dict = {}

    try:
        # ---------- V1 + V2 + V3 (static grep) ----------
        v1, v2, v3 = verify_v1_v2_v3()
        results["V1"] = ("PASS" if v1 else "FAIL", "normalize_url + url_normalized")
        results["V2"] = ("PASS" if v2 else "FAIL", "store.is_processed('news', ...)")
        results["V3"] = ("PASS" if v3 else "FAIL", "get_recent(source_type='news', days=N)")

        # ---------- V4 ----------
        v4_kept, v4_skipped, _, v4_ids = run_v4(store, run_id)
        inserted_records.extend(v4_ids)
        v4_ok = (v4_kept == 0 and v4_skipped == 8)
        results["V4"] = (
            "PASS" if v4_ok else "FAIL",
            f"8 old fixtures: kept=0 expected, skipped=8 expected; "
            f"got kept={v4_kept}, skipped={v4_skipped}",
        )

        # ---------- V5 ----------
        v5_kept, v5_skipped, _, v5_ids = run_v5(store, run_id)
        inserted_records.extend(v5_ids)
        v5_ok = (v5_kept == 0 and v5_skipped == 5)
        results["V5"] = (
            "PASS" if v5_ok else "FAIL",
            f"3-day-old + today: kept=0 expected, skipped=5 expected; "
            f"got kept={v5_kept}, skipped={v5_skipped}",
        )

        # ---------- V6 (pre-cleanup: ensure no stale qa-task4 records in 3-day window) ----------
        _pre_v6_cleaned = cleanup_records(store, run_id)
        if _pre_v6_cleaned:
            print(f"  [pre-V6 cleanup] removed {_pre_v6_cleaned} stale qa-task4 records")
        # ---------- V6 ----------
        v6_kept, v6_skipped, _, v6_ids = run_v6(store, run_id)
        inserted_records.extend(v6_ids)
        # V6 expectation: 5 items in, 4 today's items pass, the 1 7-day-old
        # fixture is skipped by exact-match dedup. So kept=4, skipped=1.
        v6_ok = (v6_kept == 4 and v6_skipped == 1)
        results["V6"] = (
            "PASS" if v6_ok else "FAIL",
            f"7-day-old + today: kept=4 expected (today's items), "
            f"skipped=1 expected (the 7d-old fixture itself, exact-match); "
            f"got kept={v6_kept}, skipped={v6_skipped}",
        )

    finally:
        # ---------- Cleanup ----------
        if not args.keep:
            banner("Cleanup — deleting every record tagged with this run_id")
            try:
                n = cleanup_records(store, run_id)
                print(f"  deleted {n} record(s)")
            except Exception as e:  # noqa: BLE001
                print(f"  cleanup failed: {e}", file=sys.stderr)
        else:
            print(f"\n[--keep] Skipping cleanup. {len(inserted_records)} "
                  f"records remain in {PROCESSED_BASE_ID}/{PROCESSED_TABLE} "
                  f"with pipeline_run_id={run_id}")

    # ---------- Summary ----------
    banner("Summary")
    for k, (status, msg) in results.items():
        flag = "✅" if status == "PASS" else "❌"
        print(f"  {flag} {k}  {status}  — {msg}")

    overall_ok = all(s == "PASS" for s, _ in results.values())
    print()
    print(f"Overall: {'PASS ✅' if overall_ok else 'FAIL ❌'}")
    return 0 if overall_ok else 1


if __name__ == "__main__":
    sys.exit(main())
