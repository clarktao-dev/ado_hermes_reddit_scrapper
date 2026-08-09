#!/usr/bin/env python3
"""Bonus QA checks (B1, B2, B3) — independent of the Backend engineer's driver.

Tests:
  B1 — normalize_url() strips ?utm_source=... (and other utm_* / fbclid / etc.)
  B2 — source_type partition: marking ("news", X) does NOT make ("youtube", X) processed
  B3 — 7-day-old record is OUTSIDE get_recent(days=3) window

All records created by this script are tagged with pipeline_run_id = "qa-task4-bonus-<ts>"
so they can be cleaned up after (or left for the PM to clean).
"""
from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).parent
REPO_ROOT = HERE.parent.parent
sys.path.insert(0, str(REPO_ROOT))

VENV_PY = "/root/.hermes/hermes-agent/venv/bin/python"

from pipeline.lib.processed_store import (  # noqa: E402
    ProcessedStore,
    make_hash,
    normalize_url,
)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _banner(title: str) -> None:
    print()
    print("=" * 72)
    print(f"  {title}")
    print("=" * 72)


def main() -> int:
    api_key = os.environ.get("AIRTABLE_API_KEY", "")
    if not api_key:
        # Try to load from /root/.hermes/.env
        with open("/root/.hermes/.env") as f:
            for line in f:
                if line.startswith("AIRTABLE_API_KEY="):
                    api_key = line.split("=", 1)[1].strip()
                    break
        if not api_key:
            print("ERROR: AIRTABLE_API_KEY not set", file=sys.stderr)
            return 1
        os.environ["AIRTABLE_API_KEY"] = api_key

    base_id = os.environ.get("AIRTABLE_PROCESSED_CONTENT_BASE_ID", "appHilorcrC5T0p2u")
    store = ProcessedStore(base_id, table_name="ProcessedContent")
    run_id = f"qa-task4-bonus-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
    print(f"Bonus QA run id: {run_id}")

    failures: list = []
    inserted_ids: list = []

    # ---------- B1: utm_source stripped ----------
    _banner("B1 — normalize_url() strips utm_ / fbclid / gclid")
    url_dirty = "https://Example.COM/path?utm_source=foo&utm_medium=bar&keep=this&fbclid=baz&page=1"
    url_clean = normalize_url(url_dirty)
    print(f"  in:  {url_dirty}")
    print(f"  out: {url_clean}")
    b1_ok = (
        "utm_source" not in url_clean
        and "utm_medium" not in url_clean
        and "fbclid" not in url_clean
        and "keep=this" in url_clean
        and "page=1" in url_clean
        and url_clean.startswith("https://example.com/")
    )
    print(f"  B1 result: {'PASS' if b1_ok else 'FAIL'}")
    if not b1_ok:
        failures.append("B1: utm_source / fbclid not fully stripped, or host not lowercased, "
                        f"or non-tracking query lost. Got: {url_clean}")

    # ---------- B2: source_type partition ----------
    _banner("B2 — source_type partition (news vs youtube same URL)")
    # Pick a URL unique to this run so we don't collide with anyone.
    unique_url = f"https://example.com/qa-task4-bonus/b2/{run_id}/article"
    unique_norm = normalize_url(unique_url)
    print(f"  marking source_type='news' id={unique_norm}")
    rid_news = store.mark_processed(
        source_type="news",
        source_id=unique_norm,
        title="B2 news test",
        channels=["news.daily_top3"],
        pipeline_run_id=run_id,
        output_path=None,
        metadata={"epoch": None, "source": "qa-task4-bonus"},
        tags=[],
    )
    inserted_ids.append(rid_news)
    print(f"    record_id={rid_news}")
    # Now: is_processed("news", ...) should be True
    news_hit = store.is_processed("news", unique_norm)
    # is_processed("youtube", ...) should be False (different source_hash)
    youtube_hit = store.is_processed("youtube", unique_norm)
    print(f"  is_processed('news',     url) = {news_hit}   (expect True)")
    print(f"  is_processed('youtube',  url) = {youtube_hit}  (expect False)")
    # Also verify the source_hashes actually differ
    h_news = make_hash("news", unique_norm)
    h_yt = make_hash("youtube", unique_norm)
    print(f"  source_hash('news')    = {h_news[:16]}…")
    print(f"  source_hash('youtube') = {h_yt[:16]}…")
    print(f"  hashes distinct:        {h_news != h_yt}")
    b2_ok = (news_hit is True and youtube_hit is False and h_news != h_yt)
    print(f"  B2 result: {'PASS' if b2_ok else 'FAIL'}")
    if not b2_ok:
        failures.append("B2: source_type partition broken — news/youtube collide.")

    # ---------- B3: 7-day-old record outside get_recent(days=3) ----------
    _banner("B3 — 7-day-old record is outside get_recent(days=3)")
    b3_url = f"https://example.com/qa-task4-bonus/b3/{run_id}/old-article"
    b3_norm = normalize_url(b3_url)
    backdate_7d = datetime.now(timezone.utc) - timedelta(days=7, hours=1)
    print(f"  inserting record backdated to {_iso(backdate_7d)} "
          f"(should be OUTSIDE 3-day window)")
    rid_old = store.mark_processed(
        source_type="news",
        source_id=b3_norm,
        title="B3 7-day-old test",
        channels=["news.daily_top3"],
        pipeline_run_id=run_id,
        output_path=None,
        metadata={"epoch": int(backdate_7d.timestamp()), "source": "qa-task4-bonus"},
        tags=[],
        first_seen_at=backdate_7d,
    )
    # Need to backdate processed_at too — mark_processed sets processed_at to now.
    # Patch via private API (same pattern as the test driver).
    store._request(
        "PATCH",
        f"{store._table_path()}/{rid_old}",
        body={"typecast": True, "fields": {"processed_at": _iso(backdate_7d),
                                           "first_seen_at": _iso(backdate_7d)}},
    )
    store.clear_cache()
    inserted_ids.append(rid_old)
    print(f"    record_id={rid_old} (backdated)")

    # Now ask for news in last 3 days — this URL must NOT appear
    recent_3d = store.get_recent(source_type="news", days=3, limit=200)
    recent_3d_urls = [r.get("fields", {}).get("source_id", "") for r in recent_3d]
    print(f"  get_recent('news', days=3) returned {len(recent_3d)} records")
    b3_hit_in_3d = b3_norm in recent_3d_urls
    print(f"  B3 URL present in 3d window: {b3_hit_in_3d}  (expect False)")

    # Sanity: ask for news in last 10 days — this URL SHOULD appear
    recent_10d = store.get_recent(source_type="news", days=10, limit=200)
    recent_10d_urls = [r.get("fields", {}).get("source_id", "") for r in recent_10d]
    b3_hit_in_10d = b3_norm in recent_10d_urls
    print(f"  get_recent('news', days=10) returned {len(recent_10d)} records")
    print(f"  B3 URL present in 10d window: {b3_hit_in_10d}  (expect True)")

    b3_ok = (not b3_hit_in_3d) and b3_hit_in_10d
    print(f"  B3 result: {'PASS' if b3_ok else 'FAIL'}")
    if not b3_ok:
        failures.append("B3: 7-day-old record leaked into get_recent(days=3) "
                        "or didn't appear in get_recent(days=10).")

    # ---------- Cleanup (default on; pass --keep to leave for PM) ----------
    _banner("Cleanup")
    if "--keep" in sys.argv:
        print(f"  --keep set; leaving {len(inserted_ids)} records for PM cleanup:")
        for rid in inserted_ids:
            print(f"    {rid}")
    else:
        # Delete all records tagged with this run_id
        records = store._list_all_records(
            filter_formula=f"{{pipeline_run_id}}='{run_id}'"
        )
        for r in records:
            try:
                store._request("DELETE", f"{store._table_path()}/{r['id']}")
                print(f"  deleted {r['id']}")
            except Exception as e:
                print(f"  failed to delete {r['id']}: {e}")
        store.clear_cache()

    _banner("Summary")
    print(f"  B1: {'PASS' if 'B1' not in [f.split(':')[0] for f in failures] else 'FAIL'}")
    print(f"  B2: {'PASS' if 'B2' not in [f.split(':')[0] for f in failures] else 'FAIL'}")
    print(f"  B3: {'PASS' if 'B3' not in [f.split(':')[0] for f in failures] else 'FAIL'}")
    if failures:
        print()
        for f in failures:
            print(f"  FAILURE: {f}")
        return 1
    print("\nAll bonus checks PASSED ✅")
    return 0


if __name__ == "__main__":
    sys.exit(main())
