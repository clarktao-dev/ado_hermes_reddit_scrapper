# QA Report — Task 4 (news_daily.py 接入 processed_store)

**QA Engineer**: minimax-m3 (ollama-cloud subagent)
**Date**: 2026-08-09T04:57:00Z
**Commit verified**: dc2b8af Task 4: news_daily.py processed_store integration

## Summary

| #   | Scenario                                       | Result | Evidence |
|-----|------------------------------------------------|--------|----------|
| Q1  | `normalize_url()` + `url_normalized` 賦值       | ✅     | `pipeline/news_daily.py:163-164` — `normalized = normalize_url(url); item["url_normalized"] = normalized`. Also re-used at lines 602 & 655 for `step_mark_processed` / `step_update_side_effects`. Driver counts: 3 call sites of `normalize_url(...)`, `url_normalized` assigned in code. |
| Q2  | `store.is_processed("news", ...)` exact-match   | ✅     | `pipeline/news_daily.py:167` — `if store.is_processed("news", normalized):`. Driver counts: 2 call sites of `store.is_processed("news", ...)`. |
| Q3  | `store.get_recent(source_type="news", days=N)` gate | ✅     | `pipeline/news_daily.py:141` — `recent_records = store.get_recent(source_type="news", days=days_threshold)`. Driver counts: 1 call site. |
| Q4  | 8 個舊 fixtures → kept=0, skipped=8            | ✅     | driver output: `filter_processed → kept=0, input=8, skipped=8`. 8 records backdated to 1d-old and exact-matched → all skipped (also confirms `get_recent(days=3)` triggers the recent-news gate). |
| Q5  | 3-day-old + today → kept=0, skipped=5          | ✅     | driver output: `filter_processed → kept=0, input=5, skipped=5`. 3 records inserted with `processed_at=now-3d-1h` (inside window), then 2 today items + 3 already-processed items → all 5 skipped via recent-news gate. |
| Q6  | 7-day-old + today → kept=4, skipped=1          | ✅     | driver output: `filter_processed → kept=4, input=5, skipped=1`. 1 record backdated to 7d-1h (outside window) → recent-news gate is CLOSED. Today's 4 items pass; the 7d-old item is skipped via exact-match dedup (its URL is in the test batch). |

### Driver exit code

`Overall: PASS ✅` (exit 0). The driver's pre-V6 cleanup removed 11 stale records (V4+V5); the final cleanup pass removed the remaining V6 record. Post-run base check: **0 records** in `appHilorcrC5T0p2u / ProcessedContent`.

## Bonus Checks

- **B1: `normalize_url()` strips `utm_` / `fbclid` / etc.** — ✅ PASS
  - Input: `https://Example.COM/path?utm_source=foo&utm_medium=bar&keep=this&fbclid=baz&page=1`
  - Output: `https://example.com/path?keep=this&page=1`
  - Verified: host lowercased, all `utm_*` removed, `fbclid` removed, non-tracking params (`keep`, `page`) preserved in sorted order.

- **B2: source_type partition (news vs youtube)** — ✅ PASS
  - Inserted URL X under `source_type="news"`.
  - `is_processed("news", X)` → **True** ✅
  - `is_processed("youtube", X)` → **False** ✅
  - Hashes confirmed distinct: `make_hash("news", X)=cec3c2405b91bf94…` vs `make_hash("youtube", X)=c381cfc583df88f2…`.
  - The partition is enforced by `make_hash()`'s `'<source_type>:<source_id>'` format — different `source_type` → different SHA256 space.

- **B3: 7-day-old record outside `get_recent(days=3)`** — ✅ PASS
  - Inserted record with `processed_at = now - 7d - 1h`.
  - `get_recent(source_type="news", days=3)` → URL **NOT present** ✅
  - `get_recent(source_type="news", days=10)` → URL **present** ✅ (sanity check that the record exists)
  - Airtable's `DATETIME_DIFF(NOW(), processed_at, 'days') <= 3` correctly excludes the 7-day-old record from the 3-day window while including it in the 10-day window.

## Bugs Found

**None.** All 6 mandated scenarios pass with the exact expected counts. All 3 bonus checks pass. The driver's auto-cleanup correctly leaves the base empty.

### Minor observations (not bugs)

- The driver's V4 assertion (`kept=0, skipped=8`) is satisfied primarily by **exact-match dedup** (the 8 fixtures are re-fed with the same URLs they were inserted under). Looking at the news_daily logs: `filter_processed: 0 kept, 8 skipped(exact), 0 skipped(recent)` — so V4 is testing the `is_processed` path, not the recent-news gate path. V5 (`kept=0, skipped=5`) is the scenario that actually exercises the recent-news gate, because the 2 "today" fixtures have URLs that are NOT in the ledger — they only get skipped because `get_recent(days=3)` returns the 3 inserted 3d-old records. This is the intended design (and the news_daily source confirms it at line 174-184). ✅ Behavior is correct.

- `pipeline/news_daily.py` line 209 — `n = len(result) if hasattr(result, "__len__") and not isinstance(result, (str, dict)) else "—"`. The `_step()` helper computes a count for the result. This is a pre-existing pattern (not introduced by Task 4), no action needed.

- The test driver uses internal `_request`, `_list_all_records`, `_table_path`, `clear_cache` methods. These are private but stable across Tasks 1-4 — no concern for Task 4 specifically.

## Recommendation

**APPROVE** — Task 4 is correctly integrated:
- `filter_processed()` does both URL normalization + exact-match dedup via `is_processed` + recent-news gate via `get_recent`, in that order.
- `step_mark_processed()` records every vault-written item into the ledger with proper metadata (epoch, source, lang, relevance, date).
- `step_update_side_effects()` backfills discord message IDs and the github commit SHA without raising on failure.
- `run_pipeline()` wires Step 10 (`mark_processed` + `update_side_effects`) into the main flow; `--skip-store` cleanly bypasses both.
- The legacy in-process dedup in `pipeline/lib/dedup.py` is now correctly delegated to `filter_processed` when a store is available, with the old behavior preserved as a `--skip-store` fallback.

The pipeline is ready to run end-to-end against the real Airtable ledger. No code changes requested.

## Test Records (給 PM 清)

**None remain.** Both the test driver and the bonus script auto-cleaned their records (keyed by `pipeline_run_id`). Final base check via direct Airtable API: `len(records) == 0` on the first page; `filterByFormula` for `FIND('qa-task4', {pipeline_run_id})` returned 0 records. PM does not need to clean anything.

For traceability, the run ids created during this QA pass were:
- `qa-task4-20260809-045557` (driver, all deleted)
- `qa-task4-bonus-20260809-045654` (bonus script, all deleted)

## Artifacts

- Driver used: `pipeline/tests/run_news_daily_with_fixtures.py` (Backend engineer; behavior re-verified end-to-end).
- Bonus script authored: `pipeline/tests/qa_task4_bonus.py` (independent inline check; B1-B3 with auto-cleanup, `--keep` flag to leave records for PM).

## Test method summary

- Q1-Q3: `grep -n` over `pipeline/news_daily.py`, cross-checked with the driver's static-grep function (which found the same call sites).
- Q4-Q6: executed the Backend engineer's driver (`pipeline/tests/run_news_daily_with_fixtures.py`) against the live Airtable base, then verified the post-run base is empty via direct REST `GET`.
- B1-B3: wrote `pipeline/tests/qa_task4_bonus.py` — does NOT use the driver, exercises `ProcessedStore` and `normalize_url`/`make_hash` directly. All bonus records auto-cleaned.
