# QA Report — Task 1+2 (Airtable ProcessedContent ledger + processed_store.py)

**QA Engineer**: minimax-m3 (Ollama-Cloud subagent)
**Date**: 2026-08-09T04:38:30Z
**QA Table**: `appHilorcrC5T0p2u / ProcessedContent_QA_TEST` (`tblsaRK3Z73NFEBKV`)
**Production table touched**: � no
**Production code modified**: ❌ no

## Summary

| # | Scenario | Result | Evidence |
|---|----------|--------|----------|
| 1 | Fresh insert | ✅ PASS | `mark_processed("youtube", "qa_test_001_…")` → `recQJMkBNaWepLvMJ` |
| 2 | Query after insert | ✅ PASS | `is_processed("youtube", "qa_test_001_…")` → `True` |
| 3 | Idempotent re-insert | ✅ PASS | first `rec8NYAjHwwIv31kv` == second `rec8NYAjHwwIv31kv`, table count = 1 |
| 4 | Side effects | ✅ PASS | after `update_side_effects` GET shows `discord_message_id="qa_msg_001"`, `github_commit_sha="qa_sha_abc123"` |
| 5 | Stats / get_recent | ✅ PASS | `stats(days=7)` → `{youtube:14, news:5, reddit:1}`; `get_recent(source_type="youtube", days=7)` → 14 records (≥3) |

All 5 mandatory scenarios green. Edge cases below also ran green.

## Edge Cases (bonus)

- **A. Special chars** — ✅ PASS
  - `mark_processed("youtube", "id with spaces & symbols !@#$%^&*()_+-={}[]|:;\"'<>,./?", …)` produced record `recC0xVKF0qOsKtY2`; second call with the same id returned the same record id (idempotent) and the leading/trailing-whitespace variant hashes equal the bare id (`make_hash` strips whitespace).
- **B. Network failure** — ✅ PASS
  - Patched `urllib.request.urlopen` to throw `HTTPError 500`; the outer `_request` retry loop made exactly **4 attempts** (1 initial + 3 retries), with backoffs 1s/2s/4s skipped via `time.sleep` mock, then raised `ProcessedStoreError`. Non-retriable errors (401/403/404/422/409) are still re-raised immediately per the inner `except error.HTTPError` branches.
- **C. source_type 分離** — ✅ PASS
  - Same `source_id` across `youtube`, `news`, `reddit` produced **3 distinct record ids** (`recRuwW2SJv2H1OCq`, `reco4JCvudbfN7c3R`, `recnOoQF5u0ilcVgr`); `make_hash` for the three `source_type`s yielded 3 distinct hashes (dedup space properly partitioned by source_type).

## Pure Helpers (sanity, no I/O)

- `make_hash` is SHA256 hex, lowercases `source_type`, strips whitespace in `source_id`, and the format `'<source_type>:<source_id>'` is the fixed PM contract.
- `normalize_url` lowercases scheme + host, preserves path case, preserves explicit port, drops `utm_*` + the known trackers (`fbclid`, `gclid`, `gbraid`, `wbraid`, `mc_cid`, `mc_eid`, `yclid`), sorts remaining query params alphabetically, drops empty fragments.

## Bugs Found

None. Behaviour matches the documented contract:

- `mark_processed` is genuinely idempotent (verified by hashing via `source_hash` filter, table_count=1 after two inserts with the same key).
- `update_side_effects` preserves `first_seen_at` (the helper pops it from the patch payload) — consistent with `mark_processed` docstring "preserved on subsequent updates".
- `_seen_cache` works across operations — second `mark_processed` for the same key short-circuits to PATCH without a re-lookup.
- `stats` and `get_recent` correctly use `DATETIME_DIFF(NOW(), {processed_at}, 'days') <= N` and only count source_types that actually have records.
- Retry policy: `_RETRY_BACKOFFS_SEC = (1.0, 2.0, 4.0)` → 4 attempts total before surfacing `ProcessedStoreError`; auth/notfound/conflict (401/403/404/422/409) are correctly non-retriable.

## Recommendation

**APPROVE** — `pipeline/lib/processed_store.py` is fit for production. All 5 PM-mandated scenarios pass against a real Airtable base using the same API path the production pipeline would take, and edge cases (special chars, transient network failure, source_type isolation) confirm the design's robustness. No code changes required.

## Test Records (for PM to clean up)

Created in `ProcessedContent_QA_TEST` during the QA run. These can be deleted from the table (or the whole table dropped) at PM's discretion — none of them touch the production `ProcessedContent` table.

| # | Scenario | Record ID | Key |
|---|----------|-----------|-----|
| 1 | S1 fresh insert | `recQJMkBNaWepLvMJ` | `youtube:qa_test_001_81657465` |
| 2 | S3 idempotent (final state, 1 row) | `rec8NYAjHwwIv31kv` | `youtube:qa_test_002_91a17099` |
| 3 | S4 side effects | `recu3hM4KLSeKZgad` | `youtube:qa_test_003_2ce27da4` |
| 4 | S5 stats (youtube #0) | `recUfD3nlN4iIfo3H` | `youtube:qa_stats_y_0_7b3bbe` |
| 5 | S5 stats (youtube #1) | `recx50tR8yTVs7Dsh` | `youtube:qa_stats_y_1_dbafe8` |
| 6 | S5 stats (youtube #2) | `recCslNMOxE4VZXKZ` | `youtube:qa_stats_y_2_5205d2` |
| 7 | S5 stats (news #0) | `recFR2Zpp0VH701C8` | `news:qa_stats_n_0_7d0d46` |
| 8 | S5 stats (news #1) | `recDRZNvnEtO9OHUy` | `news:qa_stats_n_1_430f8d` |
| 9 | Edge A special chars | `recC0xVKF0qOsKtY2` | `youtube:id with spaces & symbols !@#$%^&*()_+-={}[]|:;"'<>,./?` |
| 10 | Edge C youtube | `recRuwW2SJv2H1OCq` | `youtube:qa_shared_955b4c` |
| 11 | Edge C news | `reco4JCvudbfN7c3R` | `news:qa_shared_955b4c` |
| 12 | Edge C reddit | `recnOoQF5u0ilcVgr` | `reddit:qa_shared_955b4c` |

The earlier (first-run) record IDs are also still present because the QA harness was re-run to validate the edge-B mock fix; the table now contains the second-run set above plus the first-run equivalents. Cleanup can either delete by these 12 IDs or drop the whole `ProcessedContent_QA_TEST` table.

### Cleanup helper (run from the project root)

```bash
cd /root/projects/ado_hermes_reddit_scrapper
AIRTABLE_API_KEY=... \
BASE=appHilorcrC5T0p2u \
TABLE=ProcessedContent_QA_TEST \
/root/.hermes/hermes-agent/venv/bin/python -c "
import os, json, urllib.request as r, urllib.parse as p
key = os.environ['AIRTABLE_API_KEY']
table = os.environ['TABLE']
base  = os.environ['BASE']
ids = '''recQJMkBNaWepLvMJ
rec8NYAjHwwIv31kv
recu3hM4KLSeKZgad
recUfD3nlN4iIfo3H
recx50tR8yTVs7Dsh
recCslNMOxE4VZXKZ
recFR2Zpp0VH701C8
recDRZNvnEtO9OHUy
recC0xVKF0qOsKtY2
recRuwW2SJv2H1OCq
reco4JCvudbfN7c3R
recnOoQF5u0ilcVgr'''.split()
url = f'https://api.airtable.com/v0/{base}/{p.quote(table)}?records[]=' + '&records[]='.join(ids)
req = r.Request(url, method='DELETE', headers={'Authorization': f'Bearer {key}'})
print(json.loads(r.urlopen(req).read()))
"
```

To drop the table entirely (Airtable Meta API):

```bash
curl -X DELETE "https://api.airtable.com/v0/meta/bases/appHilorcrC5T0p2u/tables/tblsaRK3Z73NFEBKV" \
  -H "Authorization: Bearer $AIRTABLE_API_KEY"
```
