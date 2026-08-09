# ProcessedContent ledger — integration guide

This ledger records every item the pipeline has already processed so the
`youtube_daily.py`, `news_daily.py`, and future `reddit_daily.py` /
`podcast_daily.py` runs can skip duplicates and track side effects
(Discord message IDs, GitHub commit SHAs).

Source: `pipeline/lib/processed_store.py`
Schema: `pipeline/scripts/airtable_processed_content_schema.json`
Setup:  `pipeline/scripts/setup_airtable_processed_content.py`

## 1. One-time setup (PM runs)

```bash
# 1. Add a Personal Access Token (pat_...) to ~/.hermes/.env:
echo 'AIRTABLE_API_KEY=pat_your_token' >> ~/.hermes/.env

# 2. Create the "Pipelines" base by hand at https://airtable.com
#    and grant the token access to it.

# 3. Provision the table + fields:
cd /root/projects/ado_hermes_reddit_scrapper
python3 pipeline/scripts/setup_airtable_processed_content.py
#  -> prints BASE_ID and instructions for the 3 views (Meta API
#     cannot create views — make them in the Airtable UI).

# 4. Wire the base id into your runtime env:
echo 'PIPELINE_AIRTABLE_BASE_ID=app_xxxx' >> ~/.hermes/.env
```

The 13 fields, their types, and the 3 views are documented in
`pipeline/scripts/airtable_processed_content_schema.json`.

## 2. Standard usage from a daily pipeline

```python
from pipeline.lib.processed_store import ProcessedStore

store = ProcessedStore(base_id="app_xxxx")  # or read PIPELINE_AIRTABLE_BASE_ID

# Skip items we've already done
for item in candidates:
    if store.is_processed("youtube", item["video_id"]):
        continue
    artifact = render(item)              # your existing work
    record_id = store.mark_processed(
        source_type="youtube",
        source_id=item["video_id"],
        title=item["title"],
        channels=[item["channel"]],      # e.g. "youtube.ex_makler"
        pipeline_run_id=run_id,
        output_path=artifact.path,
        metadata={"duration": item["duration"], "lang": "de"},
        tags=["long-form"],
    )

# After Discord push / GitHub push
store.update_side_effects(
    source_hash=make_hash("youtube", item["video_id"]),
    discord_message_id=msg.id,
    github_commit_sha=commit_sha,
)
```

`mark_processed` is **idempotent**: re-running with the same
`source_type` + `source_id` updates the existing record instead of
creating a duplicate. `is_processed` uses an in-process cache so a
single run only does one round-trip per unique item.

## 3. Quick read APIs

```python
# Recent items for a given source
recent = store.get_recent("news", days=7, limit=50)

# Counts per source for the last N days (for dashboards / health checks)
counts = store.stats(days=7)
# -> {"youtube": 12, "news": 4, "reddit": 7}
```

## 4. Error handling

| Exception | When |
|---|---|
| `ProcessedStoreAuthError` | Token missing, invalid, or base not granted. Surface & stop. |
| `ProcessedStoreNotFoundError` | Base/table missing, or `update_side_effects` for an unknown hash. |
| `ProcessedStoreConflictError` | Schema mismatch / bad payload. Stop and inspect. |
| `ProcessedStoreError` | All other failures (after 3 retries with 1s/2s/4s backoff). |

Wrap each pipeline's main loop in a try/except for `ProcessedStoreError`
and decide whether to fail the run or skip the item — the ledger is
designed so a single bad row never breaks the batch.

## 5. Testing

```bash
# Offline tests (no token required) — 26 tests, ~4s
python3 -m pytest pipeline/tests/test_processed_store.py -v

# Live tests (writes to a real base — needs PIPELINE_TEST_BASE_ID + AIRTABLE_API_KEY)
PIPELINE_TEST_BASE_ID=app_xxx \
  python3 -m pytest pipeline/tests/test_processed_store.py::TestLiveAirtable -v
```

## 6. What this module does NOT do

- It does **not** push to Discord or GitHub — that's the caller's job.
  It only stores the IDs once the caller has them.
- It does **not** migrate the existing `state/*.json` files. That's a
  separate task (Task 4 in the original plan).
- It does **not** create Airtable views. Make them in the UI once.

## 7. Open questions for PM

- Do we want a "soft delete" flag column? (Not in current schema.)
- Should `metadata` enforce a JSON shape per source_type, or stay free-form?
- Should the daily runs use one ProcessedStore per source_type, or one
  shared instance? (Current API supports both; recommend one shared.)
