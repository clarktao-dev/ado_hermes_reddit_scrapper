# ProcessedContent ledger — integration guide (Firestore)

This ledger records every item the pipeline has already processed so the
`youtube_daily.py`, `news_daily.py`, and future `reddit_daily.py` /
`podcast_daily.py` runs can skip duplicates and track side effects
(Discord message IDs, GitHub commit SHAs).

Source: `pipeline/lib/processed_store.py`
Schema: `pipeline/scripts/airtable_processed_content_schema.json` (field names unchanged)
Setup:  `pipeline/scripts/setup_firestore.py`
Migration: `pipeline/scripts/migrate_airtable_to_firestore.py`

## 1. One-time setup

```bash
# 1. Create a GCP project + enable Firestore (Native mode).
# 2. Create a service account with Cloud Datastore User role.
# 3. Download the JSON key to e.g. ~/.hermes/firestore-sa.json

echo 'FIRESTORE_PROJECT_ID=your-gcp-project' >> ~/.hermes/.env
echo 'GOOGLE_APPLICATION_CREDENTIALS=/root/.hermes/firestore-sa.json' >> ~/.hermes/.env
echo 'FIRESTORE_PROCESSED_COLLECTION=processed' >> ~/.hermes/.env
echo 'FIRESTORE_REACTIONS_COLLECTION=reactions' >> ~/.hermes/.env

pip install -r pipeline/requirements.txt
python3 pipeline/scripts/setup_firestore.py --write-probe
```

## 2. Migrate existing Airtable data (one-time)

You still need `AIRTABLE_API_KEY` for the export step only:

```bash
AIRTABLE_API_KEY=pat_xxx python3 pipeline/scripts/migrate_airtable_to_firestore.py --dry-run
AIRTABLE_API_KEY=pat_xxx python3 pipeline/scripts/migrate_airtable_to_firestore.py
```

This copies:
- `ProcessedContent` → Firestore `processed` collection (document id = `source_hash`)
- `ReactionPicks` → Firestore `reactions` collection (document id = `reaction_id`)

## 3. Standard usage from a daily pipeline

```python
from pipeline.lib.processed_store import ProcessedStore, make_hash

store = ProcessedStore()  # reads Firestore config from env

for item in candidates:
    if store.is_processed("youtube", item["video_id"]):
        continue
    artifact = render(item)
    record_id = store.mark_processed(
        source_type="youtube",
        source_id=item["video_id"],
        title=item["title"],
        channels=[item["channel"]],
        pipeline_run_id=run_id,
        output_path=artifact.path,
        metadata={"duration": item["duration"], "lang": "de"},
        tags=["long-form"],
    )

store.update_side_effects(
    source_hash=make_hash("youtube", item["video_id"]),
    discord_message_id=msg.id,
    github_commit_sha=commit_sha,
)
```

`mark_processed` is **idempotent**: re-running with the same
`source_type` + `source_id` updates the existing Firestore document.

## 4. AI agent / cron compatibility

No code changes are needed in `youtube_daily.py`, `news_daily.py`, or
`destatis_daily.py` beyond setting the new env vars. The public
`ProcessedStore` API is unchanged — only the backend moved from Airtable to
Firestore.

Required env for cron / daemons:
- `FIRESTORE_PROJECT_ID`
- `GOOGLE_APPLICATION_CREDENTIALS` (or `FIRESTORE_CREDENTIALS_JSON`)
- `DISCORD_BOT_TOKEN` (unchanged)
- `DISCORD_ALLOWED_USERS` (unchanged)

Legacy `AIRTABLE_*` env vars are no longer read by the ledger.

## 5. Testing

```bash
# Offline tests (no GCP credentials) — uses in-memory backend
python3 -m pytest pipeline/tests/test_processed_store.py -v

# Live Firestore tests
FIRESTORE_PROJECT_ID=your-project \
  python3 -m pytest pipeline/tests/test_processed_store.py::TestLiveFirestore -v
```

## 6. Collections

| Legacy Airtable table | Firestore collection | Document id |
|-----------------------|----------------------|-------------|
| ProcessedContent | `processed` | `source_hash` |
| ReactionPicks | `reactions` | `reaction_id` |
