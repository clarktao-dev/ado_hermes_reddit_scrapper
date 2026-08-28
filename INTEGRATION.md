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

## 7. Vault split (code repo ↔ vault collection)

The pipeline code lives in ``ado_hermes_reddit_scrapper``; vault markdown
lives in a separate git repo ``hermes_vault_collection``:

```
/root/projects/ado_hermes_reddit_scrapper/   # PIPELINE_ROOT — code only
/root/projects/hermes_vault_collection/      # HERMES_VAULT_ROOT — vault only
  immobilien-kb/vault/
  podcast-kb/vault/
```

Add to ``~/.hermes/.env`` (Phase 2b, after merging this PR):

```bash
PIPELINE_ROOT=/root/projects/ado_hermes_reddit_scrapper
HERMES_VAULT_ROOT=/root/projects/hermes_vault_collection
# SSH key for vault-repo pushes (required on VPS if ado_reddit_deploy is
# scoped to the code repo deploy key only):
HERMES_VAULT_GITHUB_KEY_PATH=/root/.ssh/github_deploy_key
# optional overrides (defaults shown):
# HERMES_VAULT_GITHUB_REPO=hermes_vault_collection
# HERMES_VAULT_GITHUB_OWNER=clarktao-dev
```

**SSH keys:** ``/root/.ssh/ado_reddit_deploy`` is a deploy key for
``ado_hermes_reddit_scrapper`` only. Pushing ``hermes_vault_collection``
requires either ``HERMES_VAULT_GITHUB_KEY_PATH`` (user-level key or a
vault-specific deploy key) or the default ``github_deploy_key`` when
``HERMES_VAULT_ROOT`` is set.

Path resolution is centralised in ``pipeline/lib/paths.py``. When
``HERMES_VAULT_ROOT`` is unset, ``VAULT_ROOT`` falls back to
``PIPELINE_ROOT`` so the mono-repo layout still works until Phase 3.

**Boundaries (do not change):**

- ``push_to_github.py`` stages only ``podcast-kb/vault/`` or
  ``immobilien-kb/vault/`` — never ``podcast-kb/content/`` (fetch-stage
  artifacts stay in the pipeline repo).
- ``push_to_github.py`` tries ``git push`` first (SSH deploy key), then
  falls back to the legacy dulwich/paramiko wire protocol. Set
  ``HERMES_PUSH_PREFER_GIT=0`` to try paramiko first.
- YouTube vault has two historical directory schemas under
  ``immobilien-kb/vault/YouTube/``; pipelines preserve both.
- ``podcast-kb/vault/Daily/_stubs_backup/`` is a local staging area; rsync
  ``--delete`` from the pipeline repo does not remove it.

**Verify after deploy:**

```bash
cd /root/projects/ado_hermes_reddit_scrapper
python3 -m pytest pipeline/tests/test_paths.py -v
python3 pipeline/youtube_daily.py --dry-run   # writes should target HERMES_VAULT_ROOT
python3 pipeline/news_daily.py --dry-run
```

