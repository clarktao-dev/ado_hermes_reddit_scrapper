# QA Environment

QA_TABLE = appHilorcrC5T0p2u / ProcessedContent_QA_TEST
TABLE_ID = tblsaRK3Z73NFEBKV
created: 2026-08-09T04:30:00Z
created_by: QA Engineer (minimax-m3)
purpose: isolated test surface for pipeline/lib/processed_store.py verification — does not touch the production ProcessedContent table (tblyJl2IBTgnImkM5).

Schema mirror: 13 fields identical to pipeline/scripts/airtable_processed_content_schema.json (source_hash primary, source_type singleSelect, channels + tags multipleSelects, two dateTime fields with ISO/24h/UTC, discord_message_id + github_commit_sha side-effect text fields).

Notes for PM cleanup:
- Records written here use the `qa_test_001..003`, `qa_stats_y_*`, `qa_stats_n_*`, and edge-case ids — see QA_REPORT.md for the exact list.
- Do not delete the table unless downstream tests confirm they do not rely on it; backend engineer's TestLiveAirtable suite uses ProcessedContent (not this one).
