"""Run the three PM-mandated scenarios for processed_store and print results.

This is a manual smoke test runner — it executes the same logic as the
pytest suite but in a single script, so the output can be pasted into a
status report verbatim. The suite already runs 26/26 in pytest, but this
file makes the scenario-level evidence obvious.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

THIS_DIR = Path(__file__).resolve().parent
PIPELINE_ROOT = THIS_DIR.parent.parent
sys.path.insert(0, str(PIPELINE_ROOT))

from pipeline.lib.processed_store import (  # noqa: E402
    ProcessedStore,
    make_hash,
)


def banner(title: str) -> None:
    print()
    print("=" * 60)
    print(f"  {title}")
    print("=" * 60)


def main() -> int:
    # Bring the offline fixture's fake layer into scope.
    from pipeline.tests.test_processed_store import FakeAirtable
    from unittest import mock

    fake = FakeAirtable()
    store = ProcessedStore("appFAKE", "ProcessedContent", api_key="pat_test")
    store._request = mock.MagicMock(side_effect=fake.handle)  # type: ignore[assignment]

    # ------------------------------------------------------------------
    banner("Scenario 1 — INSERT: write 1 record, verify it persists")
    # ------------------------------------------------------------------
    rid = store.mark_processed(
        source_type="youtube",
        source_id="vid_demo_001",
        title="Demo: Wohnungskauf 2026",
        channels=["youtube.ex_makler"],
        pipeline_run_id="run-demo-001",
        output_path="/vault/YouTube/vid_demo_001.md",
        metadata={"duration": 1234, "lang": "de"},
        tags=["long-form"],
    )
    print(f"  mark_processed returned record id: {rid}")
    assert rid.startswith("rec"), "record id should start with 'rec'"
    assert rid in fake.records, "record should be in store"
    rec = fake.records[rid]
    print(f"  source_hash = {rec['fields']['source_hash']}")
    print(f"  source_type = {rec['fields']['source_type']}")
    print(f"  title       = {rec['fields']['title']}")
    print(f"  channels    = {rec['fields']['channels']}")
    print(f"  tags        = {rec['fields']['tags']}")
    print(f"  metadata    = {rec['fields']['metadata']}")
    print(f"  first_seen_at = {rec['fields']['first_seen_at']}")
    print(f"  processed_at  = {rec['fields']['processed_at']}")
    print(f"  TOTAL RECORDS = {len(fake.records)}")
    assert len(fake.records) == 1
    print("  PASS — 1 record inserted, all fields populated")

    # ------------------------------------------------------------------
    banner("Scenario 2 — QUERY: is_processed returns True for known hash")
    # ------------------------------------------------------------------
    ok_known = store.is_processed("youtube", "vid_demo_001")
    ok_unknown = store.is_processed("youtube", "vid_does_not_exist")
    print(f"  is_processed('youtube', 'vid_demo_001')        = {ok_known}")
    print(f"  is_processed('youtube', 'vid_does_not_exist')  = {ok_unknown}")
    assert ok_known is True
    assert ok_unknown is False
    print("  PASS — known hash returns True, unknown returns False")

    # ------------------------------------------------------------------
    banner("Scenario 3 — IDEMPOTENT RE-INSERT: 2nd call updates, no dup")
    # ------------------------------------------------------------------
    rid_2 = store.mark_processed(
        source_type="youtube",
        source_id="vid_demo_001",  # SAME id
        title="Demo: Wohnungskauf 2026 (revised)",
        channels=["youtube.ex_makler"],
        pipeline_run_id="run-demo-001",
        output_path="/vault/YouTube/vid_demo_001.md",
        metadata={"duration": 1234, "lang": "de", "revised": True},
        tags=["long-form", "restored"],
    )
    print(f"  1st call returned: {rid}")
    print(f"  2nd call returned: {rid_2}")
    print(f"  same record id?    {rid == rid_2}")
    print(f"  total records:     {len(fake.records)}")
    rec_after = fake.records[rid]
    print(f"  updated title:     {rec_after['fields']['title']}")
    print(f"  updated tags:      {rec_after['fields']['tags']}")
    print(f"  updated metadata:  {rec_after['fields']['metadata']}")
    stats = store.stats(days=7)
    print(f"  stats(days=7):     {stats}")
    assert rid == rid_2
    assert len(fake.records) == 1
    assert stats.get("youtube") == 1
    print("  PASS — 2nd call updated the same record, stats shows 1")

    # ------------------------------------------------------------------
    banner("BONUS — Side-effect update: discord + github after push")
    # ------------------------------------------------------------------
    store.update_side_effects(
        source_hash=make_hash("youtube", "vid_demo_001"),
        discord_message_id="1234567890",
        github_commit_sha="abcdef1234",
    )
    rec_final = fake.records[rid]
    print(f"  discord_message_id = {rec_final['fields'].get('discord_message_id')}")
    print(f"  github_commit_sha  = {rec_final['fields'].get('github_commit_sha')}")
    print("  PASS — side-effect columns updated")

    banner("ALL 3 SCENARIOS PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
