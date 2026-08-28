"""Unit tests for pipeline.lib.processed_store.

The tests are written in two layers:

1. **Pure helper tests** (``make_hash``, ``normalize_url``) — no I/O, always run.
2. **Store tests** — run against an in-memory Firestore backend (default)
   so the suite is fully green without GCP credentials. To exercise the
   live API instead, set ``FIRESTORE_PROJECT_ID`` and service-account
   credentials — the live tests clean up after themselves.

The three PM-mandated scenarios (insert / query / idempotent re-insert)
are all covered by the offline suite. Live tests add network resilience
against real Firestore.
"""
from __future__ import annotations

import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytest

# Make the pipeline package importable when running this file directly.
THIS_DIR = Path(__file__).resolve().parent
PIPELINE_ROOT = THIS_DIR.parent.parent
sys.path.insert(0, str(PIPELINE_ROOT))

from pipeline.lib.processed_store import (  # noqa: E402
    ProcessedStore,
    ProcessedStoreAuthError,
    ProcessedStoreError,
    ProcessedStoreNotFoundError,
    make_hash,
    normalize_url,
)


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

class TestMakeHash:
    def test_stable_across_calls(self) -> None:
        h1 = make_hash("youtube", "abc123")
        h2 = make_hash("youtube", "abc123")
        assert h1 == h2
        assert len(h1) == 64  # SHA256 hex

    def test_lowercases_type(self) -> None:
        # Normalising the type at hash time prevents the same id
        # producing two different hashes because of casing.
        assert make_hash("YouTube", "abc") == make_hash("youtube", "abc")

    def test_different_types_yield_different_hashes(self) -> None:
        assert make_hash("youtube", "abc") != make_hash("news", "abc")

    def test_whitespace_in_id_trimmed(self) -> None:
        assert make_hash("youtube", " abc ") == make_hash("youtube", "abc")

    def test_different_ids_yield_different_hashes(self) -> None:
        assert make_hash("youtube", "abc") != make_hash("youtube", "xyz")


class TestNormalizeUrl:
    def test_lowercases_host(self) -> None:
        assert normalize_url("https://Example.COM/path") == "https://example.com/path"

    def test_preserves_path_case(self) -> None:
        # URL paths are case-sensitive in general
        assert (
            normalize_url("https://example.com/Foo/Bar")
            == "https://example.com/Foo/Bar"
        )

    def test_strips_utm_params(self) -> None:
        url = "https://example.com/a?utm_source=x&utm_campaign=y&keep=1"
        out = normalize_url(url)
        assert "utm_source" not in out
        assert "utm_campaign" not in out
        assert "keep=1" in out

    def test_strips_known_trackers(self) -> None:
        url = "https://example.com/a?fbclid=1&gclid=2&gbraid=3&wbraid=4&real=ok"
        out = normalize_url(url)
        assert "fbclid" not in out
        assert "gclid" not in out
        assert "gbraid" not in out
        assert "wbraid" not in out
        assert "real=ok" in out

    def test_sorts_query_params(self) -> None:
        a = normalize_url("https://example.com/?b=2&a=1&c=3")
        b = normalize_url("https://example.com/?c=3&a=1&b=2")
        assert a == b == "https://example.com/?a=1&b=2&c=3"

    def test_drops_empty_fragment(self) -> None:
        assert normalize_url("https://example.com/page#") == "https://example.com/page"

    def test_preserves_port(self) -> None:
        assert (
            normalize_url("https://Example.com:8443/x")
            == "https://example.com:8443/x"
        )


# ---------------------------------------------------------------------------
# In-memory backend (offline mode)
# ---------------------------------------------------------------------------

MemoryBackend = Dict[str, Dict[str, Any]]


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def store() -> Tuple[ProcessedStore, MemoryBackend]:
    memory: MemoryBackend = {}
    s = ProcessedStore("appFAKE", "ProcessedContent", _memory=memory)
    return s, memory


# ---------------------------------------------------------------------------
# Scenario 1 — Insert
# ---------------------------------------------------------------------------

class TestInsertScenario:
    def test_insert_one_record_returns_id(self, store: Tuple[ProcessedStore, MemoryBackend]) -> None:
        s, memory = store
        rid = s.mark_processed(
            source_type="youtube",
            source_id="vid_001",
            title="Wohnungskauf 2026",
            channels=["youtube.ex_makler"],
            pipeline_run_id="run-2026-08-09-001",
            output_path="/vault/YouTube/vid_001.md",
            metadata={"duration": 1234, "lang": "de"},
            tags=["long-form"],
        )
        assert len(rid) == 64
        assert rid in memory
        rec = memory[rid]
        assert rec["source_hash"] == make_hash("youtube", "vid_001")
        assert rec["source_type"] == "youtube"
        assert rec["title"] == "Wohnungskauf 2026"
        assert rec["channels"] == ["youtube.ex_makler"]
        assert rec["tags"] == ["long-form"]
        assert rec["metadata"] == json.dumps(
            {"duration": 1234, "lang": "de"}, ensure_ascii=False
        )
        assert "first_seen_at" in rec
        assert "processed_at" in rec

    def test_is_processed_after_insert(self, store: Tuple[ProcessedStore, MemoryBackend]) -> None:
        s, _ = store
        s.mark_processed("news", "art_42", "Mietpreisbremse verlängert")
        assert s.is_processed("news", "art_42") is True
        assert s.is_processed("news", "never_seen") is False


# ---------------------------------------------------------------------------
# Scenario 2 — Query
# ---------------------------------------------------------------------------

class TestQueryScenario:
    def test_is_processed_true_after_insert(self, store: Tuple[ProcessedStore, MemoryBackend]) -> None:
        s, _ = store
        s.mark_processed("reddit", "post_xyz", "Berlin Mietendeckel thread")
        assert s.is_processed("reddit", "post_xyz") is True

    def test_is_processed_false_for_unknown(self, store: Tuple[ProcessedStore, MemoryBackend]) -> None:
        s, _ = store
        assert s.is_processed("reddit", "unknown_id") is False

    def test_processed_at_bumped_on_second_call(
        self, store: Tuple[ProcessedStore, MemoryBackend]
    ) -> None:
        s, memory = store
        # Force the first mark to record a known old timestamp so we can
        # see the second one bump it.
        s.mark_processed("youtube", "vid_a", "Title 1")
        rec = list(memory.values())[0]
        first_processed = rec["processed_at"]
        # Update with a different title so the path is clearly the
        # idempotent update (not a re-insert).
        time.sleep(1.05)  # ensure second-resolution timestamp differs
        s.mark_processed("youtube", "vid_a", "Title 1 (revised)")
        rec = list(memory.values())[0]
        second_processed = rec["processed_at"]
        # Same record id, processed_at advanced
        assert len(memory) == 1
        assert first_processed != second_processed
        assert rec["title"] == "Title 1 (revised)"


# ---------------------------------------------------------------------------
# Scenario 3 — Idempotent re-insert
# ---------------------------------------------------------------------------

class TestIdempotentReinsert:
    def test_second_mark_processed_updates_not_creates(
        self, store: Tuple[ProcessedStore, MemoryBackend]
    ) -> None:
        s, memory = store
        rid_1 = s.mark_processed(
            "podcast", "ep_001", "Folge 1", tags=["long-form"]
        )
        assert len(memory) == 1
        rid_2 = s.mark_processed(
            "podcast",
            "ep_001",
            "Folge 1 (updated title)",
            tags=["long-form", "restored"],
            output_path="/vault/podcast/ep_001.md",
        )
        # Same record id, no duplicate
        assert rid_1 == rid_2
        assert len(memory) == 1
        rec = list(memory.values())[0]
        assert rec["title"] == "Folge 1 (updated title)"
        assert "restored" in rec["tags"]
        assert rec["output_path"] == "/vault/podcast/ep_001.md"

    def test_stats_does_not_double_count(
        self, store: Tuple[ProcessedStore, MemoryBackend]
    ) -> None:
        s, _ = store
        s.mark_processed("youtube", "v1", "A")
        s.mark_processed("youtube", "v2", "B")
        s.mark_processed("news", "n1", "C")
        s.mark_processed("youtube", "v1", "A again")  # idempotent re-insert
        s.mark_processed("youtube", "v1", "A again 2")  # and again
        counts = s.stats(days=7)
        # v1+v2 = 2 youtube, 1 news, no double-count
        assert counts.get("youtube") == 2
        assert counts.get("news") == 1
        assert counts.get("reddit", 0) == 0


# ---------------------------------------------------------------------------
# Side-effect updates
# ---------------------------------------------------------------------------

class TestSideEffects:
    def test_update_discord_and_github(
        self, store: Tuple[ProcessedStore, MemoryBackend]
    ) -> None:
        s, memory = store
        rid = s.mark_processed("youtube", "vid_z", "Title Z")
        s.update_side_effects(
            source_hash=make_hash("youtube", "vid_z"),
            discord_message_id="1234567890",
            github_commit_sha="abcdef1234",
        )
        rec = memory[rid]
        assert rec["discord_message_id"] == "1234567890"
        assert rec["github_commit_sha"] == "abcdef1234"

    def test_update_side_effects_only_one(
        self, store: Tuple[ProcessedStore, MemoryBackend]
    ) -> None:
        s, memory = store
        rid = s.mark_processed("news", "x", "X")
        s.update_side_effects(
            source_hash=make_hash("news", "x"), discord_message_id="987"
        )
        rec = memory[rid]
        assert rec["discord_message_id"] == "987"
        # github_commit_sha still empty
        assert "github_commit_sha" not in rec

    def test_update_side_effects_requires_a_field(
        self, store: Tuple[ProcessedStore, MemoryBackend]
    ) -> None:
        s, _ = store
        s.mark_processed("news", "y", "Y")
        with pytest.raises(ProcessedStoreError):
            s.update_side_effects(source_hash=make_hash("news", "y"))

    def test_update_side_effects_missing_record(
        self, store: Tuple[ProcessedStore, MemoryBackend]
    ) -> None:
        s, _ = store
        with pytest.raises(ProcessedStoreNotFoundError):
            s.update_side_effects(
                source_hash=make_hash("youtube", "ghost"),
                discord_message_id="0",
            )


# ---------------------------------------------------------------------------
# Auth, retry, and error mapping
# ---------------------------------------------------------------------------

class TestAuthAndRetry:
    def test_missing_credentials_raises_auth_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        for key in (
            "FIRESTORE_PROJECT_ID",
            "GOOGLE_APPLICATION_CREDENTIALS",
            "FIRESTORE_CREDENTIALS_JSON",
        ):
            monkeypatch.delenv(key, raising=False)
        s = ProcessedStore("appX")
        with pytest.raises(ProcessedStoreAuthError):
            s.is_processed("youtube", "x")

    def test_retry_on_transient_failure(self) -> None:
        class FlakyMemory(dict):
            attempts = 0

            def __setitem__(self, key, value):
                FlakyMemory.attempts += 1
                if FlakyMemory.attempts < 3:
                    raise ProcessedStoreError("boom")
                super().__setitem__(key, value)

        memory: MemoryBackend = FlakyMemory()
        s = ProcessedStore("appX", _memory=memory)
        rid = s.mark_processed("youtube", "v1", "Title")
        assert len(rid) == 64
        assert FlakyMemory.attempts == 3

    def test_non_retriable_auth_error_does_not_retry(self) -> None:
        s = ProcessedStore("appX")
        calls = {"n": 0}

        def always_auth(doc_id: str):
            calls["n"] += 1
            raise ProcessedStoreAuthError("nope")

        s._get_doc_data = always_auth  # type: ignore[assignment]
        with pytest.raises(ProcessedStoreAuthError):
            s.is_processed("youtube", "x")
        assert calls["n"] == 1


# ---------------------------------------------------------------------------
# Live Firestore tests — only run when explicitly opted in
# ---------------------------------------------------------------------------

LIVE_PROJECT = os.environ.get("FIRESTORE_PROJECT_ID")


@pytest.mark.skipif(
    not LIVE_PROJECT,
    reason="FIRESTORE_PROJECT_ID not set; live Firestore tests disabled",
)
class TestLiveFirestore:
    """Exercise the real Firestore API.

    Set ``FIRESTORE_PROJECT_ID`` and service-account credentials to run.
    """

    @classmethod
    def setup_class(cls) -> None:
        cls.store = ProcessedStore()
        cls.created_ids: List[str] = []

    @classmethod
    def teardown_class(cls) -> None:
        for doc_id in cls.created_ids:
            try:
                cls.store.delete_record(doc_id)
            except Exception:
                pass

    def test_full_lifecycle(self) -> None:
        suffix = uuid.uuid4().hex[:10]
        title = f"test_live_{suffix}"
        rid = self.store.mark_processed(
            "youtube",
            f"live_{suffix}",
            title,
            tags=["long-form"],
        )
        self.created_ids.append(rid)
        assert len(rid) == 64
        assert self.store.is_processed("youtube", f"live_{suffix}")
        rid2 = self.store.mark_processed(
            "youtube",
            f"live_{suffix}",
            title + " (revised)",
        )
        assert rid == rid2
        self.store.update_side_effects(
            source_hash=make_hash("youtube", f"live_{suffix}"),
            discord_message_id="live_test_msg",
        )
        counts = self.store.stats(days=1)
        assert counts.get("youtube", 0) >= 1
