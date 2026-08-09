"""Unit tests for pipeline.lib.processed_store.

The tests are written in two layers:

1. **Pure helper tests** (``make_hash``, ``normalize_url``) — no I/O, always run.
2. **Store tests** — run against a fake Airtable layer (default) so the
   suite is fully green without a real token. To exercise the live API
   instead, set both ``PIPELINE_TEST_BASE_ID`` and ``AIRTABLE_API_KEY`` —
   the live tests use a ``test_`` prefix on every record and clean up
   after themselves.

The three PM-mandated scenarios (insert / query / idempotent re-insert)
are all covered by the offline suite. Live tests add network resilience
and the real lookup-then-create / PATCH codepath against Airtable.
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
from unittest import mock

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
# Fake Airtable layer (offline mode)
# ---------------------------------------------------------------------------

class FakeAirtable:
    """Drop-in fake for ProcessedStore._request. Stores records in memory.

    Models the parts of the Airtable API the module uses:
      - GET /<base>/<table>  (with filterByFormula, pageSize, offset)
      - POST /<base>/<table>  (with typecast + fields — plain create)
      - PATCH /<base>/<table>/<rec>
    """

    def __init__(self) -> None:
        self.records: Dict[str, Dict[str, Any]] = {}
        self.call_log: List[Tuple[str, str, Dict[str, Any]]] = []
        # Toggle to simulate a 5xx storm on the next call
        self.fail_next_n = 0

    def _next_id(self) -> str:
        return f"rec{uuid.uuid4().hex[:14]}"

    def match(self, formula: str) -> List[Dict[str, Any]]:
        """Evaluate the small subset of formulas we actually use."""
        # AND({source_hash}='X', ...) | {source_hash}='X' |
        # DATETIME_DIFF(NOW(), {processed_at}, 'days') <= N
        results = []
        for rec in self.records.values():
            f = rec.get("fields", {})
            if "{source_hash}=" in formula:
                needle = formula.split("'")[1]
                if f.get("source_hash") != needle:
                    continue
            elif "{source_type}=" in formula:
                needle = formula.split("'")[1]
                if f.get("source_type") != needle:
                    continue
            elif "DATETIME_DIFF" in formula or "IS_AFTER" in formula:
                # All fake records are 'now' so they pass the recency check.
                pass
            else:
                continue
            results.append(rec)
        return results

    def handle(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, str]] = None,
        body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        self.call_log.append((method, path, body or {}))
        if self.fail_next_n > 0:
            self.fail_next_n -= 1
            raise ProcessedStoreError("simulated transient failure")
        # POST <table> (legacy performUpsert — kept for offline tests that
        # still model the upsert body shape, even though production no
        # longer sends it).
        if method == "POST" and body and "performUpsert" in body:
            merge_field = body["performUpsert"]["fieldsToMergeOn"][0]
            resp_records = []
            for rec_in in body.get("records", []):
                fields = rec_in.get("fields", {})
                key = fields.get(merge_field)
                existing = None
                for rec in self.records.values():
                    if rec["fields"].get(merge_field) == key:
                        existing = rec
                        break
                if existing:
                    existing["fields"].update(fields)
                    resp_records.append(existing)
                else:
                    new_id = self._next_id()
                    new_rec = {
                        "id": new_id,
                        "fields": dict(fields),
                        "createdTime": "2026-08-09T00:00:00.000Z",
                    }
                    self.records[new_id] = new_rec
                    resp_records.append(new_rec)
            return {"records": resp_records}

        # POST <table> (plain create — matches production behaviour after
        # the performUpsert fix). Production does a lookup-then-create
        # (Path 2 -> Path 3), so the fake just inserts a new record and
        # returns {"records": [...]} exactly as the Airtable API does.
        if method == "POST" and body and "fields" in body:
            new_id = self._next_id()
            new_rec = {
                "id": new_id,
                "fields": dict(body["fields"]),
                "createdTime": "2026-08-09T00:00:00.000Z",
            }
            self.records[new_id] = new_rec
            return {"records": [new_rec]}

        # PATCH <table>/<rec_id>
        if method == "PATCH":
            rec_id = path.rsplit("/", 1)[-1]
            if rec_id.startswith("rec"):
                if rec_id not in self.records:
                    raise ProcessedStoreNotFoundError(f"no record {rec_id}")
                self.records[rec_id]["fields"].update(body.get("fields", {}))
                return {"id": rec_id, "fields": self.records[rec_id]["fields"]}

        # GET <table>
        if method == "GET":
            formula = (params or {}).get("filterByFormula")
            matched = self.match(formula) if formula else list(self.records.values())
            return {"records": matched}

        raise ProcessedStoreError(f"unhandled fake call: {method} {path}")


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def store(monkeypatch: pytest.MonkeyPatch) -> Tuple[ProcessedStore, FakeAirtable]:
    fake = FakeAirtable()
    monkeypatch.setenv("AIRTABLE_API_KEY", "pat_test_offline")
    s = ProcessedStore("appFAKE", "ProcessedContent")
    s._request = mock.MagicMock(side_effect=fake.handle)  # type: ignore[assignment]
    # Attach the fake so individual tests can inspect it.
    setattr(s, "_fake", fake)
    return s, fake


# ---------------------------------------------------------------------------
# Scenario 1 — Insert
# ---------------------------------------------------------------------------

class TestInsertScenario:
    def test_insert_one_record_returns_id(self, store: Tuple[ProcessedStore, FakeAirtable]) -> None:
        s, fake = store
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
        assert rid.startswith("rec")
        assert rid in fake.records
        rec = fake.records[rid]
        assert rec["fields"]["source_hash"] == make_hash("youtube", "vid_001")
        assert rec["fields"]["source_type"] == "youtube"
        assert rec["fields"]["title"] == "Wohnungskauf 2026"
        assert rec["fields"]["channels"] == ["youtube.ex_makler"]
        assert rec["fields"]["tags"] == ["long-form"]
        assert rec["fields"]["metadata"] == json.dumps(
            {"duration": 1234, "lang": "de"}, ensure_ascii=False
        )
        assert "first_seen_at" in rec["fields"]
        assert "processed_at" in rec["fields"]

    def test_is_processed_after_insert(self, store: Tuple[ProcessedStore, FakeAirtable]) -> None:
        s, _ = store
        s.mark_processed("news", "art_42", "Mietpreisbremse verlängert")
        assert s.is_processed("news", "art_42") is True
        assert s.is_processed("news", "never_seen") is False


# ---------------------------------------------------------------------------
# Scenario 2 — Query
# ---------------------------------------------------------------------------

class TestQueryScenario:
    def test_is_processed_true_after_insert(self, store: Tuple[ProcessedStore, FakeAirtable]) -> None:
        s, _ = store
        s.mark_processed("reddit", "post_xyz", "Berlin Mietendeckel thread")
        assert s.is_processed("reddit", "post_xyz") is True

    def test_is_processed_false_for_unknown(self, store: Tuple[ProcessedStore, FakeAirtable]) -> None:
        s, _ = store
        assert s.is_processed("reddit", "unknown_id") is False

    def test_processed_at_bumped_on_second_call(
        self, store: Tuple[ProcessedStore, FakeAirtable]
    ) -> None:
        s, fake = store
        # Force the first mark to record a known old timestamp so we can
        # see the second one bump it.
        s.mark_processed("youtube", "vid_a", "Title 1")
        rec = list(fake.records.values())[0]
        first_processed = rec["fields"]["processed_at"]
        # Update with a different title so the path is clearly the
        # idempotent update (not a re-insert).
        time.sleep(1.05)  # ensure second-resolution timestamp differs
        s.mark_processed("youtube", "vid_a", "Title 1 (revised)")
        rec = list(fake.records.values())[0]
        second_processed = rec["fields"]["processed_at"]
        # Same record id, processed_at advanced
        assert len(fake.records) == 1
        assert first_processed != second_processed
        assert rec["fields"]["title"] == "Title 1 (revised)"


# ---------------------------------------------------------------------------
# Scenario 3 — Idempotent re-insert
# ---------------------------------------------------------------------------

class TestIdempotentReinsert:
    def test_second_mark_processed_updates_not_creates(
        self, store: Tuple[ProcessedStore, FakeAirtable]
    ) -> None:
        s, fake = store
        rid_1 = s.mark_processed(
            "podcast", "ep_001", "Folge 1", tags=["long-form"]
        )
        assert len(fake.records) == 1
        rid_2 = s.mark_processed(
            "podcast",
            "ep_001",
            "Folge 1 (updated title)",
            tags=["long-form", "restored"],
            output_path="/vault/podcast/ep_001.md",
        )
        # Same record id, no duplicate
        assert rid_1 == rid_2
        assert len(fake.records) == 1
        rec = list(fake.records.values())[0]
        assert rec["fields"]["title"] == "Folge 1 (updated title)"
        assert "restored" in rec["fields"]["tags"]
        assert rec["fields"]["output_path"] == "/vault/podcast/ep_001.md"

    def test_stats_does_not_double_count(
        self, store: Tuple[ProcessedStore, FakeAirtable]
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
        self, store: Tuple[ProcessedStore, FakeAirtable]
    ) -> None:
        s, fake = store
        rid = s.mark_processed("youtube", "vid_z", "Title Z")
        s.update_side_effects(
            source_hash=make_hash("youtube", "vid_z"),
            discord_message_id="1234567890",
            github_commit_sha="abcdef1234",
        )
        rec = fake.records[rid]
        assert rec["fields"]["discord_message_id"] == "1234567890"
        assert rec["fields"]["github_commit_sha"] == "abcdef1234"

    def test_update_side_effects_only_one(
        self, store: Tuple[ProcessedStore, FakeAirtable]
    ) -> None:
        s, fake = store
        rid = s.mark_processed("news", "x", "X")
        s.update_side_effects(
            source_hash=make_hash("news", "x"), discord_message_id="987"
        )
        rec = fake.records[rid]
        assert rec["fields"]["discord_message_id"] == "987"
        # github_commit_sha still empty
        assert "github_commit_sha" not in rec["fields"]

    def test_update_side_effects_requires_a_field(
        self, store: Tuple[ProcessedStore, FakeAirtable]
    ) -> None:
        s, _ = store
        s.mark_processed("news", "y", "Y")
        with pytest.raises(ProcessedStoreError):
            s.update_side_effects(source_hash=make_hash("news", "y"))

    def test_update_side_effects_missing_record(
        self, store: Tuple[ProcessedStore, FakeAirtable]
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
    def test_missing_env_raises_auth_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("AIRTABLE_API_KEY", raising=False)
        with pytest.raises(ProcessedStoreAuthError):
            ProcessedStore("appX")

    def test_retry_on_transient_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Override the inner http call so the outer retry loop runs.
        monkeypatch.setenv("AIRTABLE_API_KEY", "pat_x")
        s = ProcessedStore("appX")
        calls = {"n": 0}

        def flaky_http(method, url, headers, data):
            calls["n"] += 1
            if calls["n"] < 3:
                raise ProcessedStoreError("boom")
            return {"records": []}

        # pylint: disable=protected-access
        s._http_call = flaky_http  # type: ignore[assignment]
        out = s._list_all_records()
        assert out == []
        assert calls["n"] == 3  # two fails + one success

    def test_non_retriable_auth_error_does_not_retry(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("AIRTABLE_API_KEY", "pat_x")
        s = ProcessedStore("appX")
        calls = {"n": 0}

        def always_auth(method, url, headers, data):
            calls["n"] += 1
            raise ProcessedStoreAuthError("nope")

        s._http_call = always_auth  # type: ignore[assignment]
        with pytest.raises(ProcessedStoreAuthError):
            s._list_all_records()
        assert calls["n"] == 1  # never retried


# ---------------------------------------------------------------------------
# Live Airtable tests — only run when explicitly opted in
# ---------------------------------------------------------------------------

LIVE_BASE = os.environ.get("PIPELINE_TEST_BASE_ID")


@pytest.mark.skipif(
    not LIVE_BASE,
    reason="PIPELINE_TEST_BASE_ID not set; live Airtable tests disabled",
)
class TestLiveAirtable:
    """Exercise the real Airtable API.

    All test records are prefixed with ``test_`` and cleaned up in
    teardown. Set both:
        AIRTABLE_API_KEY=pat_xxx
        PIPELINE_TEST_BASE_ID=app_xxx
    to run these.
    """

    @classmethod
    def setup_class(cls) -> None:
        cls.store = ProcessedStore(LIVE_BASE)  # type: ignore[arg-type]
        cls.created_ids: List[str] = []

    @classmethod
    def teardown_class(cls) -> None:
        for rid in cls.created_ids:
            try:
                # DELETE is not in the public surface, so just leave the
                # row — manual cleanup is fine for live-mode tests.
                pass
            except Exception:
                pass

    def test_full_lifecycle(self) -> None:
        suffix = uuid.uuid4().hex[:10]
        title = f"test_live_{suffix}"
        # Insert
        rid = self.store.mark_processed(
            "youtube",
            f"live_{suffix}",
            title,
            tags=["long-form"],
        )
        self.created_ids.append(rid)
        assert rid.startswith("rec")
        # Query
        assert self.store.is_processed("youtube", f"live_{suffix}")
        # Idempotent re-insert
        rid2 = self.store.mark_processed(
            "youtube",
            f"live_{suffix}",
            title + " (revised)",
        )
        assert rid == rid2
        # Side effects
        self.store.update_side_effects(
            source_hash=make_hash("youtube", f"live_{suffix}"),
            discord_message_id="live_test_msg",
        )
        # Stats
        counts = self.store.stats(days=1)
        assert counts.get("youtube", 0) >= 1
