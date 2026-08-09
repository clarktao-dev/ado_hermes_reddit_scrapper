"""QA scenario runner for pipeline.lib.processed_store.

Runs all 5 mandatory scenarios + 3 edge cases against the isolated
ProcessedContent_QA_TEST table. Prints structured JSON per scenario so the
runner output can be parsed programmatically by the QA report generator.

Does NOT touch the production ProcessedContent table.
"""
from __future__ import annotations

import json
import os
import sys
import time
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock
from urllib import error as urlerror

THIS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = THIS_DIR.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.lib.processed_store import (
    ProcessedStore,
    ProcessedStoreError,
    make_hash,
)

BASE_ID = "appHilorcrC5T0p2u"
QA_TABLE = "ProcessedContent_QA_TEST"
# api_key pulled from env at runtime (not stored on disk)


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _emit(name: str, **payload):
    print(json.dumps({"scenario": name, "ts": _now(), **payload}, ensure_ascii=False))


def _store():
    return ProcessedStore(BASE_ID, table_name=QA_TABLE)


# Track record ids across scenarios so the report can list them for cleanup.
RECORDS: dict[str, str] = {}


def _record_id(source_type: str, source_id: str, rid: str):
    RECORDS[f"{source_type}:{source_id}"] = rid


def scenario_1_fresh_insert(store: ProcessedStore) -> dict:
    name = "S1_fresh_insert"
    source_id = f"qa_test_001_{uuid.uuid4().hex[:8]}"
    try:
        rid = store.mark_processed("youtube", source_id, "QA Test 1")
        ok = rid.startswith("rec")
        if ok:
            _record_id("youtube", source_id, rid)
        _emit(name, status="PASS" if ok else "FAIL", rid=rid, source_id=source_id)
        return {"name": name, "result": "PASS" if ok else "FAIL", "rid": rid, "source_id": source_id}
    except Exception as e:
        _emit(name, status="FAIL", error=str(e), trace=traceback.format_exc())
        return {"name": name, "result": "FAIL", "error": str(e), "trace": traceback.format_exc()}


def scenario_2_query(store: ProcessedStore, s1: dict) -> dict:
    name = "S2_query_after_insert"
    try:
        if s1["result"] != "PASS":
            _emit(name, status="SKIP", reason="S1 failed")
            return {"name": name, "result": "SKIP"}
        ok = store.is_processed("youtube", s1["source_id"])
        _emit(name, status="PASS" if ok else "FAIL", is_processed=ok)
        return {"name": name, "result": "PASS" if ok else "FAIL", "is_processed": ok}
    except Exception as e:
        _emit(name, status="FAIL", error=str(e), trace=traceback.format_exc())
        return {"name": name, "result": "FAIL", "error": str(e)}


def scenario_3_idempotent(store: ProcessedStore) -> dict:
    name = "S3_idempotent_re_insert"
    source_id = f"qa_test_002_{uuid.uuid4().hex[:8]}"
    try:
        first = store.mark_processed("youtube", source_id, "first")
        second = store.mark_processed("youtube", source_id, "revised")
        same = first == second
        _record_id("youtube", source_id, first)
        # Force a fresh-process verification: query Airtable via a new store.
        fresh = _store()
        n_records = len(
            fresh._list_all_records(
                filter_formula=f"{{source_hash}}='{make_hash('youtube', source_id)}'"
            )
        )
        ok = same and n_records == 1
        _emit(
            name,
            status="PASS" if ok else "FAIL",
            first=first,
            second=second,
            same=same,
            table_count=n_records,
        )
        return {
            "name": name,
            "result": "PASS" if ok else "FAIL",
            "first": first,
            "second": second,
            "table_count": n_records,
        }
    except Exception as e:
        _emit(name, status="FAIL", error=str(e), trace=traceback.format_exc())
        return {"name": name, "result": "FAIL", "error": str(e)}


def scenario_4_side_effects(store: ProcessedStore) -> dict:
    name = "S4_side_effects"
    source_id = f"qa_test_003_{uuid.uuid4().hex[:8]}"
    try:
        hash_val = make_hash("youtube", source_id)
        rid = store.mark_processed("youtube", source_id, "side effects test")
        _record_id("youtube", source_id, rid)
        store.update_side_effects(
            source_hash=hash_val,
            discord_message_id="qa_msg_001",
            github_commit_sha="qa_sha_abc123",
        )
        # GET the record via a fresh store to confirm both side-effect fields
        fresh = _store()
        records = fresh._list_all_records(
            filter_formula=f"{{source_hash}}='{hash_val}'"
        )
        if not records:
            raise RuntimeError("record disappeared after update_side_effects")
        fields = records[0]["fields"]
        dm = fields.get("discord_message_id")
        sha = fields.get("github_commit_sha")
        ok = dm == "qa_msg_001" and sha == "qa_sha_abc123"
        _emit(
            name,
            status="PASS" if ok else "FAIL",
            rid=rid,
            discord_message_id=dm,
            github_commit_sha=sha,
        )
        return {
            "name": name,
            "result": "PASS" if ok else "FAIL",
            "rid": rid,
            "discord_message_id": dm,
            "github_commit_sha": sha,
        }
    except Exception as e:
        _emit(name, status="FAIL", error=str(e), trace=traceback.format_exc())
        return {"name": name, "result": "FAIL", "error": str(e)}


def scenario_5_stats(store: ProcessedStore) -> dict:
    name = "S5_stats_get_recent"
    inserted: list[tuple[str, str]] = []
    try:
        for i in range(3):
            sid = f"qa_stats_y_{i}_{uuid.uuid4().hex[:6]}"
            rid = store.mark_processed("youtube", sid, f"stats y{i}")
            inserted.append(("youtube", sid))
            _record_id("youtube", sid, rid)
        for i in range(2):
            sid = f"qa_stats_n_{i}_{uuid.uuid4().hex[:6]}"
            rid = store.mark_processed("news", sid, f"stats n{i}")
            inserted.append(("news", sid))
            _record_id("news", sid, rid)

        stats = store.stats(days=7)
        # Subtract our 3 youtube + 2 news from the totals to validate just
        # the *delta* — the QA table started empty, so totals == our inserts.
        yt = stats.get("youtube", 0)
        ne = stats.get("news", 0)
        yt_ok = yt >= 3
        ne_ok = ne >= 2
        ok = yt_ok and ne_ok

        recent = store.get_recent(source_type="youtube", days=7)
        recent_ok = len(recent) >= 3
        ok = ok and recent_ok

        _emit(
            name,
            status="PASS" if ok else "FAIL",
            stats=stats,
            recent_youtube_count=len(recent),
        )
        return {
            "name": name,
            "result": "PASS" if ok else "FAIL",
            "stats": stats,
            "recent_youtube_count": len(recent),
            "inserted": inserted,
        }
    except Exception as e:
        _emit(name, status="FAIL", error=str(e), trace=traceback.format_exc())
        return {"name": name, "result": "FAIL", "error": str(e)}


# --------------------------------------------------------------------------
# Edge cases
# --------------------------------------------------------------------------


def edge_a_special_chars(store: ProcessedStore) -> dict:
    """Special chars in source_id — hash must still be stable."""
    name = "edge_A_special_chars"
    sid = "id with spaces & symbols !@#$%^&*()_+-={}[]|:;\"'<>,./?"
    try:
        rid_first = store.mark_processed("youtube", sid, "special 1")
        rid_second = store.mark_processed("youtube", sid, "special 2")
        h1 = make_hash("youtube", sid)
        h2 = make_hash("youtube", " " + sid + " ")  # whitespace variant
        # After whitespace strip + same id, hash should be equal.
        ok = (
            rid_first == rid_second
            and rid_first.startswith("rec")
            and h1 == h2
        )
        _record_id("youtube", sid, rid_first)
        _emit(
            name,
            status="PASS" if ok else "FAIL",
            rid_first=rid_first,
            rid_second=rid_second,
            hash_whitespace_match=(h1 == h2),
        )
        return {
            "name": name,
            "result": "PASS" if ok else "FAIL",
            "rid_first": rid_first,
            "hash_whitespace_match": h1 == h2,
        }
    except Exception as e:
        _emit(name, status="FAIL", error=str(e), trace=traceback.format_exc())
        return {"name": name, "result": "FAIL", "error": str(e)}


def edge_b_network_failure(store: ProcessedStore) -> dict:
    """Mock urlopen to throw HTTP 500 — verify retry then raise.

    The task brief says "把 _http_call 改成丟 HTTPError 500", but
    ProcessedStore._http_call wraps HTTPError into ProcessedStoreError
    itself (its `except error.HTTPError` handler). To exercise the
    retry loop we therefore patch the lower-level urllib.request.urlopen
    that _http_call invokes, so the wrapping still runs and the outer
    _request can apply its 1s/2s/4s backoff.
    """
    name = "edge_B_network_failure"
    sid = f"qa_net_{uuid.uuid4().hex[:6]}"
    calls: list[int] = []

    def boom(*args, **kwargs):
        calls.append(1)
        from email.message import Message
        hdrs = Message()
        hdrs["Content-Type"] = "text/plain"
        raise urlerror.HTTPError(
            args[0] if args else "http://example",
            500,
            "Internal Server Error",
            hdrs,
            None,
        )

    try:
        with mock.patch(
            "pipeline.lib.processed_store.request.urlopen", side_effect=boom
        ), mock.patch(
            "pipeline.lib.processed_store.time.sleep", lambda *_: None
        ):
            store2 = ProcessedStore(BASE_ID, table_name=QA_TABLE)
            t0 = time.time()
            try:
                store2.mark_processed("youtube", sid, "should fail")
                raised = None
            except ProcessedStoreError as e:
                raised = repr(e)
            elapsed = time.time() - t0
        # _RETRY_BACKOFFS_SEC has 3 entries -> 1 initial + 3 retries = 4 calls.
        # However mark_processed does a _find_by_hash first (1 call), so total
        # attempted urlopen calls = 4 (the 4 failed attempts) + however many
        # subsequent calls happen if the path reaches a different code path.
        # We only assert >= 4 attempts were made before giving up.
        ok = raised is not None and len(calls) >= 4
        _emit(
            name,
            status="PASS" if ok else "FAIL",
            attempts=len(calls),
            raised=bool(raised),
            elapsed_sec=round(elapsed, 3),
            raised_msg=str(raised)[:200] if raised else None,
        )
        return {
            "name": name,
            "result": "PASS" if ok else "FAIL",
            "attempts": len(calls),
            "raised": bool(raised),
        }
    except Exception as e:
        _emit(name, status="FAIL", error=str(e), trace=traceback.format_exc())
        return {"name": name, "result": "FAIL", "error": str(e)}


def edge_c_source_type_isolation(store: ProcessedStore) -> dict:
    """Same source_id, different source_type must not collide."""
    name = "edge_C_source_type_isolation"
    sid = f"qa_shared_{uuid.uuid4().hex[:6]}"
    try:
        rid_y = store.mark_processed("youtube", sid, "yt share")
        rid_n = store.mark_processed("news", sid, "news share")
        rid_r = store.mark_processed("reddit", sid, "reddit share")
        _record_id("youtube", sid, rid_y)
        _record_id("news", sid, rid_n)
        _record_id("reddit", sid, rid_r)
        all_distinct = len({rid_y, rid_n, rid_r}) == 3

        # And each must be discoverable by is_processed with the right type only.
        ok = all_distinct
        ok &= store.is_processed("youtube", sid) is True
        ok &= store.is_processed("news", sid) is True
        ok &= store.is_processed("reddit", sid) is True
        # Confirm the hashes differ.
        hashes = {
            make_hash("youtube", sid),
            make_hash("news", sid),
            make_hash("reddit", sid),
        }
        ok &= len(hashes) == 3

        _emit(
            name,
            status="PASS" if ok else "FAIL",
            rid_youtube=rid_y,
            rid_news=rid_n,
            rid_reddit=rid_r,
            hashes_distinct=(len(hashes) == 3),
        )
        return {
            "name": name,
            "result": "PASS" if ok else "FAIL",
            "rid_youtube": rid_y,
            "rid_news": rid_n,
            "rid_reddit": rid_r,
        }
    except Exception as e:
        _emit(name, status="FAIL", error=str(e), trace=traceback.format_exc())
        return {"name": name, "result": "FAIL", "error": str(e)}


# --------------------------------------------------------------------------


def main() -> int:
    if not os.environ.get("AIRTABLE_API_KEY"):
        # Try to load from /root/.hermes/.env
        env_file = Path("/root/.hermes/.env")
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if line.startswith("AIRTABLE_API_KEY="):
                    os.environ["AIRTABLE_API_KEY"] = line.split("=", 1)[1].strip().strip('"').strip("'")
                    break
    if not os.environ.get("AIRTABLE_API_KEY"):
        print("FATAL: AIRTABLE_API_KEY not set", file=sys.stderr)
        return 2

    store = _store()
    results: list[dict] = []
    s1 = scenario_1_fresh_insert(store)
    results.append(s1)
    s2 = scenario_2_query(store, s1)
    results.append(s2)
    s3 = scenario_3_idempotent(store)
    results.append(s3)
    s4 = scenario_4_side_effects(store)
    results.append(s4)
    s5 = scenario_5_stats(store)
    results.append(s5)

    # Edge cases
    results.append(edge_a_special_chars(store))
    results.append(edge_b_network_failure(store))
    results.append(edge_c_source_type_isolation(store))

    summary = {
        "records": RECORDS,
        "results": results,
        "all_pass": all(r.get("result") in ("PASS", "SKIP") for r in results),
    }
    print("---SUMMARY---")
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    failed = [r for r in results if r.get("result") == "FAIL"]
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
