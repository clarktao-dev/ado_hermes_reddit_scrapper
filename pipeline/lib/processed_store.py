"""ProcessedContent ledger — Firestore-backed deduplication for pipeline outputs.

Provides a single source of truth for "have we already processed this item?"
across the youtube / news / reddit / podcast pipelines.

Design notes
------------
- **Idempotency**: ``mark_processed`` with the same ``source_hash`` always
  updates the same Firestore document (document id = ``source_hash``).
- **Type safety**: all public methods carry type hints and short docstrings.
- **Resilience**: Firestore writes use merge semantics; transient errors
  are retried with exponential backoff (1s, 2s, 4s).
- **Process-local cache**: a single ``_seen_cache`` dict eliminates
  round-trips for repeats inside the same run.

Environment
-----------
- ``FIRESTORE_PROJECT_ID`` — GCP project id.
- ``GOOGLE_APPLICATION_CREDENTIALS`` — service-account JSON path, or
- ``FIRESTORE_CREDENTIALS_JSON`` — inline service-account JSON.
- ``FIRESTORE_PROCESSED_COLLECTION`` — collection name (default ``processed``).

Legacy constructor args ``base_id`` and ``api_key`` are accepted for
backward compatibility but ignored (Airtable migration).
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from pipeline.lib.firestore_client import (
    FirestoreConfigError,
    collection_name_for_table,
    get_firestore_client,
)

logger = logging.getLogger(__name__)

DEFAULT_TABLE = "ProcessedContent"

_RETRY_BACKOFFS_SEC = (1.0, 2.0, 4.0)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class ProcessedStoreError(RuntimeError):
    """Base error for ``ProcessedStore`` failures."""


class ProcessedStoreAuthError(ProcessedStoreError):
    """Raised when Firestore credentials are missing or invalid."""


class ProcessedStoreNotFoundError(ProcessedStoreError):
    """Raised when a record does not exist."""


class ProcessedStoreConflictError(ProcessedStoreError):
    """Raised on validation / write conflicts."""


# ---------------------------------------------------------------------------
# Pure helpers (no I/O)
# ---------------------------------------------------------------------------

def make_hash(source_type: str, source_id: str) -> str:
    """Stable SHA256 hex digest of ``'<source_type>:<source_id>'``."""
    payload = f"{source_type.strip().lower()}:{source_id.strip()}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def parse_metadata(raw: Any) -> dict:
    """Defensive parser for the metadata field (JSON string or dict)."""
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


_TRACKING_PREFIXES = ("utm_",)
_TRACKING_EXACT = {"fbclid", "gclid", "gbraid", "wbraid", "mc_cid", "mc_eid", "yclid"}


def normalize_url(url: str) -> str:
    """Normalize a URL for dedup: strip tracking params, lowercase host, sort query."""
    from urllib import parse

    parsed = parse.urlparse(url.strip())
    scheme = (parsed.scheme or "http").lower()
    host = (parsed.hostname or "").lower()
    netloc = host
    if parsed.port:
        netloc = f"{host}:{parsed.port}"

    raw_q = parse.parse_qsl(parsed.query, keep_blank_values=True)
    cleaned_q = [
        (k, v)
        for k, v in raw_q
        if not k.lower().startswith(_TRACKING_PREFIXES)
        and k.lower() not in _TRACKING_EXACT
    ]
    cleaned_q.sort()
    new_query = parse.urlencode(cleaned_q)
    return parse.urlunparse((scheme, netloc, parsed.path, parsed.params, new_query, ""))


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _coerce_iso(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    if isinstance(value, str):
        return value
    raise ProcessedStoreError(
        f"first_seen_at must be datetime or ISO str, got {type(value).__name__}"
    )


def _parse_iso(value: Any) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _days_between(now: datetime, then: datetime) -> int:
    return max(0, int((now - then).total_seconds() // 86400))


def _extract_quoted(formula: str, field: str) -> Optional[str]:
    pattern = rf"\{{{re.escape(field)}\}}='((?:[^'\\]|\\.)*)'"
    match = re.search(pattern, formula)
    if not match:
        return None
    return match.group(1).replace("\\'", "'")


def _matches_filter_formula(fields: Dict[str, Any], formula: Optional[str]) -> bool:
    """Evaluate the small subset of Airtable formulas this codebase uses."""
    if not formula:
        return True

    now = datetime.now(timezone.utc)

    source_hash = _extract_quoted(formula, "source_hash")
    if source_hash is not None:
        return fields.get("source_hash") == source_hash

    pipeline_run_id = _extract_quoted(formula, "pipeline_run_id")
    if pipeline_run_id is not None and "{pipeline_run_id}" in formula:
        return fields.get("pipeline_run_id") == pipeline_run_id

    article_type = _extract_quoted(formula, "article_type")
    if article_type is not None and "AND(" not in formula:
        return fields.get("article_type") == article_type

    source_type = _extract_quoted(formula, "source_type")
    source_id = _extract_quoted(formula, "source_id")
    if source_type is not None and source_id is not None:
        if fields.get("source_type") != source_type:
            return False
        if fields.get("source_id") != source_id:
            return False
        return True

    days_match = re.search(
        r"DATETIME_DIFF\(NOW\(\), \{processed_at\}, 'days'\) <= (\d+)",
        formula,
    )
    if days_match:
        max_days = int(days_match.group(1))
        processed_at = _parse_iso(fields.get("processed_at"))
        if processed_at is None:
            return False
        if _days_between(now, processed_at) > max_days:
            return False
        if source_type is not None and fields.get("source_type") != source_type:
            return False
        return True

    reaction_date_after = re.search(
        r"IS_AFTER\(\{reaction_date\}, '([^']+)'\)",
        formula,
    )
    if reaction_date_after:
        cutoff = _parse_iso(reaction_date_after.group(1))
        reaction_date = _parse_iso(fields.get("reaction_date"))
        if cutoff is None or reaction_date is None:
            return False
        return reaction_date > cutoff

    logger.warning("unhandled filter formula — including record: %s", formula[:120])
    return True


def _record_from_doc(doc_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
    return {"id": doc_id, "fields": dict(data)}


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------

class ProcessedStore:
    """Firestore-backed dedup ledger for processed content items."""

    def __init__(
        self,
        base_id: str = "",
        table_name: str = DEFAULT_TABLE,
        *,
        api_key: Optional[str] = None,
        timeout: float = 30.0,
        _memory: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> None:
        """Create a store client.

        Args:
            base_id: Legacy Airtable base id — ignored (kept for callers).
            table_name: Maps to a Firestore collection (default ProcessedContent
                → ``processed``).
            api_key: Legacy Airtable PAT — ignored.
            timeout: Reserved for API compatibility.
            _memory: In-memory backend for unit tests only.
        """
        _ = (base_id, api_key, timeout)
        self.table_name = table_name
        self.collection_name = collection_name_for_table(table_name)
        self._memory = _memory
        self._seen_cache: Dict[str, Dict[str, Any]] = {}
        logger.info(
            "ProcessedStore initialised: collection=%s (table=%s)",
            self.collection_name,
            table_name,
        )

    def _collection(self):
        if self._memory is not None:
            return None
        try:
            return get_firestore_client().collection(self.collection_name)
        except FirestoreConfigError as e:
            raise ProcessedStoreAuthError(str(e)) from e

    def _retry(self, fn, *args, **kwargs):
        last_exc: Optional[Exception] = None
        attempts = 1 + len(_RETRY_BACKOFFS_SEC)
        for attempt in range(attempts):
            try:
                return fn(*args, **kwargs)
            except ProcessedStoreError as e:
                if isinstance(
                    e,
                    (
                        ProcessedStoreAuthError,
                        ProcessedStoreNotFoundError,
                        ProcessedStoreConflictError,
                    ),
                ):
                    raise
                last_exc = e
                logger.warning(
                    "retryable error attempt %d/%d: %s",
                    attempt + 1,
                    attempts,
                    last_exc,
                )
            except Exception as e:
                last_exc = e
                msg = str(e).lower()
                if "permission" in msg or "credentials" in msg or "auth" in msg:
                    raise ProcessedStoreAuthError(str(e)) from e
                if "not found" in msg and "document" in msg:
                    raise ProcessedStoreNotFoundError(str(e)) from e
                logger.warning(
                    "retryable Firestore error attempt %d/%d: %s",
                    attempt + 1,
                    attempts,
                    e,
                )
            if attempt < len(_RETRY_BACKOFFS_SEC):
                time.sleep(_RETRY_BACKOFFS_SEC[attempt])
        assert last_exc is not None
        raise last_exc if isinstance(last_exc, ProcessedStoreError) else ProcessedStoreError(str(last_exc)) from last_exc

    def _get_doc_data(self, doc_id: str) -> Optional[Dict[str, Any]]:
        def _read() -> Optional[Dict[str, Any]]:
            if self._memory is not None:
                return self._memory.get(doc_id)
            doc = self._collection().document(doc_id).get()
            if not getattr(doc, "exists", False):
                return None
            return doc.to_dict() or {}

        return self._retry(_read)

    def _set_doc_data(self, doc_id: str, fields: Dict[str, Any], *, merge: bool) -> None:
        def _write() -> None:
            if self._memory is not None:
                if merge and doc_id in self._memory:
                    self._memory[doc_id].update(fields)
                else:
                    self._memory[doc_id] = dict(fields)
                return
            self._collection().document(doc_id).set(fields, merge=merge)

        self._retry(_write)

    def _list_all_records(
        self, filter_formula: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List records matching ``filter_formula`` (Airtable-shaped output)."""
        source_hash = (
            _extract_quoted(filter_formula, "source_hash") if filter_formula else None
        )
        if source_hash:
            data = self._get_doc_data(source_hash)
            if data and _matches_filter_formula(data, filter_formula):
                return [_record_from_doc(source_hash, data)]
            return []

        records: List[Dict[str, Any]] = []
        if self._memory is not None:
            for doc_id, data in self._memory.items():
                if _matches_filter_formula(data, filter_formula):
                    records.append(_record_from_doc(doc_id, data))
            return records

        for doc in self._retry(lambda: list(self._collection().stream())):
            data = doc.to_dict() or {}
            if _matches_filter_formula(data, filter_formula):
                records.append(_record_from_doc(doc.id, data))
        return records

    def _find_by_hash(self, source_hash: str) -> Optional[Dict[str, Any]]:
        data = self._get_doc_data(source_hash)
        if data is None:
            return None
        return _record_from_doc(source_hash, data)

    def is_processed(self, source_type: str, source_id: str) -> bool:
        source_hash = make_hash(source_type, source_id)
        if source_hash in self._seen_cache:
            return True
        record = self._find_by_hash(source_hash)
        if record is None:
            return False
        self._seen_cache[source_hash] = record
        return True

    def mark_processed(
        self,
        source_type: str,
        source_id: str,
        title: str,
        channels: Optional[List[str]] = None,
        pipeline_run_id: Optional[str] = None,
        output_path: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None,
        first_seen_at: Optional[datetime] = None,
        article_type: Optional[str] = None,
        paywall_preview_kept: Optional[bool] = None,
        paywall_preview_kind: Optional[str] = None,
    ) -> str:
        source_hash = make_hash(source_type, source_id)
        fields: Dict[str, Any] = {
            "source_hash": source_hash,
            "source_type": source_type,
            "source_id": source_id,
            "title": title,
            "processed_at": _utcnow_iso(),
        }
        if channels:
            fields["channels"] = list(channels)
        if pipeline_run_id:
            fields["pipeline_run_id"] = pipeline_run_id
        if output_path:
            fields["output_path"] = output_path
        if metadata is not None:
            fields["metadata"] = json.dumps(metadata, ensure_ascii=False)
        if tags:
            fields["tags"] = list(tags)
        if article_type:
            fields["article_type"] = article_type
        if paywall_preview_kept is True:
            fields["paywall_preview_kept"] = True
            if paywall_preview_kind:
                fields["paywall_preview_kind"] = paywall_preview_kind

        cached = self._seen_cache.get(source_hash)
        if cached is not None:
            return self._patch_record(cached["id"], fields, source_hash=source_hash)

        existing = self._find_by_hash(source_hash)
        if existing is not None:
            self._seen_cache[source_hash] = existing
            return self._patch_record(
                existing["id"], fields, source_hash=source_hash
            )

        if first_seen_at is None:
            fields["first_seen_at"] = _utcnow_iso()
        else:
            fields["first_seen_at"] = _coerce_iso(first_seen_at)

        self._set_doc_data(source_hash, fields, merge=False)
        record = _record_from_doc(source_hash, fields)
        self._seen_cache[source_hash] = record
        logger.info(
            "marked processed source_type=%s source_id=%s -> %s",
            source_type,
            source_id,
            source_hash[:12],
        )
        return source_hash

    def _patch_record(
        self,
        record_id: str,
        fields: Dict[str, Any],
        *,
        source_hash: str,
    ) -> str:
        fields = dict(fields)
        fields.pop("first_seen_at", None)
        self._set_doc_data(record_id, fields, merge=True)

        cached = self._seen_cache.get(source_hash, {"id": record_id, "fields": {}})
        cached_fields = dict(cached.get("fields", {}))
        cached_fields.update(fields)
        cached["fields"] = cached_fields
        cached["id"] = record_id
        self._seen_cache[source_hash] = cached
        logger.info("updated processed record %s (%s)", record_id[:12], source_hash[:12])
        return record_id

    def update_side_effects(
        self,
        source_hash: str,
        discord_message_id: Optional[str] = None,
        github_commit_sha: Optional[str] = None,
    ) -> None:
        fields: Dict[str, Any] = {}
        if discord_message_id:
            fields["discord_message_id"] = discord_message_id
        if github_commit_sha:
            fields["github_commit_sha"] = github_commit_sha
        if not fields:
            raise ProcessedStoreError(
                "update_side_effects called with no fields to set"
            )

        cached = self._seen_cache.get(source_hash)
        if cached is None:
            existing = self._find_by_hash(source_hash)
            if existing is None:
                raise ProcessedStoreNotFoundError(
                    f"no record with source_hash={source_hash}"
                )
            self._seen_cache[source_hash] = existing
            cached = existing
        self._patch_record(cached["id"], fields, source_hash=source_hash)
        logger.info(
            "side-effects updated %s discord=%s github=%s",
            source_hash[:12],
            bool(discord_message_id),
            bool(github_commit_sha),
        )

    def get_recent(
        self, source_type: str, days: int = 7, limit: int = 50
    ) -> List[Dict[str, Any]]:
        formula = (
            f"AND({{source_type}}='{source_type}',"
            f"DATETIME_DIFF(NOW(), {{processed_at}}, 'days') <= {days})"
        )
        records = self._list_all_records(filter_formula=formula)
        records.sort(
            key=lambda r: r.get("fields", {}).get("processed_at", ""),
            reverse=True,
        )
        return records[:limit]

    def stats(self, days: int = 7) -> Dict[str, int]:
        formula = f"DATETIME_DIFF(NOW(), {{processed_at}}, 'days') <= {days}"
        records = self._list_all_records(filter_formula=formula)
        counts: Dict[str, int] = {}
        for r in records:
            st = r.get("fields", {}).get("source_type", "")
            if st:
                counts[st] = counts.get(st, 0) + 1
        return counts

    def clear_cache(self) -> None:
        self._seen_cache.clear()

    # Legacy Airtable test hooks — map to Firestore operations.
    def _request(self, method: str, path: str, **kwargs) -> Dict[str, Any]:
        """Compatibility shim for tests that still patch the old HTTP layer."""
        raise ProcessedStoreError(
            "ProcessedStore no longer uses Airtable HTTP (_request is deprecated)"
        )

    def _table_path(self) -> str:
        return f"/{self.collection_name}"

    def delete_record(self, record_id: str) -> None:
        """Delete a document by id (tests / migration tooling)."""
        if self._memory is not None:
            self._memory.pop(record_id, None)
            return
        self._retry(lambda: self._collection().document(record_id).delete())


__all__ = [
    "ProcessedStore",
    "ProcessedStoreError",
    "ProcessedStoreAuthError",
    "ProcessedStoreNotFoundError",
    "ProcessedStoreConflictError",
    "make_hash",
    "normalize_url",
    "parse_metadata",
]
