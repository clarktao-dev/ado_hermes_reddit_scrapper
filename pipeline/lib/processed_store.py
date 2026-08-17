"""ProcessedContent ledger — Airtable-backed deduplication for pipeline outputs.

Provides a single source of truth for "have we already processed this item?"
across the youtube / news / reddit / podcast pipelines.

Design notes
------------
- **Idempotency** is the central contract: ``mark_processed`` with the same
  ``source_hash`` always updates the same record instead of creating a
  duplicate. The lookup happens client-side via ``_find_by_hash`` (filter
  formula on ``source_hash``); on hit we PATCH, on miss we POST a plain
  create. We intentionally do **not** use Airtable's ``performUpsert``
  body parameter because the PAT rejects it (HTTP 422).
- **Type safety**: all public methods carry type hints and short docstrings
  so linters and humans can use them interchangeably.
- **Resilience**: every HTTP call goes through ``_request_with_retry`` which
  retries on transient failures (5xx, network) with exponential backoff
  (1s, 2s, 4s) and surfaces the Airtable error body on non-retriable 4xx.
- **Process-local cache**: a single ``_seen_cache`` dict eliminates
  round-trips to Airtable for repeats inside the same run.

Environment
-----------
- ``AIRTABLE_API_KEY`` — Personal Access Token (starts with ``pat...``).
- The base + table must already exist; see
  ``pipeline/scripts/setup_airtable_processed_content.py``.
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib import error, parse, request

logger = logging.getLogger(__name__)

API_BASE = "https://api.airtable.com/v0"
DEFAULT_TABLE = "ProcessedContent"

# Retry policy: transient errors get 1s, 2s, 4s; then surface.
_RETRY_BACKOFFS_SEC = (1.0, 2.0, 4.0)
_PAGE_SIZE = 100  # Airtable max


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class ProcessedStoreError(RuntimeError):
    """Base error for ``ProcessedStore`` failures."""


class ProcessedStoreAuthError(ProcessedStoreError):
    """Raised on 401 / 403 — token missing, invalid, or base not granted."""


class ProcessedStoreNotFoundError(ProcessedStoreError):
    """Raised when the configured base / table does not exist."""


class ProcessedStoreConflictError(ProcessedStoreError):
    """Raised on 422 / 409 — payload validation issues."""


# ---------------------------------------------------------------------------
# Pure helpers (no I/O)
# ---------------------------------------------------------------------------

def make_hash(source_type: str, source_id: str) -> str:
    """Stable SHA256 hex digest of ``'<source_type>:<source_id>'``.

    This is the dedup key. The format is fixed by PM decision — never
    reorder the components or add a separator beyond the colon, or you
    will split the hash space from any pre-existing records.
    """
    payload = f"{source_type.strip().lower()}:{source_id.strip()}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def parse_metadata(raw: Any) -> dict:
    """Defensive parser for Airtable's metadata field.

    The metadata field is stored as a JSON string in Airtable (see
    ``fields["metadata"] = json.dumps(...)`` in ``mark_processed``), but
    historical rows may already be a dict if written by an older pipeline.
    Returns an empty dict on any parse failure so callers can use
    ``meta.get(...)`` unconditionally.
    """
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


# Tracking query params to strip. utm_* catches utm_source, utm_medium,
# utm_campaign, utm_term, utm_content, utm_id, gclid (with leading gbraid
# and wbraid variants in the same family).
_TRACKING_PREFIXES = ("utm_",)
_TRACKING_EXACT = {"fbclid", "gclid", "gbraid", "wbraid", "mc_cid", "mc_eid", "yclid"}


def normalize_url(url: str) -> str:
    """Normalize a URL for dedup: strip tracking params, lowercase host, sort query.

    Behaviour:
      - Lowercases scheme + host (``http://Example.COM`` -> ``http://example.com``)
      - Preserves path case (URL paths are case-sensitive in general)
      - Drops query params whose name starts with ``utm_`` or is in
        ``_TRACKING_EXACT``
      - Sorts remaining query params alphabetically for stable hashing
      - Drops empty fragment
    """
    parsed = parse.urlparse(url.strip())
    scheme = (parsed.scheme or "http").lower()
    host = (parsed.hostname or "").lower()
    # Re-assemble netloc (host + port) so we don't lose the port
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
    """Return an Airtable-friendly UTC ISO8601 string with millisecond precision."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _coerce_iso(value: Any) -> str:
    """Coerce a datetime or ISO string into the canonical Airtable form."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    if isinstance(value, str):
        # Trust the caller's formatting; Airtable accepts ISO8601 broadly.
        return value
    raise ProcessedStoreError(
        f"first_seen_at must be datetime or ISO str, got {type(value).__name__}"
    )


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------

class ProcessedStore:
    """Airtable-backed dedup ledger for processed content items."""

    def __init__(
        self,
        base_id: str,
        table_name: str = DEFAULT_TABLE,
        *,
        api_key: Optional[str] = None,
        timeout: float = 30.0,
    ) -> None:
        """Create a store client.

        Args:
            base_id: Airtable base id (``app...``). If you only have a
                base name, resolve it with ``list_bases`` first.
            table_name: Defaults to ``ProcessedContent``.
            api_key: Personal Access Token. If omitted, reads from
                ``AIRTABLE_API_KEY`` env var.
            timeout: Per-request timeout in seconds.
        """
        import os

        self.base_id = base_id
        self.table_name = table_name
        self.timeout = timeout
        self._api_key = api_key or os.environ.get("AIRTABLE_API_KEY", "")
        if not self._api_key:
            raise ProcessedStoreAuthError(
                "AIRTABLE_API_KEY not set. Add a PAT to ~/.hermes/.env "
                "or pass api_key=... explicitly."
            )
        # cache: source_hash -> (record_id, fields_snapshot)
        self._seen_cache: Dict[str, Dict[str, Any]] = {}
        logger.info(
            "ProcessedStore initialised: base=%s table=%s", base_id, table_name
        )

    # ------------------------------------------------------------------ I/O

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, str]] = None,
        body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Issue one HTTP request with exponential-backoff retries.

        Splits into two layers so tests can stub the inner ``_http_call``
        while the real retry/backoff logic runs in this outer method.
        """
        url = f"{API_BASE}{path}"
        if params:
            url = f"{url}?{parse.urlencode(params)}"
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Accept": "application/json",
        }
        data: Optional[bytes] = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"

        last_exc: Optional[Exception] = None
        attempts = 1 + len(_RETRY_BACKOFFS_SEC)
        for attempt in range(attempts):
            try:
                return self._http_call(method, url, headers, data)
            except ProcessedStoreError as e:
                # Auth/notfound/conflict are non-retriable; re-raise immediately.
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
            if attempt < len(_RETRY_BACKOFFS_SEC):
                time.sleep(_RETRY_BACKOFFS_SEC[attempt])
        assert last_exc is not None
        raise last_exc

    def _http_call(
        self,
        method: str,
        url: str,
        headers: Dict[str, str],
        data: Optional[bytes],
    ) -> Dict[str, Any]:
        """Single HTTP attempt. Extracted so tests can stub it cheaply."""
        req = request.Request(url, data=data, method=method, headers=headers)
        try:
            with request.urlopen(req, timeout=self.timeout) as resp:
                payload = resp.read().decode("utf-8")
                if not payload:
                    return {}
                return json.loads(payload)
        except error.HTTPError as e:
            err_body = e.read().decode("utf-8", errors="replace")
            status = e.code
            if status == 401 or status == 403:
                raise ProcessedStoreAuthError(
                    f"{method} {url} -> {status}: {err_body}"
                ) from e
            if status == 404:
                raise ProcessedStoreNotFoundError(
                    f"{method} {url} -> {status}: {err_body}"
                ) from e
            if status == 422 or status == 409:
                raise ProcessedStoreConflictError(
                    f"{method} {url} -> {status}: {err_body}"
                ) from e
            if status >= 500 or status == 429:
                raise ProcessedStoreError(
                    f"{method} {url} -> {status}: {err_body}"
                ) from e
            raise ProcessedStoreError(
                f"{method} {url} -> {status}: {err_body}"
            ) from e
        except (error.URLError, TimeoutError, ConnectionError) as e:
            raise ProcessedStoreError(
                f"{method} {url} -> network: {e}"
            ) from e

    def _table_path(self) -> str:
        return f"/{self.base_id}/{parse.quote(self.table_name, safe='')}"

    def _list_all_records(
        self, filter_formula: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List every record matching ``filter_formula`` (None = all)."""
        records: List[Dict[str, Any]] = []
        offset: Optional[str] = None
        while True:
            params: Dict[str, str] = {"pageSize": str(_PAGE_SIZE)}
            if filter_formula:
                params["filterByFormula"] = filter_formula
            if offset:
                params["offset"] = offset
            page = self._request("GET", self._table_path(), params=params)
            records.extend(page.get("records", []))
            offset = page.get("offset")
            if not offset:
                break
        return records

    # ------------------------------------------------------------------ Lookups

    def _find_by_hash(self, source_hash: str) -> Optional[Dict[str, Any]]:
        """Find a record by source_hash. Returns the raw Airtable record or None."""
        # URL-encoded filter formula
        formula = f"{{source_hash}}='{source_hash}'"
        records = self._list_all_records(filter_formula=formula)
        if not records:
            return None
        if len(records) > 1:
            logger.warning(
                "multiple records found for source_hash=%s (%d); using first",
                source_hash,
                len(records),
            )
        return records[0]

    def is_processed(self, source_type: str, source_id: str) -> bool:
        """Check whether the given (source_type, source_id) has been processed.

        Uses the in-process cache first; on miss, queries Airtable.
        """
        source_hash = make_hash(source_type, source_id)
        if source_hash in self._seen_cache:
            return True
        record = self._find_by_hash(source_hash)
        if record is None:
            return False
        self._seen_cache[source_hash] = record
        return True

    # ------------------------------------------------------------------ Writes

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
        """Idempotently mark an item as processed. Returns the record ID.

        Behaviour:
          - If a record with the same ``source_hash`` exists, PATCH it
            (merge supplied fields, preserve side-effect columns).
          - If not, create it.
          - In both cases, ``processed_at`` is bumped to "now" (UTC).

        ``first_seen_at`` defaults to "now" on first insert and is
        preserved on subsequent updates.

        ``article_type`` (Task 7, 2026-08-09): optional ``singleSelect`` on
        the ProcessedContent ledger — one of ``short-summary``,
        ``long-form``, ``pending-long-form``, ``skipped-long-form``. The
        default daily pipeline uses ``short-summary`` to save tokens;
        ``long-form`` is on-demand via
        ``pipeline/scripts/recommend_long_form.py confirm``. When None,
        the field is left untouched on PATCH or omitted on create
        (backward-compatible — callers from Task 1-6 still work).

        ``paywall_preview_kept`` / ``paywall_preview_kind`` (Plan 1,
        2026-08-17): set by ``news_daily`` when an item was kept as a
        paywall-preview body. ``paywall_preview_kept`` is a checkbox
        (``True``/``False``/omitted); ``paywall_preview_kind`` is a
        singleSelect (``"paywall-preview"`` or
        ``"short-paywall-preview"``). When ``paywall_preview_kept`` is
        ``False`` or ``None`` we deliberately do NOT write
        ``paywall_preview_kind`` so non-preview records keep the field
        empty.
        """
        source_hash = make_hash(source_type, source_id)
        # Build the field payload (always include processed_at = now)
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
        # Plan 1 (2026-08-17): paywall preview flags. Only write the
        # checkbox when True; on PATCH, False would clobber a previous
        # True (edge case — re-processing a paywall item after it was
        # already classified). For now we only stamp the record on the
        # first insert + a True re-classify.
        if paywall_preview_kept is True:
            fields["paywall_preview_kept"] = True
            if paywall_preview_kind:
                fields["paywall_preview_kind"] = paywall_preview_kind

        # Path 1: cache hit — update the existing record.
        cached = self._seen_cache.get(source_hash)
        if cached is not None:
            return self._patch_record(cached["id"], fields, source_hash=source_hash)

        # Path 2: search Airtable for the same hash.
        existing = self._find_by_hash(source_hash)
        if existing is not None:
            self._seen_cache[source_hash] = existing
            return self._patch_record(
                existing["id"], fields, source_hash=source_hash
            )

        # Path 3: brand new record. Set first_seen_at = now if not provided.
        if first_seen_at is None:
            fields["first_seen_at"] = _utcnow_iso()
        else:
            fields["first_seen_at"] = _coerce_iso(first_seen_at)

        # Plain POST create — no performUpsert. The PAT rejects the
        # performUpsert body shape with HTTP 422, and Path 2 already
        # handled the "existing record" case above, so a bare create is
        # correct here. A concurrent caller racing past Path 2 would
        # land on Path 3 here too and produce a duplicate row — we
        # accept that edge case because (a) the Airtable API does not
        # expose a server-side unique constraint we can rely on, and
        # (b) downstream reads tolerate occasional duplicates.
        body = {
            "typecast": True,
            "fields": fields,
        }
        resp = self._request("POST", self._table_path(), body=body)
        record = resp.get("records", [{}])[0] if "records" in resp else resp
        if not record or not record.get("id"):
            raise ProcessedStoreError(
                f"create returned no record for {source_hash}: {resp}"
            )
        record_id = record.get("id", "")
        # Refresh cache
        self._seen_cache[source_hash] = record
        logger.info(
            "marked processed source_type=%s source_id=%s -> %s",
            source_type,
            source_id,
            record_id,
        )
        return record_id

    def _patch_record(
        self,
        record_id: str,
        fields: Dict[str, Any],
        *,
        source_hash: str,
    ) -> str:
        """PATCH an existing record. Returns the record ID."""
        # first_seen_at must NOT be overwritten on updates — strip it.
        fields.pop("first_seen_at", None)
        body = {"typecast": True, "fields": fields}
        path = f"{self._table_path()}/{record_id}"
        resp = self._request("PATCH", path, body=body)
        # Update cache snapshot
        cached = self._seen_cache.get(source_hash, {"id": record_id, "fields": {}})
        cached_fields = dict(cached.get("fields", {}))
        cached_fields.update(fields)
        cached["fields"] = cached_fields
        self._seen_cache[source_hash] = cached
        logger.info("updated processed record %s (%s)", record_id, source_hash[:12])
        return resp.get("id", record_id)

    def update_side_effects(
        self,
        source_hash: str,
        discord_message_id: Optional[str] = None,
        github_commit_sha: Optional[str] = None,
    ) -> None:
        """Update side-effect columns after a Discord/GitHub push.

        At least one of ``discord_message_id`` / ``github_commit_sha``
        must be provided. The target record is located by ``source_hash``
        (which is itself a SHA256 hex string).
        """
        fields: Dict[str, Any] = {}
        if discord_message_id:
            fields["discord_message_id"] = discord_message_id
        if github_commit_sha:
            fields["github_commit_sha"] = github_commit_sha
        if not fields:
            raise ProcessedStoreError(
                "update_side_effects called with no fields to set"
            )
        # Use cache first
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

    # ------------------------------------------------------------------ Reads

    def get_recent(
        self, source_type: str, days: int = 7, limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Return recent records for ``source_type``, newest first.

        Args:
            source_type: One of the schema's enum values.
            days: Lookback window in days (default 7).
            limit: Max records to return (default 50).
        """
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
        """Return counts per source_type for the last ``days`` days.

        Output is a dict like ``{'youtube': 12, 'news': 4, 'reddit': 7}``.
        Source types with zero records in the window are omitted.
        """
        formula = f"DATETIME_DIFF(NOW(), {{processed_at}}, 'days') <= {days}"
        records = self._list_all_records(filter_formula=formula)
        counts: Dict[str, int] = {}
        for r in records:
            st = r.get("fields", {}).get("source_type", "")
            if st:
                counts[st] = counts.get(st, 0) + 1
        return counts

    # ------------------------------------------------------------------ Cache

    def clear_cache(self) -> None:
        """Drop the in-process cache. Useful between pipeline runs."""
        self._seen_cache.clear()


__all__ = [
    "ProcessedStore",
    "ProcessedStoreError",
    "ProcessedStoreAuthError",
    "ProcessedStoreNotFoundError",
    "ProcessedStoreConflictError",
    "make_hash",
    "normalize_url",
]
