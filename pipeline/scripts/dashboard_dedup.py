#!/usr/bin/env python3
"""Dashboard CLI for the Airtable ProcessedContent ledger.

Read-only tool for inspecting the dedup ledger that
``pipeline/lib/processed_store.py`` writes to. Three subcommands:

  stats                         — per-source_type counts over the last 7 days
  recent <source_type>          — newest N records (default 20, --limit N to change)
  find <source_type> <source_id>— look up one record by source_id

Loads ``AIRTABLE_API_KEY`` and ``AIRTABLE_PROCESSED_CONTENT_BASE_ID`` from
``/root/.hermes/.env`` automatically (no need to export). Errors are mapped
to a friendly message + non-zero exit code; never an unhandled traceback.

Exit codes:
  0  — success (record found for ``find``)
  1  — record not found (only for ``find``)
  2  — missing env var (AIRTABLE_API_KEY / BASE_ID)
  3  — Airtable auth / permission error (401 / 403)
  4  — Airtable not-found error (404)
  5  — Airtable conflict / validation error (422 / 409)
  6  — Airtable network / 5xx error
  7  — invalid CLI usage (argparse handles this with exit 2 already)

Examples
--------
    python3 dashboard_dedup.py stats
    python3 dashboard_dedup.py recent youtube
    python3 dashboard_dedup.py recent news --limit 50
    python3 dashboard_dedup.py find youtube AcNbIi4_gbY
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

# ---------------------------------------------------------------------------
# Make the pipeline package importable when this script is run directly.
# ---------------------------------------------------------------------------
THIS_DIR = Path(__file__).resolve().parent
PIPELINE_ROOT = THIS_DIR.parent.parent  # .../ado_hermes_reddit_scrapper
if str(PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(PIPELINE_ROOT))

# Suppress noisy "retryable error attempt N" log lines from the store;
# we already surface errors in the CLI exit code.
import logging  # noqa: E402

logging.getLogger("pipeline.lib.processed_store").setLevel(logging.ERROR)

from pipeline.lib.processed_store import (  # noqa: E402
    DEFAULT_TABLE,
    ProcessedStore,
    ProcessedStoreAuthError,
    ProcessedStoreConflictError,
    ProcessedStoreError,
    ProcessedStoreNotFoundError,
)

ENV_FILE = Path("/root/.hermes/.env")
KNOWN_SOURCE_TYPES = ("youtube", "news", "reddit", "podcast")
TITLE_PREVIEW = 50  # how many chars of title to show in `recent` / `find`


# ---------------------------------------------------------------------------
# Env loading
# ---------------------------------------------------------------------------

def _load_env_file(path: Path) -> Dict[str, str]:
    """Parse a ``KEY=value`` env file. Ignores comments and blank lines.

    Returns the parsed dict. Does NOT mutate ``os.environ`` — callers can
    decide which keys to set. We deliberately don't shell-source the file
    because a) this script may run on a system without ``bash`` and
    b) we want explicit control over which keys we read.
    """
    out: Dict[str, str] = {}
    if not path.exists():
        return out
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        # Strip optional leading ``export ``
        if line.lower().startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        # Drop inline comments outside of quotes
        if value and value[0] in ('"', "'") and value[-1] == value[0]:
            value = value[1:-1]
        else:
            value = value.split(" #", 1)[0].split("\t#", 1)[0].rstrip()
        if key:
            out[key] = value
    return out


def _resolve_creds() -> Tuple[Optional[str], Optional[str]]:
    """Return ``(api_key, base_id)`` from /root/.hermes/.env or current env.

    Does not raise — missing values are returned as ``None`` so the caller
    can emit a single friendly error message.
    """
    file_vars = _load_env_file(ENV_FILE)
    api_key = os.environ.get("AIRTABLE_API_KEY") or file_vars.get("AIRTABLE_API_KEY")
    base_id = (
        os.environ.get("AIRTABLE_PROCESSED_CONTENT_BASE_ID")
        or file_vars.get("AIRTABLE_PROCESSED_CONTENT_BASE_ID")
    )
    return api_key, base_id


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _truncate(text: str, n: int) -> str:
    if len(text) <= n:
        return text
    return text[: max(0, n - 1)] + "…"


def _format_stats(counts: Dict[str, int]) -> str:
    """Render the stats output. Always shows all 4 source_types, even at 0."""
    lines: list[str] = []
    lines.append("ProcessedContent stats (last 7 days, source_type breakdown):")
    lines.append("")
    total = 0
    for st in KNOWN_SOURCE_TYPES:
        n = int(counts.get(st, 0))
        total += n
        lines.append(f"  {st:<14}{n}")
    lines.append("  ----")
    lines.append(f"  TOTAL         {total}")
    return "\n".join(lines)


def _format_recent(records: list[Dict[str, Any]], source_type: str) -> str:
    if not records:
        return (
            f"No {source_type} records found in the lookback window.\n"
            "(Table is empty or no records match ProcessedStore.get_recent.)"
        )
    lines: list[str] = []
    lines.append(f"Recent {source_type} records (newest first):")
    lines.append("")
    for r in records:
        rec_id = r.get("id", "<no-id>")
        fields = r.get("fields", {}) or {}
        processed_at = fields.get("processed_at", "<no-timestamp>")
        title = _truncate(str(fields.get("title", "")), TITLE_PREVIEW)
        lines.append(f"  {rec_id}  {processed_at}  {title}")
    return "\n".join(lines)


def _format_found(record: Dict[str, Any]) -> str:
    fields = record.get("fields", {}) or {}
    rec_id = record.get("id", "<no-id>")
    lines: list[str] = ["Record found:"]
    lines.append(f"  id: {rec_id}")
    lines.append(f"  source_hash: {fields.get('source_hash', '')}")
    lines.append(f"  source_type: {fields.get('source_type', '')}")
    lines.append(f"  source_id: {fields.get('source_id', '')}")
    lines.append(f"  title: {_truncate(str(fields.get('title', '')), 50)}")
    channels = fields.get("channels", [])
    if isinstance(channels, list):
        channels_str = ", ".join(str(c) for c in channels)
    else:
        channels_str = str(channels)
    lines.append(f"  channels: {channels_str}")
    lines.append(f"  processed_at: {fields.get('processed_at', '')}")
    lines.append(f"  first_seen_at: {fields.get('first_seen_at', '')}")
    lines.append(f"  discord_message_id: {fields.get('discord_message_id', '')}")
    lines.append(f"  github_commit_sha: {fields.get('github_commit_sha', '')}")
    lines.append(f"  pipeline_run_id: {fields.get('pipeline_run_id', '')}")
    lines.append(f"  output_path: {fields.get('output_path', '')}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Subcommand handlers
# ---------------------------------------------------------------------------

def _make_store() -> Tuple[Optional["ProcessedStore"], Optional[str]]:
    """Build a ProcessedStore. Returns (store, error_message).

    On success ``store`` is a real ``ProcessedStore`` and ``error_message``
    is ``None``. On failure ``store`` is ``None`` and ``error_message`` is
    a human-readable explanation. We do not raise from here so the
    subcommand handlers can present a single uniform error to the user.
    """
    api_key, base_id = _resolve_creds()
    if not api_key:
        return None, (
            "AIRTABLE_API_KEY not found in /root/.hermes/.env. "
            "Add a PAT (starts with pat...) and re-run."
        )
    if not base_id:
        return None, (
            "AIRTABLE_PROCESSED_CONTENT_BASE_ID not found in /root/.hermes/.env. "
            "Expected something like 'app...'. Add it and re-run."
        )
    try:
        return ProcessedStore(base_id, DEFAULT_TABLE, api_key=api_key), None
    except ProcessedStoreAuthError as e:
        return None, f"auth error while initialising ProcessedStore: {e}"
    except ProcessedStoreError as e:
        return None, f"failed to initialise ProcessedStore: {e}"


def cmd_stats(_args: argparse.Namespace) -> int:
    store, err = _make_store()
    if err:
        print(f"error: {err}", file=sys.stderr)
        return 2
    assert store is not None  # narrow Optional[ProcessedStore] for type checkers
    try:
        counts = store.stats(days=7)
    except ProcessedStoreAuthError as e:
        print(f"error: Airtable auth/permission denied (401/403): {e}", file=sys.stderr)
        return 3
    except ProcessedStoreNotFoundError as e:
        print(f"error: base/table not found (404): {e}", file=sys.stderr)
        return 4
    except ProcessedStoreConflictError as e:
        print(f"error: Airtable rejected the request (422/409): {e}", file=sys.stderr)
        return 5
    except ProcessedStoreError as e:
        print(f"error: Airtable request failed: {e}", file=sys.stderr)
        return 6
    print(_format_stats(counts))
    return 0


def cmd_recent(args: argparse.Namespace) -> int:
    store, err = _make_store()
    if err:
        print(f"error: {err}", file=sys.stderr)
        return 2
    assert store is not None  # narrow Optional[ProcessedStore] for type checkers
    source_type = args.source_type
    if source_type not in KNOWN_SOURCE_TYPES:
        print(
            f"error: unknown source_type '{source_type}'. "
            f"Valid options: {', '.join(KNOWN_SOURCE_TYPES)}",
            file=sys.stderr,
        )
        return 7
    try:
        records = store.get_recent(source_type=source_type, days=args.days, limit=args.limit)
    except ProcessedStoreAuthError as e:
        print(f"error: Airtable auth/permission denied (401/403): {e}", file=sys.stderr)
        return 3
    except ProcessedStoreNotFoundError as e:
        print(f"error: base/table not found (404): {e}", file=sys.stderr)
        return 4
    except ProcessedStoreConflictError as e:
        print(f"error: Airtable rejected the request (422/409): {e}", file=sys.stderr)
        return 5
    except ProcessedStoreError as e:
        print(f"error: Airtable request failed: {e}", file=sys.stderr)
        return 6
    print(_format_recent(records, source_type))
    return 0


def _find_record(store: ProcessedStore, source_type: str, source_id: str) -> Optional[Dict[str, Any]]:
    """Look up a single record by (source_type, source_id).

    ``ProcessedStore.is_processed`` only returns ``bool``, so we go via
    ``_list_all_records`` with a filter formula. The store's
    ``_list_all_records`` already handles pagination + URL encoding, so
    we just call it.
    """
    # Sanitise: source_id must not contain single quotes (it would break
    # the filter formula). Replace with the unicode right single quote
    # as a safe fallback so we never inject into Airtable's formula parser.
    safe_id = source_id.replace("'", "\u2019")
    safe_type = source_type.replace("'", "\u2019")
    formula = f"AND({{source_type}}='{safe_type}', {{source_id}}='{safe_id}')"
    records = store._list_all_records(filter_formula=formula)
    if not records:
        return None
    return records[0]


def cmd_find(args: argparse.Namespace) -> int:
    store, err = _make_store()
    if err:
        print(f"error: {err}", file=sys.stderr)
        return 2
    assert store is not None  # narrow Optional[ProcessedStore] for type checkers
    source_type = args.source_type
    source_id = args.source_id
    if source_type not in KNOWN_SOURCE_TYPES:
        print(
            f"error: unknown source_type '{source_type}'. "
            f"Valid options: {', '.join(KNOWN_SOURCE_TYPES)}",
            file=sys.stderr,
        )
        return 7
    if not source_id or not source_id.strip():
        print("error: source_id is empty", file=sys.stderr)
        return 7
    try:
        record = _find_record(store, source_type, source_id.strip())
    except ProcessedStoreAuthError as e:
        print(f"error: Airtable auth/permission denied (401/403): {e}", file=sys.stderr)
        return 3
    except ProcessedStoreNotFoundError as e:
        print(f"error: base/table not found (404): {e}", file=sys.stderr)
        return 4
    except ProcessedStoreConflictError as e:
        print(f"error: Airtable rejected the request (422/409): {e}", file=sys.stderr)
        return 5
    except ProcessedStoreError as e:
        print(f"error: Airtable request failed: {e}", file=sys.stderr)
        return 6
    if record is None:
        print("Record not found.")
        return 1
    print(_format_found(record))
    return 0


# ---------------------------------------------------------------------------
# Argparse
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dashboard_dedup.py",
        description=(
            "Dashboard CLI for the Airtable ProcessedContent dedup ledger. "
            "Read-only — does not write to Airtable."
        ),
        epilog=(
            "Credentials are read from /root/.hermes/.env "
            "(AIRTABLE_API_KEY + AIRTABLE_PROCESSED_CONTENT_BASE_ID)."
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True, metavar="<command>")

    # -- stats -----------------------------------------------------------
    p_stats = sub.add_parser(
        "stats",
        help="Per-source_type counts over the last 7 days.",
        description="Show record counts per source_type for the last 7 days.",
    )
    p_stats.set_defaults(func=cmd_stats)

    # -- recent <source_type> -------------------------------------------
    p_recent = sub.add_parser(
        "recent",
        help="List recent records for a single source_type.",
        description="Show newest records (with title) for the given source_type.",
    )
    p_recent.add_argument(
        "source_type",
        choices=KNOWN_SOURCE_TYPES,
        help="Source type to query.",
    )
    p_recent.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Max records to show (default: 20).",
    )
    p_recent.add_argument(
        "--days",
        type=int,
        default=7,
        help="Lookback window in days (default: 7).",
    )
    p_recent.set_defaults(func=cmd_recent)

    # -- find <source_type> <source_id> ----------------------------------
    p_find = sub.add_parser(
        "find",
        help="Look up a single record by (source_type, source_id).",
        description="Find one record by its source_type and source_id.",
    )
    p_find.add_argument(
        "source_type",
        choices=KNOWN_SOURCE_TYPES,
        help="Source type to query.",
    )
    p_find.add_argument(
        "source_id",
        help="Platform-native ID (YouTube video ID, news URL slug, etc.).",
    )
    p_find.set_defaults(func=cmd_find)

    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args) or 0)


if __name__ == "__main__":
    sys.exit(main())
