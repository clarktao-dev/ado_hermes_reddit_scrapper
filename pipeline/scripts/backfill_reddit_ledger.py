"""Backfill the Firestore processed ledger for Reddit digests that
already exist in the vault but never got mark_processed.

Background
----------
``2686cfd36e23 reddit_daily_cest_0230`` ran successfully every day from
2026-08-30 through 2026-09-02, but the ``mark_processed`` step failed
silently because the cron's prompt was missing
``set -a && source /root/.hermes/.env && set +a``. As a result, ~75
reddit post ids never made it into the Firestore ledger, and the next
cron run would re-score the same posts (wasted LLM tokens).

This script scans the existing vault digest files and re-runs
``mark_processed`` for each one. It is idempotent (the Firestore doc id
is derived from ``make_hash(source_type, source_id)``, so re-runs simply
patch the same record).

Usage
-----
    # Default: scan the 4 days known to be affected, dry-run.
    python -m pipeline.scripts.backfill_reddit_ledger

    # Actually commit the writes.
    python -m pipeline.scripts.backfill_reddit_ledger --commit

    # Different date range / different vault location.
    python -m pipeline.scripts.backfill_reddit_ledger \\
        --date-from 2026-08-30 --date-to 2026-09-02 \\
        --vault-root /path/to/vault/Reddit
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

THIS_DIR = Path(__file__).resolve().parent
PIPELINE_ROOT = THIS_DIR.parent.parent
if str(PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(PIPELINE_ROOT))

from pipeline.lib.processed_store import (  # noqa: E402
    DEFAULT_TABLE,
    ProcessedStore,
    ProcessedStoreError,
)

logger = logging.getLogger(__name__)


# Filename pattern: <date>_<sub_short>_summary[reddit-]<post_id>.md
# Also accepts the older "_summary-<post_id>.md" form (no "reddit-" tag).
# Reddit post ids are alphanumeric, 5-10 chars, base36-ish.
_FILENAME_RE = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})_.+?_summary[_-](?:reddit-)?(?P<post_id>[a-z0-9]{5,10})\.md$",
    re.IGNORECASE,
)

# URL pattern: /r/<sub>/comments/<post_id>/...
_URL_RE = re.compile(
    r"/comments/(?:t3_)?(?P<post_id>[a-z0-9]{5,10})(?:/|$)",
    re.IGNORECASE,
)


def parse_reddit_id_from_filename(filename: str) -> Optional[str]:
    """Extract the Reddit post id from a vault digest filename.

    Returns ``None`` for filenames that don't match (e.g. ``_index.md``,
    unrelated notes files). The caller is responsible for filtering.
    """
    m = _FILENAME_RE.match(filename)
    return m.group("post_id") if m else None


def parse_reddit_id_from_url(url: str) -> Optional[str]:
    """Extract the Reddit post id from a permalink.

    Handles both modern ``/comments/<id>/...`` and legacy
    ``/comments/t3_<id>/...`` forms.
    """
    m = _URL_RE.search(url)
    return m.group("post_id") if m else None


@dataclass(frozen=True)
class VaultEntry:
    post_id: str
    date: str
    path: Path
    subreddit: str  # e.g. "r/Finanzen" — derived from filename or frontmatter
    title: str
    url: str

    def to_mark_kwargs(self) -> dict:
        """Return kwargs for ``ProcessedStore.mark_processed``."""
        first_seen_at: Optional[datetime] = None
        try:
            first_seen_at = datetime.fromisoformat(self.date).replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            pass
        return {
            "source_type": "reddit",
            "source_id": self.post_id,
            "title": self.title,
            "channels": [f"reddit.{self.subreddit.replace('/', '_')}"],
            "output_path": str(self.path),
            "metadata": {
                "source": "reddit",
                "subreddit": self.subreddit,
                "url": self.url,
                "pub_date": self.date,
                "digest_date": self.date,
                "backfilled": True,
                "backfill_date": datetime.now(timezone.utc).date().isoformat(),
            },
            "tags": ["short", "backfilled"],
            "article_type": "short-summary",
            "first_seen_at": first_seen_at,
        }


def _read_md_meta(path: Path) -> dict:
    """Read a vault .md file and extract title + url + subreddit.

    Best-effort: each line is a markdown list item like ``- **Key**: value``.
    We don't need to be perfect — the fields are optional.
    """
    out: dict = {"title": "", "url": "", "subreddit": ""}
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return out
    for line in text.splitlines():
        m = re.match(r"^#\s+(.+)$", line)
        if m and not out["title"]:
            out["title"] = m.group(1).strip()
        m = re.match(r"^-\s+\*\*URL\*\*:\s*(\S+)", line)
        if m and not out["url"]:
            out["url"] = m.group(1).strip()
        m = re.match(r"^-\s+\*\*Subreddit\*\*:\s*(\S+)", line)
        if m and not out["subreddit"]:
            out["subreddit"] = m.group(1).strip()
    return out


def extract_post_ids_from_vault(
    vault_root: Path,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> list[VaultEntry]:
    """Walk ``<vault_root>/<date>/*.md`` and return one entry per post.

    ``date_from`` / ``date_to`` are inclusive ISO date strings (YYYY-MM-DD).
    """
    if not vault_root.is_dir():
        return []

    out: list[VaultEntry] = []
    for date_dir in sorted(vault_root.iterdir()):
        if not date_dir.is_dir():
            continue
        date = date_dir.name
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
            continue
        if date_from and date < date_from:
            continue
        if date_to and date > date_to:
            continue
        for md in sorted(date_dir.glob("*.md")):
            if md.name.startswith("_"):
                continue
            post_id = parse_reddit_id_from_filename(md.name)
            if not post_id:
                logger.warning("skip (no post_id in filename): %s", md)
                continue
            meta = _read_md_meta(md)
            # Fall back to URL parse if frontmatter didn't yield one
            url = meta["url"] or ""
            if not post_id and url:
                post_id = parse_reddit_id_from_url(url)
            # Derive subreddit from filename if frontmatter missing:
            # "2026-09-02_r-finanzen_summary_reddit-1w4kv96.md" → "r/finanzen"
            subreddit = meta["subreddit"]
            if not subreddit:
                m = re.match(r"^\d{4}-\d{2}-\d{2}_(.+?)_summary", md.name)
                if m:
                    short = m.group(1)
                    subreddit = f"r/{short.replace('_', '/')}"
            if not subreddit:
                subreddit = "r/unknown"
            out.append(
                VaultEntry(
                    post_id=post_id,
                    date=date,
                    path=md,
                    subreddit=subreddit,
                    title=meta["title"] or md.stem,
                    url=url,
                )
            )
    return out


def run(
    vault_root: Path,
    date_from: Optional[str],
    date_to: Optional[str],
    commit: bool,
) -> int:
    entries = extract_post_ids_from_vault(vault_root, date_from, date_to)
    if not entries:
        logger.warning("no vault entries found under %s (range %s..%s)",
                       vault_root, date_from, date_to)
        return 0
    logger.info("found %d vault entries to backfill (commit=%s)",
                len(entries), commit)

    store: Optional[ProcessedStore] = None
    if commit:
        # First arg is legacy Airtable base_id — ignored, kept for API compat.
        store = ProcessedStore("", DEFAULT_TABLE)

    ok = 0
    errs: list[tuple[str, str]] = []
    for entry in entries:
        kwargs = entry.to_mark_kwargs()
        if not commit:
            logger.info(
                "[dry-run] would mark_processed: post_id=%s date=%s path=%s",
                entry.post_id, entry.date, entry.path,
            )
            ok += 1
            continue
        assert store is not None
        try:
            store.mark_processed(**kwargs)
            ok += 1
        except ProcessedStoreError as e:
            logger.error("mark_processed failed for %s: %s", entry.post_id, e)
            errs.append((entry.post_id, str(e)))
    logger.info("done: %d ok, %d errors (commit=%s)", ok, len(errs), commit)
    for pid, err in errs:
        logger.error("  %s: %s", pid, err)
    return len(errs)


def main(argv: Optional[list[str]] = None) -> int:
    default_vault = PIPELINE_ROOT / "immobilien-kb" / "vault" / "Reddit"
    p = argparse.ArgumentParser(
        description="Backfill Firestore processed ledger for Reddit vault digests.",
    )
    p.add_argument(
        "--vault-root", type=Path, default=default_vault,
        help=f"Path to Reddit vault dir (default: {default_vault})",
    )
    p.add_argument(
        "--date-from", type=str, default="2026-08-30",
        help="Inclusive YYYY-MM-DD lower bound (default: 2026-08-30 — first known-affected day)",
    )
    p.add_argument(
        "--date-to", type=str, default="2026-09-02",
        help="Inclusive YYYY-MM-DD upper bound (default: 2026-09-02 — last known-affected day)",
    )
    p.add_argument(
        "--commit", action="store_true",
        help="Actually write to Firestore (default: dry-run)",
    )
    p.add_argument(
        "--json", action="store_true",
        help="Emit a JSON summary at the end (for log scraping).",
    )
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    err_count = run(
        vault_root=args.vault_root,
        date_from=args.date_from,
        date_to=args.date_to,
        commit=args.commit,
    )
    if args.json:
        print(json.dumps({"err_count": err_count, "commit": args.commit}))
    return 1 if err_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
