"""Unit tests for pipeline.scripts.backfill_reddit_ledger.

Background
----------
Between 2026-08-30 and 2026-09-02 (4 days, ~75 posts), the reddit_daily
cron's mark_processed step failed silently because the cron prompt was
missing ``source /root/.hermes/.env``. Vault output was intact — only
the Firestore ledger wasn't updated. This backfill scans the existing
vault digests and re-marks them in the ledger so next run's dedup works.

Test strategy
-------------
Pure function tests only — no Firestore calls. We test the
``extract_post_ids_from_vault`` parser in isolation.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

THIS_DIR = Path(__file__).resolve().parent
PIPELINE_ROOT = THIS_DIR.parent.parent
sys.path.insert(0, str(PIPELINE_ROOT))

from pipeline.scripts.backfill_reddit_ledger import (  # noqa: E402
    extract_post_ids_from_vault,
    parse_reddit_id_from_filename,
    parse_reddit_id_from_url,
    VaultEntry,
)


# ---------------------------------------------------------------------------
# filename / url parsers
# ---------------------------------------------------------------------------

class TestParseRedditIdFromFilename:
    def test_standard_filename(self) -> None:
        assert parse_reddit_id_from_filename(
            "2026-09-02_r-finanzen_summary_reddit-1w4kv96.md"
        ) == "1w4kv96"

    def test_no_reddit_prefix(self) -> None:
        # Backwards-compatible: if filename doesn't carry the "reddit-" tag,
        # we should still be able to find an alphanumeric post id at the end.
        assert parse_reddit_id_from_filename(
            "2026-09-02_r-finanzen_summary-1w4kv96.md"
        ) == "1w4kv96"

    def test_returns_none_for_index(self) -> None:
        # _index.md is not a post — must be filtered out by the caller,
        # but the parser itself returns None for safety.
        assert parse_reddit_id_from_filename("_index.md") is None

    def test_returns_none_for_garbage(self) -> None:
        assert parse_reddit_id_from_filename("notes.md") is None
        assert parse_reddit_id_from_filename("") is None


class TestParseRedditIdFromUrl:
    def test_standard_url(self) -> None:
        assert parse_reddit_id_from_url(
            "https://www.reddit.com/r/Finanzen/comments/1w4kv96/foo/"
        ) == "1w4kv96"

    def test_no_trailing_slash(self) -> None:
        assert parse_reddit_id_from_url(
            "https://www.reddit.com/r/Finanzen/comments/1w4kv96"
        ) == "1w4kv96"

    def test_old_style_with_t3(self) -> None:
        # Old Reddit links sometimes use /comments/t3_xxx/ — the parser
        # should still extract the post id (without the t3_ prefix).
        assert parse_reddit_id_from_url(
            "https://old.reddit.com/r/x/comments/t3_1abc23/foo"
        ) == "1abc23"

    def test_returns_none_for_garbage(self) -> None:
        assert parse_reddit_id_from_url("not a url") is None
        assert parse_reddit_id_from_url("") is None


# ---------------------------------------------------------------------------
# extract_post_ids_from_vault — full pipeline
# ---------------------------------------------------------------------------

class TestExtractPostIdsFromVault:
    @pytest.fixture
    def fake_vault(self, tmp_path: Path) -> Path:
        """Create a minimal vault layout: 2 days, 3 posts + 1 index per day."""
        for day in ("2026-09-01", "2026-09-02"):
            d = tmp_path / "Reddit" / day
            d.mkdir(parents=True)
            (d / f"{day}_r-finanzen_summary_reddit-1abc23.md").write_text(
                "# Foo bar\n"
                "- **Subreddit**: r/Finanzen\n"
                "- **URL**: https://www.reddit.com/r/Finanzen/comments/1abc23/foo\n"
            )
            (d / f"{day}_r-wohnen_summary_reddit-1def45.md").write_text(
                "# Baz qux\n"
                "- **Subreddit**: r/wohnen\n"
                "- **URL**: https://www.reddit.com/r/wohnen/comments/1def45/bar\n"
            )
            (d / "_index.md").write_text("# index\n")
        return tmp_path / "Reddit"

    def test_scans_all_dates(self, fake_vault: Path) -> None:
        result = extract_post_ids_from_vault(fake_vault)
        assert len(result) == 4
        ids = {e.post_id for e in result}
        assert ids == {"1abc23", "1def45"}

    def test_each_entry_has_required_attrs(self, fake_vault: Path) -> None:
        result = extract_post_ids_from_vault(fake_vault)
        for entry in result:
            assert isinstance(entry, VaultEntry)
            assert entry.post_id
            assert entry.date
            assert isinstance(entry.path, Path)
            assert entry.subreddit
            assert entry.title
            assert entry.url

    def test_skips_index_file(self, fake_vault: Path) -> None:
        result = extract_post_ids_from_vault(fake_vault)
        for entry in result:
            assert not entry.path.name.startswith("_")

    def test_filters_by_date_range(self, fake_vault: Path) -> None:
        result = extract_post_ids_from_vault(
            fake_vault, date_from="2026-09-02", date_to="2026-09-02"
        )
        assert len(result) == 2
        assert all(e.date == "2026-09-02" for e in result)

    def test_empty_dir_returns_empty(self, tmp_path: Path) -> None:
        assert extract_post_ids_from_vault(tmp_path) == []

    def test_missing_dir_returns_empty(self, tmp_path: Path) -> None:
        assert extract_post_ids_from_vault(tmp_path / "does_not_exist") == []
