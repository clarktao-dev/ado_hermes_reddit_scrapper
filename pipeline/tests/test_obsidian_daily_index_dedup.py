"""Regression test: write_daily_index must collapse same-URL duplicates
so total_items matches the number of vault files written.

Bug (2026-08-31): index listed 5 items but vault had only 3 .md files
because Zeit RSS returned 3 copies of the same URL, each got its own
index entry, but write_news_item overwrote the same filename 3 times.

Fix Plan 9: write_daily_index now dedupes by normalized URL before
counting + listing, as a defensive layer behind filter_processed.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

THIS_DIR = Path(__file__).resolve().parent
PIPELINE_ROOT = THIS_DIR.parent.parent
sys.path.insert(0, str(PIPELINE_ROOT))

from pipeline.lib.obsidian import write_daily_index  # noqa: E402

SUDPARK_URL = (
    "https://www.zeit.de/wirtschaft/2026-06/"
    "suedpark-halle-neustadt-plattenbau-verfall-afd"
)


def _fake_item(url, title_zh="t", title="T", source="Zeit", summary_zh="s"):
    return {
        "url": url,
        "title_zh": title_zh,
        "title": title,
        "source_name": source,
        "summary_zh": summary_zh,
    }


class TestWriteDailyIndexDedup:
    def test_three_duplicates_become_one_in_index(self, tmp_path: Path) -> None:
        """Three Zeit Südpark copies must collapse to one index entry."""
        items = [
            _fake_item(SUDPARK_URL, title_zh="Südpark A"),
            _fake_item(SUDPARK_URL, title_zh="Südpark B"),
            _fake_item(SUDPARK_URL, title_zh="Südpark C"),
        ]
        write_daily_index(items, str(tmp_path), "2026-08-31", "owner/repo")

        idx = (tmp_path / "Daily" / "2026-08-31" / "_index.md").read_text(
            encoding="utf-8"
        )
        assert "total_items: 1" in idx, (
            f"frontmatter total_items should be 1 after dedup; got:\n{idx[:200]}"
        )
        assert "共 **1 則新聞**" in idx
        # Only first-occurrence (Südpark A) should appear
        assert "Südpark A" in idx
        assert "Südpark B" not in idx
        assert "Südpark C" not in idx
        # Source distribution: Zeit only 1 entry, not 3
        assert "- **Zeit**: 1 則" in idx

    def test_dedup_uses_normalize_url(self, tmp_path: Path) -> None:
        """utm_source / fragment variants must also collapse."""
        items = [
            _fake_item(SUDPARK_URL + "?utm_source=rss"),
            _fake_item(SUDPARK_URL + "#comments"),
            _fake_item(SUDPARK_URL),
        ]
        write_daily_index(items, str(tmp_path), "2026-08-31", "owner/repo")
        idx = (tmp_path / "Daily" / "2026-08-31" / "_index.md").read_text(
            encoding="utf-8"
        )
        assert "total_items: 1" in idx

    def test_unique_items_pass_through(self, tmp_path: Path) -> None:
        items = [
            _fake_item("https://www.handelsblatt.com/x", source="Handelsblatt"),
            _fake_item("https://www.morgenpost.de/y", source="Google News"),
        ]
        write_daily_index(items, str(tmp_path), "2026-08-31", "owner/repo")
        idx = (tmp_path / "Daily" / "2026-08-31" / "_index.md").read_text(
            encoding="utf-8"
        )
        assert "total_items: 2" in idx
        assert "共 **2 則新聞**" in idx
        assert "Handelsblatt" in idx
        assert "Google News" in idx