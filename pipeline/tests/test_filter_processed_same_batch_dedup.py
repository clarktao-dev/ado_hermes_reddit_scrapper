"""Regression test: filter_processed must dedupe duplicate URLs WITHIN a
single batch, not just against the Airtable ledger.

Bug (2026-08-31): news_daily 2026-08-31 wrote _index.md with 5 entries but
the vault contained only 3 .md files. Root cause: the Zeit Wirtschaft RSS
feed returned 3 separate <item> entries for the same Südpark URL during
one fetch. filter_processed only checked `store.is_processed()` (cross-run
ledger), so all 3 copies survived to translate → rank → vault, where the
3 items all wrote the same filename (overwriting each other) while the
3 entries each got their own slot in _index.md → 5 vs 3 mismatch.

This test proves filter_processed drops intra-batch URL duplicates BEFORE
the items reach the rest of the pipeline.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

THIS_DIR = Path(__file__).resolve().parent
PIPELINE_ROOT = THIS_DIR.parent.parent
sys.path.insert(0, str(PIPELINE_ROOT))

from pipeline.lib.processed_store import ProcessedStore  # noqa: E402
from pipeline.news_daily import filter_processed  # noqa: E402


SUDPARK_URL = (
    "https://www.zeit.de/wirtschaft/2026-06/"
    "suedpark-halle-neustadt-plattenbau-verfall-afd"
)


def _in_memory_store() -> tuple[ProcessedStore, dict]:
    """ProcessedStore backed by a plain dict — no GCP needed."""
    memory: dict = {}
    s = ProcessedStore("appFAKE", "ProcessedContent", _memory=memory)
    return s, memory


class TestFilterProcessedSameBatchDedup:
    """Plan 9 (2026-08-31): same-batch URL dedup in filter_processed."""

    def test_three_zeit_duplicates_become_one(self) -> None:
        """The exact bug from 2026-08-31: Zeit RSS returns 3 <item>s for
        the same URL. filter_processed must collapse them to 1.

        The store is empty (this is a brand-new article) so cross-run
        dedup cannot catch it — only same-batch dedup can.
        """
        store, memory = _in_memory_store()
        items = [
            {"url": SUDPARK_URL, "title": "Südpark A", "source_name": "Zeit"},
            {"url": SUDPARK_URL, "title": "Südpark B", "source_name": "Zeit"},
            {"url": SUDPARK_URL, "title": "Südpark C", "source_name": "Zeit"},
        ]
        kept = filter_processed(items, store=store)

        assert len(kept) == 1, (
            f"filter_processed kept {len(kept)} of 3 same-URL entries; "
            "expected 1 (same-batch dedup missing)."
        )
        # First one wins (deterministic + matches RSS ordering)
        assert kept[0]["title"] == "Südpark A"

    def test_dedup_uses_normalize_url(self) -> None:
        """utm_source / fragment differences must dedup (host casing is
        normalised per normalize_url spec; path case is preserved).

        Zeit RSS occasionally re-issues an article with `?utm_source=rss`
        or `#comments`. Both should collapse to the canonical entry.
        """
        store, _ = _in_memory_store()
        items = [
            {"url": SUDPARK_URL + "?utm_source=rss", "title": "Südpark raw"},
            {"url": SUDPARK_URL + "#comments", "title": "Südpark with fragment"},
            {"url": SUDPARK_URL, "title": "Südpark canonical"},
        ]
        kept = filter_processed(items, store=store)
        # First-occurrence wins (deterministic + matches RSS feed
        # ordering). We assert only that *exactly one* item survives —
        # not which one — because the choice is arbitrary as long as the
        # same article isn't published twice.
        assert len(kept) == 1, f"normalize_url not respected: kept {len(kept)}"

    def test_dedup_still_honours_ledger(self) -> None:
        """Regression guard: cross-run ledger dedup MUST keep working.

        If a URL is already in the Airtable ledger from a previous run,
        filter_processed must still drop it. The same-batch logic is
        additive — it does NOT replace the existing ledger check.
        """
        store, _ = _in_memory_store()
        # Pretend Zeit Südpark was processed yesterday. filter_processed
        # passes the *normalized URL* as the source_id to
        # `store.is_processed("news", url_normalized)` (see news_daily.py
        # line 169), so that's the key we register here.
        from pipeline.lib.processed_store import normalize_url
        store.mark_processed(
            "news", normalize_url(SUDPARK_URL),
            "Südpark (already processed)",
        )

        items = [
            {"url": SUDPARK_URL, "title": "Südpark today"},
        ]
        kept = filter_processed(items, store=store)
        assert kept == [], (
            f"ledger dedup broken: kept {len(kept)} of 1 already-processed item"
        )

    def test_mixed_batch_keeps_unique_drops_dupes(self) -> None:
        """Realistic batch: 5 unique articles + 3 Zeit Südpark dupes
        sprinkled throughout. Result must be exactly 6 (5 unique URLs +
        1 Südpark copy), in original order, with Südpark appearing once
        at its first position."""
        store, _ = _in_memory_store()
        unique_urls = [
            "https://www.handelsblatt.com/foo",
            "https://www.morgenpost.de/bar",
            "https://example.com/baz",
            "https://example.com/qux",
            "https://example.com/quux",
        ]
        items = []
        for u in unique_urls:
            items.append({"url": u, "title": f"unique {u[-3:]}"})
        # Sprinkle 3 Südpark copies throughout (one is the "real" entry,
        # the other two are the same-batch dupes filter_processed must drop)
        items.insert(1, {"url": SUDPARK_URL, "title": "Südpark copy 1"})
        items.insert(3, {"url": SUDPARK_URL, "title": "Südpark copy 2"})
        items.append({"url": SUDPARK_URL, "title": "Südpark copy 3"})

        kept = filter_processed(items, store=store)
        assert len(kept) == 6
        kept_urls = [it["url"] for it in kept]
        assert SUDPARK_URL in kept_urls
        assert kept_urls.count(SUDPARK_URL) == 1
        # First-occurrence wins: copy 1 is the Südpark that survives
        kept_suedpark = [it for it in kept if it["url"] == SUDPARK_URL]
        assert len(kept_suedpark) == 1
        assert kept_suedpark[0]["title"] == "Südpark copy 1"
        # Order of uniques preserved
        assert kept[0]["url"] == unique_urls[0]  # handelsblatt
        assert kept[-1]["url"] == unique_urls[-1]  # quux

    def test_no_url_items_are_not_coalesced(self) -> None:
        """Items without a URL pass through unchanged (defensive — RSS
        shouldn't produce any, but we don't want to break that edge)."""
        store, _ = _in_memory_store()
        items = [
            {"url": "", "title": "no-url A"},
            {"url": "", "title": "no-url B"},
        ]
        kept = filter_processed(items, store=store)
        assert len(kept) == 2