"""Regression test: canonical content-id dedup must catch slug-rotated
duplicates that exact-URL dedup misses.

Background (2026-11-11 .. 2026-11-14): the same Handelsblatt article was
published on four consecutive days under four different URL slugs but the
same article id (100266848). Exact-URL dedup treated them as four different
articles, so the daily digest repeated the same story four times.

The fix adds a second gate: ``canonical_content_id()`` extracts the
publisher's stable article id and ``ProcessedStore.is_processed_canonical()``
looks it up in a cached index built from the ledger.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

PIPELINE_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PIPELINE_ROOT))

from pipeline.lib.processed_store import (  # noqa: E402
    ProcessedStore,
    canonical_content_id,
)

# The real pair observed on 2026-11-12 / 2026-11-13.
URL_OLD = (
    "https://www.handelsblatt.com/finanzen/immobilien/"
    "immobilien-hier-steigen-die-mieten-in-deutschland-am-staerksten-01/"
    "100266848.html"
)
URL_NEW = (
    "https://www.handelsblatt.com/finanzen/immobilien/"
    "immobilien-warum-die-mieten-steigen-und-die-kaufpreise-stagnieren/"
    "100266848.html"
)

# The 4th appearance (2026-11-14) of the very same article.
URL_NEWER = (
    "https://www.handelsblatt.com/finanzen/immobilien/"
    "immobilien-warum-sich-die-kaufpreise-anders-entwickeln-als-die-mieten/"
    "100266848.html"
)


def _store(rows):
    """Build an in-memory ProcessedStore seeded with ledger rows."""
    from pipeline.lib.processed_store import make_hash

    mem = {}
    for source_id, title, run in rows:
        h = make_hash("news", source_id)
        mem[h] = {
            "source_hash": h,
            "source_type": "news",
            "source_id": source_id,
            "title": title,
            "pipeline_run_id": run,
        }
    return ProcessedStore(_memory=mem)


class TestCanonicalContentId:
    def test_handelsblatt_same_id_different_slug(self):
        assert canonical_content_id(URL_OLD) == canonical_content_id(URL_NEW)
        assert canonical_content_id(URL_OLD) == "hbl:100266848"

    def test_handelsblatt_fourth_slug_is_same_content(self):
        assert canonical_content_id(URL_NEWER) == canonical_content_id(URL_OLD)

    def test_ntv_id_suffix(self):
        a = "https://www.n-tv.de/wirtschaft/Inflation-faellt-unter-Marke-id31520141.html"
        b = "https://www.n-tv.de/wirtschaft/Neue-Ueberschrift-id31520141.html"
        assert canonical_content_id(a) == canonical_content_id(b)
        assert canonical_content_id(a) == "ntv:31520141"

    def test_spiegel_uuid(self):
        a = ("https://www.spiegel.de/wirtschaft/unternehmen/bundesbank-warnt-"
            "a-b53aea9b-ff04-4b9f-a3e5-59f07fd19cdc#ref=rss")
        b = ("https://www.spiegel.de/wirtschaft/unternehmen/anderer-slug-"
            "b53aea9b-ff04-4b9f-a3e5-59f07fd19cdc")
        assert canonical_content_id(a) == canonical_content_id(b)
        assert canonical_content_id(a) == (
            "spiegel:b53aea9b-ff04-4b9f-a3e5-59f07fd19cdc"
        )

    def test_google_news_token(self):
        a = "https://news.google.com/rss/articles/CBMi3AFBVV95cUxQeDF6T2o0UC14TktWZG54clVuYnBibmpRYWlJSUowOEJvamUxVmVURGdJRnh1SXNTUDdjUWE2Yzk3M21rMW15Zktfb2pfbGZGRDhkbHVYUTBHMkJMRzRZV0ZFajkyZzVTUFl4WWE0S2s2dFRGRDJMMEZUTktUel85STBQTXV1OG1iRWMzSWlkbGVXM0hUZlFtbHh3elJDckg4SC01d05MM2RDbm9JZ21RRU9ZS3J1ajZaWVhTY1lwX2JiVzY2VUxjaHNGUGxVN082OHVJTmdfcmRNU3ZM?oc=5"
        assert canonical_content_id(a).startswith("gnews:")

    def test_unknown_host_returns_none(self):
        assert canonical_content_id("https://example.com/some/article") is None

    def test_faz_returns_none(self):
        # FAZ has no stable id in the path — must fall back to exact-URL dedup.
        assert canonical_content_id(
            "https://www.faz.net/aktuell/wirtschaft/immobilien-preise-123.html"
        ) is None

    def test_empty_and_garbage(self):
        assert canonical_content_id("") is None
        assert canonical_content_id("not a url") is None

    def test_ids_are_host_namespaced(self):
        hbl = canonical_content_id(URL_OLD)
        ntv = canonical_content_id(
            "https://www.n-tv.de/wirtschaft/x-id100266848.html"
        )
        assert hbl != ntv


class TestIsProcessedCanonical:
    def test_detects_slug_rotated_duplicate(self):
        store = _store([(URL_OLD, "舊標題", "news-20261112-030102")])
        match = store.is_processed_canonical("news", URL_NEW, source_id=URL_NEW)
        assert match is not None
        assert match["pipeline_run_id"] == "news-20261112-030102"
        assert match["canonical_id"] == "hbl:100266848"

    def test_exact_same_url_is_not_reported_as_rotation(self):
        store = _store([(URL_OLD, "舊標題", "news-20261112-030102")])
        # Same normalized URL → the exact-match gate already handles it, so the
        # canonical gate must stay quiet and let the exact gate do the work.
        assert store.is_processed_canonical(
            "news", URL_OLD, source_id=URL_OLD
        ) is None

    def test_new_article_returns_none(self):
        store = _store([(URL_OLD, "舊標題", "news-20261112-030102")])
        other = (
            "https://www.handelsblatt.com/finanzen/immobilien/neuer-artikel/"
            "100999999.html"
        )
        assert store.is_processed_canonical(
            "news", other, source_id=other
        ) is None

    def test_unknown_host_falls_back_to_none(self):
        store = _store([])
        assert store.is_processed_canonical(
            "news", "https://example.com/x", source_id="https://example.com/x"
        ) is None

    def test_index_is_cached_not_rebuilt(self):
        store = _store([(URL_OLD, "舊標題", "news-20261112-030102")])
        store.is_processed_canonical("news", URL_NEW, source_id=URL_NEW)
        first = store._canonical_index
        store.is_processed_canonical("news", URL_NEW, source_id=URL_NEW)
        assert store._canonical_index is first


class TestFilterProcessedGate:
    def test_filter_processed_drops_slug_rotated_item(self):
        from pipeline.news_daily import filter_processed

        store = _store([(URL_OLD, "舊標題", "news-20261112-030102")])
        items = [
            {"url": URL_OLD, "title": "same"},
            {"url": URL_NEW, "title": "rotated"},
            {
                "url": "https://www.n-tv.de/wirtschaft/echt-id31520666.html",
                "title": "new",
            },
        ]
        kept = filter_processed(items, store=store)
        urls = [i["url"] for i in kept]
        assert URL_NEW not in urls, "slug-rotated duplicate was not dropped"
        assert URL_OLD not in urls, "exact duplicate was not dropped"
        assert len(kept) == 1

    def test_filter_processed_drops_second_rotation(self):
        """A URL rotated a second time must still be caught (2026-11-14 case)."""
        from pipeline.news_daily import filter_processed

        store = _store([(URL_OLD, "舊標題", "news-20261111-030129")])
        kept = filter_processed([{"url": URL_NEWER, "title": "rotated twice"}],
                                store=store)
        assert kept == [], "second slug rotation was not dropped"
