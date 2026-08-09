"""Deduplicate news items by URL and fuzzy title match.

.. deprecated::
    This module is deprecated for the ``news_daily`` pipeline. As of Task 4
    (2026-08-09), news deduplication is delegated to
    :func:`pipeline.news_daily.filter_processed`, which uses the Airtable
    ``ProcessedContent`` ledger as the single source of truth.

    This file is kept for backward compatibility only — any other pipeline
    that imports :func:`dedup_items` still works, but the in-process
    fuzzy-title logic it implements is now bypassed by the news pipeline
    in favour of URL normalization + Airtable lookups.

    New code should ``from pipeline.news_daily import filter_processed``
    and pass an explicit ``ProcessedStore`` instance.
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Iterable, List, Optional

from rapidfuzz import fuzz

from pipeline.lib.processed_store import (
    ProcessedStore,
    normalize_url,
)

logger = logging.getLogger(__name__)


def dedup_items(
    items: Iterable[dict],
    title_threshold: float = 0.85,
    *,
    store: Optional[ProcessedStore] = None,
    days_threshold: int = 3,
    fallback_factory: Optional[Callable[[], ProcessedStore]] = None,
) -> List[dict]:
    """Deduplicate news items.

    Backward-compatible wrapper. Two behaviours:

    * If ``store`` (or a callable returned by ``fallback_factory``) is
      provided, dispatch to :func:`pipeline.news_daily.filter_processed`
      (the new Airtable-backed behaviour used by ``news_daily``).
    * Otherwise fall back to the legacy in-process URL + fuzzy-title
      matcher that previous versions of this pipeline used.

    Parameters
    ----------
    items:
        Iterable of item dicts. Each must have at least ``url`` and
        ``title`` keys. ``url_normalized`` will be set on every kept
        item (both code paths).
    title_threshold:
        Legacy fuzzy-title match threshold (0–1). Ignored when
        ``store`` is provided.
    store, fallback_factory:
        New-style Airtable-backed dedup. ``fallback_factory`` is a
        zero-arg callable returning a ``ProcessedStore`` — useful for
        callers that want to instantiate the store lazily.
    days_threshold:
        Passed through to the Airtable-backed filter.
    """
    items_list = list(items)
    if store is None and fallback_factory is not None:
        try:
            store = fallback_factory()
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "fallback_factory raised %s; using legacy dedup", e,
            )
            store = None

    if store is not None:
        # Late import: news_daily imports from lib/dedup historically;
        # doing this here avoids a circular import at module load.
        from pipeline.news_daily import filter_processed  # type: ignore[import-not-found]

        logger.info(
            "dedup_items: dispatching to Airtable filter_processed "
            "(items=%d, days_threshold=%d)",
            len(items_list), days_threshold,
        )
        return filter_processed(
            items_list, store=store, days_threshold=days_threshold,
        )

    # ---- Legacy in-process URL + fuzzy-title fallback ---------------------
    logger.warning(
        "dedup_items: running legacy in-process dedup "
        "(no ProcessedStore supplied). This is deprecated — "
        "callers should pass store=...",
    )
    seen_urls: set = set()
    out: List[dict] = []
    for it in items_list:
        url = (it.get("url", "") or "").strip()
        # Always populate url_normalized so downstream code is consistent
        # regardless of which code path was taken.
        if url:
            it["url_normalized"] = normalize_url(url)
        else:
            it["url_normalized"] = ""

        norm = it["url_normalized"]
        if norm and norm in seen_urls:
            continue
        title = (it.get("title", "") or "").strip()
        is_dup = False
        for kept in out:
            t_ratio = fuzz.ratio(title.lower(), kept.get("title", "").lower()) / 100.0
            if t_ratio >= title_threshold:
                if it.get("priority", 99) < kept.get("priority", 99):
                    out.remove(kept)
                    if kept.get("url_normalized"):
                        seen_urls.discard(kept["url_normalized"])
                    out.append(it)
                    if norm:
                        seen_urls.add(norm)
                is_dup = True
                break
        if is_dup:
            continue
        if norm:
            seen_urls.add(norm)
        out.append(it)
    return out
