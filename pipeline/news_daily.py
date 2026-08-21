#!/usr/bin/env python3
"""Daily German real-estate news pipeline (9 steps).

Steps:
  1. load_config
  2. fetch_rss
  3. filter_keywords
  3b. quick_score   — title-only LLM pre-filter (cheap)
  4. dedup_cross_source  — Airtable ``ProcessedContent`` is the single source of truth
     (via :func:`filter_processed`). state.json is **deprecated** — kept only as a
     in-process fallback when ``--skip-store`` is set.
  4b. age filter (≤ max_days)
  4c. fetch_full_text (only for survivors)
  5. translate + analyse
  5b. relevance filter (≥ min_relevance)
  6. rank_by_relevance + source quota
  7. write_vault         — wipe + write immobilien-kb/vault/Daily/<date>/
  8. send_discord        — push to channel `headlines` (alias)
  9. push_to_github      — git add + commit + push via paramiko
  10. mark_processed + update_side_effects — backfill into the Airtable ledger
      (best-effort; failure here doesn't lose the vault write).

Usage:
    python3 news_daily.py                 # full run (fetch → translate → vault → discord → github)
    python3 news_daily.py --dry-run       # steps 1-6 only (no vault / discord / github side effects)
    python3 news_daily.py --limit N       # only fetch first N sources (testing)
    python3 news_daily.py --skip-store    # bypass ProcessedStore (legacy in-process dedup)
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

# Ensure the pipeline package is importable when invoked as a script.
_PIPELINE_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_DIR = os.path.dirname(_PIPELINE_DIR)
if _PROJECT_DIR not in sys.path:
    sys.path.insert(0, _PROJECT_DIR)

from pipeline.lib import (  # noqa: E402
    config_loader,
    dedup,
    filter_news,
    obsidian,
    rss_fetch,
    translate,
)
from pipeline.lib.processed_store import (  # noqa: E402
    DEFAULT_TABLE,
    ProcessedStore,
    make_hash,
    normalize_url,
)

# discord_sender lives outside the package — load it by path.
_DISCORD_SENDER = os.path.join(_PROJECT_DIR, "immobilien-kb", "tools", "discord_sender.py")
_PUSH_TO_GITHUB = os.path.join(_PROJECT_DIR, "push_to_github.py")

# ProcessedContent Airtable config (Task 4 — single source of truth for dedup).
PROCESSED_BASE_ID = os.environ.get(
    "AIRTABLE_PROCESSED_CONTENT_BASE_ID", "appHilorcrC5T0p2u",
)
PROCESSED_TABLE = os.environ.get(
    "AIRTABLE_PROCESSED_CONTENT_TABLE", DEFAULT_TABLE,
)

logger = logging.getLogger("news_daily")
if not logger.handlers:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )


# --------------------------------------------------------------------------- #
# ProcessedStore helpers (Task 4 — Airtable as source of truth for news dedup).
# --------------------------------------------------------------------------- #

def _get_store() -> ProcessedStore:
    """Instantiate the module-level ProcessedStore (lazy, cached)."""
    global _PROCESSED_STORE
    if _PROCESSED_STORE is None:
        _PROCESSED_STORE = ProcessedStore(
            PROCESSED_BASE_ID, table_name=PROCESSED_TABLE,
        )
        logger.info(
            "ProcessedStore ready: base=%s table=%s",
            PROCESSED_BASE_ID, PROCESSED_TABLE,
        )
    return _PROCESSED_STORE


_PROCESSED_STORE: Optional[ProcessedStore] = None


def filter_processed(
    items: List[dict],
    store: Optional[ProcessedStore] = None,
    days_threshold: int = 3,
) -> List[dict]:
    """Drop items already in the Airtable ``ProcessedContent`` ledger.

    This is the news pipeline's new dedup gate (Task 4). The previous
    in-process URL + fuzzy-title matcher in ``pipeline.lib.dedup`` is
    kept only as a fallback when ``store`` is None.

    For each item:

    1. ``url_normalized`` is set (strip ``utm_*`` / ``fbclid`` / ``gclid``,
       lowercase host, sort query).
    2. If the normalized URL is already in the ledger → **skip** (exact
       dedup; this is the only gate — the previous "any news in past
       N days" batch gate was removed in Task 9).

    Items without a URL pass through unchanged.

    Returns the kept list. Mutates input dicts in place to add
    ``url_normalized`` on the kept items (so downstream code can hash
    the same value when calling ``mark_processed``).

    .. note::
       The ``days_threshold`` parameter is kept for backwards compatibility
       with callers; it is no longer used internally (no batch gate).
    """
    if store is None:
        store = _get_store()
    kept: List[dict] = []
    skipped_exact = 0
    skipped_no_url = 0

    # Dedup is now EXACT-URL-ONLY (Task 9, 2026-08-09).
    # The previous "any news in past N days → skip all" gate was removed
    # because it meant once the pipeline ran once, every subsequent run
    # in that window would skip all *new* items too — which defeats the
    # user's intent of "process new articles each run". The max_days gate
    # (Step 4b age filter) still drops old news; URL dedup blocks the
    # exact article from being processed twice.
    # `days_threshold` is kept in the signature for backwards compatibility
    # with existing callers; it is no longer used internally.
    _ = days_threshold  # intentionally unused

    for item in items:
        url = (item.get("url") or "").strip()
        if not url:
            skipped_no_url += 1
            kept.append(item)
            continue

        normalized = normalize_url(url)
        item["url_normalized"] = normalized

        # Exact-match dedup via Airtable ledger.
        if store.is_processed("news", normalized):
            skipped_exact += 1
            logger.info(
                "[skip already-processed] %s | %s",
                normalized, item.get("title", "")[:60],
            )
            continue

        kept.append(item)

    logger.info(
        "filter_processed: %d kept, %d skipped(exact), %d skipped(no-url)",
        len(kept), skipped_exact, skipped_no_url,
    )
    return kept


# --------------------------------------------------------------------------- #
# Step helpers (each prints progress, count, elapsed; swallows exceptions).
# --------------------------------------------------------------------------- #

def _step(name, fn):
    """Run one step with timing + error capture. Returns the result or None.

    Plan 7 (2026-08-19): the previous version only printed the exception
    message, hiding the stack trace. Real bugs (e.g. discord send raising
    HTTPError, or subprocess failing non-zero) were silently logged as
    "[OK]" because the outer swallow swallowed the actual cause. Now we
    ``traceback.print_exc()`` so the cron log shows exactly which line
    failed.
    """
    print(f"\n=== STEP: {name} ===", flush=True)
    t0 = time.time()
    try:
        result = fn()
        elapsed = time.time() - t0
        n = len(result) if hasattr(result, "__len__") and not isinstance(result, (str, dict)) else "—"
        print(f"[OK] {name} ({elapsed:.2f}s, count={n})", flush=True)
        return result
    except Exception as e:
        elapsed = time.time() - t0
        print(f"[ERROR] {name} failed after {elapsed:.2f}s: {type(e).__name__}: {e}",
              flush=True)
        traceback.print_exc()
        return None


def _import_discord():
    """Import discord_sender.py from the tools/ dir without installing it."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("discord_sender", _DISCORD_SENDER)
    if spec is None or spec.loader is None:
        raise ImportError(f"could not load module spec for {_DISCORD_SENDER}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# --------------------------------------------------------------------------- #
# Step bodies (each is a no-arg callable for _step()).
# --------------------------------------------------------------------------- #

def step_load_config():
    cfg = config_loader.load_config()
    print(f"  vault={cfg.get('vault', {}).get('root')}")
    print(f"  sources enabled={len(config_loader.get_sources())}")
    return cfg


def step_fetch(sources):
    return rss_fetch.fetch_all_sources(sources)


def step_filter(items):
    return filter_news.filter_items(items)


def step_dedup(items, *, skip_store: bool = False, days_threshold: int = 3):
    """Drop items already in the Airtable ``ProcessedContent`` ledger.

    Two code paths (controlled by ``skip_store``):

    * **default** — delegates to :func:`filter_processed`, which uses
      ``store.is_processed('news', url_normalized)`` for exact match
      and ``store.get_recent('news', days=N)`` for the recent-window
      gate. This is the production path (Task 4).

    * **skip_store=True** — falls back to the legacy in-process URL +
      fuzzy-title matcher in :mod:`pipeline.lib.dedup`. Kept for
      ``--skip-store`` debugging only.
    """
    if skip_store:
        logger.warning(
            "step_dedup: --skip-store set → using legacy in-process dedup",
        )
        return dedup.dedup_items(items, fallback_factory=_get_store)
    return filter_processed(items, days_threshold=days_threshold)


# Module-level lazy singleton for the Google News decoder.
# google-news-api internally uses httpx + a rate limiter; instantiating it
# per-call is wasteful and triggers a fresh TLS handshake each time.
_GN_DECODER = None


def _get_gn_decoder():
    """Return a process-wide GoogleNewsClient (lazy-initialized).

    The client is built with ``language=de`` and ``country=DE`` so the
    batchexecute endpoint returns German-language payloads. We instantiate
    a new one if either of those settings differ, but otherwise reuse.
    """
    global _GN_DECODER
    if _GN_DECODER is None:
        try:
            from google_news_api import GoogleNewsClient
        except ImportError as e:
            logger.warning("[google-news-api] not installed: %s", e)
            return None
        try:
            _GN_DECODER = GoogleNewsClient(language="de", country="DE")
        except Exception as e:
            logger.warning("[google-news-api] init failed: %s", e)
            return None
    return _GN_DECODER


def _fetch_google_news_text(item, delay_sec: float = 1.0) -> Optional[str]:
    """Decode a Google News RSS redirect URL to its publisher URL, then
    fetch the publisher article body. Falls back to RSS title + summary
    if decode or fetch fails.

    Paywall detection (Task 12, 2026-08-09): we inspect the decoded URL
    for known paid-content markers BEFORE the HTTP fetch. If a match is
    found, we set ``item['_paywalled'] = True`` and return a sentinel
    string ``"<PAYWALLED>"`` so the caller can drop the item rather than
    wasting LLM tokens on a "sorry, this article is paywalled" summary.

    Two detection passes:
    1. URL pattern (``/plus/`` for WELT+ / Spiegel+, ``/_-`` for
       Handelsblatt+, ``/premium/`` for FAZ+) — cheap and definitive
       when the marker is present.
    2. Post-fetch char count (``full_text_chars < 1000``) — catches
       generic paywalls where the publisher doesn't URL-tag (e.g. some
       WiWo/Morgenpost articles).
    """
    raw_url = item.get("url")
    title = item.get("title") or ""
    summary = item.get("summary") or ""
    fallback = " ".join([title, summary]).strip()

    if not raw_url:
        return fallback or None

    client = _get_gn_decoder()
    if client is None:
        return fallback or None

    try:
        decoded_url = client.decode_url(raw_url, timeout=20.0)
    except Exception as e:
        logger.info("[gn-decode] failed %s: %s", raw_url[:80], type(e).__name__)
        return fallback or None

    if not decoded_url or decoded_url == raw_url:
        return fallback or None

    # Cache the decoded URL on the item so dedup re-runs hit the same key
    # AND so step_mark_processed can persist it as source_url (Task 12c).
    item["url"] = decoded_url
    item["_decoded_url"] = decoded_url
    item["_original_gn_url"] = raw_url

    # ─── Paywall pass 1: URL pattern (early detection, before HTTP) ────
    # Plan 1.5 (2026-08-17): switched from _is_paywalled_url to
    # _is_paywall_url_pattern_only so that GN-decoded URLs on host-blacklisted
    # publishers (e.g. spiegel.de/wirtschaft/..., welt.de/.../standard)
    # are NOT early-dropped. They fall through to the fetch + char_count +
    # had_hint path and get the same keep-as-preview / drop decision as
    # the RSS path. WELT+ hex / Spiegel+ uuid / FAZ+ /premium/ URLs still
    # match the path pattern and are still early-dropped.
    if _is_paywall_url_pattern_only(decoded_url):
        item["_paywalled"] = True
        item["_paywall_reason"] = "url-pattern"
        logger.info(
            "[paywall-detected] url-pattern skip | %s | %s",
            decoded_url[:80], title[:50],
        )
        return "<PAYWALLED>"

    if delay_sec > 0:
        time.sleep(delay_sec)

    try:
        from pipeline.lib.rss_fetch import fetch_full_text as _fetch_full_text
        result = _fetch_full_text(decoded_url)
    except Exception as e:
        logger.info("[gn-fetch] failed %s: %s", decoded_url[:80], type(e).__name__)
        return fallback or None

    text = result.get("full_text") or ""
    char_count = result.get("char_count") or len(text)
    publisher_paywalled = result.get("paywalled", False)
    had_paywall_hint = result.get("had_paywall_hint", False)

    # ─── Paywall pass 2: char count + publisher paywall hint ────────────
    # Some legitimate short news (e.g. mini briefs) are under 1000 chars;
    # but if rss_fetch ALSO flagged paywalled or saw a paywall hint, that's
    # a much stronger signal — drop unconditionally. Otherwise 1000 is a
    # safe threshold for German full-length real-estate articles.
    if publisher_paywalled or had_paywall_hint or char_count < 1000:
        item["_paywalled"] = True
        item["_paywall_reason"] = (
            "publisher-hint" if (publisher_paywalled or had_paywall_hint)
            else "short-content"
        )
        logger.info(
            "[paywall-detected] %s (%d chars) | %s",
            item["_paywall_reason"], char_count, decoded_url[:80],
        )
        return "<PAYWALLED>"

    if len(text) < 100:
        return fallback or None
    return text


# Paywall URL patterns (Task 12 + Task 13, 2026-08-09).
#
# Two complementary detection strategies. They are *helpers* — the late
# char-count + publisher-hint pass still runs as the safety net (Task 12).
#
# 1. ``_PAYWALL_URL_PATTERNS`` — substring scan, cheap. Catches FAZ+
#    ("/premium/"), Handelsblatt-style ("/-/"), generic markers
#    ("/paywall/", "/epaper/"). Note: WELT+ and Spiegel+ use a
#    different shape (see below) so they don't appear here.
#
# 2. ``_PAYWALL_HOST_BLACKLIST`` — exact domain match against a known
#    list of publishers whose content is always (or near-always) behind
#    a paywall: WELT+, Spiegel+, Manager Magazin, Handelsblatt paid tier,
#    FAZ+, WiWo+, ZEIT+. We match the host portion of the URL. Any URL
#    on these hosts is treated as paywalled, no need to inspect the
#    path — the publisher doesn't publish free content.
#
# 3. ``_PAYWALL_PATH_REGEXES`` — pattern check on the URL path
#    component. Catches the WELT+ shape ("/plus6a..." / "/plus7b..." —
#    plus glued to a hex prefix, no closing slash) and the Spiegel+ /
#    Manager Magazin shape ("/a-{uuid}" — a literal 'a-' followed by an
#    article UUID). Each entry is a compiled regex.

import re

_PAYWALL_URL_PATTERNS = [
    ("/premium/", "FAZ+"),                # FAZ paid tier
    ("/paywall/", "explicit-paywall"),
    ("/epaper/", "epaper-only"),
]

# Publishers whose content is always / nearly always paywalled.
# We only match the host portion so we don't accidentally catch
# subdomains that publish free content (e.g. zeit.de/magazin/...).
_PAYWALL_HOST_BLACKLIST = {
    "www.welt.de":            "WELT+",          # plus.welt.de premium tier
    "welt.de":                "WELT+",
    "www.spiegel.de":         "Spiegel+",       # spiegel-plus premium tier
    "www.manager-magazin.de": "Manager-Magazin+",
    "www.handelsblatt.com":   "Handelsblatt+",  # mostly paid
    "www.faz.net":            "FAZ+",           # /premium/ paths but blacklist as safety
    "www.wiwo.de":            "WiWo+",          # many paid articles
    "www.zeit.de":            "ZEIT+",          # zeit-plus premium
}

# Regex patterns on the path. These catch specific paywall URL shapes
# that the host blacklist misses (e.g. Google News decoded URLs that
# are hosted on www.welt.de but already covered above; or any path-shape
# that indicates paywall even on an unlisted host).
_PAYWALL_PATH_REGEXES = [
    (re.compile(r"/plus[A-Za-z0-9]+"), "WELT+-hex"),   # /plus6a37f069...
    (re.compile(r"/-a-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"),
     "Spiegel+/MM-uuid"),                                # /a-fda65d69-8781-4cb4-... uuid
]


# --------------------------------------------------------------------------- #
# Plan 1 (2026-08-17): Paywall 預覽門檻保留.
#
# 舊邏輯(Layer 0)只要 URL 命中 host blacklist(Spiegel/Handelsblatt/FAZ/WELT/
# WiWo/ZEIT/Manager Magazin 等)就 early drop。新邏輯改跑 fetch_full_text
# 並依 char_count + had_paywall_hint 判定:
#   - had_hint=True AND >=1500c  → KEEP as paywall-preview
#   - had_hint=True AND 800-1500c → KEEP as short-paywall-preview
#   - char_count < 800           → DROP
#   - had_hint=False             → 走全文路徑(content_kind: full-article)
# WELT+ hex / Spiegel+ uuid 這類「URL pattern 就能確定」的真一字都拿不到的
# item 仍維持 early drop(由 _is_paywalled_url 判定)。
# --------------------------------------------------------------------------- #

# Paywall-preview 判定的字數門檻(Plan 1, 2026-08-17)。
_PAYWALL_PREVIEW_KEEP_CHARS = 1500      # had_hint=True 且 >= 此值 → paywall-preview
_PAYWALL_PREVIEW_SHORT_CHARS = 800      # had_hint=True 且 >= 此值 < 1500 → short-paywall-preview
                                        # 短於此值一律 DROP(不論 hint 與否)


def _decide_paywall_preview(
    char_count: int, had_paywall_hint: bool,
) -> tuple[str, str]:
    """Plan 1 (2026-08-17) Paywall 預覽判定矩陣。

    依 ``rss_fetch.fetch_full_text`` 回傳的 ``char_count`` 與
    ``had_paywall_hint`` 決定該 item 的歸屬:

    - ``("keep", "paywall-preview")``         hint=True 且 char_count >= 1500
    - ``("keep", "short-paywall-preview")``   hint=True 且 800 <= char_count < 1500
    - ``("drop", "")``                        char_count < 800(無論 hint)
    - ``("full", "full-article")``            had_hint=False(全文路徑)

    呼叫端負責把 ``content_kind`` 串到 ``item`` 上並決定是否繼續翻譯/
    寫入 vault。`decision` 對應到:
      ``"keep"`` → 保留並標 paywall-preview
      ``"drop"`` → 丟掉
      ``"full"`` → 走全文翻譯路徑(原 paywall 邏輯不變)
    """
    if not had_paywall_hint:
        return "full", "full-article"
    if char_count >= _PAYWALL_PREVIEW_KEEP_CHARS:
        return "keep", "paywall-preview"
    if char_count >= _PAYWALL_PREVIEW_SHORT_CHARS:
        return "keep", "short-paywall-preview"
    return "drop", ""


def _is_paywalled_url(url: str) -> bool:
    """Return True if ``url`` matches any known paywall signal.

    Three-layer check (Task 13):
    1. **Path substring scan** — cheap; matches ``/premium/`` etc.
    2. **Host blacklist** — matches publishers where all content is paid.
    3. **Path regex** — matches paywall URL shapes (``/plus6...``,
       ``/a-{uuid}``) on hosts that aren't in the blacklist.

    Runs BEFORE HTTP fetch (called by :func:`_fetch_google_news_text`)
    so we don't waste bandwidth on pages we know will be blocked.
    The late char-count + publisher-hint pass is still the safety net.
    """
    if not url:
        return False
    u = url.lower()

    # Layer 1: path substring patterns.
    for pat, _reason in _PAYWALL_URL_PATTERNS:
        if pat in u:
            return True

    # Layer 2: host blacklist.
    from urllib.parse import urlparse
    try:
        host = urlparse(url).hostname or ""
    except Exception:
        host = ""
    if host in _PAYWALL_HOST_BLACKLIST:
        return True

    # Layer 3: path regex (catches paywall URL shapes on unlisted hosts).
    for rx, _reason in _PAYWALL_PATH_REGEXES:
        if rx.search(u):
            return True

    return False


def _is_paywall_url_pattern_only(url: str) -> bool:
    """Plan 1 (2026-08-17): return True only for paywall path patterns.

    Mirrors :func:`_is_paywalled_url` Layer 1 (path substring) + Layer 3
    (path regex) — but **excludes** the Layer 2 host blacklist. Use this
    when you want to early-drop URLs that are *guaranteed* to have no
    content (e.g. WELT+ ``/plus6a37f...``, Spiegel+ ``/a-{uuid}``,
    FAZ+ ``/premium/``) but still try a fetch on host-blacklist items
    that *might* have a preview body.

    Task spec: "WELT+ hex 仍 early drop" — only the path-pattern layer
    can guarantee "一字都拿不到", so we keep the early-drop on those
    while letting host-blacklist-only URLs flow through to fetch.
    """
    if not url:
        return False
    u = url.lower()
    for pat, _reason in _PAYWALL_URL_PATTERNS:
        if pat in u:
            return True
    for rx, _reason in _PAYWALL_PATH_REGEXES:
        if rx.search(u):
            return True
    return False


def step_fetch_full_text(items, delay_sec=1.0):
    """Step 2.5: enrich surviving items with fetched article body.

    Runs AFTER dedup + age filter, BEFORE translate — so only the items that
    actually need translation (5-15 items) trigger an HTTP request, not the
    raw 150+ RSS entries.

    Items whose source declares ``"no_full_text": true`` (e.g. Google News,
    whose RSS <link> is a redirect URL) are special-cased: we run them
    through ``google-news-api`` ``decode_url`` to extract the real publisher
    URL (Spiegel, WELT, etc.), then fetch that. If decode or fetch fails,
    we drop the item (rather than fall back to title+summary — Task 12).

    Paywall handling (Task 12, 2026-08-09): ``_fetch_google_news_text``
    returns ``"<PAYWALLED>"`` if it detects a known paywall via URL
    pattern (WELT+ / Spiegel+ / FAZ+) or post-fetch char count (<1000).
    Such items are dropped here so LLM tokens aren't wasted translating a
    "this article is paywalled" stub.

    Paywall handling for RSS sources (Task 13, 2026-08-09 + Plan 1,
    2026-08-17): previously, any RSS item whose URL matched the paywall
    host blacklist (Spiegel, WELT, Manager Magazin, etc.) was dropped at
    Layer 0 BEFORE fetching — wasting the preview body. Plan 1 changes
    this: host-blacklist-only matches (no path pattern) are now fetched
    and judged by ``_decide_paywall_preview(char_count, had_paywall_hint)``.
    Path-pattern matches (``/plus6a...``, ``/a-{uuid}``, ``/premium/``,
    etc.) are still early-dropped since those URLs return zero content.
    After ``rss_fetch`` runs, items flagged with ``had_paywall_hint=True``
    are again evaluated by ``_decide_paywall_preview`` so the same
    threshold is applied uniformly — both for host-blacklist items
    (detected pre-fetch) and unlisted-host items where rss_fetch's
    content heuristic fired post-fetch.
    """
    keep = []
    skipped_paywall = 0
    skipped_empty = 0
    preview_fetched = 0  # host-blacklist items we re-routed through fetch
    for it in items:
        item_url = it.get("url", "")

        # Layer 0a (Plan 1, 2026-08-17): path-pattern early drop.
        # WELT+ hex, Spiegel+/MM uuid, FAZ+ /premium/, /paywall/, /epaper/
        # — these URLs return zero content, so don't even try to fetch.
        if _is_paywall_url_pattern_only(item_url):
            it["_paywalled"] = True
            it["_paywall_reason"] = "rss-url-pattern"
            skipped_paywall += 1
            logger.info(
                "[paywall-detected] rss-url-pattern | %s | %s",
                item_url[:80], it.get("title", "")[:50],
            )
            continue

        # Layer 0b (Plan 1, 2026-08-17): host-blacklist early-drop REMOVED.
        # Previously, an item on www.spiegel.de / www.welt.de / etc. was
        # dropped here with reason "rss-url-pattern" even though some of
        # those pages actually expose a usable preview body. New path:
        #   - If the source already supplied a body (no_full_text, handled
        #     by Google News path), use that.
        #   - Otherwise, run rss_fetch.fetch_full_text(url) once, set
        #     char_count + had_paywall_hint, and mark _fetch_done=True so
        #     fetch_full_text_for_items() below skips it.
        #   - The post-fetch Layer 2 will then call _decide_paywall_preview
        #     to decide keep-as-preview / drop / keep-as-full.
        if it.get("no_full_text"):
            full_text = _fetch_google_news_text(it, delay_sec=delay_sec)
            if full_text == "<PAYWALLED>":
                skipped_paywall += 1
                continue  # drop silently; don't translate
            if not full_text or len(full_text) < 30:
                skipped_empty += 1
                logger.info(
                    "[skip no-full-text empty] %s | %s",
                    it.get("source_name"), it.get("title", "")[:60],
                )
                continue
            it["full_text"] = full_text
            it["content_html"] = full_text
            it["_fetch_done"] = True  # mark so fetch_full_text_for_items skips
            keep.append(it)
            continue

        # Plan 1: host-blacklist items (e.g. Spiegel Wirtschaft RSS) get a
        # pre-emptive single-item fetch so we can decide whether their
        # preview body is worth keeping. The result is stored on the item
        # so the batch fetch below (fetch_full_text_for_items) skips it
        # via the _fetch_done flag.
        host_blacklisted = bool(item_url) and _is_paywalled_url(item_url)
        if host_blacklisted:
            try:
                fetch_result = rss_fetch.fetch_full_text(item_url)
            except Exception as e:  # noqa: BLE001
                logger.info(
                    "[paywall-preview fetch failed] %s | %s | %s",
                    it.get("source_name", "?"), item_url[:80],
                    type(e).__name__,
                )
                fetch_result = {
                    "full_text": None, "paywalled": False, "char_count": 0,
                    "had_paywall_hint": False, "error": f"{type(e).__name__}: {e}",
                }
            char_count = int(fetch_result.get("char_count") or 0)
            had_hint = bool(fetch_result.get("had_paywall_hint"))
            decision, content_kind = _decide_paywall_preview(char_count, had_hint)
            it["_fetch_done"] = True
            it["_host_blacklisted"] = True
            it["full_text_chars"] = char_count
            it["full_text_paywalled"] = bool(fetch_result.get("paywalled"))
            it["had_paywall_hint"] = had_hint
            it["full_text_error"] = fetch_result.get("error")
            text = fetch_result.get("full_text") or ""
            if decision == "keep":
                # paywall-preview / short-paywall-preview: keep body for vault
                it["full_text"] = text
                it["content_html"] = text
                it["_paywall_preview_kept"] = True
                it["_paywall_preview_kind"] = content_kind
                it["content_kind"] = content_kind
                it["_paywall_reason"] = "host-blacklist-preview"
                preview_fetched += 1
                logger.info(
                    "[paywall-preview kept] %s | %d chars, hint=%s → %s",
                    it.get("source_name", "?"), char_count, had_hint, content_kind,
                )
                keep.append(it)
                continue
            if decision == "drop":
                it["_paywalled"] = True
                it["_paywall_reason"] = "host-blacklist-short"
                skipped_paywall += 1
                logger.info(
                    "[paywall-detected] host-blacklist short (%d chars) | %s | %s",
                    char_count, it.get("source_name", "?"), item_url[:80],
                )
                continue
            # decision == "full": had_hint=False on a blacklisted host —
            # the publisher returned a usable body. Fall through to the
            # normal full-text batch so translate sees the body.
            it["full_text"] = text
            it["content_html"] = text
            it["content_kind"] = content_kind  # "full-article"
            logger.info(
                "[paywall-bypassed] %s host-blacklisted but body usable (%d chars)",
                it.get("source_name", "?"), char_count,
            )
            keep.append(it)
            continue

        # Default path: not flagged, not blacklisted, not no_full_text —
        # defer to the batch fetch below.
        keep.append(it)

    if skipped_paywall:
        logger.info("[paywall] dropped %d item(s) at fetch stage", skipped_paywall)
    if skipped_empty:
        logger.info("[no_full_text] skipped %d item(s) with empty content", skipped_empty)
    if preview_fetched:
        logger.info(
            "[paywall-preview] kept %d item(s) as paywall-preview / short-paywall-preview",
            preview_fetched,
        )

    result = rss_fetch.fetch_full_text_for_items(keep, delay_sec=delay_sec)
    # Layer 2 (Task 13 + Plan 1): post-fetch, apply paywall decision.
    #
    # Two flavours of paywall signal can surface here:
    #   (a) ``_paywalled`` / ``paywalled_flag`` set by rss_fetch itself
    #       (cookie banners, "Abo erforderlich" body text, etc.)
    #   (b) ``had_paywall_hint=True`` from rss_fetch's body-text scan
    #
    # Plan 1 (2026-08-17) routes (b) through ``_decide_paywall_preview``
    # so we keep the preview body (800-1500c → short-paywall-preview;
    # >=1500c → paywall-preview) instead of dropping. Only items that
    # BOTH have a hint AND come back with <800 chars are dropped.
    # Items without a hint fall through to the full-article path.
    final = []
    extra_dropped = 0
    extra_preview = 0
    for it in result:
        had_hint = bool(it.get("had_paywall_hint") or it.get("full_text_paywalled"))
        if it.get("_paywalled") or (it.get("paywalled_flag") and not had_hint):
            extra_dropped += 1
            logger.info(
                "[paywall-detected] rss_fetch hint | %s | %s",
                it.get("url", "")[:80], it.get("title", "")[:50],
            )
            continue
        if had_hint and not it.get("_paywall_preview_kept"):
            char_count = int(it.get("full_text_chars") or 0)
            decision, content_kind = _decide_paywall_preview(char_count, True)
            if decision == "drop":
                it["_paywalled"] = True
                it["_paywall_reason"] = "rss-fetch-hint-short"
                extra_dropped += 1
                logger.info(
                    "[paywall-detected] rss-fetch hint short (%d chars) | %s",
                    char_count, it.get("url", "")[:80],
                )
                continue
            if decision == "keep":
                it["_paywall_preview_kept"] = True
                it["_paywall_preview_kind"] = content_kind
                it["content_kind"] = content_kind
                it["_paywall_reason"] = "rss-fetch-hint-preview"
                extra_preview += 1
                logger.info(
                    "[paywall-preview kept] rss-fetch hint | %d chars → %s",
                    char_count, content_kind,
                )
        final.append(it)
    if extra_dropped:
        logger.info(
            "[paywall] dropped %d more item(s) via rss_fetch hint", extra_dropped
        )
    if extra_preview:
        logger.info(
            "[paywall-preview] kept %d more item(s) via rss_fetch hint", extra_preview
        )
    return final


def step_translate(items, chunk_size=2):
    """Translate+analyze each item.

    Per-item mode (2026-08-07): chunked batch (chunk_size=2/4/8) keeps hanging
    on ollama-cloud — large prompts (~10K+ tokens for 5K-char full_text × 2)
    cause the backend to silently drop requests after the first batch. Per-item
    calls keep each prompt under 5K tokens and have been verified to return
    reliably. The inter-item 3s cooldown prevents sequential LLM queue buildup.

    The `chunk_size` parameter is kept for CLI compatibility but ignored.
    """
    import time as _time
    out = []
    for i, it in enumerate(items):
        result = translate.analyze_item(it)
        out.append(result if isinstance(result, dict) else it)
        if i < len(items) - 1:
            _time.sleep(3.0)  # per-item cooldown
    return out


def step_rank(items):
    return translate.rank_by_relevance(items)


# --------------------------------------------------------------------------- #
# Source quota + date filter
# --------------------------------------------------------------------------- #

# Per-source cap. Handelsblatt is the specialist real-estate feed → higher
# quota; other Wirtschaft feeds → lower. Configurable via CLI.
_DEFAULT_QUOTAS = {
    "Handelsblatt Immobilien": 8,
}
_DEFAULT_OTHER_QUOTA = 5


def filter_by_age(items, max_days):
    """Keep only items whose pub_date is within the last `max_days` days.
    Items with no pub_date are kept (assumed recent)."""
    if max_days is None or max_days <= 0:
        return items
    cutoff = datetime.now(timezone.utc) - timedelta(days=max_days)
    out = []
    for it in items:
        pub = it.get("pub_date")
        if pub is None:
            out.append(it)  # no date → keep
            continue
        if isinstance(pub, str):
            # parse ISO string
            try:
                pub = datetime.fromisoformat(pub.replace("Z", "+00:00"))
            except Exception:
                out.append(it)
                continue
        if pub.tzinfo is None:
            pub = pub.replace(tzinfo=timezone.utc)
        if pub >= cutoff:
            out.append(it)
    return out


def apply_source_quota(items, primary_max=8, other_max=3):
    """Cap items per source to prevent any single feed from dominating.

    Rules (per user's 2026-08-07 spec):
      - "Primary" sources (Handelsblatt Immobilien — specialist feed) cap at
        `primary_max` items.
      - All other sources cap at `other_max` items.
      - No minimum floor: if a source has only 1 quality item, that 1 is kept.
        The final relevance judgement is left to the LLM (--min-relevance).
      - Items within each bucket are kept in their incoming (rank) order.

    Edge cases:
      - `primary_max` / `other_max` <= 0 → disable upper cap for that tier
        (not recommended: this can let a noisy source flood the digest).
    """
    if not items:
        return items
    buckets: dict[str, list] = {}
    for it in items:
        buckets.setdefault(it.get("source_name", "?"), []).append(it)
    out = []
    for src, src_items in buckets.items():
        upper = primary_max if (src in _DEFAULT_QUOTAS and primary_max > 0) \
                else (other_max if other_max > 0 else len(src_items))
        out.extend(src_items[:upper])
    return out


def filter_by_relevance(items, min_score=5):
    """Drop items whose LLM-assigned relevance_to_buyer is below min_score.

    Items without a relevance score (None) are kept (safer default).
    """
    if not items or min_score is None or min_score <= 0:
        return items
    out = []
    for it in items:
        score = it.get("relevance_to_buyer")
        if score is None:
            out.append(it)  # no score → keep
            continue
        try:
            if int(score) >= min_score:
                out.append(it)
        except (TypeError, ValueError):
            out.append(it)
    return out


def step_write_vault(items, cfg, content_kind: str = "longform",
                     strict_traditional: bool = True):
    """Write each item as a markdown file + the daily index file.

    Wipes the existing daily folder BEFORE writing so re-runs don't
    accumulate stale items. The wipe is opt-in: pass cfg.vault.wipe=False
    (or env VAULT_KEEP=1) to disable for debugging.

    ``content_kind`` (Task 7, 2026-08-09): ``"short-summary"`` or
    ``"longform"`` (default). Controls the file suffix in
    ``obsidian.write_news_item`` (``_summary.md`` vs ``_longform.md``).
    Wiping is scoped to ``*<suffix>.md`` so a short-summary run can't
    delete long-form artifacts (and vice versa) when both modes coexist
    in the same daily folder.

    Plan 1 (2026-08-17): each item may carry its own ``content_kind``
    (e.g. ``"paywall-preview"``, ``"short-paywall-preview"``) set by
    :func:`step_fetch_full_text`. We honour that per-item kind when
    present (so a paywall-preview file gets ``_paywallpreview.md``
    suffix) and fall back to the function-level ``content_kind`` for
    regular items. Wipe is now scoped to the union of all suffixes that
    will be written in this run.

    Returns a dict mapping each item to its written output path (used by
    :func:`step_mark_processed` to record ``output_path`` in the ledger).
    """
    import shutil
    vault_cfg = cfg.get("vault", {})
    vault_root = vault_cfg.get("root")
    if not vault_root:
        raise ValueError("cfg.vault.root missing — check config/pipeline.json")
    github_url = f"{cfg.get('github', {}).get('owner', '')}/{cfg.get('github', {}).get('repo', '')}"
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    out_dir = os.path.join(vault_root, "Daily", date_str)
    # Plan 1: collect the suffix for each item so we wipe every suffix
    # we'll write this run, not just the global content_kind's suffix.
    # Use the suffix map from obsidian so the wipe stays in sync if new
    # content_kind values are added later.
    per_item_kinds = [
        it.get("content_kind") or content_kind for it in items
    ]
    suffixes = {
        obsidian._CONTENT_KIND_SUFFIX.get(k, "_longform")
        for k in per_item_kinds
    }
    # Wipe only files of *this* content_kind so short + long + paywall
    # can coexist. Disable with cfg.vault.wipe=False or VAULT_KEEP=1.
    wipe = vault_cfg.get("wipe", True) and os.environ.get("VAULT_KEEP") != "1"
    wiped = False
    if wipe and os.path.isdir(out_dir):
        for suffix in suffixes:
            for p in Path(out_dir).glob(f"*{suffix}.md"):
                p.unlink()
        wiped = True
    item_paths: dict[int, str] = {}
    for i, it in enumerate(items):
        # normalize deprecated content_kind values (Plan 1 era). Plan 5 dropped
        # "full-article" — treat it as "longform" so legacy items still write.
        item_kind = it.get("content_kind") or content_kind
        if item_kind == "full-article":
            item_kind = "longform"
        path = obsidian.write_news_item(it, vault_root, date_str,
                                        strict_traditional=strict_traditional,
                                        content_kind=item_kind)
        item_paths[i] = path
    # Index file is shared across all modes in the same daily folder —
    # only the first call writes it.
    index_path = obsidian.write_daily_index(items, vault_root, date_str, github_url)
    kinds_summary = ",".join(sorted(suffixes)) or "?"
    print(f"  wrote {len(item_paths) + 1} files under {vault_root}/Daily/{date_str}/"
          f" (suffixes={kinds_summary}{' wiped existing' if wiped else ''})")
    return {"items": items, "item_paths": item_paths, "index_path": index_path}


def step_send_discord(items, cfg, dry_run):
    """Send the daily digest to Discord. dry_run=True just prints the would-be payload length.

    Sends 1 header + N body embeds (configurable via discord.items_per_embed,
    default 3) so each embed stays well under Discord's 4096-char limit and
    the burst never triggers per-channel rate limits.

      - Header embed — index of all titles + source/date.
      - Body embeds — every `items_per_embed` items, with full Chinese
        summary + URL.

    `cfg.discord.per_item_summary_chars` (default 600) caps each item's
    summary so 3 items + headers + URLs comfortably fit in one embed.

    Returns a dict ``{"ok": bool, "per_item": {item_index: [message_ids]}}``
    so :func:`step_update_side_effects` can backfill the ledger with the
    Discord message ids. ``per_item`` is keyed by the item's position in
    the input list.
    """
    result: dict = {"ok": False, "per_item": {}}
    discord_cfg = cfg.get("discord", {})
    alias = discord_cfg.get("channel_alias", "headlines")
    summary_max = int(discord_cfg.get("summary_chars_per_embed", 3800))
    per_item_max = int(discord_cfg.get("per_item_summary_chars", 600))
    items_per_embed = int(discord_cfg.get("items_per_embed", 3))
    if not items:
        print("  no items to send")
        return result
    mod = _import_discord()
    channel_id = mod._resolve_channel(alias)  # noqa: SLF001 (intentional; alias→id mapping lives there)

    if dry_run:
        body_count = (len(items) + items_per_embed - 1) // items_per_embed
        print(f"  [dry-run] would send 1 header + {body_count} body embeds to "
              f"channel '{alias}' ({len(items)} items, {items_per_embed}/embed)")
        return result

    # ---- Header embed ----
    header_lines = [f"📰 德國房地產每日頭條 — {datetime.utcnow().strftime('%Y-%m-%d')} ({len(items)} 則)"]
    for i, it in enumerate(items, 1):
        title = it.get("title_zh") or it.get("title", "")
        src = it.get("source_name", "?")
        header_lines.append(f"{i}. [{src}] {title}")
    header_body = "\n".join(header_lines)[:3800]
    try:
        header_resp = mod.send_to_channel(channel_id, header_body, as_embed=True,
                                          title="📰 德國房地產每日頭條",
                                          color=0x3498db)
    except Exception as exc:  # noqa: BLE001
        print(f"  [discord] header embed raised: {type(exc).__name__}: {exc}",
              flush=True)
        traceback.print_exc()
        header_resp = None
    header_msg_id = None
    if isinstance(header_resp, dict):
        header_msg_id = header_resp.get("id")
    elif isinstance(header_resp, str):
        header_msg_id = header_resp
    if header_msg_id:
        # Attribute header to index 0 if available, else drop it
        if 0 in result["per_item"]:
            result["per_item"][0].append(header_msg_id)
        else:
            result["per_item"][0] = [header_msg_id]
    else:
        ok = False

    # ---- Body embeds: ONE message per item (Plan 8 — Plan 3 spec) ----
    # Before Plan 8 the loop bundled items_per_embed (default 3) items into
    # one embed, making ✅/❌/🟡 reactions useless (a single emoji could
    # not be attributed to a single article). Now each item gets its own
    # embed so ``discord_picks.py`` can record one ReactionPicks row per
    # article. The header embed above is still sent for context but is not
    # required for scoring.
    ok = True
    for i, it in enumerate(items, 1):
        title = it.get("title_zh") or it.get("title", "")
        summary = it.get("summary_zh") or ""
        if len(summary) > per_item_max:
            summary = summary[:per_item_max] + "…(截斷)"
        url = it.get("url", "")
        src = it.get("source_name", "?")
        body_lines = [
            f"### {i}. {title}",
            f"*來源：{src}*",
        ]
        if summary:
            body_lines.append(summary)
        if url:
            body_lines.append(f"🔗 {url}")
        body = "\n".join(body_lines)[:summary_max]
        # Color gradient blue → green → teal across items.
        color = 0x2ecc71 + (i * 0x050505)
        try:
            resp = mod.send_to_channel(
                channel_id,
                body,
                as_embed=True,
                title=f"📖 第 {i} 則",
                color=color,
            )
        except Exception as exc:  # noqa: BLE001
            print(f"  [discord] body embed #{i} raised: "
                  f"{type(exc).__name__}: {exc}", flush=True)
            traceback.print_exc()
            resp = None
        msg_id = None
        # Plan 12f (2026-08-21): discord_sender.send_to_channel returns
        # `{"ok": bool, "message_ids": [...], "error": str|None}`,
        # NOT `{"id": ...}`. Previous code looked up wrong field, so
        # msg_id was always None and per_item ledger was silently empty,
        # causing Airtable discord=False. Use the correct field.
        if isinstance(resp, dict):
            if resp.get("ok") and resp.get("message_ids"):
                msg_id = resp["message_ids"][0]
            elif not resp.get("ok"):
                print(f"  [discord] body embed #{i} send failed: "
                      f"{resp.get('error')!r}", flush=True)
        elif isinstance(resp, str):
            msg_id = resp
        if msg_id:
            result["per_item"].setdefault(i - 1, []).append(msg_id)
        if not resp or not msg_id:
            ok = False
    result["ok"] = ok
    return result


def step_push_github(dry_run):
    """Stage + commit + push immobilien-kb/ to GitHub.

    Plan 7 (2026-08-19): the previous version just shell-out to
    ``push_to_github.py``, which pushes the *current* HEAD via dulwich /
    paramiko. If the working tree has untracked vault files (Reddit/*,
    YouTube/*, Daily/*) and nobody ran ``git add`` + ``git commit`` first,
    the push is a silent no-op — local HEAD == remote HEAD, the pack has
    no new objects, and ``[OK] 9. push_to_github`` is logged while
    nothing actually leaves the box. Mirror ``youtube_daily.py:319-323``
    and do the add + commit here before delegating the wire push.

    Returns ``{"pushed": bool, "commit_sha": str|None}`` so
    :func:`step_update_side_effects` can backfill the ledger with the
    GitHub commit SHA.
    """
    out: dict = {"pushed": False, "commit_sha": None}
    if dry_run:
        print("  [dry-run] would git add + commit + push immobilien-kb/")
        return out
    today = datetime.utcnow().strftime("%Y-%m-%d")
    msg = f"news-kb: {today} daily digest"

    cmds = [
        ["git", "-C", _PROJECT_DIR, "add", "immobilien-kb/"],
        ["git", "-C", _PROJECT_DIR, "-c", "user.email=ado@hermes.local",
         "-c", "user.name=Ado", "commit", "-m", msg],
    ]
    for cmd in cmds:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if proc.returncode != 0:
            # "nothing to commit" is fine — vault already committed earlier today.
            if "nothing to commit" in proc.stderr or "nothing to commit" in proc.stdout:
                print(f"  [git] {' '.join(cmd[:4])}: nothing to commit (ok)")
                continue
            print(f"  [git] {' '.join(cmd[:4])} FAILED rc={proc.returncode}")
            print(f"  [git] stderr: {proc.stderr.strip()[-300:]}")
            return out

    # Now the paramiko push (bypasses @-mangle on the Discord-side SSH).
    print(f"  exec: python3 {_PUSH_TO_GITHUB}")
    res = subprocess.run([sys.executable, _PUSH_TO_GITHUB], cwd=_PROJECT_DIR,
                         capture_output=True, text=True, timeout=120)
    if res.stdout:
        print(f"  [push-to-github stdout] {res.stdout.strip()[-300:]}")
    if res.stderr:
        print(f"  [push-to-github stderr] {res.stderr.strip()[-300:]}")
    out["pushed"] = res.returncode == 0
    if out["pushed"]:
        sha_proc = subprocess.run(
            ["git", "-C", _PROJECT_DIR, "rev-parse", "HEAD"],
            capture_output=True, text=True, timeout=10,
        )
        if sha_proc.returncode == 0:
            out["commit_sha"] = sha_proc.stdout.strip()
    return out


# --------------------------------------------------------------------------- #
# Step 10: ledger writes (Task 4)
# --------------------------------------------------------------------------- #

def _build_news_metadata(item: dict, date_str: str) -> dict:
    """Build the metadata JSON for :func:`ProcessedStore.mark_processed`.

    Task 12 (2026-08-09) also persists ``decoded_url`` (the real publisher
    URL after Google News ``decode_url``) and ``paywalled`` (whether the
    item was dropped at fetch time). Without these fields, you can't tell
    from the Airtable ledger whether a record came from a paywalled
    publisher or what the real article URL was.

    Plan 1 (2026-08-17) also persists ``paywall_preview_kept`` and
    ``paywall_preview_kind`` for items whose body was kept as a
    paywall-preview (host-blacklist item with >= 800 chars of preview).
    """
    epoch = item.get("pub_date_epoch")
    if epoch is None and isinstance(item.get("pub_date"), str):
        try:
            epoch = int(datetime.fromisoformat(
                item["pub_date"].replace("Z", "+00:00")
            ).timestamp())
        except Exception:  # noqa: BLE001
            epoch = None
    meta = {
        "epoch": epoch,
        "source": item.get("source_name", "?"),
        "lang": item.get("source_language", "de"),
        "relevance_rank": item.get("relevance_rank"),
        "relevance_to_buyer": item.get("relevance_to_buyer"),
        "date": date_str,
    }
    # Task 12: capture decoded real publisher URL + paywall metadata.
    if item.get("_decoded_url"):
        meta["decoded_url"] = item["_decoded_url"]
    if item.get("_original_gn_url"):
        meta["original_gn_url"] = item["_original_gn_url"]
    if item.get("_paywalled"):
        meta["paywalled"] = True
        meta["paywall_reason"] = item.get("_paywall_reason", "unknown")
    # Plan 1 (2026-08-17): paywall preview markers in metadata JSON.
    if item.get("_paywall_preview_kept"):
        meta["paywall_preview_kept"] = True
        meta["paywall_preview_kind"] = item.get("_paywall_preview_kind", "")
    if item.get("had_paywall_hint") is not None:
        meta["had_paywall_hint"] = bool(item.get("had_paywall_hint"))
    if item.get("full_text_chars") is not None:
        meta["full_text_chars"] = int(item.get("full_text_chars") or 0)
    return meta


def step_mark_processed(
    items: List[dict],
    item_paths: dict,
    *,
    store: Optional[ProcessedStore] = None,
    run_id: str = "",
    date_str: str = "",
    article_type: Optional[str] = None,
) -> dict:
    """Mark every vault-written item as processed in the Airtable ledger.

    Failure of any single ``mark_processed`` is logged but does **not**
    raise — the vault write already succeeded, and the next run will
    simply re-process the item. Worst case is a duplicate vault write,
    not a missed dedup.

    ``article_type`` (Task 7, 2026-08-09): optional. When provided, every
    record written here is tagged with the same value (``short-summary``
    or ``long-form``). When None, the field is omitted entirely so older
    callers keep working unchanged.

    Returns ``{"ok": int, "errors": [(idx, item, exc_str), ...], "record_ids": [...]}``.
    """
    if store is None:
        store = _get_store()
    errors: list = []
    record_ids: list = []
    for idx, item in enumerate(items):
        url_norm = item.get("url_normalized") or normalize_url(item.get("url", ""))
        if not url_norm:
            logger.warning(
                "mark_processed: skipping item idx=%d (no url_normalized)",
                idx,
            )
            continue
        title = item.get("title_zh") or item.get("title", "")
        try:
            record_id = store.mark_processed(
                source_type="news",
                source_id=url_norm,
                title=title,
                channels=["news.daily_top3"],
                pipeline_run_id=run_id,
                output_path=item_paths.get(idx),
                metadata=_build_news_metadata(item, date_str),
                tags=[],
                article_type=article_type,
                # Plan 1 (2026-08-17): stamp the paywall preview flags
                # so the Airtable ledger can be filtered / reported on
                # by ``paywall_preview_kept=True``.
                paywall_preview_kept=(
                    True if item.get("_paywall_preview_kept") else None
                ),
                paywall_preview_kind=item.get("_paywall_preview_kind") or None,
            )
            record_ids.append(record_id)
            logger.info(
                "marked processed: news | %s -> %s (article_type=%s)",
                url_norm, record_id, article_type,
            )
        except Exception as e:  # noqa: BLE001
            logger.error(
                "mark_processed failed for idx=%d url=%s: %s",
                idx, url_norm, e,
            )
            errors.append((idx, item, str(e)))
    return {"ok": len(record_ids), "errors": errors, "record_ids": record_ids}


def step_update_side_effects(
    items: List[dict],
    *,
    discord_summary: Optional[dict] = None,
    github_summary: Optional[dict] = None,
    store: Optional[ProcessedStore] = None,
) -> dict:
    """Backfill discord_message_id + github_commit_sha into the ledger.

    Best-effort: any per-item failure is logged but never raised.
    Returns ``{"ok": int, "errors": [...]}``.
    """
    if store is None:
        store = _get_store()
    errors: list = []
    ok_count = 0
    per_item = (discord_summary or {}).get("per_item", {}) if discord_summary else {}
    commit_sha = (github_summary or {}).get("commit_sha") if github_summary else None

    for idx, item in enumerate(items):
        url_norm = item.get("url_normalized") or normalize_url(item.get("url", ""))
        if not url_norm:
            continue
        msg_ids = per_item.get(idx, []) if per_item else []
        # Flatten list-of-lists / single str / None into a comma-separated str
        if isinstance(msg_ids, list):
            msg_ids_flat: list = []
            for m in msg_ids:
                if isinstance(m, list):
                    msg_ids_flat.extend(m)
                else:
                    msg_ids_flat.append(m)
            msg_ids_str = ",".join(str(m) for m in msg_ids_flat if m) or None
        else:
            msg_ids_str = str(msg_ids) if msg_ids else None
        if not msg_ids_str and not commit_sha:
            continue
        try:
            store.update_side_effects(
                source_hash=make_hash("news", url_norm),
                discord_message_id=msg_ids_str,
                github_commit_sha=commit_sha,
            )
            ok_count += 1
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "update_side_effects failed for %s: %s", url_norm, e,
            )
            errors.append((idx, item, str(e)))
    return {"ok": ok_count, "errors": errors}


# --------------------------------------------------------------------------- #
# Main orchestration.
# --------------------------------------------------------------------------- #

def run_pipeline(dry_run=False, source_limit=None, chunk_size=8,
                 max_days=7, quota_primary=8, quota_other=3,
                 min_relevance=3, min_quick_score=4,
                 skip_store=False, pipeline_run_id="",
                 mode: str = "short"):
    t_start = time.time()
    run_id = pipeline_run_id or datetime.now(timezone.utc).strftime(
        "news-%Y%m%d-%H%M%S"
    )
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    content_kind = "short-summary" if mode == "short" else "longform"
    article_type = content_kind
    print(f"[news_daily] starting at {datetime.utcnow().isoformat()}Z "
          f"(dry_run={dry_run}, source_limit={source_limit}, "
          f"mode={mode}, run_id={run_id}, skip_store={skip_store})")

    # Step 1: load config
    cfg = _step("1. load_config", step_load_config)
    if cfg is None:
        print("Aborting — config load failed.")
        return 1

    sources = config_loader.get_sources()
    if source_limit is not None and source_limit > 0:
        sources = sources[:source_limit]
        print(f"  --limit {source_limit}: using {len(sources)} sources")

    # Step 2: fetch RSS
    items = _step("2. fetch_rss", lambda: step_fetch(sources))
    if items is None:
        items = []
    print(f"  raw fetched: {len(items)} items")

    # Step 3: filter
    items = _step("3. filter_keywords", lambda: step_filter(items)) or []

    # Step 3b: quick relevance score (title-only, cheap LLM pre-filter)
    if min_quick_score is not None and min_quick_score > 0:
        def _do_quick():
            translate.quick_score_items(items, min_score=min_quick_score, chunk_size=12)
            return [it for it in items if it.get("quick_score") is None or it.get("quick_score", 0) >= min_quick_score]
        before = len(items)
        result = _step("3b. quick_score", _do_quick)
        items = result if result is not None else []
        print(f"  quick filter (≥ {min_quick_score}/10): {before} → {len(items)} items", flush=True)

    # Step 4: dedup (Airtable-backed, see filter_processed)
    items = _step("4. dedup_cross_source",
                  lambda: step_dedup(items, skip_store=skip_store,
                                     days_threshold=max_days or 3)) or []

    # Step 4b: age filter (keep only items within max_days)
    if max_days is not None and max_days > 0:
        before = len(items)
        items = filter_by_age(items, max_days)
        print(f"  age filter (≤ {max_days}d): {before} → {len(items)} items", flush=True)

    # Step 4b': apply source quota EARLY (Task 10, 2026-08-09) — without this,
    # Google News alone can flood the pipeline with 30+ items and balloon
    # translate time / token usage. Cap per-source BEFORE full-text fetch
    # and translation so we don't waste work on items that will be dropped.
    if quota_primary > 0 or quota_other > 0:
        before = len(items)
        items = apply_source_quota(
            items,
            primary_max=quota_primary,
            other_max=quota_other,
        )
        from collections import Counter as _C
        dist = _C(it.get("source_name", "?") for it in items)
        print(f"  source quota (primary≤{quota_primary}, other≤{quota_other}): "
              f"{before} → {len(items)} items | {dict(dist)}", flush=True)

    # Step 4c: fetch full article body (only for items that survived every
    # gate so far — typically 5-15 items, not the raw 150+ RSS entries).
    items = _step("4c. fetch_full_text",
                  lambda: step_fetch_full_text(items, delay_sec=1.0)) or []

    # Step 5: translate (batched LLM)
    items = _step("5. translate_batch",
                  lambda: step_translate(items, chunk_size=chunk_size)) or []

    # Step 5b: relevance filter (drop low LLM-scored items)
    if min_relevance is not None and min_relevance > 0:
        before = len(items)
        items = filter_by_relevance(items, min_score=min_relevance)
        print(f"  relevance filter (≥ {min_relevance}/10): {before} → {len(items)} items", flush=True)

    # Step 6: rank
    items = _step("6. rank_by_relevance", lambda: step_rank(items)) or []
    # Tag each item with its position so obsidian frontmatter knows the rank.
    for i, it in enumerate(items):
        it["relevance_rank"] = i + 1

    # Step 6b (legacy): keep a no-op quota pass for callers that disabled
    # the Step 4b' early quota. Step 4b' already enforces the cap; this
    # pass is a no-op when items are already within quota.
    if quota_primary > 0 or quota_other > 0:
        before = len(items)
        items = apply_source_quota(
            items,
            primary_max=quota_primary,
            other_max=quota_other,
        )
        from collections import Counter as _C
        dist = _C(it.get("source_name", "?") for it in items)
        print(f"  source quota re-check (no-op expected): "
              f"{before} → {len(items)} items | {dict(dist)}", flush=True)

    # Print a dry-run summary so --dry-run is verifiable without side effects.
    if dry_run:
        print("\n=== DRY-RUN SUMMARY ===")
        print(f"total items after dedup: {len(items)}")
        for i, it in enumerate(items[:10], 1):
            print(f"  {i}. [{it.get('source_name')}] {it.get('title_zh') or it.get('title')}")
        if len(items) > 10:
            print(f"  … (+{len(items) - 10} more)")
        print(f"\ntotal elapsed: {time.time() - t_start:.2f}s")
        return 0

    # Step 7: write vault
    vault_summary = _step("7. write_vault",
                          lambda: step_write_vault(items, cfg, content_kind=content_kind))
    vault_items: List[dict] = list(items) if isinstance(items, list) else []
    item_paths: dict = {}
    if isinstance(vault_summary, dict):
        _vi = vault_summary.get("items", items)
        vault_items = list(_vi) if isinstance(_vi, list) else []
        item_paths = vault_summary.get("item_paths", {}) or {}

    # Step 8: send discord (capture message_ids per item)
    discord_summary = _step("8. send_discord",
                            lambda: step_send_discord(vault_items, cfg,
                                                      dry_run=False))
    if not isinstance(discord_summary, dict):
        discord_summary = None

    # Step 9: push to github (capture commit_sha)
    github_summary = _step("9. push_to_github",
                           lambda: step_push_github(dry_run=False))
    if not isinstance(github_summary, dict):
        github_summary = None

    # Step 10: ledger writes (Task 4)
    if not skip_store and vault_items:
        _step("10a. mark_processed",
              lambda: step_mark_processed(
                  vault_items, item_paths,
                  run_id=run_id, date_str=date_str,
                  article_type=article_type,
              ))
        _step("10b. update_side_effects",
              lambda: step_update_side_effects(
                  vault_items,
                  discord_summary=discord_summary,
                  github_summary=github_summary,
              ))
    elif skip_store:
        print("\n[Step 10] skipped (--skip-store)")

    print(f"\n[news_daily] done — total {time.time() - t_start:.2f}s")
    return 0


def main():
    p = argparse.ArgumentParser(description="Daily German real-estate news pipeline.")
    p.add_argument("--dry-run", action="store_true",
                   help="Run steps 1-6 only (no vault / discord / github side effects).")
    p.add_argument("--mode", choices=("short", "long"), default="short",
                   help="Pipeline output mode. ``short`` (default) is the "
                        "default lightweight short-summary; ``long`` runs "
                        "the full editorial commentary pass. Both modes "
                        "produce a vault file with a mode-appropriate "
                        "suffix (``_summary.md`` vs ``_longform.md``) and "
                        "write ``article_type`` to the Airtable ledger.")
    p.add_argument("--limit", type=int, default=None,
                   help="Limit the number of RSS sources fetched (for testing).")
    p.add_argument("--chunk-size", type=int, default=2,
                   help="Batch size for the LLM translation step. chunk=2 keeps each "
                        "prompt small (~10K tokens total for 5K-char full_text × 2) "
                        "which avoids ollama-cloud hangs. chunk=4/8 produced prompts "
                        "large enough to silently hang. Slower per-batch but reliable.")
    p.add_argument("--max-days", type=int, default=7,
                   help="Only include items published within the last N days. "
                        "0 disables age filtering. Also drives filter_processed's "
                        "recent-news window.")
    p.add_argument("--quota-primary", type=int, default=8,
                   help="Max items per run from primary source (Handelsblatt). "
                        "0 disables source quota.")
    p.add_argument("--quota-other", type=int, default=5,
                   help="Max items per run from each other source.")
    p.add_argument("--min-relevance", type=int, default=3,
                   help="Drop items whose LLM-assigned relevance_to_buyer is "
                        "below this score (0-10). 0 disables the filter.")
    p.add_argument("--min-quick-score", type=int, default=4,
                   help="Title-only pre-filter (cheaper than --min-relevance). "
                        "Drops items whose quick_score is below this. 0 disables.")
    p.add_argument("--skip-store", action="store_true",
                   help="Bypass ProcessedStore (fall back to legacy in-process "
                        "dedup). Useful for local debugging when the Airtable "
                        "PAT is unavailable.")
    p.add_argument("--pipeline-run-id", default="",
                   help="Override the auto-generated pipeline run id "
                        "(default: news-YYYYMMDD-HHMMSS).")
    args = p.parse_args()
    return run_pipeline(
        dry_run=args.dry_run,
        source_limit=args.limit,
        chunk_size=args.chunk_size,
        max_days=args.max_days,
        quota_primary=args.quota_primary,
        quota_other=args.quota_other,
        min_relevance=args.min_relevance,
        min_quick_score=args.min_quick_score,
        skip_store=args.skip_store,
        mode=args.mode,
        pipeline_run_id=args.pipeline_run_id,
    )


if __name__ == "__main__":
    sys.exit(main())
