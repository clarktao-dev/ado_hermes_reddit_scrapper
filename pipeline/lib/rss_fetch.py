"""RSS fetcher for German news sources.

Strategy (per news-monitoring-pipeline skill):
- Single browser User-Agent (no rotation; rotation breaks token-pinned sites)
- 4-8 second random delay between sources (avoids burst 429s)
- Returns parsed items as list of dicts with: source, url, title, summary, pub_date, raw_content
"""
import random
import re
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests


_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


def _parse_pub_date(s):
    if not s:
        return None
    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _extract_items_atom(xml_bytes, source):
    """Reddit-style Atom feed: <entry><title><link href=...><updated>..."""
    out = []
    for m in re.finditer(rb"<entry>(.*?)</entry>", xml_bytes, re.DOTALL):
        block = m.group(1).decode("utf-8", errors="replace")
        title_m = re.search(r"<title[^>]*>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>", block, re.DOTALL)
        link_m = re.search(r'<link[^>]+href="([^"]+)"', block)
        updated_m = re.search(r"<updated>(.*?)</updated>", block)
        content_m = re.search(r"<content[^>]*>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</content>", block, re.DOTALL)
        summary_m = re.search(r"<summary[^>]*>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</summary>", block, re.DOTALL)
        out.append({
            "source_id": source["id"],
            "source_name": source["name"],
            "priority": source.get("priority", 99),
            "title": (title_m.group(1) if title_m else "").strip(),
            "url": (link_m.group(1) if link_m else "").strip(),
            "pub_date": _parse_pub_date(updated_m.group(1) if updated_m else None),
            "summary": re.sub(r"<[^>]+>", " ", (summary_m.group(1) if summary_m else "")).strip()[:500],
            "content_html": (content_m.group(1) if content_m else "").strip(),
        })
    return out


def _extract_items_rss(xml_bytes, source):
    """RSS 2.0: <item><title><link><pubDate><description>..."""
    out = []
    for m in re.finditer(rb"<item>(.*?)</item>", xml_bytes, re.DOTALL):
        block = m.group(1).decode("utf-8", errors="replace")
        title_m = re.search(r"<title[^>]*>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>", block, re.DOTALL)
        link_m = re.search(r"<link[^>]*>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</link>", block, re.DOTALL)
        guid_m = re.search(r"<guid[^>]*>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</guid>", block, re.DOTALL)
        pub_m = re.search(r"<pubDate>(.*?)</pubDate>", block, re.DOTALL)
        desc_m = re.search(r"<description[^>]*>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</description>", block, re.DOTALL)
        content_m = re.search(r"<content:encoded[^>]*>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</content:encoded>", block, re.DOTALL)
        url = (link_m.group(1) if link_m else "") or (guid_m.group(1) if guid_m else "")
        url = url.strip()
        out.append({
            "source_id": source["id"],
            "source_name": source["name"],
            "priority": source.get("priority", 99),
            "title": (title_m.group(1) if title_m else "").strip(),
            "url": url,
            "pub_date": _parse_pub_date(pub_m.group(1) if pub_m else None),
            "summary": re.sub(r"<[^>]+>", " ", (desc_m.group(1) if desc_m else "")).strip()[:500],
            "content_html": (content_m.group(1) if content_m else (desc_m.group(1) if desc_m else "")).strip(),
        })
    return out


def fetch_source(source, timeout=20):
    """Fetch a single RSS source. Returns list of items or [] on failure."""
    try:
        r = requests.get(
            source["url"],
            headers={"User-Agent": _USER_AGENT, "Accept": "application/rss+xml,application/xml;q=0.9,*/*;q=0.8"},
            timeout=timeout,
        )
        if r.status_code != 200:
            return []
        body = r.content
        if b"<entry>" in body[:5000]:
            return _extract_items_atom(body, source)
        elif b"<item>" in body[:5000] or b"<rss" in body[:5000]:
            return _extract_items_rss(body, source)
        return []
    except Exception:
        return []


def fetch_all_sources(sources, min_delay=4.0, max_delay=8.0):
    """Fetch all sources with random inter-source delays. Returns combined items."""
    all_items = []
    for i, src in enumerate(sources):
        items = fetch_source(src)
        all_items.extend(items)
        if i < len(sources) - 1:
            time.sleep(random.uniform(min_delay, max_delay))
    return all_items


# --------------------------------------------------------------------------- #
# Full-text fetcher — step 2.5 of the pipeline.
#
# Called AFTER dedup/age-filter, BEFORE translation, on only the items that
# survived every other gate (typically 5-15 per run). Adds 1 HTTP request
# per surviving item — not per raw RSS entry.
# --------------------------------------------------------------------------- #

_ARTICLE_RE = re.compile(r"<article[^>]*>(.*?)</article>", re.DOTALL | re.IGNORECASE)
_MAIN_RE = re.compile(r"<main[^>]*>(.*?)</main>", re.DOTALL | re.IGNORECASE)
_PAYWALL_HINTS = (
    "Abo erforderlich",
    "kostenpflichtig",
    "Plus-Abonnement",
    "Sie sind bereits",
    "F.A.Z. Plus",
    "Spiegel Plus",
    "Handelsblatt Abo",
    "Abonnieren Sie",
)


def _strip_html(html: str) -> str:
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"&[a-z]+;", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def fetch_full_text(url: str, timeout: float = 10.0) -> dict:
    """Fetch the article body for a single news URL.

    Returns a dict:
        {"full_text": str|None, "paywalled": bool, "char_count": int,
         "had_paywall_hint": bool, "error": str|None}

    Strategy:
      1. GET the URL with a desktop User-Agent and Accept-Language: de-DE.
      2. Try to extract <article>...</article>.
      3. Fall back to <main>...</main>.
      4. Strip HTML to plain text.
      5. If extracted text is shorter than 200 chars, mark paywalled (likely
         a teaser page only).
      6. If 200 < chars < 1000, still return the text — it might be the full
         article for a short piece.
    Caller should keep the original RSS `summary` if `full_text` is None.
    """
    if not url:
        return {"full_text": None, "paywalled": False, "char_count": 0,
                "had_paywall_hint": False, "error": "no url"}
    try:
        r = requests.get(
            url,
            headers={
                "User-Agent": _USER_AGENT,
                "Accept-Language": "de-DE,de;q=0.9,en;q=0.5",
            },
            timeout=timeout,
            allow_redirects=True,
        )
    except Exception as e:
        return {"full_text": None, "paywalled": False, "char_count": 0,
                "had_paywall_hint": False, "error": f"{type(e).__name__}: {e}"}
    if r.status_code != 200:
        return {"full_text": None, "paywalled": False, "char_count": 0,
                "had_paywall_hint": False, "error": f"HTTP {r.status_code}"}
    html = r.text
    body = None
    m = _ARTICLE_RE.search(html)
    if m:
        body = m.group(1)
    else:
        m = _MAIN_RE.search(html)
        if m:
            body = m.group(1)
    if body is None:
        return {"full_text": None, "paywalled": False, "char_count": 0,
                "had_paywall_hint": False, "error": "no article/main tag"}
    text = _strip_html(body)
    had_hint = any(hint.lower() in text.lower() for hint in _PAYWALL_HINTS)
    paywalled = len(text) < 200 or had_hint
    return {
        "full_text": text if text else None,
        "paywalled": paywalled,
        "char_count": len(text),
        "had_paywall_hint": had_hint,
        "error": None,
    }


def fetch_full_text_for_items(items, delay_sec: float = 1.0,
                              verbose: bool = True) -> list:
    """Call fetch_full_text() for each item. Mutates each item in place:
        item['full_text'] = str|None
        item['full_text_chars'] = int
        item['full_text_paywalled'] = bool
        item['full_text_error'] = str|None
    Returns the input list for chaining. Sleeps `delay_sec` between requests
    to avoid bursting the source servers.
    """
    for i, it in enumerate(items):
        url = it.get("url")
        result = fetch_full_text(url)
        it["full_text"] = result["full_text"]
        it["full_text_chars"] = result["char_count"]
        it["full_text_paywalled"] = result["paywalled"]
        it["full_text_error"] = result["error"]
        if verbose:
            char_count = result["char_count"]
            mark = " [paywalled]" if result["paywalled"] else ""
            err = f" err={result['error']}" if result["error"] else ""
            print(f"[fulltext] {i+1}/{len(items)} {it.get('source_name','?'):28} "
                  f"{char_count:>5} chars{mark}{err}")
        if i < len(items) - 1 and delay_sec > 0:
            time.sleep(delay_sec)
    return items
