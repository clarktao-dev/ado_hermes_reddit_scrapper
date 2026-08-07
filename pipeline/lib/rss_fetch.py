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
