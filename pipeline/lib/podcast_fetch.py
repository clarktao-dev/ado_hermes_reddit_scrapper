"""Podcast RSS fetcher + transcript downloader for the daily pipeline.

Plan 10 (2026-08-31): adds podcast support alongside the YouTube pipeline
in ``pipeline.lib.youtube_fetch``. Same dedup story (ProcessedStore),
same round-robin cadence (config/channels.json), same vault layout — but
the source is an RSS feed (no Invidious, no kome.ai).

Why this module exists
----------------------
l'Immo stopped publishing on YouTube but still posts weekly on the Haufe/
Podigee-hosted RSS. The hosting provider publishes VTT + JSON transcripts
next to the MP3 audio, so we can ingest them without running our own ASR.
This module handles the three cases transparently:

  1. <podcast:transcript type="application/json"> — preferred (already
     {start, end, text} structured)
  2. <podcast:transcript type="text/vtt"> — fallback (parsed inline; we
     strip cue headers and Speaker labels so the resulting "text" is
     plain prose)
  3. No transcript tag — fall back to the RSS <description> field,
     which most Haufe/Podigee shows populate with a guest bio + chapter
     outline (not full transcript, but better than nothing).

Design constraints
------------------
- Mirrors ``pipeline.lib.youtube_fetch.VideoMeta`` shape (``id``,
  ``title``, ``channel``, ``published``, ``duration``) so callers can
  reuse the same downstream stages (digest → translate → vault).
- No new dependencies — stdlib only (urllib, xml.etree, json, re).
- One network call per transcript (VTT/JSON); <500 ms typical.
- Returns ``None`` if a transcript cannot be obtained — the caller
  decides whether to log + skip or queue for manual ASR.

Skipped for now (documented as future work):
- Local faster-whisper ASR fallback (deferred — user opted for
  description-only for now).
"""
from __future__ import annotations

import json
import logging
import re
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Optional

logger = logging.getLogger(__name__)

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


# Podcast Index transcript namespace
_NS_PODCAST = "https://podcastindex.org/namespace/1.0"
_NS_ITUNES = "http://www.itunes.com/dtds/podcast-1.0.dtd"


@dataclass
class PodcastEpisode:
    """Mirrors :class:`youtube_fetch.VideoMeta` shape so the daily
    pipeline can dispatch to the same downstream stages."""
    id: str              # unique per episode — we use the RSS <guid>
    title: str
    channel: str         # channel name from channels.json
    published: datetime  # timezone-aware UTC
    duration: int        # seconds; 0 if unknown
    audio_url: str       # MP3 URL from <enclosure>
    description: str     # RSS <description> — fallback when no transcript
    transcript_url: Optional[str] = None      # resolved from <podcast:transcript>
    transcript_type: Optional[str] = None     # 'application/json' | 'text/vtt'
    rss_url: str = ""    # for back-reference


def _parse_pub_date(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _parse_duration_to_seconds(raw: str) -> int:
    """itunes:duration accepts either ``SS`` or ``HH:MM:SS`` or ``MM:SS``."""
    if not raw:
        return 0
    raw = raw.strip()
    if ":" not in raw:
        try:
            return int(float(raw))
        except ValueError:
            return 0
    parts = raw.split(":")
    try:
        nums = [int(p) for p in parts]
    except ValueError:
        return 0
    if len(nums) == 3:
        h, m, s = nums
    elif len(nums) == 2:
        h, m, s = 0, nums[0], nums[1]
    else:
        return 0
    return h * 3600 + m * 60 + s


def _extract_episode(item: ET.Element, channel_name: str, rss_url: str) -> Optional[PodcastEpisode]:
    """Parse one <item> into a PodcastEpisode."""
    def text(tag: str) -> str:
        el = item.find(tag)
        return (el.text or "").strip() if el is not None else ""

    guid = text("guid") or text("link")
    if not guid:
        return None
    title = text("title")
    if not title:
        return None

    enc = item.find("enclosure")
    audio_url = enc.get("url") if enc is not None else ""

    dur_el = item.find(f"{{{_NS_ITUNES}}}duration")
    duration = _parse_duration_to_seconds(dur_el.text if dur_el is not None else "")

    pub = _parse_pub_date(text("pubDate"))

    # Pick the best transcript tag — prefer JSON, fallback to VTT
    tx_url = None
    tx_type = None
    for tx in item.findall(f"{{{_NS_PODCAST}}}transcript"):
        t_type = (tx.get("type") or "").lower()
        if t_type == "application/json" and tx_url is None:
            tx_url, tx_type = tx.get("url"), t_type
        elif t_type == "text/vtt" and tx_url is None:
            tx_url, tx_type = tx.get("url"), t_type
        # If we already found a JSON one, skip VTT
        if tx_url and tx_type == "application/json":
            break

    # Strip HTML tags from description (RSS feeds commonly use CDATA-wrapped HTML)
    desc = re.sub(r"<[^>]+>", " ", text("description"))
    desc = re.sub(r"\s+", " ", desc).strip()

    return PodcastEpisode(
        id=guid,
        title=title,
        channel=channel_name,
        published=pub or datetime.now(timezone.utc),
        duration=duration,
        audio_url=audio_url,
        description=desc[:500],  # cap to keep vault frontmatter reasonable
        transcript_url=tx_url,
        transcript_type=tx_type,
        rss_url=rss_url,
    )


def list_podcast_episodes(channel: dict, limit: int = 10) -> list[PodcastEpisode]:
    """Fetch latest episodes from a podcast channel via RSS.

    Symmetric to :func:`youtube_fetch.list_channel_videos` so the daily
    pipeline can dispatch by ``channel["source_type"] == "podcast"``.
    """
    rss_url = channel.get("url", "")
    if not rss_url:
        logger.warning("podcast channel %s has no url", channel.get("id"))
        return []

    try:
        req = urllib.request.Request(
            rss_url,
            headers={"User-Agent": _USER_AGENT, "Accept": "application/rss+xml"},
        )
        with urllib.request.urlopen(req, timeout=20) as r:
            body = r.read()
    except Exception as e:  # noqa: BLE001
        logger.warning("podcast RSS fetch failed for %s: %s",
                       channel.get("id"), type(e).__name__)
        return []

    try:
        root = ET.fromstring(body)
    except ET.ParseError as e:
        logger.warning("podcast RSS parse failed for %s: %s",
                       channel.get("id"), e)
        return []

    channel_name = channel.get("name", "")
    items = root.findall(".//item")
    episodes = []
    for item in items:
        ep = _extract_episode(item, channel_name, rss_url)
        if ep is not None:
            episodes.append(ep)
        if len(episodes) >= limit:
            break
    return episodes


# ---------------------------------------------------------------------------
# Transcript fetching (JSON > VTT > description)
# ---------------------------------------------------------------------------

def _fetch_url(url: str, timeout: float = 15.0) -> Optional[str]:
    if not url:
        return None
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": _USER_AGENT, "Accept": "*/*"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", errors="replace")
    except Exception as e:  # noqa: BLE001
        logger.info("transcript fetch failed %s: %s",
                    url[:80], type(e).__name__)
        return None


def _parse_vtt_to_text(vtt: str) -> str:
    """Strip VTT cue headers, timestamps, and Speaker labels; concat the
    cue text lines into plain prose. Good enough for LLM ingestion."""
    out = []
    for block in vtt.split("\n\n"):
        lines = []
        for line in block.splitlines():
            line = line.strip()
            if not line:
                continue
            if line.startswith("WEBVTT"):
                continue
            # timestamp lines
            if re.match(r"\d{2,}:\d{2}:\d{2}[.,]\d{3}\s*-->\s*\d{2,}:\d{2}:\d{2}", line):
                continue
            # strip Speaker X: prefix
            line = re.sub(r"^Speaker\s+\d+:\s*", "", line)
            lines.append(line)
        if lines:
            out.append(" ".join(lines))
    return "\n".join(out).strip()


def _parse_json_transcript_to_text(raw: str) -> Optional[str]:
    """Parse Podigee-style JSON transcript [{start, end, text}, ...]."""
    try:
        segs = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(segs, list):
        return None
    parts = []
    for seg in segs:
        if isinstance(seg, dict):
            t = seg.get("text", "").strip()
        else:
            t = str(seg).strip()
        if t:
            parts.append(t)
    return "\n".join(parts).strip() or None


def fetch_transcript(episode: PodcastEpisode) -> tuple[str, str]:
    """Return (text, source_label) for an episode.

    Tries, in order:
      1. The <podcast:transcript> JSON URL (already structured)
      2. The <podcast:transcript> VTT URL (parsed to prose)
      3. RSS <description> field (capped at 500 chars upstream)
    Always returns a non-empty string — the description is the floor.
    """
    # 1. JSON
    if episode.transcript_type == "application/json" and episode.transcript_url:
        raw = _fetch_url(episode.transcript_url)
        text = _parse_json_transcript_to_text(raw) if raw else None
        if text:
            return text, "podcast-transcript-json"

    # 2. VTT (also fires if type wasn't parsed but URL is present)
    if episode.transcript_url and (
        episode.transcript_type == "text/vtt"
        or episode.transcript_url.endswith(".vtt")
    ):
        raw = _fetch_url(episode.transcript_url)
        text = _parse_vtt_to_text(raw) if raw else None
        if text:
            return text, "podcast-transcript-vtt"

    # 3. Description floor
    return episode.description or "(no description)", "rss-description"