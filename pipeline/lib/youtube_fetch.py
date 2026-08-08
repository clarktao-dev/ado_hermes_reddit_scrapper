"""YouTube transcript fetcher.

Two-step pipeline:
1. yt-dlp --flat-playlist → list of video metadata (id, title, duration, epoch)
2. kome.ai POST /api/transcript → transcript text

Why kome.ai instead of youtube-transcript-api? On a VPS / cloud IP, both
yt-dlp and youtube-transcript-api are blocked by YouTube's bot detection.
kome.ai is a third-party transcript API that runs on rotated / residential IPs
and serves transcript requests anonymously. Verified: 24,445 chars returned
in 1.8s for German auto-generated caption. See skill `fetch-bypassing-vps-ip-blocks`.

Re-test kome.ai before relying on it for scheduled pipelines; the skill warns
it's a single point of failure.
"""
from __future__ import annotations
import json
import subprocess
import time
from dataclasses import dataclass
from typing import List, Optional

import requests


_KOME_URL = "https://kome.ai/api/transcript"
_KOME_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Content-Type": "application/json",
}


@dataclass
class VideoMeta:
    id: str
    title: str
    duration_sec: int
    epoch: Optional[int]  # published timestamp (Unix seconds)
    url: str
    channel_id: str
    channel_name: str
    view_count: Optional[int] = None


@dataclass
class TranscriptResult:
    video: VideoMeta
    language: str  # 'de' for kome.ai (it returns whatever language YouTube has)
    text: str      # joined transcript text (newlines preserved)
    n_chars: int
    is_premium: bool  # kome.ai premium flag; if True, content may be truncated


def _run_yt_dlp(channel_url: str, limit: int = 10) -> List[dict]:
    """List latest videos on a YouTube channel via yt-dlp flat-playlist."""
    if not channel_url.rstrip("/").endswith("/videos"):
        url = channel_url.rstrip("/") + "/videos"
    else:
        url = channel_url
    cmd = [
        "yt-dlp",
        "--flat-playlist",
        "--dump-json",
        "--playlist-end", str(limit),
        url,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if proc.returncode != 0:
        raise RuntimeError(f"yt-dlp failed for {channel_url}: {proc.stderr[:500]}")
    out = []
    for line in proc.stdout.strip().splitlines():
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out


def _fetch_published_from_rss(channel_id: str, cache_ttl_sec: int = 3600) -> dict:
    """Fetch YouTube RSS feed for a channel and parse <published> per <yt:videoId>.

    Returns {video_id: epoch_seconds}. Caches the feed result for `cache_ttl_sec`.
    yt-dlp's flat-playlist 'epoch' is actually the playlist-add timestamp, which
    can be off by a year. The RSS feed's <published> is the real upload time.
    """
    cache_key = f"_rss_cache_{channel_id}"
    now = time.time()
    cached = _RSS_CACHE.get(cache_key)
    if cached and now - cached["fetched_at"] < cache_ttl_sec:
        return cached["data"]
    url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
    r = requests.get(url, timeout=15, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    })
    if r.status_code != 200:
        _RSS_CACHE[cache_key] = {"fetched_at": now, "data": {}}
        return {}
    # Naive parse: <entry>...<yt:videoId>X</yt:videoId>...<published>YYYY-MM-DD...</published>
    out: dict = {}
    for entry in r.text.split("<entry>")[1:]:
        # video id
        vid = None
        i = entry.find("<yt:videoId>")
        if i != -1:
            j = entry.find("</yt:videoId>", i)
            if j != -1:
                vid = entry[i + len("<yt:videoId>"):j].strip()
        # published
        epoch = None
        k = entry.find("<published>")
        if k != -1:
            l = entry.find("</published>", k)
            if l != -1:
                from datetime import datetime
                ts = entry[k + len("<published>"):l].strip()
                try:
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    epoch = int(dt.timestamp())
                except ValueError:
                    pass
        if vid and epoch:
            out[vid] = epoch
    _RSS_CACHE[cache_key] = {"fetched_at": now, "data": out}
    return out


_RSS_CACHE: dict = {}


def list_channel_videos(channel_id: str, channel_name: str, channel_url: str,
                        limit: int = 10, *, youtube_channel_id: Optional[str] = None) -> List[VideoMeta]:
    """Fetch latest videos metadata for a channel. Sorted newest first by real publish date.

    Args:
        channel_id: internal id used by the pipeline (e.g. '1alage')
        channel_name: human channel name (e.g. '1aLAGE Immobilienpodcast')
        channel_url: public YouTube URL (https://www.youtube.com/@handle/videos)
        limit: max videos to return
        youtube_channel_id: YouTube's UC... channel id (from channels.json). When
            provided, we also pull the real published timestamp from the YouTube
            RSS feed, since yt-dlp's flat-playlist epoch is the playlist-add time
            and can be wildly wrong.
    """
    raw = _run_yt_dlp(channel_url, limit=limit)
    rss_published: dict = {}
    if youtube_channel_id:
        try:
            rss_published = _fetch_published_from_rss(youtube_channel_id)
        except Exception:
            rss_published = {}
    metas: List[VideoMeta] = []
    for d in raw[:limit]:
        try:
            # Prefer RSS published timestamp; fall back to yt-dlp epoch (which is
            # the playlist-add time and may be off by months/years).
            vid = d["id"]
            rss_epoch = rss_published.get(vid)
            yt_dlp_epoch = d.get("epoch") or d.get("timestamp") or d.get("release_timestamp")
            epoch = rss_epoch if rss_epoch else (int(yt_dlp_epoch) if yt_dlp_epoch else None)
            metas.append(VideoMeta(
                id=vid,
                title=d.get("title", ""),
                duration_sec=int(d.get("duration") or 0),
                epoch=int(epoch) if epoch else None,
                url=d.get("webpage_url") or f"https://www.youtube.com/watch?v={vid}",
                channel_id=d.get("playlist_channel_id") or channel_id,
                channel_name=d.get("playlist_channel") or channel_name,
                view_count=d.get("view_count"),
            ))
        except (KeyError, ValueError):
            continue
    # Newest first; videos without epoch go to the end
    metas.sort(key=lambda v: v.epoch or 0, reverse=True)
    return metas


def fetch_transcript(video: VideoMeta, timeout: int = 30,
                     max_chars: int = 200_000) -> TranscriptResult:
    """Fetch transcript via kome.ai.

    Returns TranscriptResult with the joined text. If kome.ai returns nothing or
    raises, returns text='' and language='none' so the caller can decide what
    to do (skip / fallback to YouTube HTML scrape).
    """
    payload = {"video_id": video.id, "format": "true"}
    try:
        resp = requests.post(_KOME_URL, json=payload, headers=_KOME_HEADERS,
                             timeout=timeout)
        resp.raise_for_status()
    except requests.RequestException as e:
        return TranscriptResult(
            video=video, language="none", text="", n_chars=0,
            is_premium=False,
        )
    data = resp.json()
    text = (data.get("transcript") or "").strip()
    if not text:
        return TranscriptResult(
            video=video, language="none", text="", n_chars=0,
            is_premium=bool(data.get("isPremium")),
        )
    # kome.ai returns language based on what YouTube has; we don't need to detect it
    truncated = text[:max_chars]
    return TranscriptResult(
        video=video,
        language="auto",
        text=truncated,
        n_chars=len(truncated),
        is_premium=bool(data.get("isPremium")),
    )
