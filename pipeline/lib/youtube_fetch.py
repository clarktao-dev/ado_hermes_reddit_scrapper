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
import logging
import subprocess
import time
import urllib.request
from dataclasses import dataclass
from typing import List, Optional

import requests


logger = logging.getLogger(__name__)


# Invidious public instances used as the primary source for channel-video
# listings. YouTube's official RSS feed is blocked on this VPS (1/8 channels
# return data; the rest 500/404), so we rotate through Invidious instances
# before falling back to yt-dlp. Order matters: first healthy instance wins.
INVIDIOUS_INSTANCES = [
    "https://invidious.materialio.us",
    "https://invidious.flokinet.to",
]


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


def _list_via_invidious(channel: dict, limit: int = 10) -> List[VideoMeta]:
    """Fetch latest videos for a channel via the Invidious public API.

    Tries each instance in ``INVIDIOUS_INSTANCES`` in order; returns as soon
    as one returns at least one video. Raises ``RuntimeError`` only when every
    instance fails (network / 4xx / 5xx / empty payload).

    Invidious returns rich metadata that YouTube's official RSS feed does not
    expose — most importantly ``lengthSeconds`` and a real Unix-epoch
    ``published`` timestamp — so this is the preferred primary path.
    """
    channel_id = channel.get("channel_id") or channel.get("id")
    if not channel_id:
        return []
    canonical_id = channel["id"]
    for instance in INVIDIOUS_INSTANCES:
        try:
            url = f"{instance}/api/v1/channels/{channel_id}"
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "Mozilla/5.0 (compatible; hermes-youtube-fetch/1.0)"},
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                raw = resp.read().decode("utf-8")
            data = json.loads(raw)
            videos = data.get("latestVideos", [])
            if not videos:
                logger.warning(
                    "invidious %s returned no latestVideos for %s (UC=%s)",
                    instance, canonical_id, channel_id,
                )
                continue
            metas: List[VideoMeta] = []
            for v in videos[:limit]:
                vid = v.get("videoId")
                if not vid:
                    continue
                length = v.get("lengthSeconds")
                try:
                    duration_sec: int = int(length) if length is not None else 0
                except (TypeError, ValueError):
                    duration_sec = 0
                published = v.get("published")
                try:
                    epoch: Optional[int] = int(published) if published is not None else None
                except (TypeError, ValueError):
                    epoch = None
                metas.append(VideoMeta(
                    id=vid,
                    title=v.get("title", ""),
                    duration_sec=duration_sec,
                    epoch=epoch,
                    url=f"https://www.youtube.com/watch?v={vid}",
                    channel_id=channel_id,
                    channel_name=channel.get("name") or data.get("author", ""),
                    view_count=v.get("viewCount"),
                ))
            logger.info(
                "invidious: got %d videos from %s for %s",
                len(metas), instance, canonical_id,
            )
            return metas
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "invidious %s failed for %s (UC=%s): %s",
                instance, canonical_id, channel_id, e,
            )
            continue
    raise RuntimeError(f"All Invidious instances failed for {canonical_id}")


def _list_via_ytdlp_inline(channel_id: str, channel_name: str, channel_url: str,
                            limit: int, youtube_channel_id: Optional[str]) -> List[VideoMeta]:
    """Legacy yt-dlp + YouTube-RSS fallback. Inlined to avoid signature drift.

    Kept because Invidious is a third-party service that may disappear; yt-dlp
    on a residential / non-VPS IP still works for channel listings. Broken on
    the VPS (YouTube bot detection) but this branch is only hit if every
    Invidious instance fails.
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
    rss_videos = [m for m in metas if m.epoch and m.epoch > 0]
    if rss_videos:
        metas = rss_videos
    confirmed = [m for m in metas if m.id in rss_published and rss_published[m.id]]
    metas = confirmed
    metas.sort(key=lambda v: v.epoch or 0, reverse=True)
    return metas


def list_channel_videos(channel_id: str, channel_name: str, channel_url: str,
                        limit: int = 10, *, youtube_channel_id: Optional[str] = None) -> List[VideoMeta]:
    """Fetch latest videos metadata for a channel. Sorted newest first.

    Resolution order:
      1. Invidious public instances (primary — works on the VPS; rich metadata).
      2. yt-dlp + YouTube RSS fallback (legacy; broken on VPS, kept for local).

    Args:
        channel_id: internal id used by the pipeline (e.g. '1alage').
        channel_name: human channel name (e.g. '1aLAGE Immobilienpodcast').
        channel_url: public YouTube URL (https://www.youtube.com/@handle/videos).
        limit: max videos to return.
        youtube_channel_id: YouTube's UC... channel id (from channels.json).
            Passed through to the Invidious URL and to the yt-dlp RSS layer.
    """
    channel = {
        "id": channel_id,
        "name": channel_name,
        "url": channel_url,
        "channel_id": youtube_channel_id or channel_id,
    }
    # 1. Invidious (primary).
    try:
        metas = _list_via_invidious(channel, limit)
        if metas:
            metas.sort(key=lambda v: v.epoch or 0, reverse=True)
            return metas
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "Invidious path failed for %s — falling back to yt-dlp: %s",
            channel_id, e,
        )
    # 2. yt-dlp + RSS (final fallback; broken on VPS but kept for local).
    return _list_via_ytdlp_inline(
        channel_id, channel_name, channel_url, limit, youtube_channel_id,
    )


def fetch_transcript(video: VideoMeta, timeout: int = 30,
                     max_chars: int = 200_000) -> TranscriptResult:
    """Fetch transcript via kome.ai, then persist to the transcript cache.

    The cache (immobilien-kb/vault/YouTube/<Channel>/_transcripts/<video_id>.md)
    is the source of truth for long-form re-runs within the 30-day TTL window —
    see ``pipeline.lib.transcript_cache`` for the cleanup contract.

    If the cache already has a non-expired entry for this video, return it
    directly without hitting kome.ai. Returns text='' / language='none' when
    both the network call and the cache miss — the caller decides whether
    to skip the video or fall back to a YouTube HTML scrape.
    """
    # Cheap check first: if a fresh cached transcript exists, use it.
    try:
        from pipeline.lib import transcript_cache
        cached = transcript_cache.read_cached_transcript(video.channel_name, video.id)
        if cached is not None:
            return TranscriptResult(
                video=video,
                language=cached.language,
                text=cached.text[:max_chars],
                n_chars=min(cached.n_chars, max_chars),
                is_premium=False,
            )
    except Exception:  # noqa: BLE001 — cache read is best-effort
        pass

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
    # Persist to the transcript cache so long-form re-runs can skip kome.ai.
    try:
        from pipeline.lib import transcript_cache
        transcript_cache.write_transcript(
            channel=video.channel_name,
            video_id=video.id,
            text=truncated,
            language="auto",
            source="kome.ai",
        )
    except Exception as e:  # noqa: BLE001 — cache write failure is non-fatal
        logger.warning("transcript cache write failed for %s: %s", video.id, e)
    return TranscriptResult(
        video=video,
        language="auto",
        text=truncated,
        n_chars=len(truncated),
        is_premium=bool(data.get("isPremium")),
    )
