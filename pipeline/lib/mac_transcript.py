"""Mac → VPS transcript bridge (residential-IP caption fetch).

Architecture
------------
- Mac ``youtube-mac-worker`` fetches YouTube captions (cookie + dual-channel)
  and POSTs a JSON payload to the VPS webhook.
- VPS writes into ``transcript_cache`` and never requests YouTube captions
  itself when ``YOUTUBE_TRANSCRIPT_SOURCE=mac``.
- Optional: VPS may enqueue a video URL to the Mac worker over a Cloudflare
  Tunnel (``MAC_WORKER_URL``) when cache misses.

Webhook JSON (Mac → VPS), as produced by youtube-mac-worker::

    {
      "video_id", "url", "title", "language", "is_generated",
      "source", "text", "segments", "meta", "fetched_at"
    }
"""
from __future__ import annotations

import json
import logging
import os
import re
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Optional

from pipeline.lib import transcript_cache

logger = logging.getLogger(__name__)

# mac = cache only (filled by Mac webhook); never call kome / YouTube captions.
# kome = legacy VPS path (cache → kome.ai).
# auto = cache first, then kome (transition default until Mac is live).
TRANSCRIPT_SOURCE_ENV = "YOUTUBE_TRANSCRIPT_SOURCE"
MAC_WORKER_URL_ENV = "MAC_WORKER_URL"
MAC_WEBHOOK_TOKEN_ENV = "MAC_WEBHOOK_TOKEN"


def transcript_source_mode() -> str:
    raw = (os.environ.get(TRANSCRIPT_SOURCE_ENV) or "auto").strip().lower()
    if raw in ("mac", "mac_only", "cache", "cache_only"):
        return "mac"
    if raw in ("kome", "kome.ai", "legacy"):
        return "kome"
    return "auto"


def webhook_token() -> str:
    return (os.environ.get(MAC_WEBHOOK_TOKEN_ENV) or "").strip()


def mac_worker_base_url() -> str:
    return (os.environ.get(MAC_WORKER_URL_ENV) or "").rstrip("/")


def _channel_from_payload(payload: dict) -> str:
    meta = payload.get("meta") or {}
    if isinstance(meta, dict):
        for key in ("channel", "channel_name", "uploader", "channel_id"):
            val = meta.get(key)
            if val:
                return str(val)
    title = payload.get("title") or ""
    # Fallback folder so the file still lands somewhere observable.
    return "mac_inbox"


def ingest_webhook_payload(payload: dict) -> dict:
    """Validate Mac worker JSON and write ``transcript_cache``.

    Returns a summary dict suitable for the HTTP response body.
    Raises ``ValueError`` on invalid payloads.
    """
    if not isinstance(payload, dict):
        raise ValueError("payload must be a JSON object")
    video_id = (payload.get("video_id") or "").strip()
    text = (payload.get("text") or "").strip()
    if not video_id:
        raise ValueError("missing video_id")
    if not re.fullmatch(r"[A-Za-z0-9_-]{6,64}", video_id):
        # YouTube ids are 11 chars; allow slightly wider for safety.
        raise ValueError(f"invalid video_id: {video_id!r}")
    if not text:
        raise ValueError("empty transcript text")

    channel = _channel_from_payload(payload)
    language = (payload.get("language") or "de").strip() or "de"
    source = (payload.get("source") or "mac-worker").strip() or "mac-worker"
    # Tag source so vault frontmatter shows Mac origin clearly.
    if not source.startswith("mac"):
        source = f"mac:{source}"

    path = transcript_cache.write_transcript(
        channel=channel,
        video_id=video_id,
        text=text,
        language=language,
        source=source,
    )
    # Also write under video_id-keyed inbox for resolvers that only know id.
    inbox_path = transcript_cache.write_transcript(
        channel="_mac_inbox",
        video_id=video_id,
        text=text,
        language=language,
        source=source,
    )
    summary = {
        "ok": True,
        "video_id": video_id,
        "channel": channel,
        "n_chars": len(text),
        "language": language,
        "source": source,
        "cache_path": str(path),
        "inbox_path": str(inbox_path),
        "title": payload.get("title") or "",
        "url": payload.get("url") or "",
        "is_generated": bool(payload.get("is_generated")),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }
    logger.info(
        "mac transcript ingested: %s | %s (%d chars) → %s",
        video_id, channel, len(text), path,
    )
    return summary


def read_mac_or_channel_cache(channel_name: str, video_id: str):
    """Prefer channel cache, then ``_mac_inbox/<video_id>``."""
    cached = transcript_cache.read_cached_transcript(channel_name, video_id)
    if cached is not None:
        return cached
    return transcript_cache.read_cached_transcript("_mac_inbox", video_id)


def enqueue_on_mac(url_or_id: str, *, timeout: float = 15.0) -> dict:
    """Ask the Mac worker (via Cloudflare Tunnel) to fetch captions.

    Expects ``MAC_WORKER_URL`` e.g. ``https://xxxx.trycloudflare.com`` and a
    worker that accepts ``POST /enqueue`` with ``{"url": "..."}``.
    """
    base = mac_worker_base_url()
    if not base:
        raise RuntimeError(
            f"{MAC_WORKER_URL_ENV} not set — cannot enqueue on Mac worker"
        )
    if re.fullmatch(r"[A-Za-z0-9_-]{11}", url_or_id.strip()):
        url = f"https://www.youtube.com/watch?v={url_or_id.strip()}"
    else:
        url = url_or_id.strip()
    body = json.dumps({"url": url}).encode("utf-8")
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    token = webhook_token()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(
        f"{base}/enqueue", data=body, headers=headers, method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            try:
                return json.loads(raw) if raw else {"ok": True, "status": resp.status}
            except json.JSONDecodeError:
                return {"ok": True, "status": resp.status, "raw": raw[:500]}
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")[:500]
        raise RuntimeError(f"Mac enqueue HTTP {e.code}: {detail}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"Mac enqueue failed: {e}") from e


def maybe_enqueue_on_cache_miss(video_id: str, video_url: str) -> Optional[dict]:
    """Best-effort enqueue when running in ``mac`` mode and cache is empty."""
    if transcript_source_mode() != "mac":
        return None
    if not mac_worker_base_url():
        logger.info(
            "cache miss for %s and %s unset — skip enqueue",
            video_id, MAC_WORKER_URL_ENV,
        )
        return None
    try:
        result = enqueue_on_mac(video_url or video_id)
        logger.info("enqueued %s on Mac worker: %s", video_id, result)
        return result
    except Exception as e:  # noqa: BLE001
        logger.warning("enqueue on Mac failed for %s: %s", video_id, e)
        return {"ok": False, "error": str(e)}
