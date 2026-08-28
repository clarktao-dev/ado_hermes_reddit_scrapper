"""Transcript cache: write kome.ai transcripts to vault + cleanup expired entries.

Layout:
    immobilien-kb/vault/YouTube/<Channel>/_transcripts/<video_id>.md

Each file has a YAML frontmatter so the TTL is self-describing — no external
index needed. Cleanup scans ``fetched_at`` and deletes files older than the
TTL (default 30 days).

Why this module exists:
- kome.ai is a single point of failure. We pay one API call per video per
  long-form cycle, which costs ~5s + rate-limit budget.
- Caching for 30 days lets a video be re-processed for long-form repeatedly
  without re-hitting kome.ai.
- A weekly cleanup cron keeps disk bounded; transcript files are ~20-25 KB
  each, so 30 days × 3 channels/day × ~3 videos/channel = ~6 MB worst case.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

from pipeline.lib.paths import IMMO_VAULT, VAULT_ROOT

# Backward-compat alias used by cleanup_transcripts.py
REPO_ROOT = VAULT_ROOT
TRANSCRIPTS_ROOT = IMMO_VAULT / "YouTube"

DEFAULT_TTL_DAYS = 30


@dataclass
class CachedTranscript:
    video_id: str
    channel: str
    path: Path
    fetched_at: datetime
    expires_at: datetime
    language: str
    text: str
    n_chars: int

    @property
    def age_days(self) -> int:
        return (datetime.now(timezone.utc) - self.fetched_at).days

    @property
    def expired(self) -> bool:
        return datetime.now(timezone.utc) >= self.expires_at


def _channel_dir(channel: str) -> Path:
    """Path to the per-channel _transcripts/ folder (creates parents on write)."""
    safe = re.sub(r"[^A-Za-z0-9_\-]", "_", channel)
    return TRANSCRIPTS_ROOT / safe / "_transcripts"


def transcript_path(channel: str, video_id: str) -> Path:
    """Canonical transcript cache path. Safe to call before the file exists."""
    return _channel_dir(channel) / f"{video_id}.md"


def write_transcript(channel: str, video_id: str, text: str,
                     language: str = "de",
                     source: str = "kome.ai",
                     ttl_days: int = DEFAULT_TTL_DAYS) -> Path:
    """Persist a transcript with TTL frontmatter.

    Idempotent: overwrites an existing file for the same (channel, video_id).
    The ``expires_at`` is always recomputed from ``now + ttl_days`` so a
    re-fetch resets the 30-day window.
    """
    now = datetime.now(timezone.utc)
    expires = now + timedelta(days=ttl_days)
    path = transcript_path(channel, video_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    body = (
        "---\n"
        "type: transcript\n"
        f"video_id: {video_id}\n"
        f"channel: {channel}\n"
        f"lang: {language}\n"
        f"fetched_at: {now.isoformat()}\n"
        f"expires_at: {expires.isoformat()}\n"
        f"source: {source}\n"
        f"n_chars: {len(text)}\n"
        "ttl_days: {ttl}\n"
        "---\n\n"
        "# Transcript ({lang})\n\n"
        "{body}\n"
    ).format(lang=language, body=text.rstrip(), ttl=ttl_days)
    path.write_text(body, encoding="utf-8")
    return path


_FRONTMATTER_RE = re.compile(r"^---\n(.*?)\n---\n", re.DOTALL)


def read_cached_transcript(channel: str, video_id: str) -> Optional[CachedTranscript]:
    """Load a cached transcript. Returns None if missing or expired.

    Expired files are left in place (cleanup cron handles deletion); callers
    see ``None`` so they fall back to fetching fresh.
    """
    path = transcript_path(channel, video_id)
    if not path.exists():
        return None
    raw = path.read_text(encoding="utf-8")
    m = _FRONTMATTER_RE.match(raw)
    if not m:
        return None
    fm = m.group(1)
    body = raw[m.end():].lstrip("\n")
    fields = {}
    for line in fm.splitlines():
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        fields[k.strip()] = v.strip()
    try:
        fetched_at = datetime.fromisoformat(fields["fetched_at"])
        expires_at = datetime.fromisoformat(fields["expires_at"])
    except (KeyError, ValueError):
        return None
    if datetime.now(timezone.utc) >= expires_at:
        return None
    # Strip the human-readable `# Transcript (<lang>)` header that
    # write_transcript adds. Downstream callers (Google Translate, long-form
    # LLM prompt) expect raw transcript text only — feeding them the header
    # silently degrades translation quality and inflates token usage.
    text = body
    if text.startswith("# Transcript"):
        # Drop the first line and any blank lines that follow it.
        text = text.split("\n", 1)[1].lstrip("\n")
    return CachedTranscript(
        video_id=video_id,
        channel=channel,
        path=path,
        fetched_at=fetched_at,
        expires_at=expires_at,
        language=fields.get("lang", "de"),
        text=text.strip(),
        n_chars=int(fields.get("n_chars", "0") or 0),
    )


def cleanup_expired(now: Optional[datetime] = None) -> List[Path]:
    """Delete all cached transcripts past their ``expires_at``.

    Returns the list of deleted paths so the caller can log them. Safe to run
    repeatedly — non-existent directories are skipped, already-deleted files
    are silently ignored.
    """
    now = now or datetime.now(timezone.utc)
    deleted: List[Path] = []
    if not TRANSCRIPTS_ROOT.exists():
        return deleted
    for channel_dir in TRANSCRIPTS_ROOT.iterdir():
        transcripts_dir = channel_dir / "_transcripts"
        if not transcripts_dir.exists():
            continue
        for p in transcripts_dir.glob("*.md"):
            raw = p.read_text(encoding="utf-8")
            m = _FRONTMATTER_RE.match(raw)
            if not m:
                continue
            try:
                expires_at = datetime.fromisoformat(
                    m.group(1).split("expires_at:")[1].split("\n")[0].strip()
                )
            except (IndexError, ValueError):
                continue
            if now >= expires_at:
                p.unlink()
                deleted.append(p)
    return deleted


def stats() -> dict:
    """Quick stats: how many transcripts cached, how many expired (waiting for cleanup)."""
    if not TRANSCRIPTS_ROOT.exists():
        return {"cached": 0, "expired": 0, "channels": []}
    cached = 0
    expired = 0
    channels = []
    now = datetime.now(timezone.utc)
    for channel_dir in TRANSCRIPTS_ROOT.iterdir():
        transcripts_dir = channel_dir / "_transcripts"
        if not transcripts_dir.exists():
            continue
        ch_cached = 0
        ch_expired = 0
        for p in transcripts_dir.glob("*.md"):
            raw = p.read_text(encoding="utf-8")
            m = _FRONTMATTER_RE.match(raw)
            if not m:
                continue
            try:
                expires_at = datetime.fromisoformat(
                    m.group(1).split("expires_at:")[1].split("\n")[0].strip()
                )
            except (IndexError, ValueError):
                continue
            cached += 1
            ch_cached += 1
            if now >= expires_at:
                expired += 1
                ch_expired += 1
        if ch_cached:
            channels.append({"channel": channel_dir.name, "cached": ch_cached,
                             "expired": ch_expired})
    return {"cached": cached, "expired": expired, "channels": channels}
