"""Long-form vault helpers — centralise path + cache logic.

All long-form articles for the German real-estate newsletter live at:

    immobilien-kb/vault/Longform/<YYYY-MM-DD>/<video_id>---<slug>.md

This module owns:
- The canonical path format (lookup + write)
- The idempotent cache check (existence = "already written, don't redo")
- The short-summary expansion hook (Idea 2: skip translation, expand in-place)
- Move-or-copy from `podcast-kb/vault/Daily/...` after `youtube_daily.py --mode long`
  finishes (you don't have to touch youtube_daily.py's vault layout to use this).

Naming convention:
    <date>-<video_id>-<slug>.md        → short summary (existing, in YouTube/<Channel>/)
    <video_id>---<slug>.md             → long-form article (NEW, in Longform/<date>/)
    The `---` separator visually distinguishes the two layers.
"""
from __future__ import annotations

import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Repo root — `immobilien-kb/` sits next to `pipeline/`
REPO_ROOT = Path(__file__).resolve().parents[2]
LONGFORM_ROOT = REPO_ROOT / "immobilien-kb" / "vault" / "Longform"
SHORTSUMMARY_ROOT = REPO_ROOT / "immobilien-kb" / "vault" / "YouTube"
PODCAST_LONGFORM_ROOT = REPO_ROOT / "podcast-kb" / "vault" / "Daily"


def _slug(text: str, max_len: int = 60) -> str:
    s = re.sub(r"[^\w\-]+", "-", text.lower()).strip("-")
    return s[:max_len] or "untitled"


def longform_path(video_id: str, slug_hint: str = "",
                  date_str: Optional[str] = None) -> Path:
    """Canonical long-form path. Exists-=False by default.

    The ``date_str`` defaults to today UTC; pass it explicitly when migrating
    a file written earlier (so the date folder matches the original run).
    """
    if date_str is None:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    slug = _slug(slug_hint) if slug_hint else "untitled"
    return LONGFORM_ROOT / date_str / f"{video_id}---{slug}.md"


def find_longform(video_id: str) -> Optional[Path]:
    """Find any existing long-form file for this video across all date folders.

    Returns the first match (newest first), or None if absent.
    """
    if not LONGFORM_ROOT.exists():
        return None
    for date_dir in sorted(LONGFORM_ROOT.iterdir(), reverse=True):
        if not date_dir.is_dir() or date_dir.name.startswith("."):
            continue
        for p in date_dir.glob(f"{video_id}---*.md"):
            return p
    return None


def short_summary_path(channel_name: str, video_id: str) -> Optional[Path]:
    """Find the short-summary vault file for a given video.

    Path pattern (existing): ``YouTube/<Channel>/<date>-<video_id>-<slug>.md``
    We don't know the date up-front; the channel dir contains all dates so we
    scan. Returns the newest match.
    """
    channel_dir = SHORTSUMMARY_ROOT / channel_name
    if not channel_dir.exists():
        return None
    for p in sorted(channel_dir.glob(f"*-{video_id}-*.md"), reverse=True):
        return p
    return None


def read_short_summary(channel_name: str, video_id: str) -> Optional[str]:
    """Load the short-summary markdown for Idea 2 expansion.

    Returns the full file content (including frontmatter) so the LLM can
    see what we already wrote and decide whether to expand, rewrite, or skip.
    Returns None if no short summary exists — caller should fall back to
    the full long-form path (fetch transcript, translate, then expand).
    """
    p = short_summary_path(channel_name, video_id)
    if p is None:
        return None
    return p.read_text(encoding="utf-8")


def migrate_podcast_longform(video_id: str, source_md: Path,
                             date_str: str, slug_hint: str = "") -> Path:
    """Move a freshly-written ``podcast-kb/vault/Daily/.../_longform.md`` to
    the canonical ``immobilien-kb/vault/Longform/<date>/<video_id>---<slug>.md``.

    Returns the new path. If the canonical path already exists (cache hit),
    the source is removed and the existing one is returned — never duplicate.
    """
    dest = longform_path(video_id, slug_hint or source_md.stem, date_str=date_str)
    if dest.exists():
        source_md.unlink(missing_ok=True)
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(source_md), str(dest))
    return dest


def longform_already_exists(video_id: str) -> bool:
    """Single-line cache check used by cmd_confirm before spawning youtube_daily.

    If this returns True, confirm can skip the entire LLM/translation pipeline
    and just push the cached file to Discord — 0 tokens.
    """
    return find_longform(video_id) is not None
