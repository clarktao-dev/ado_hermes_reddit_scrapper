"""Tests for pipeline.lib.youtube_discord embed builder (Plan 11, 2026-08-31).

User feedback: Discord pushes for YouTube daily were missing the video
title and published date — only channel name, duration, and link were
shown. Now ``_build_embed_body`` adds those two lines explicitly.

These tests pin the new behavior and lock down the formatting (so
localised strings or unit-of-time tweaks need an explicit decision).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import sys

THIS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(THIS_DIR.parent.parent))


@dataclass
class FakeDigest:
    """Duck-typed VideoDigest (avoids importing the real one with side effects)."""
    video_id: str = "abc12345678"
    title: str = "Wie man Immobilien richtig kauft | 1aLAGE #300"
    channel_name: str = "1aLAGE Immobilienpodcast"
    url: str = "https://www.youtube.com/watch?v=abc12345678"
    published_epoch: object = 1787810173  # 2026-08-27 05:56 UTC
    duration_sec: int = 1630
    summary_zh: str = "一句話摘要"
    analyst_zh: str = "分析"
    producer_zh: str = "製作"
    vocab_zh: str = "詞彙"


def test_embed_body_includes_title_and_date() -> None:
    from pipeline.lib.youtube_discord import _build_embed_body
    d = FakeDigest()
    body = _build_embed_body(d)
    assert "**標題**：Wie man Immobilien richtig kauft | 1aLAGE #300" in body
    assert "**發布日期**：2026-08-27" in body
    # channel name + duration + link still there (don't break legacy layout)
    assert "**頻道**：1aLAGE Immobilienpodcast" in body
    assert "**影片時長**：27 分 10 秒" in body
    assert "**連結**：https://www.youtube.com/watch?v=abc12345678" in body


def test_embed_body_missing_epoch_renders_em_dash() -> None:
    """When Invidious is down for --video-id fetch, epoch comes back None.
    The date line should still appear with a placeholder, not crash."""
    from pipeline.lib.youtube_discord import _build_embed_body
    d = FakeDigest(published_epoch=None)
    body = _build_embed_body(d)
    assert "**發布日期**：——" in body


def test_embed_body_missing_title_renders_placeholder() -> None:
    """Defensive — title might be empty for an unparsed digest."""
    from pipeline.lib.youtube_discord import _build_embed_body
    d = FakeDigest(title="")
    body = _build_embed_body(d)
    assert "**標題**：（無標題）" in body


def test_format_published_handles_invalid_epoch() -> None:
    from pipeline.lib.youtube_discord import _format_published
    assert _format_published(None) == "——"
    assert _format_published(0) == "——"
    assert _format_published("not-a-number") == "——"
    # Real epoch
    assert _format_published(1787810173) == "2026-08-27"


def test_embed_body_order_title_then_date() -> None:
    """Pin the line order so Discord renders consistently across runs.

    Channel → Title → Date → Duration → URL → Summary. The title+date
    pair is the user-requested addition and must appear right after
    channel (the natural 'about this video' group)."""
    from pipeline.lib.youtube_discord import _build_embed_body
    d = FakeDigest()
    body = _build_embed_body(d)
    ch_idx = body.index("**頻道**")
    title_idx = body.index("**標題**")
    date_idx = body.index("**發布日期**")
    dur_idx = body.index("**影片時長**")
    url_idx = body.index("**連結**")
    assert ch_idx < title_idx < date_idx < dur_idx < url_idx, (
        f"order wrong: ch={ch_idx} title={title_idx} date={date_idx} "
        f"dur={dur_idx} url={url_idx}"
    )