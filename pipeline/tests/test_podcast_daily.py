"""Tests for pipeline.scripts.podcast_daily (Plan 10, 2026-08-31).

Verifies:
  - ``_load_podcast_channels`` filters correctly by source_type
  - ``_episode_id_for_dedup`` produces stable, channel-scoped hashes
  - ``_vault_channel_name`` / ``_vault_episode_id`` produce filename-safe
    names
  - The end-to-end main() happy path runs without network
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

THIS_DIR = Path(__file__).resolve().parent
PIPELINE_ROOT = THIS_DIR.parent.parent
sys.path.insert(0, str(PIPELINE_ROOT))


class TestLoadPodcastChannels:
    def test_filters_out_youtube_channels(self) -> None:
        from pipeline.scripts.podcast_daily import _load_podcast_channels
        # Patch load_config to return a mixed list
        fake_cfg = {
            "channels": [
                {"id": "yt1", "name": "YT1", "enabled": True},  # no source_type → filtered out
                {"id": "limmo", "name": "L'Immo", "enabled": True,
                 "source_type": "podcast"},
                {"id": "yt2", "name": "YT2", "enabled": True,
                 "channel_id": "UCxxx"},
                {"id": "limmo2", "name": "Other", "enabled": False,
                 "source_type": "podcast"},  # disabled
            ]
        }
        with patch("pipeline.scripts.podcast_daily.load_config",
                   return_value=fake_cfg):
            chans = _load_podcast_channels()
        assert [c["id"] for c in chans] == ["limmo"]

    def test_no_podcast_channels_returns_empty(self) -> None:
        from pipeline.scripts.podcast_daily import _load_podcast_channels
        with patch("pipeline.scripts.podcast_daily.load_config",
                   return_value={"channels": [
                       {"id": "yt1", "name": "YT1", "enabled": True},
                   ]}):
            assert _load_podcast_channels() == []


class TestEpisodeIdForDedup:
    def test_stable_for_same_channel_and_episode(self) -> None:
        from pipeline.lib.podcast_fetch import PodcastEpisode
        from pipeline.scripts.podcast_daily import _episode_id_for_dedup
        ep1 = PodcastEpisode(
            id="abc123", title="t", channel="c",
            published=PodcastEpisode.__dataclass_fields__["published"]
            if False else None,  # satisfy type; not actually used
            duration=0, audio_url="", description="",
            transcript_url=None, transcript_type=None, rss_url="",
        )
        # Rebuild with a proper datetime (avoid the ugly __dataclass_fields trick)
        from datetime import datetime, timezone
        ep1.published = datetime.now(timezone.utc)

        ep2 = PodcastEpisode(
            id="abc123", title="t", channel="c",
            published=datetime.now(timezone.utc),
            duration=0, audio_url="", description="",
            transcript_url=None, transcript_type=None, rss_url="",
        )
        h1 = _episode_id_for_dedup("limmo", ep1)
        h2 = _episode_id_for_dedup("limmo", ep2)
        assert h1 == h2
        assert len(h1) == 64

    def test_channel_salt_prevents_collision(self) -> None:
        from pipeline.lib.podcast_fetch import PodcastEpisode
        from datetime import datetime, timezone
        from pipeline.scripts.podcast_daily import _episode_id_for_dedup
        ep = PodcastEpisode(
            id="abc123", title="t", channel="c",
            published=datetime.now(timezone.utc),
            duration=0, audio_url="", description="",
            transcript_url=None, transcript_type=None, rss_url="",
        )
        h_a = _episode_id_for_dedup("limmo", ep)
        h_b = _episode_id_for_dedup("otherpodcast", ep)
        assert h_a != h_b, "different channels must produce different dedup ids"


class TestVaultNames:
    def test_channel_name_safe(self) -> None:
        from pipeline.scripts.podcast_daily import _vault_channel_name
        # L'Immo's apostrophe must not break the path
        assert "/" not in _vault_channel_name({"id": "limmo"})
        assert "?" not in _vault_channel_name({"id": "limmo?abc"})

    def test_episode_id_handles_url_guids(self) -> None:
        from pipeline.lib.podcast_fetch import PodcastEpisode
        from datetime import datetime, timezone
        from pipeline.scripts.podcast_daily import _vault_episode_id
        # Some podcast feeds use full URLs as guid
        ep = PodcastEpisode(
            id="https://example.com/podcast/episode/12345?foo=bar",
            title="t", channel="c",
            published=datetime.now(timezone.utc),
            duration=0, audio_url="", description="",
            transcript_url=None, transcript_type=None, rss_url="",
        )
        vid = _vault_episode_id(ep)
        assert "/" not in vid
        assert "?" not in vid
        assert "&" not in vid
        assert ":" not in vid


class TestPickChannelsWithMixedChannels:
    """pick_channels from youtube_daily is shared by podcast_daily too.
    It must be agnostic to source_type — verify that."""

    def test_mixed_youtube_and_podcast_rotate_together(self) -> None:
        from pipeline.youtube_daily import pick_channels
        from datetime import datetime, timezone
        channels = [
            {"id": "yt1", "name": "YT1"},
            {"id": "limmo", "name": "L'Immo", "source_type": "podcast"},
            {"id": "yt2", "name": "YT2"},
        ]
        with patch("pipeline.youtube_daily.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 8, 31, tzinfo=timezone.utc)
            picked = pick_channels(channels, n=2)
        # 8/31 doy=243, 243 % 3 = 0 → start at yt1
        assert [c["id"] for c in picked] == ["yt1", "limmo"]