"""L2 / L3 / Phase 2 — YouTube vs podcast source_type isolation tests.

Covers:
  - load_channels keeps podcast channels in the enabled pool
  - yt-dlp URL guard rejects Podigee / Anchor / Libsyn / empty URLs
  - _is_youtube_channel_url classifier matrix
  - pick_video_for_channel dispatches podcast → podcast_fetch
  - L2 per-channel isolation: one channel RuntimeError does not abort siblings
  - podcast ledger source_type + metadata
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

THIS_DIR = Path(__file__).resolve().parent
PIPELINE_ROOT = THIS_DIR.parent.parent
sys.path.insert(0, str(PIPELINE_ROOT))


# ---------------------------------------------------------------------------
# load_channels — podcast stays in the pool
# ---------------------------------------------------------------------------

class TestLoadChannelsEnabled:
    def test_includes_podcast_channel(self, tmp_path, monkeypatch) -> None:
        from pipeline import youtube_daily as yd
        cfg = {
            "channels": [
                {"id": "yt1", "name": "YT", "url": "https://www.youtube.com/@yt",
                 "enabled": True},
                {"id": "limmo", "name": "L'Immo",
                 "url": "https://haufe-immobilienpodcast.podigee.io/feed/mp3",
                 "source_type": "podcast", "enabled": True},
            ]
        }
        p = tmp_path / "channels.json"
        p.write_text(json.dumps(cfg), encoding="utf-8")
        monkeypatch.setattr(yd, "CHANNELS_CONFIG", p)
        chans = yd.load_channels()
        assert [c["id"] for c in chans] == ["yt1", "limmo"]

    def test_disabled_podcast_excluded(self, tmp_path, monkeypatch) -> None:
        from pipeline import youtube_daily as yd
        cfg = {
            "channels": [
                {"id": "limmo", "name": "L'Immo",
                 "url": "https://haufe-immobilienpodcast.podigee.io/feed/mp3",
                 "source_type": "podcast", "enabled": False},
                {"id": "yt1", "name": "YT", "url": "https://www.youtube.com/@yt",
                 "enabled": True},
            ]
        }
        p = tmp_path / "channels.json"
        p.write_text(json.dumps(cfg), encoding="utf-8")
        monkeypatch.setattr(yd, "CHANNELS_CONFIG", p)
        assert [c["id"] for c in yd.load_channels()] == ["yt1"]

    def test_missing_source_type_treated_as_youtube(self, tmp_path, monkeypatch) -> None:
        from pipeline import youtube_daily as yd
        cfg = {
            "channels": [
                {"id": "yt1", "name": "YT", "url": "https://www.youtube.com/@yt",
                 "enabled": True},
            ]
        }
        p = tmp_path / "channels.json"
        p.write_text(json.dumps(cfg), encoding="utf-8")
        monkeypatch.setattr(yd, "CHANNELS_CONFIG", p)
        chans = yd.load_channels()
        assert len(chans) == 1
        assert not yd._is_podcast_channel(chans[0])

    def test_real_channels_json_includes_limmo(self) -> None:
        from pipeline import youtube_daily as yd
        chans = yd.load_channels()
        ids = {c["id"] for c in chans}
        assert "limmo" in ids
        limmo = next(c for c in chans if c["id"] == "limmo")
        assert limmo.get("source_type") == "podcast"


# ---------------------------------------------------------------------------
# L3 — yt-dlp URL guard
# ---------------------------------------------------------------------------

class TestRunYtDlpUrlGuard:
    def test_podigee_returns_empty(self) -> None:
        from pipeline.lib import youtube_fetch as yf
        with patch.object(yf.subprocess, "run") as mock_run:
            out = yf._run_yt_dlp(
                "https://haufe-immobilienpodcast.podigee.io/feed/mp3",
            )
        assert out == []
        mock_run.assert_not_called()

    def test_anchor_returns_empty(self) -> None:
        from pipeline.lib import youtube_fetch as yf
        with patch.object(yf.subprocess, "run") as mock_run:
            out = yf._run_yt_dlp("https://anchor.fm/s/abc/podcast/rss")
        assert out == []
        mock_run.assert_not_called()

    def test_libsyn_returns_empty(self) -> None:
        from pipeline.lib import youtube_fetch as yf
        with patch.object(yf.subprocess, "run") as mock_run:
            out = yf._run_yt_dlp("https://feeds.libsyn.com/12345/rss")
        assert out == []
        mock_run.assert_not_called()

    def test_empty_string_returns_empty(self) -> None:
        from pipeline.lib import youtube_fetch as yf
        with patch.object(yf.subprocess, "run") as mock_run:
            assert yf._run_yt_dlp("") == []
            assert yf._run_yt_dlp("   ") == []
        mock_run.assert_not_called()

    def test_youtube_url_still_invokes_yt_dlp(self) -> None:
        from pipeline.lib import youtube_fetch as yf
        proc = MagicMock()
        proc.returncode = 0
        proc.stdout = ""
        proc.stderr = ""
        with patch.object(yf.subprocess, "run", return_value=proc) as mock_run:
            out = yf._run_yt_dlp("https://www.youtube.com/@finanztip")
        assert out == []
        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        assert cmd[0] == "yt-dlp"
        assert cmd[-1].endswith("/videos")


class TestIsYoutubeChannelUrl:
    @pytest.mark.parametrize("url,expected", [
        ("https://www.youtube.com/@finanztip", True),
        ("https://youtube.com/@finanztip/videos", True),
        ("https://m.youtube.com/@finanztip", True),
        ("https://youtu.be/dQw4w9WgXcQ", True),
        ("https://music.youtube.com/channel/UCxxx", True),
        ("https://haufe-immobilienpodcast.podigee.io/feed/mp3", False),
        ("https://anchor.fm/s/abc/podcast/rss", False),
        ("https://feeds.libsyn.com/12345/rss", False),
        ("https://example.com/feed", False),
        ("", False),
        ("   ", False),
        (None, False),
    ])
    def test_classifier(self, url, expected) -> None:
        from pipeline.lib import youtube_fetch as yf
        assert yf._is_youtube_channel_url(url) is expected

    def test_list_channel_videos_skips_podigee(self) -> None:
        from pipeline.lib import youtube_fetch as yf
        with patch.object(yf, "_list_via_invidious") as inv, \
             patch.object(yf, "_run_yt_dlp") as ytdlp:
            out = yf.list_channel_videos(
                "limmo", "L'Immo",
                "https://haufe-immobilienpodcast.podigee.io/feed/mp3",
            )
        assert out == []
        inv.assert_not_called()
        ytdlp.assert_not_called()


# ---------------------------------------------------------------------------
# Phase 2 — podcast dispatch
# ---------------------------------------------------------------------------

def _fake_episode(**overrides):
    from pipeline.lib.podcast_fetch import PodcastEpisode
    defaults = dict(
        id="ep-guid-abc123",
        title="Test Folge",
        channel="L'Immo",
        published=datetime(2026, 9, 1, tzinfo=timezone.utc),
        duration=1800,
        audio_url="https://example.com/ep.mp3",
        description="Guest bio and chapter outline.",
        transcript_url=None,
        transcript_type=None,
        rss_url="https://haufe-immobilienpodcast.podigee.io/feed/mp3",
    )
    defaults.update(overrides)
    return PodcastEpisode(**defaults)


class TestPodcastDispatch:
    def test_pick_video_for_channel_uses_podcast_fetch(self) -> None:
        from pipeline import youtube_daily as yd
        ch = {
            "id": "limmo",
            "name": "L'Immo",
            "url": "https://haufe-immobilienpodcast.podigee.io/feed/mp3",
            "source_type": "podcast",
        }
        store = MagicMock()
        store.is_processed.return_value = False
        ep = _fake_episode()
        with patch.object(yd.podcast_fetch, "list_podcast_episodes",
                          return_value=[ep]) as list_ep, \
             patch.object(yd.youtube_fetch, "list_channel_videos") as list_yt:
            cands = yd.pick_video_for_channel(ch, store=store, look_back=5)
        list_ep.assert_called_once()
        list_yt.assert_not_called()
        assert len(cands) == 1
        assert cands[0].id == "ep-guid-abc123"
        assert cands[0].title == "Test Folge"
        store.is_processed.assert_called_with("podcast", "ep-guid-abc123")

    def test_pick_video_youtube_path_unchanged(self) -> None:
        from pipeline import youtube_daily as yd
        from pipeline.lib.youtube_fetch import VideoMeta
        ch = {
            "id": "finanztip",
            "name": "Finanztip",
            "url": "https://www.youtube.com/@finanztip",
            "channel_id": "UCxxx",
        }
        store = MagicMock()
        store.is_processed.return_value = False
        meta = VideoMeta(
            id="AAAAAAAAAAA", title="t", duration_sec=60, epoch=1,
            url="https://www.youtube.com/watch?v=AAAAAAAAAAA",
            channel_id="UCxxx", channel_name="Finanztip",
        )
        with patch.object(yd.youtube_fetch, "list_channel_videos",
                          return_value=[meta]) as list_yt, \
             patch.object(yd.podcast_fetch, "list_podcast_episodes") as list_ep:
            cands = yd.pick_video_for_channel(ch, store=store)
        list_yt.assert_called_once()
        list_ep.assert_not_called()
        assert cands[0].id == "AAAAAAAAAAA"
        store.is_processed.assert_called_with("youtube", "AAAAAAAAAAA")

    def test_fetch_transcript_podcast_path(self) -> None:
        from pipeline import youtube_daily as yd
        ch = {"id": "limmo", "name": "L'Immo", "source_type": "podcast",
              "url": "https://haufe-immobilienpodcast.podigee.io/feed/mp3"}
        ep = _fake_episode()
        meta = yd._podcast_to_video_meta(ep, ch)
        with patch.object(yd.podcast_fetch, "fetch_transcript",
                          return_value=("Hallo Welt transcript", "rss-description")), \
             patch.object(yd.youtube_fetch, "fetch_transcript") as yt_fetch:
            tr = yd._fetch_transcript_for_channel(ch, meta, write_cache=False)
        yt_fetch.assert_not_called()
        assert tr.text == "Hallo Welt transcript"
        assert tr.n_chars == len("Hallo Welt transcript")

    def test_build_metadata_includes_podcast_fields(self) -> None:
        from pipeline import youtube_daily as yd
        ch = {"id": "limmo", "name": "L'Immo", "source_type": "podcast",
              "url": "https://haufe-immobilienpodcast.podigee.io/feed/mp3"}
        ep = _fake_episode()
        meta = yd._podcast_to_video_meta(ep, ch)
        digest = SimpleNamespace(
            source_language="de", n_chars=100, map_calls=0, reduce_calls=1,
            elapsed_sec=1.0, summary_zh="ok",
        )
        md = yd._build_metadata(digest, meta, ch)
        assert md["source_type"] == "podcast"
        assert md["episode_guid"] == "ep-guid-abc123"
        assert md["rss_url"].endswith("/feed/mp3")


# ---------------------------------------------------------------------------
# L2 — per-channel isolation
# ---------------------------------------------------------------------------

class TestPerChannelIsolation:
    def test_sibling_survives_channel_crash(self) -> None:
        """One channel RuntimeError must not prevent sibling digests."""
        from pipeline import youtube_daily as yd
        from pipeline.lib.youtube_fetch import VideoMeta
        from pipeline.lib.youtube_translate import VideoDigest

        ch_bad = {"id": "limmo", "name": "L'Immo", "source_type": "podcast",
                  "url": "https://haufe-immobilienpodcast.podigee.io/feed/mp3"}
        ch_ok = {"id": "insightsimmo", "name": "Insights Immo",
                 "url": "https://www.youtube.com/@insightsimmo",
                 "channel_id": "UCxxx"}

        good_meta = VideoMeta(
            id="GOODVIDEO01", title="Good", duration_sec=120, epoch=1,
            url="https://www.youtube.com/watch?v=GOODVIDEO01",
            channel_id="UCxxx", channel_name="Insights Immo",
        )
        good_digest = VideoDigest(
            video_id="GOODVIDEO01", title="Good", channel_name="Insights Immo",
            url=good_meta.url, published_epoch=1, duration_sec=120,
            source_language="de", n_chars=50,
            summary_zh="摘要", analyst_zh="- a", producer_zh="觀點",
            vocab_zh="", map_calls=0, reduce_calls=1, elapsed_sec=0.1,
        )

        call_count = {"n": 0}

        def fake_process(ch, **kwargs):
            call_count["n"] += 1
            if ch["id"] == "limmo":
                raise RuntimeError("yt-dlp failed: Unsupported URL")
            return (ch, good_meta, good_digest)

        args = SimpleNamespace(
            dry_run=True, mode="short", channels="limmo,insightsimmo",
            n_channels=2, skip_store=True, pipeline_run_id="test-run",
            video_id="", force=False,
        )

        with patch.object(yd, "load_channels", return_value=[ch_bad, ch_ok]), \
             patch.object(yd, "_process_one_channel", side_effect=fake_process), \
             patch.object(yd, "ProcessedStore"), \
             patch.object(yd.youtube_state, "StateStore"), \
             patch.object(yd.youtube_obsidian, "step_write_vault",
                          return_value={"n_files": 0, "n_errors": 0}), \
             patch.object(yd.youtube_discord, "step_send_discord",
                          return_value={"n_embeds": 1, "errors": [],
                                        "per_video": []}), \
             patch.object(yd, "push_to_github",
                          return_value={"pushed": False, "commit_sha": None,
                                        "dry_run": True}), \
             patch.object(yd.time, "sleep"), \
             patch("sys.argv", [
                 "youtube_daily.py", "--channels", "limmo,insightsimmo",
                 "--dry-run", "--skip-store",
             ]):
            rc = yd.main()

        assert rc == 0
        assert call_count["n"] == 2

    def test_systemexit_propagates(self) -> None:
        from pipeline import youtube_daily as yd
        ch = {"id": "limmo", "name": "L'Immo", "source_type": "podcast",
              "url": "https://haufe-immobilienpodcast.podigee.io/feed/mp3"}

        def boom(ch, **kwargs):
            raise SystemExit(3)

        with patch.object(yd, "load_channels", return_value=[ch]), \
             patch.object(yd, "_process_one_channel", side_effect=boom), \
             patch.object(yd, "ProcessedStore"), \
             patch.object(yd.youtube_state, "StateStore"), \
             patch.object(yd.time, "sleep"), \
             patch("sys.argv", [
                 "youtube_daily.py", "--channels", "limmo",
                 "--dry-run", "--skip-store",
             ]):
            with pytest.raises(SystemExit) as ei:
                yd.main()
        assert ei.value.code == 3


class TestPodcastDailyShim:
    def test_shim_forwards_to_youtube_daily(self) -> None:
        from pipeline.scripts import podcast_daily as pd

        captured = {}

        def fake_yt_main():
            captured["argv"] = list(sys.argv)
            return 0

        with patch.object(pd, "_load_podcast_channels", return_value=[
                {"id": "limmo", "name": "L'Immo", "source_type": "podcast",
                 "enabled": True},
             ]), \
             patch.object(pd, "youtube_daily_main", side_effect=fake_yt_main), \
             patch("sys.argv", ["podcast_daily.py", "--dry-run"]):
            rc = pd.main()
        assert rc == 0
        assert captured["argv"][0] == "youtube_daily.py"
        assert "--channels" in captured["argv"]
        assert "limmo" in captured["argv"]
        assert "--dry-run" in captured["argv"]
