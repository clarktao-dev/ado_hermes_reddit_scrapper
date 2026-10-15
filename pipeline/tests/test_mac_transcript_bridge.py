"""Tests for Mac → VPS transcript bridge."""
from __future__ import annotations

import importlib
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

THIS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(THIS_DIR.parent.parent))


def _reload_vault(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    from pipeline.lib import paths
    monkeypatch.setenv("HERMES_VAULT_ROOT", str(tmp_path))
    paths.reset_path_cache()
    import pipeline.lib.transcript_cache as tc
    import pipeline.lib.mac_transcript as mt
    import pipeline.lib.youtube_fetch as yf
    importlib.reload(tc)
    importlib.reload(mt)
    importlib.reload(yf)
    return tc, mt, yf


class TestIngestWebhook:
    def test_writes_channel_and_inbox(self, tmp_path, monkeypatch) -> None:
        tc, mt, _yf = _reload_vault(tmp_path, monkeypatch)
        monkeypatch.setenv("YOUTUBE_TRANSCRIPT_SOURCE", "mac")
        payload = {
            "video_id": "dQw4w9WgXcQ",
            "url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
            "title": "Test Video",
            "language": "de",
            "is_generated": True,
            "source": "youtube-transcript-api",
            "text": "Hallo Welt. Das ist ein Testtranskript.",
            "segments": [],
            "meta": {"channel": "Finanztip", "channel_name": "Finanztip"},
            "fetched_at": "2026-10-15T12:00:00Z",
        }
        summary = mt.ingest_webhook_payload(payload)
        assert summary["ok"] is True
        assert summary["n_chars"] > 0
        cached = tc.read_cached_transcript("Finanztip", "dQw4w9WgXcQ")
        assert cached is not None
        assert "Hallo Welt" in cached.text
        inbox = tc.read_cached_transcript("_mac_inbox", "dQw4w9WgXcQ")
        assert inbox is not None

    def test_rejects_empty_text(self, tmp_path, monkeypatch) -> None:
        _tc, mt, _yf = _reload_vault(tmp_path, monkeypatch)
        with pytest.raises(ValueError, match="empty"):
            mt.ingest_webhook_payload({
                "video_id": "dQw4w9WgXcQ", "text": "",
            })

    def test_rejects_bad_video_id(self, tmp_path, monkeypatch) -> None:
        _tc, mt, _yf = _reload_vault(tmp_path, monkeypatch)
        with pytest.raises(ValueError, match="invalid video_id"):
            mt.ingest_webhook_payload({
                "video_id": "../etc/passwd", "text": "x" * 20,
            })


class TestFetchTranscriptMacMode:
    def test_mac_mode_returns_cache_without_kome(self, tmp_path, monkeypatch) -> None:
        _tc, mt, yf = _reload_vault(tmp_path, monkeypatch)
        monkeypatch.setenv("YOUTUBE_TRANSCRIPT_SOURCE", "mac")
        mt.ingest_webhook_payload({
            "video_id": "AAAAAAAAAAA",
            "text": "Cached DE transcript from Mac.",
            "language": "de",
            "source": "yt-dlp",
            "meta": {"channel": "Der Ex-Makler"},
        })
        video = yf.VideoMeta(
            id="AAAAAAAAAAA", title="t", duration_sec=60, epoch=1,
            url="https://www.youtube.com/watch?v=AAAAAAAAAAA",
            channel_id="UCxxx", channel_name="Der Ex-Makler",
        )
        with patch.object(yf.requests, "post") as post:
            tr = yf.fetch_transcript(video, write_cache=False)
        post.assert_not_called()
        assert "Cached DE" in tr.text

    def test_mac_mode_miss_skips_kome_and_enqueues(self, tmp_path, monkeypatch) -> None:
        _tc, mt, yf = _reload_vault(tmp_path, monkeypatch)
        monkeypatch.setenv("YOUTUBE_TRANSCRIPT_SOURCE", "mac")
        monkeypatch.setenv("MAC_WORKER_URL", "https://mac.example.trycloudflare.com")
        video = yf.VideoMeta(
            id="BBBBBBBBBBB", title="t", duration_sec=60, epoch=1,
            url="https://www.youtube.com/watch?v=BBBBBBBBBBB",
            channel_id="UCxxx", channel_name="Finanztip",
        )
        with patch.object(yf.requests, "post") as kome_post, \
             patch.object(mt, "enqueue_on_mac",
                          return_value={"ok": True, "queued": True}) as enq:
            # fetch_transcript imports mac_transcript fresh; patch the module used inside
            with patch("pipeline.lib.mac_transcript.enqueue_on_mac",
                       return_value={"ok": True, "queued": True}) as enq2:
                tr = yf.fetch_transcript(video, write_cache=False)
        kome_post.assert_not_called()
        assert enq2.called or enq.called
        assert tr.text == ""
        assert tr.n_chars == 0


class TestModeHelpers:
    def test_transcript_source_mode_aliases(self, monkeypatch) -> None:
        from pipeline.lib import mac_transcript as mt
        monkeypatch.setenv("YOUTUBE_TRANSCRIPT_SOURCE", "mac_only")
        assert mt.transcript_source_mode() == "mac"
        monkeypatch.setenv("YOUTUBE_TRANSCRIPT_SOURCE", "kome.ai")
        assert mt.transcript_source_mode() == "kome"
        monkeypatch.delenv("YOUTUBE_TRANSCRIPT_SOURCE", raising=False)
        assert mt.transcript_source_mode() == "auto"
