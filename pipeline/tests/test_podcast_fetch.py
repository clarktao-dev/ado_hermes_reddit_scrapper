"""Tests for pipeline.lib.podcast_fetch.

Plan 10 (2026-08-31): exercises RSS parsing + transcript fetching
against the saved l'Immo feed (/tmp/limmo_feed.xml) which is a real
artifact from the Podigee-hosted RSS — same shape as production.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

THIS_DIR = Path(__file__).resolve().parent
PIPELINE_ROOT = THIS_DIR.parent.parent
sys.path.insert(0, str(PIPELINE_ROOT))

from pipeline.lib import podcast_fetch  # noqa: E402

LIMMO_CHANNEL = {
    "id": "limmo",
    "name": "L'Immo (Haufe Immobilien Podcast)",
    "url": "https://haufe-immobilienpodcast.podigee.io/feed/mp3",
    "category": "real_estate",
    "source_type": "podcast",
    "enabled": True,
}

# Saved during research; we don't re-hit Podigee in unit tests.
SAVED_FEED = Path("/tmp/limmo_feed.xml")


class TestParseHelpers:
    def test_parse_duration_seconds(self) -> None:
        assert podcast_fetch._parse_duration_to_seconds("1835") == 1835
        assert podcast_fetch._parse_duration_to_seconds("30:45") == 30 * 60 + 45
        assert podcast_fetch._parse_duration_to_seconds("01:30:45") == 3600 + 30 * 60 + 45
        assert podcast_fetch._parse_duration_to_seconds("") == 0
        assert podcast_fetch._parse_duration_to_seconds("garbage") == 0

    def test_parse_pub_date(self) -> None:
        d = podcast_fetch._parse_pub_date("Mon, 31 Aug 2026 05:00:00 +0000")
        assert d is not None
        assert d.year == 2026 and d.month == 8 and d.day == 31

    def test_parse_pub_date_none(self) -> None:
        assert podcast_fetch._parse_pub_date("") is None
        assert podcast_fetch._parse_pub_date("not a date") is None

    def test_parse_vtt_to_text(self) -> None:
        vtt = """WEBVTT

00:00:00.140 --> 00:00:05.800
Speaker 0: Herzlich willkommen zu einer neuen Folge von Limo.

00:00:06.180 --> 00:00:14.980
Speaker 0: Das Thema Wohnen betrifft uns alle.
"""
        text = podcast_fetch._parse_vtt_to_text(vtt)
        assert "Herzlich willkommen" in text
        assert "Speaker 0:" not in text, "Speaker label must be stripped"
        assert "00:00:00" not in text, "Timestamp lines must be removed"
        assert "Das Thema Wohnen" in text

    def test_parse_json_transcript_to_text(self) -> None:
        raw = '[{"start":0.14,"end":5.8,"text":"Hello world"},{"start":6,"end":10,"text":"Second line"}]'
        text = podcast_fetch._parse_json_transcript_to_text(raw)
        assert text == "Hello world\nSecond line"

    def test_parse_json_transcript_invalid(self) -> None:
        assert podcast_fetch._parse_json_transcript_to_text("not json") is None
        assert podcast_fetch._parse_json_transcript_to_text('{"not":"a list"}') is None


class TestListPodcastEpisodes:
    def test_parses_saved_feed(self) -> None:
        """End-to-end RSS parse against the saved l'Immo feed."""
        # patch the URL fetcher to read the saved file instead of hitting network
        def fake_urlopen(req, timeout=20):
            class FakeResp:
                def __init__(self, body):
                    self.body = body
                def read(self):
                    return self.body
                def __enter__(self):
                    return self
                def __exit__(self, *a):
                    return False
            return FakeResp(SAVED_FEED.read_bytes())

        with patch("urllib.request.urlopen", fake_urlopen):
            eps = podcast_fetch.list_podcast_episodes(LIMMO_CHANNEL, limit=5)

        assert len(eps) == 5
        ep = eps[0]
        assert "Klimaschutz" in ep.title or "Zwischen" in ep.title
        assert ep.channel == LIMMO_CHANNEL["name"]
        assert ep.audio_url.startswith("https://audio.podigee-cdn.net/")
        assert ep.duration > 0
        # First episode is 2026-08-31
        assert ep.published.year == 2026 and ep.published.month == 8

    def test_at_least_some_episodes_have_transcripts(self) -> None:
        """The l'Immo feed publishes 26/329 episodes with transcripts.
        We assert ≥1 episode in the saved feed has a transcript URL."""
        def fake_urlopen(req, timeout=20):
            class FakeResp:
                def __init__(self, body):
                    self.body = body
                def read(self):
                    return self.body
                def __enter__(self):
                    return self
                def __exit__(self, *a):
                    return False
            return FakeResp(SAVED_FEED.read_bytes())

        with patch("urllib.request.urlopen", fake_urlopen):
            eps = podcast_fetch.list_podcast_episodes(LIMMO_CHANNEL, limit=329)

        with_tx = [e for e in eps if e.transcript_url]
        # 26 transcripts in 329 episodes; we fetch all 329 here.
        # _extract_episode prefers application/json over text/vtt, so
        # we get ~13 entries (matching the unique-JSON count).
        assert len(with_tx) >= 10, (
            f"expected ≥10 episodes with transcript URL, got {len(with_tx)}"
        )

    def test_no_url_returns_empty(self) -> None:
        eps = podcast_fetch.list_podcast_episodes(
            {"id": "x", "name": "X", "url": "", "source_type": "podcast"},
            limit=5,
        )
        assert eps == []


class TestFetchTranscriptFallback:
    def _make_ep(self, **overrides) -> podcast_fetch.PodcastEpisode:
        defaults = dict(
            id="ep1", title="t", channel="c",
            published=podcast_fetch.datetime.now(podcast_fetch.timezone.utc),
            duration=600, audio_url="https://example.com/a.mp3",
            description="Fallback description text.",
            transcript_url=None, transcript_type=None,
            rss_url="https://example.com/feed",
        )
        defaults.update(overrides)
        return podcast_fetch.PodcastEpisode(**defaults)

    def test_falls_back_to_description_when_no_transcript(self) -> None:
        ep = self._make_ep()
        text, src = podcast_fetch.fetch_transcript(ep)
        assert src == "rss-description"
        assert "Fallback description" in text

    def test_json_transcript_url_used(self) -> None:
        ep = self._make_ep(
            transcript_url="https://example.com/t.json",
            transcript_type="application/json",
        )
        raw = '[{"start":0,"end":1,"text":"Hi from JSON"}]'

        def fake_urlopen(req, timeout=15):
            class R:
                def read(self):
                    return raw.encode("utf-8")
                def __enter__(self):
                    return self
                def __exit__(self, *a):
                    return False
            return R()

        with patch("urllib.request.urlopen", fake_urlopen):
            text, src = podcast_fetch.fetch_transcript(ep)
        assert src == "podcast-transcript-json"
        assert text == "Hi from JSON"

    def test_vtt_transcript_url_used(self) -> None:
        ep = self._make_ep(
            transcript_url="https://example.com/t.vtt",
            transcript_type="text/vtt",
        )
        vtt = "WEBVTT\n\n00:00:00.000 --> 00:00:05.000\nSpeaker 0: Hi from VTT\n"

        def fake_urlopen(req, timeout=15):
            class R:
                def read(self):
                    return vtt.encode("utf-8")
                def __enter__(self):
                    return self
                def __exit__(self, *a):
                    return False
            return R()

        with patch("urllib.request.urlopen", fake_urlopen):
            text, src = podcast_fetch.fetch_transcript(ep)
        assert src == "podcast-transcript-vtt"
        assert "Hi from VTT" in text
        assert "Speaker 0:" not in text