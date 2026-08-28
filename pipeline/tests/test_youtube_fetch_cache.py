"""Tests for transcript cache write gating in youtube_fetch.fetch_transcript."""
from __future__ import annotations

from pathlib import Path
from unittest import mock

import pytest

from pipeline.lib import paths
from pipeline.lib.youtube_fetch import VideoMeta, fetch_transcript


def _video() -> VideoMeta:
    return VideoMeta(
        id="dryrunvid01",
        title="Dry run test",
        duration_sec=120,
        epoch=1_700_000_000,
        channel_id="UCtest",
        channel_name="Test Channel",
        url="https://youtu.be/dryrunvid01",
    )


def test_fetch_transcript_skips_cache_write_when_disabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HERMES_VAULT_ROOT", str(tmp_path))
    paths.reset_path_cache()
    import importlib
    import pipeline.lib.transcript_cache as tc
    import pipeline.lib.youtube_fetch as yf

    importlib.reload(tc)
    importlib.reload(yf)

    video = _video()
    fake_resp = mock.Mock()
    fake_resp.raise_for_status = mock.Mock()
    fake_resp.json.return_value = {"transcript": "Hallo Welt transcript text"}

    with mock.patch.object(yf.requests, "post", return_value=fake_resp):
        with mock.patch(
            "pipeline.lib.transcript_cache.write_transcript",
        ) as write_mock:
            result = yf.fetch_transcript(video, write_cache=False)

    assert result.text.startswith("Hallo Welt")
    write_mock.assert_not_called()


def test_fetch_transcript_writes_cache_by_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HERMES_VAULT_ROOT", str(tmp_path))
    paths.reset_path_cache()
    import importlib
    import pipeline.lib.transcript_cache as tc
    import pipeline.lib.youtube_fetch as yf

    importlib.reload(tc)
    importlib.reload(yf)

    video = _video()
    fake_resp = mock.Mock()
    fake_resp.raise_for_status = mock.Mock()
    fake_resp.json.return_value = {"transcript": "Default cache write text"}

    with mock.patch.object(yf.requests, "post", return_value=fake_resp):
        with mock.patch(
            "pipeline.lib.transcript_cache.write_transcript",
        ) as write_mock:
            yf.fetch_transcript(video)

    write_mock.assert_called_once()
