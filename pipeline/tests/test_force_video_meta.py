"""Tests for --video-id metadata resolution and --force candidate wiring."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.youtube_fetch import VideoMeta, fetch_video_meta_via_invidious


def test_fetch_video_meta_via_invidious_parses_response():
    payload = {
        "videoId": "srecvluFqwQ",
        "title": "Fire Protection Goals Explained Simply",
        "lengthSeconds": 668,
        "published": 1756272000,
        "author": "So geht Brandschutz",
        "authorId": "UCxbStTzwp1DQCefSOLdBZ9w",
        "viewCount": 1234,
    }
    mock_resp = MagicMock()
    mock_resp.read.return_value = json.dumps(payload).encode("utf-8")
    mock_resp.__enter__.return_value = mock_resp
    mock_resp.__exit__.return_value = False

    with patch("pipeline.lib.youtube_fetch.urllib.request.urlopen",
               return_value=mock_resp):
        meta = fetch_video_meta_via_invidious("srecvluFqwQ")

    assert meta is not None
    assert meta.id == "srecvluFqwQ"
    assert meta.title == payload["title"]
    assert meta.duration_sec == 668
    assert meta.epoch == 1756272000
    assert meta.channel_name == "So geht Brandschutz"


def test_fetch_video_meta_via_invidious_returns_none_when_all_fail():
    with patch("pipeline.lib.youtube_fetch.urllib.request.urlopen",
               side_effect=OSError("network down")):
        meta = fetch_video_meta_via_invidious("srecvluFqwQ")
    assert meta is None


if __name__ == "__main__":
    test_fetch_video_meta_via_invidious_parses_response()
    test_fetch_video_meta_via_invidious_returns_none_when_all_fail()
    print("force video meta tests OK")
