"""Tests for GitHub mailbox transcript-bridge."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


def test_enqueue_and_import(tmp_path, monkeypatch):
    from pipeline.lib import paths, transcript_bridge, mac_transcript
    monkeypatch.setenv("HERMES_VAULT_ROOT", str(tmp_path))
    monkeypatch.setenv("PIPELINE_ROOT", str(tmp_path))
    paths.reset_path_cache()
    paths._refresh_module_constants()

    # Point bridge at tmp pipeline root
    monkeypatch.setattr(transcript_bridge, "bridge_root", lambda: tmp_path / "transcript-bridge")

    path = transcript_bridge.enqueue_job(
        "dQw4w9WgXcQ",
        "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        channel="Finanztip",
    )
    assert path.exists()
    job = json.loads(path.read_text())
    assert job["video_id"] == "dQw4w9WgXcQ"

    # Simulate iPhone outbox
    out = transcript_bridge.outbox_dir() / "dQw4w9WgXcQ.json"
    out.write_text(json.dumps({
        "video_id": "dQw4w9WgXcQ",
        "url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        "title": "Test",
        "language": "de",
        "is_generated": True,
        "source": "ios-timedtext",
        "text": "Hallo aus dem Outbox.",
        "segments": [],
        "meta": {"channel": "Finanztip"},
        "fetched_at": 1.0,
    }), encoding="utf-8")

    summaries = transcript_bridge.import_outbox()
    assert summaries and summaries[0]["ok"] is True
    assert not out.exists()
    assert (transcript_bridge.done_dir() / "dQw4w9WgXcQ.json").exists()

    cached = mac_transcript.read_mac_or_channel_cache("Finanztip", "dQw4w9WgXcQ")
    assert cached is not None
    assert "Hallo" in cached.text
