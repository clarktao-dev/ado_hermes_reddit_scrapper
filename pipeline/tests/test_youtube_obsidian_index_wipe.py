"""Regression test for Task 14 (2026-08-10): youtube_obsidian._index.md wipe bug.

Before the fix, ``step_write_vault(wipe=True)`` only wiped ``*_summary.md``
/ ``*_longform.md`` but left ``_index.md`` alone. The subsequent
``if not index_path.exists()`` guard meant a stale _index.md from an
earlier run (e.g. different channel mix, leftover from a prior day, or
polluted content) would silently survive and keep pointing at old
content even after the digest files had been replaced.

The fix:
  1. Wipe step also deletes ``_index.md`` (when present).
  2. Write step always overwrites ``_index.md`` (no ``if not exists`` guard).

Run from the repo root: ``python3 -m pytest pipeline/tests/test_youtube_obsidian_index_wipe.py -v``
or directly: ``python3 pipeline/tests/test_youtube_obsidian_index_wipe.py``
"""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

# Ensure the repo root is importable when running this file directly.
_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO))

from pipeline.lib.youtube_obsidian import step_write_vault  # noqa: E402


class _MockDigest:
    """Minimal duck-type for VideoDigest (only fields used by the renderer)."""

    def __init__(self, channel_name, title, video_id, summary_zh):
        self.channel_name = channel_name
        self.title = title
        self.video_id = video_id
        self.url = f"https://youtu.be/{video_id}"
        self.summary_zh = summary_zh
        self.analyst_zh = "（bullets）"
        self.producer_zh = "（觀點）"
        self.vocab_zh = "（vocab）"
        self.duration_sec = 600
        self.published_epoch = None
        self.n_chars = 1000


def test_wipe_rebuilds_index_md():
    """Stale _index.md must be removed and rewritten on the next run."""
    with tempfile.TemporaryDirectory() as tmp:
        digests_v1 = [
            _MockDigest("Channel A", "Title A", "vidA", "Summary A"),
            _MockDigest("Channel B", "Title B", "vidB", "Summary B"),
        ]
        step_write_vault(
            digests_v1, repo_root=tmp, date_str="2026-08-10",
            wipe=True, content_kind="short-summary",
        )
        out_dir = Path(tmp) / "podcast-kb" / "vault" / "Daily" / "2026-08-10"
        index_path = out_dir / "_index.md"
        assert index_path.exists()

        # Simulate stale _index.md (polluted content from an earlier run).
        index_path.write_text(
            "# Podcast 摘要索引 — 2026-08-10\n\n- 影片數量：99\n\n"
            "STALE CONTENT FROM OLD RUN\n",
            encoding="utf-8",
        )

        # Next run: different digest set → wipe must remove the polluted index
        # and the writer must rebuild it from the new digests.
        digests_v2 = [_MockDigest("Channel C", "Title C", "vidC", "Summary C")]
        step_write_vault(
            digests_v2, repo_root=tmp, date_str="2026-08-10",
            wipe=True, content_kind="short-summary",
        )
        content = index_path.read_text(encoding="utf-8")
        assert "STALE" not in content, "stale content survived wipe"
        assert "影片數量：1" in content, "should show 1 video, not 99"
        assert "Title C" in content
        assert "Channel C" in content
        assert "Title A" not in content, "old Title A should be gone"


def test_zero_digests_writes_valid_index():
    """0-digest edge case (cron 0-video path) still writes a valid _index.md."""
    with tempfile.TemporaryDirectory() as tmp:
        step_write_vault(
            [], repo_root=tmp, date_str="2026-08-10",
            wipe=True, content_kind="short-summary",
        )
        out_dir = Path(tmp) / "podcast-kb" / "vault" / "Daily" / "2026-08-10"
        index_path = out_dir / "_index.md"
        assert index_path.exists(), "_index.md must exist even for 0 digests"
        content = index_path.read_text(encoding="utf-8")
        assert "影片數量：0" in content


if __name__ == "__main__":
    test_wipe_rebuilds_index_md()
    test_zero_digests_writes_valid_index()
    print("✅ all tests passed")
