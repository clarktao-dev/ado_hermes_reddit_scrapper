#!/usr/bin/env python3
"""Run youtube_daily.main() with mocked fetch_transcript / digest_video.

This lets us verify the ProcessedStore integration end-to-end without paying
for kome.ai / Google Translate / LLM. The mocks are minimal: every video
returns a deterministic canned digest so we can assert mark_processed and
update_side_effects behaviour against a real Airtable base.

Usage:
    python3 pipeline/tests/run_youtube_daily_with_mocks.py \\
        --video-id AcNbIi4_gbY --dry-run
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE.parent.parent))

import pipeline.lib.youtube_fetch as yf
import pipeline.lib.youtube_translate as yt
import pipeline.youtube_daily as yd


from pipeline.lib.youtube_fetch import VideoMeta

# Canned VideoMeta lookup — keyed by video id.
_CANNED_METADATA: dict[str, dict] = {
    "AcNbIi4_gbY": {
        "title": "Why the seller is never liable for your property purchase",
        "duration_sec": 720,
        "epoch": 1786240980,
        "channel_id": "UCc6pj_-MUg_Q9NAzzShpuiQ",
        "channel_name": "Der Ex-Makler",
    },
    "pKAUkc5BsmA": {
        "title": "Eigentumswohnung: Wenn die Gemeinschaft zur Hölle wird",
        "duration_sec": 660,
        "epoch": 1786118424,
        "channel_id": "UCc6pj_-MUg_Q9NAzzShpuiQ",
        "channel_name": "Der Ex-Makler",
    },
    "G7Z0vOgQRUA": {
        "title": "These 9 Differences Between New Builds and Existing Properties",
        "duration_sec": 900,
        "epoch": 1785859210,
        "channel_id": "UCc6pj_-MUg_Q9NAzzShpuiQ",
        "channel_name": "Der Ex-Makler",
    },
}


def _mock_force_fetch(video_id: str) -> VideoMeta | None:
    """Synthetic replacement for _force_fetch_video_meta."""
    md = _CANNED_METADATA.get(video_id)
    if md is None:
        print(f"  [mock] no canned metadata for {video_id}", file=sys.stderr)
        return None
    return VideoMeta(
        id=video_id,
        title=md["title"],
        duration_sec=md["duration_sec"],
        epoch=md["epoch"],
        url=f"https://www.youtube.com/watch?v={video_id}",
        channel_id=md["channel_id"],
        channel_name=md["channel_name"],
    )


# Canned digest returned for every video the orchestrator asks us to translate.
def _make_canned_digest(video, transcript_text: str) -> yt.VideoDigest:
    return yt.VideoDigest(
        video_id=video.id,
        title=video.title,
        channel_name=video.channel_name,
        url=video.url,
        published_epoch=video.epoch,
        duration_sec=video.duration_sec,
        source_language="de",
        n_chars=len(transcript_text),
        summary_zh=f"（canned summary for {video.id}）",
        analyst_zh=f"（canned analyst view for {video.id}）",
        producer_zh=f"（canned producer view for {video.id}）",
        vocab_zh=f"（canned vocab for {video.id}）",
        map_calls=1,
        reduce_calls=1,
        elapsed_sec=0.01,
    )


def _mock_fetch_transcript(video, **kwargs):
    """Return a canned transcript so kome.ai is not called."""
    text = f"（canned transcript for {video.id}, {len(video.title)} chars title）"
    return yf.TranscriptResult(
        video=video, language="de", text=text,
        n_chars=len(text), is_premium=False,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--video-id", required=True,
                    help="YouTube video id to process (passed through)")
    ap.add_argument("--pipeline-run-id", default="mock-test-run")
    ap.add_argument("--dry-run", action="store_true", default=True)
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--skip-store", action="store_true")
    args = ap.parse_args()

    # Inject mocks BEFORE main() runs.
    yf.fetch_transcript = _mock_fetch_transcript  # type: ignore
    yt.digest_video = _make_canned_digest  # type: ignore
    yd._force_fetch_video_meta = _mock_force_fetch  # type: ignore

    # Forward to youtube_daily.main().
    sys.argv = [
        "youtube_daily.py",
        "--video-id", args.video_id,
        "--pipeline-run-id", args.pipeline_run_id,
        "--dry-run",
    ]
    if args.force:
        sys.argv.append("--force")
    if args.skip_store:
        sys.argv.append("--skip-store")
    rc = yd.main()
    print(f"\n[test driver] youtube_daily.main() returned {rc}")
    return rc


if __name__ == "__main__":
    sys.exit(main() or 0)
