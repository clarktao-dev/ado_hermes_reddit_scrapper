#!/usr/bin/env python3
"""Put a YouTube URL into transcript-bridge/inbox/ (GitHub mailbox for iPhone App)."""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from pipeline.lib import transcript_bridge  # noqa: E402


def _video_id(url_or_id: str) -> str:
    s = url_or_id.strip()
    if re.fullmatch(r"[A-Za-z0-9_-]{11}", s):
        return s
    m = re.search(r"(?:v=|/shorts/|/embed/|youtu\.be/)([A-Za-z0-9_-]{11})", s)
    if not m:
        raise SystemExit(f"cannot parse video id from: {url_or_id}")
    return m.group(1)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--url", required=True, help="YouTube URL or 11-char video id")
    ap.add_argument("--title", default="")
    ap.add_argument("--channel", default="")
    ap.add_argument("--languages", default="de,en")
    ap.add_argument("--push", action="store_true", help="git commit + push after write")
    args = ap.parse_args()

    vid = _video_id(args.url)
    url = args.url if args.url.startswith("http") else f"https://www.youtube.com/watch?v={vid}"
    langs = [x.strip() for x in args.languages.split(",") if x.strip()]
    path = transcript_bridge.enqueue_job(
        vid, url, title=args.title, channel=args.channel, languages=langs,
        requested_by="cli",
    )
    print(f"enqueued: {path}")
    if args.push:
        result = transcript_bridge.git_push_bridge(
            [path], f"transcript-bridge: enqueue {vid}",
        )
        print(result)
        return 0 if result.get("ok") else 1
    print("hint: add --push so the iPhone App can see it on GitHub")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
