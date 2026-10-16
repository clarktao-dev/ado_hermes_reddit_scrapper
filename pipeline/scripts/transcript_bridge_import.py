#!/usr/bin/env python3
"""Import transcript-bridge/outbox/*.json into transcript_cache (then archive to done/)."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from pipeline.lib import transcript_bridge  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--push", action="store_true",
                    help="git commit + push done/ moves after import")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    files = transcript_bridge.list_outbox_files()
    if not files:
        print("outbox empty — nothing to import")
        return 0
    print(f"found {len(files)} outbox file(s)")
    if args.dry_run:
        for p in files:
            print(f"  would import: {p.name}")
        return 0

    summaries = transcript_bridge.import_outbox(move_to_done=True)
    print(json.dumps(summaries, ensure_ascii=False, indent=2))

    if args.push:
        done_paths = [
            transcript_bridge.done_dir() / s["outbox_file"]
            for s in summaries if s.get("ok") and s.get("outbox_file")
        ]
        # also stage cleared outbox deletions via adding done + git add -u outbox
        root = transcript_bridge.bridge_root()
        result = transcript_bridge.git_push_bridge(
            [root / "outbox", root / "done", root / "inbox"],
            "transcript-bridge: import outbox → cache",
        )
        # git_push_bridge only adds files; force add -u for deletions
        import subprocess
        subprocess.run(
            ["git", "add", "-u", "--", "transcript-bridge"],
            cwd=str(ROOT), check=False,
        )
        print(result)
    return 0 if all(s.get("ok") for s in summaries) else 1


if __name__ == "__main__":
    raise SystemExit(main())
