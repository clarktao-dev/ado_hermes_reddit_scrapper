#!/usr/bin/env python3
"""Clean up expired transcript cache entries (cron entry, weekly).

Reads TTL frontmatter from each
``immobilien-kb/vault/YouTube/<Channel>/_transcripts/<video_id>.md``
file and deletes anything past its ``expires_at``.

Exit codes:
  0  cleanup ran (whether or not anything was deleted)
  1  cleanup ran but deleted nothing AND no entries exist (cold start)

Designed to be cron-driven: ``0 3 * * 1`` (Monday 03:00 UTC).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Allow `python pipeline/scripts/cleanup_transcripts.py` from repo root.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib import transcript_cache  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser(
        description="Delete expired transcript cache entries.",
    )
    p.add_argument("--dry-run", action="store_true",
                   help="Print what would be deleted without removing anything.")
    p.add_argument("--verbose", action="store_true",
                   help="Also print still-fresh entries.")
    args = p.parse_args()

    stats_before = transcript_cache.stats()
    print(f"📊 before: cached={stats_before['cached']}, "
          f"expired={stats_before['expired']}")
    for ch in stats_before["channels"]:
        print(f"  - {ch['channel']}: {ch['cached']} cached, {ch['expired']} expired")

    if args.dry_run:
        print("\n🔍 dry-run: listing files that would be deleted...")
        # Re-walk to surface candidate paths without mutating.
        from datetime import datetime, timezone
        from pipeline.lib.transcript_cache import (
            TRANSCRIPTS_ROOT, _FRONTMATTER_RE,
        )
        now = datetime.now(timezone.utc)
        n = 0
        if TRANSCRIPTS_ROOT.exists():
            for channel_dir in TRANSCRIPTS_ROOT.iterdir():
                transcripts_dir = channel_dir / "_transcripts"
                if not transcripts_dir.exists():
                    continue
                for f in transcripts_dir.glob("*.md"):
                    raw = f.read_text(encoding="utf-8")
                    m = _FRONTMATTER_RE.match(raw)
                    if not m:
                        continue
                    try:
                        expires_at = datetime.fromisoformat(
                            m.group(1).split("expires_at:")[1].split("\n")[0].strip()
                        )
                    except (IndexError, ValueError):
                        continue
                    if now >= expires_at:
                        print(f"  would delete: {f}")
                        n += 1
        print(f"\n→ would delete {n} files")
        return 0

    deleted = transcript_cache.cleanup_expired()
    print(f"\n🗑️  deleted {len(deleted)} expired file(s)")
    for p in deleted:
        print(f"  - {p.relative_to(transcript_cache.REPO_ROOT)}")

    stats_after = transcript_cache.stats()
    print(f"\n📊 after: cached={stats_after['cached']}, "
          f"expired={stats_after['expired']}")

    if stats_after["cached"] == 0:
        print("ℹ️  no entries remain — cold start; cron will rebuild on next run")
    return 0


if __name__ == "__main__":
    sys.exit(main())
