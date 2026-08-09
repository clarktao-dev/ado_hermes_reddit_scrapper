#!/usr/bin/env python3
"""YouTube Podcast daily pipeline (kome.ai → Map-Reduce → Obsidian + Discord).

Steps:
  1. load_config (channels.json)
  2. youtube_fetch   — pick 2 channels (round-robin by day-of-year) → newest video each
  3. youtube_state   — skip already-processed videos, walk back to next new
  4. youtube_translate — Map-Reduce + dual-lens analysis + OpenCC defense
  5. write_vault     — wipe + write podcast-kb/vault/Daily/<date>/
  6. send_discord    — push to channel `podcast` (alias)
  7. push_to_github  — git add + commit + push via existing paramiko script

Run:
  python3 youtube_daily.py                # real run
  python3 youtube_daily.py --dry-run      # no Discord, no GitHub, writes vault only
  python3 youtube_daily.py --channels '1alage,marktcheck'
"""
from __future__ import annotations
import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE.parent))

from pipeline.lib import youtube_fetch, youtube_state, youtube_translate, youtube_obsidian, youtube_discord  # noqa: E402


REPO_ROOT = "/root/projects/ado_hermes_reddit_scrapper"
CHANNELS_PATH = os.path.join(REPO_ROOT, "pipeline/config/channels.json")
STATE_PATH = os.path.join(REPO_ROOT, "podcast-kb/state.json")
PUSH_SCRIPT = os.path.join(REPO_ROOT, "push_to_github.py")


def load_channels() -> list:
    data = json.loads(Path(CHANNELS_PATH).read_text(encoding="utf-8"))
    return [c for c in data.get("channels", []) if c.get("enabled", True)]


def pick_channels(channels: list, n: int = 2) -> list:
    """Round-robin by day-of-year: pick n consecutive channels starting at
    (today.doy % len(channels)). This gives a deterministic rotation that
    covers every channel every (len / n) days.
    """
    if n >= len(channels):
        return list(channels)
    doy = datetime.now(timezone.utc).timetuple().tm_yday
    start = doy % len(channels)
    return [channels[(start + i) % len(channels)] for i in range(n)]


def pick_video_for_channel(channel: dict, state: youtube_state.StateStore,
                           look_back: int = 10) -> youtube_fetch.VideoMeta | None:
    """List videos and find the newest unprocessed one (walking back)."""
    metas = youtube_fetch.list_channel_videos(
        channel_id=channel["id"],
        channel_name=channel["name"],
        channel_url=channel["url"],
        youtube_channel_id=channel.get("channel_id"),
        limit=look_back,
    )
    for m in metas:
        if not state.is_processed(channel["id"], m.id):
            return m
    return None


def push_to_github(repo_root: str, dry_run: bool) -> dict:
    """Stage + commit + push podcast-kb/ to main."""
    if dry_run:
        return {"dry_run": True, "committed": False}
    import subprocess
    repo = Path(repo_root)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    msg = f"podcast-kb: {today} daily digest"

    cmds = [
        ["git", "-C", repo_root, "add", "podcast-kb/"],
        ["git", "-C", repo_root, "-c", "user.email=ado@hermes.local", "-c",
         "user.name=Ado", "commit", "-m", msg],
    ]
    out = {"commands": [], "pushed": False}
    for cmd in cmds:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        out["commands"].append({
            "cmd": " ".join(cmd),
            "rc": proc.returncode,
            "stdout_tail": proc.stdout[-200:],
            "stderr_tail": proc.stderr[-200:],
        })
        if proc.returncode != 0 and "nothing to commit" not in proc.stderr:
            out["error"] = proc.stderr[-300:]
            return out
    # push via existing paramiko script
    push_proc = subprocess.run(
        ["python3", PUSH_SCRIPT], capture_output=True, text=True, timeout=60,
    )
    out["push"] = {
        "rc": push_proc.returncode,
        "stdout_tail": push_proc.stdout[-200:],
        "stderr_tail": push_proc.stderr[-200:],
    }
    out["pushed"] = push_proc.returncode == 0
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="Fetch + translate + write vault, but skip Discord and GitHub push")
    ap.add_argument("--channels", default="",
                    help="Comma-separated channel IDs to override round-robin (e.g. '1alage,marktcheck')")
    ap.add_argument("--n-channels", type=int, default=2,
                    help="How many channels to process (default 2)")
    args = ap.parse_args()

    t0 = time.time()
    channels = load_channels()
    if args.channels:
        wanted = set(args.channels.split(","))
        channels = [c for c in channels if c["id"] in wanted]
    else:
        channels = pick_channels(channels, n=args.n_channels)
    print(f"[channels] picked {len(channels)}: {[c['id'] for c in channels]}")

    state = youtube_state.StateStore(STATE_PATH)

    digests = []
    for i, ch in enumerate(channels):
        # Cooldown between channels so kome.ai (third-party transcript API) doesn't
        # throttle us. Per user 2026-08-09: enforce ≥45s between two videos.
        if i > 0:
            cooldown = 45
            print(f"\n  [cooldown] sleeping {cooldown}s before next channel...")
            time.sleep(cooldown)
        print(f"\n=== {ch['name']} ({ch['id']}) ===")
        v = pick_video_for_channel(ch, state)
        if v is None:
            print(f"  [skip] no unprocessed video found in latest {10}")
            continue
        print(f"  [fetch] {v.id} | {v.title[:60]} ({v.duration_sec}s)")
        tr = youtube_fetch.fetch_transcript(v)
        if not tr.text:
            print(f"  [warn] empty transcript (lang={tr.language}); skip")
            continue
        print(f"  [transcript] {tr.n_chars} chars (premium={tr.is_premium})")
        print(f"  [translate] starting Map-Reduce...")
        digest = youtube_translate.digest_video(v, tr.text)
        digests.append(digest)
        state.mark_processed(ch["id"], v.id, v.epoch)
        print(f"  [done] elapsed={digest.elapsed_sec:.1f}s, "
              f"map_calls={digest.map_calls}, summary={len(digest.summary_zh)} chars")

    if not digests:
        print("\n[nothing to process] no digests produced")
        return 0

    state.save()

    print(f"\n=== Step 5: write_vault ===")
    vault_summary = youtube_obsidian.step_write_vault(
        digests, repo_root=REPO_ROOT,
        date_str=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        wipe=True,
    )
    print(f"  wrote {vault_summary.get('n_files', 0)} files, "
          f"errors={vault_summary.get('n_errors', 0)}")

    print(f"\n=== Step 6: send_discord ===")
    discord_summary = youtube_discord.step_send_discord(
        digests, channel="podcast", dry_run=args.dry_run,
    )
    print(f"  embeds sent: {discord_summary['n_embeds']}, errors={len(discord_summary['errors'])}")
    if discord_summary.get("errors"):
        for e in discord_summary["errors"]:
            print(f"    - {e}")

    print(f"\n=== Step 7: push_to_github ===")
    push_summary = push_to_github(REPO_ROOT, dry_run=args.dry_run)
    print(f"  pushed={push_summary.get('pushed')}, dry_run={push_summary.get('dry_run')}")

    print(f"\n[done] total {time.time()-t0:.1f}s, {len(digests)} videos")
    return 0


if __name__ == "__main__":
    sys.exit(main())
