#!/usr/bin/env python3
"""YouTube Podcast daily pipeline (kome.ai → Map-Reduce → Obsidian + Discord).

Steps:
  1. load_config (channels.json)
  2. youtube_fetch   — pick 2 channels (round-robin by day-of-year) → newest video each
  3. ProcessedStore  — Airtable ledger is the single source of truth for dedup.
     state.json is **deprecated** but still read for backward-compat (so existing
     processed_ids aren't re-processed). New marks go to Airtable, not state.json.
  4. youtube_translate — Map-Reduce + dual-lens analysis + OpenCC defense
  5. write_vault     — wipe + write podcast-kb/vault/Daily/<date>/
  6. send_discord    — push to channel `podcast` (alias)
  7. push_to_github  — git add + commit + push via existing paramiko script
  8. update_side_effects — backfill discord_message_id + github_commit_sha into
     the ProcessedContent ledger (best-effort; failure doesn't lose the mark).

Run:
  python3 youtube_daily.py                # real run
  python3 youtube_daily.py --dry-run      # no Discord, no GitHub, writes vault only
  python3 youtube_daily.py --channels '1alage,marktcheck'
"""
from __future__ import annotations
import argparse
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE.parent))

from pipeline.lib import (  # noqa: E402
    youtube_fetch,
    youtube_state,
    youtube_translate,
    youtube_obsidian,
    youtube_discord,
)
from pipeline.lib.processed_store import (  # noqa: E402
    ProcessedStore,
    make_hash,
    DEFAULT_TABLE,
)


REPO_ROOT = "/root/projects/ado_hermes_reddit_scrapper"
CHANNELS_PATH = os.path.join(REPO_ROOT, "pipeline/config/channels.json")
STATE_PATH = os.path.join(REPO_ROOT, "podcast-kb/state.json")
PUSH_SCRIPT = os.path.join(REPO_ROOT, "push_to_github.py")
PROCESSED_BASE_ID = os.environ.get(
    "AIRTABLE_PROCESSED_CONTENT_BASE_ID", "appHilorcrC5T0p2u"
)
PROCESSED_TABLE = os.environ.get(
    "AIRTABLE_PROCESSED_CONTENT_TABLE", DEFAULT_TABLE
)

logger = logging.getLogger("youtube_daily")
if not logger.handlers:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )


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


def _state_json_has(state: youtube_state.StateStore, channel_id: str,
                    video_id: str) -> bool:
    """Read legacy state.json (deprecated). Kept for backward compat only.

    state.json is no longer *written* by this pipeline — ProcessedContent
    (Airtable) is the single source of truth. But existing state.json
    records are still respected so we don't reprocess them before they
    migrate into Airtable.
    """
    try:
        return state.is_processed(channel_id, video_id)
    except Exception as e:  # noqa: BLE001
        logger.debug("state.json read failed (%s) — ignoring", e)
        return False


def pick_video_for_channel(
    channel: dict,
    store: ProcessedStore,
    state: youtube_state.StateStore | None = None,
    look_back: int = 10,
) -> youtube_fetch.VideoMeta | None:
    """List videos and find the newest unprocessed one (walking back).

    Dedup is checked against the ProcessedStore (Airtable). When state.json
    still has records not yet migrated to Airtable, those are also respected
    (backward compat).
    """
    metas = youtube_fetch.list_channel_videos(
        channel_id=channel["id"],
        channel_name=channel["name"],
        channel_url=channel["url"],
        youtube_channel_id=channel.get("channel_id"),
        limit=look_back,
    )
    for m in metas:
        # Primary check: Airtable ledger.
        if store.is_processed("youtube", m.id):
            logger.info(
                "skip already-processed (airtable): %s | %s",
                channel["id"], m.id,
            )
            continue
        # Backward-compat: legacy state.json. Remove once state.json is
        # fully migrated (see integration task notes).
        if state is not None and _state_json_has(state, channel["id"], m.id):
            logger.info(
                "skip already-processed (legacy state.json): %s | %s",
                channel["id"], m.id,
            )
            continue
        return m
    return None


def push_to_github(repo_root: str, dry_run: bool) -> dict:
    """Stage + commit + push podcast-kb/ to main.

    Returns dict with `pushed`, `commit_sha` (str|None), and command stdout/stderr.
    """
    out: dict = {"pushed": False, "commit_sha": None, "commands": [],
                 "dry_run": dry_run}
    if dry_run:
        return out
    repo = Path(repo_root)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    msg = f"podcast-kb: {today} daily digest"

    cmds = [
        ["git", "-C", repo_root, "add", "podcast-kb/"],
        ["git", "-C", repo_root, "-c", "user.email=ado@hermes.local", "-c",
         "user.name=Ado", "commit", "-m", msg],
    ]
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
    # Capture the commit SHA so we can backfill ProcessedContent.github_commit_sha
    sha_proc = subprocess.run(
        ["git", "-C", repo_root, "rev-parse", "HEAD"],
        capture_output=True, text=True, timeout=10,
    )
    if sha_proc.returncode == 0:
        out["commit_sha"] = sha_proc.stdout.strip()

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


def _video_output_md_path(repo_root: str, date_str: str, digest) -> str | None:
    """Reconstruct the per-video Markdown file path written by youtube_obsidian.

    The slug is ``<channel>_<title>_<video_id>`` (truncated to 80 chars) plus
    ``.md`` under ``podcast-kb/vault/Daily/<date>/``. We don't trust the slug
    char-for-char (OpenCC might mangle), so we look up the actual written file
    by video_id substring.
    """
    out_dir = Path(repo_root) / "podcast-kb" / "vault" / "Daily" / date_str
    if not out_dir.exists():
        return None
    needle = digest.video_id
    for p in sorted(out_dir.glob("*.md")):
        if needle in p.name:
            return str(p.relative_to(repo_root))
    return None


def _build_metadata(digest, video: youtube_fetch.VideoMeta,
                    channel: dict) -> dict:
    return {
        "epoch": int(video.epoch) if video.epoch else None,
        "published_epoch": int(video.epoch) if video.epoch else None,
        "duration_sec": int(video.duration_sec),
        "lang": digest.source_language or "de",
        "n_chars": int(digest.n_chars),
        "channel_id": channel["id"],
        "channel_name": channel["name"],
        "youtube_url": video.url,
        "map_calls": int(digest.map_calls),
        "reduce_calls": int(digest.reduce_calls),
        "elapsed_sec": float(digest.elapsed_sec),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="Fetch + translate + write vault, but skip Discord and GitHub push")
    ap.add_argument("--channels", default="",
                    help="Comma-separated channel IDs to override round-robin (e.g. '1alage,marktcheck')")
    ap.add_argument("--n-channels", type=int, default=2,
                    help="How many channels to process (default 2)")
    ap.add_argument("--skip-store", action="store_true",
                    help="Bypass ProcessedStore (for local debugging)")
    ap.add_argument("--pipeline-run-id", default="",
                    help="Pipeline run id to record (default: auto-generated)")
    ap.add_argument("--video-id", default="",
                    help="Force-process a single YouTube video id "
                         "(testing / re-processing override; bypasses "
                         "channel selection)")
    ap.add_argument("--force", action="store_true",
                    help="With --video-id, re-process even if the ledger "
                         "already has this video")
    args = ap.parse_args()

    t0 = time.time()
    run_id = args.pipeline_run_id or datetime.now(timezone.utc).strftime(
        "yt-%Y%m%d-%H%M%S"
    )

    channels = load_channels()
    forced_video: youtube_fetch.VideoMeta | None = None
    if args.video_id:
        forced_video = _force_fetch_video_meta(args.video_id)
        if forced_video is None:
            logger.error("could not resolve --video-id %s", args.video_id)
            return 2
        # Pick the channel that owns this video (if we have a matching channel
        # in channels.json); otherwise synthesise a channel dict from the
        # metadata.
        ch_match = next(
            (c for c in channels if c.get("channel_id") == forced_video.channel_id
             or c["id"] == forced_video.channel_id),
            None,
        )
        if ch_match is not None:
            channels = [ch_match]
        else:
            channels = [{
                "id": forced_video.channel_id or "forced",
                "name": forced_video.channel_name or "(unknown)",
                "url": f"https://www.youtube.com/watch?v={forced_video.id}",
                "channel_id": forced_video.channel_id,
                "_forced": True,
            }]
        logger.info(
            "forced video: id=%s channel=%s title=%s",
            forced_video.id, channels[0]["id"], forced_video.title,
        )
    elif args.channels:
        wanted = set(args.channels.split(","))
        channels = [c for c in channels if c["id"] in wanted]
    else:
        channels = pick_channels(channels, n=args.n_channels)
    print(f"[channels] picked {len(channels)}: {[c['id'] for c in channels]}")

    store = None
    if not args.skip_store:
        store = ProcessedStore(
            PROCESSED_BASE_ID, table_name=PROCESSED_TABLE,
        )
        logger.info(
            "ProcessedStore ready: base=%s table=%s run_id=%s",
            PROCESSED_BASE_ID, PROCESSED_TABLE, run_id,
        )

    # state.json is deprecated — we still load it for backward-compat reads
    # but never write to it from this pipeline.
    state = youtube_state.StateStore(STATE_PATH)

    digests = []
    selected: list[tuple[dict, youtube_fetch.VideoMeta, youtube_translate.VideoDigest]] = []
    for i, ch in enumerate(channels):
        # Cooldown between channels so kome.ai (third-party transcript API) doesn't
        # throttle us. Per user 2026-08-09: enforce ≥45s between two videos.
        if i > 0:
            cooldown = 45
            print(f"\n  [cooldown] sleeping {cooldown}s before next channel...")
            time.sleep(cooldown)
        print(f"\n=== {ch['name']} ({ch['id']}) ===")
        v: youtube_fetch.VideoMeta | None = None
        if forced_video is not None and i == 0:
            # --video-id still respects the ledger unless --force is given.
            if store is not None and store.is_processed("youtube",
                                                        forced_video.id):
                if args.force:
                    logger.warning(
                        "FORCE: re-processing already-processed video %s",
                        forced_video.id,
                    )
                else:
                    logger.info(
                        "skip already-processed: %s", forced_video.id,
                    )
                    print(f"  [skip] already-processed: {forced_video.id}")
                    v = None
            else:
                v = forced_video
                logger.info("using forced video: %s", v.id)
        elif store is not None:
            v = pick_video_for_channel(ch, store=store, state=state)
        else:
            v = _legacy_pick(ch, state)
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
        selected.append((ch, v, digest))
        # Note: do NOT call state.mark_processed() anymore — state.json is
        # deprecated. ProcessedStore.mark_processed() (below) is the new home.

    if not digests:
        print("\n[nothing to process] no digests produced")
        return 0

    print(f"\n=== Step 5: write_vault ===")
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    vault_summary = youtube_obsidian.step_write_vault(
        digests, repo_root=REPO_ROOT,
        date_str=today_str,
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
    print(f"  pushed={push_summary.get('pushed')}, "
          f"commit_sha={push_summary.get('commit_sha')}, "
          f"dry_run={push_summary.get('dry_run')}")

    # ------------------------------------------------------------------
    # Step 8: ledger writes
    # ------------------------------------------------------------------
    # Order matters:
    #   1. mark_processed() (no side effects yet) → Airtable ledger is the
    #      authoritative record of "this video was processed". Failure here
    #      is fatal: we lose dedup safety.
    #   2. update_side_effects() — best-effort. Failure here means the
    #      ledger still says "processed" but lacks discord_message_id /
    #      github_commit_sha. That's acceptable; side effects are debug info.
    print(f"\n=== Step 8: mark_processed + update_side_effects ===")
    if store is not None:
        per_video_msg_ids = {
            pv["video_id"]: pv.get("message_ids", [])
            for pv in discord_summary.get("per_video", [])
        }
        commit_sha = push_summary.get("commit_sha")

        for ch, video, digest in selected:
            output_path = _video_output_md_path(REPO_ROOT, today_str, digest)
            tags = ["long-form"]
            if digest.duration_sec and digest.duration_sec < 60 * 5:
                tags = ["short"]
            metadata = _build_metadata(digest, video, ch)
            try:
                record_id = store.mark_processed(
                    source_type="youtube",
                    source_id=video.id,
                    title=video.title,
                    channels=[f"youtube.{ch['id']}"],
                    pipeline_run_id=run_id,
                    output_path=output_path,
                    metadata=metadata,
                    tags=tags,
                )
                logger.info(
                    "marked processed: %s | %s -> %s",
                    ch["id"], video.id, record_id,
                )
            except Exception as e:  # noqa: BLE001
                logger.error(
                    "mark_processed failed for %s | %s: %s",
                    ch["id"], video.id, e,
                )
                continue

            # update_side_effects — best effort
            msg_ids = per_video_msg_ids.get(video.id, [])
            if msg_ids or commit_sha:
                try:
                    store.update_side_effects(
                        source_hash=make_hash("youtube", video.id),
                        discord_message_id=(",".join(msg_ids) or None),
                        github_commit_sha=commit_sha,
                    )
                except Exception as e:  # noqa: BLE001
                    logger.warning(
                        "update_side_effects failed for %s: %s",
                        video.id, e,
                    )

    print(f"\n[done] total {time.time()-t0:.1f}s, {len(digests)} videos")
    return 0


def _legacy_pick(channel: dict, state: youtube_state.StateStore,
                 look_back: int = 10) -> youtube_fetch.VideoMeta | None:
    """Fallback when ProcessedStore is disabled (--skip-store)."""
    metas = youtube_fetch.list_channel_videos(
        channel_id=channel["id"],
        channel_name=channel["name"],
        channel_url=channel["url"],
        youtube_channel_id=channel.get("channel_id"),
        limit=look_back,
    )
    for m in metas:
        if not _state_json_has(state, channel["id"], m.id):
            return m
    return None


def _force_fetch_video_meta(video_id: str) -> youtube_fetch.VideoMeta | None:
    """Look up a single video's metadata via yt-dlp --dump-single-json.

    Used for --video-id override (testing / re-processing). Returns a VideoMeta
    stub if the id is a known YouTube 11-char id and yt-dlp can resolve it.
    """
    import re as _re
    if not _re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id):
        logger.error("invalid video id: %s", video_id)
        return None
    url = f"https://www.youtube.com/watch?v={video_id}"
    proc = subprocess.run(
        ["yt-dlp", "--dump-single-json", "--skip-download", url],
        capture_output=True, text=True, timeout=30,
    )
    if proc.returncode != 0:
        logger.error("yt-dlp --dump-single-json failed: %s", proc.stderr[:300])
        return None
    try:
        d = json.loads(proc.stdout)
    except json.JSONDecodeError as e:
        logger.error("yt-dlp output not JSON: %s", e)
        return None
    epoch = (
        d.get("release_timestamp")
        or d.get("timestamp")
        or d.get("upload_date")
    )
    if isinstance(epoch, str):
        # upload_date is YYYYMMDD
        try:
            from datetime import datetime as _dt
            epoch = int(_dt.strptime(epoch, "%Y%m%d").replace(
                tzinfo=timezone.utc).timestamp())
        except ValueError:
            epoch = None
    elif isinstance(epoch, (int, float)):
        epoch = int(epoch)
    return youtube_fetch.VideoMeta(
        id=video_id,
        title=d.get("title", ""),
        duration_sec=int(d.get("duration") or 0),
        epoch=epoch,
        url=d.get("webpage_url") or url,
        channel_id=d.get("channel_id") or d.get("uploader_id") or "",
        channel_name=d.get("channel") or d.get("uploader") or "",
    )


if __name__ == "__main__":
    sys.exit(main())
