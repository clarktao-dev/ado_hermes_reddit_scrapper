#!/usr/bin/env python3
"""Podcast daily pipeline — Phase 2 shim (2026-09-03).

Plan 10 introduced a standalone RSS ingest script. Phase 2 folds podcast
channels into ``youtube_daily`` so they share LLM digest + Discord + vault
writes. This file remains as the cron entrypoint for job ``b4af9754c582``
and forwards to ``youtube_daily.main`` with podcast-only channel selection.

Helpers below (``_load_podcast_channels``, ``_episode_id_for_dedup``, …)
are kept for unit-test compatibility and for any one-off scripts that
still import them; the live cron path goes through :func:`main`.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Make pipeline package importable when run from cron (cwd may differ).
PIPELINE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPELINE_ROOT))

from pipeline.lib.config_loader import load_config  # noqa: E402
from pipeline.lib.podcast_fetch import (  # noqa: E402
    PodcastEpisode,
    fetch_transcript,
    list_podcast_episodes,
)
from pipeline.lib.processed_store import (  # noqa: E402
    DEFAULT_TABLE,
    ProcessedStore,
    make_hash,
)
from pipeline.lib.transcript_cache import write_transcript  # noqa: E402
from pipeline.youtube_daily import main as youtube_daily_main  # noqa: E402
from pipeline.youtube_daily import pick_channels  # noqa: E402

logger = logging.getLogger("podcast_daily")
if not logger.handlers:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

PROCESSED_BASE_ID = os.environ.get(
    "AIRTABLE_PROCESSED_CONTENT_BASE_ID", "appHilorcrC5T0p2u"
)
PROCESSED_TABLE = os.environ.get(
    "AIRTABLE_PROCESSED_CONTENT_TABLE", DEFAULT_TABLE
)


def _load_podcast_channels() -> list:
    """Return enabled channels where ``source_type == 'podcast'``."""
    cfg = load_config()
    return [
        c for c in cfg.get("channels", [])
        if c.get("enabled", True) and c.get("source_type") == "podcast"
    ]


def _episode_id_for_dedup(channel_id: str, episode: PodcastEpisode) -> str:
    """Stable, collision-resistant key per (channel, episode).

    Kept for Plan 10 test/compat. Phase 2 ``youtube_daily`` uses the
    sanitised episode guid directly as ``source_id`` with
    ``source_type='podcast'``.
    """
    return make_hash("podcast", f"{channel_id}|{episode.id}")


def step_fetch_unprocessed_episodes(
    channel: dict,
    store: Optional[ProcessedStore],
    look_back: int = 5,
) -> list[PodcastEpisode]:
    """List recent episodes and filter out anything already in the ledger.

    Legacy Plan 10 helper — retained for tests. Production path is the
    youtube_daily shim in :func:`main`.
    """
    episodes = list_podcast_episodes(channel, limit=look_back)
    if store is None:
        return episodes
    candidates = []
    for ep in episodes:
        if store.is_processed("podcast", _episode_id_for_dedup(channel["id"], ep)):
            logger.info("skip already-processed (ledger): %s | %s",
                        channel["id"], ep.title[:60])
            continue
        candidates.append(ep)
    return candidates


def step_persist_episode(
    channel: dict,
    episode: PodcastEpisode,
    store: ProcessedStore,
    run_id: str,
    dry_run: bool = False,
) -> dict:
    """Fetch transcript, write vault, mark in ledger (Plan 10 legacy helper)."""
    text, source = fetch_transcript(episode)
    vault_channel_name = _vault_channel_name(channel)
    summary = {
        "channel_id": channel["id"],
        "episode_id": episode.id,
        "title": episode.title,
        "duration": episode.duration,
        "transcript_source": source,
        "transcript_chars": len(text),
        "wrote_vault": False,
        "marked_processed": False,
        "skipped_reason": None,
    }

    if source == "rss-description" and not episode.description:
        summary["skipped_reason"] = "no transcript and no description"
        logger.warning("skip unusable episode: %s | %s",
                       channel["id"], episode.title[:60])
        return summary

    if not dry_run:
        path = write_transcript(
            channel=vault_channel_name,
            video_id=_vault_episode_id(episode),
            text=text,
            language="de",
            source=source,
            ttl_days=30,
        )
        summary["wrote_vault"] = True
        summary["vault_path"] = str(path)
        dedup_id = _episode_id_for_dedup(channel["id"], episode)
        store.mark_processed(
            "podcast",
            dedup_id,
            episode.title,
            channels=[f"podcast.{channel['id']}"],
            pipeline_run_id=run_id,
            output_path=str(path),
            metadata={
                "channel_id": channel["id"],
                "channel_name": channel["name"],
                "episode_guid": episode.id,
                "published": episode.published.isoformat(),
                "duration_sec": episode.duration,
                "transcript_source": source,
                "rss_url": episode.rss_url,
            },
            tags=["podcast"],
        )
        summary["marked_processed"] = True
        logger.info("wrote vault + marked processed: %s | %s (%d chars)",
                    channel["id"], episode.title[:60], len(text))
    return summary


def _vault_channel_name(channel: dict) -> str:
    """Channel folder name in ``immobilien-kb/vault/YouTube/<name>/``."""
    import re
    safe = re.sub(r"[^A-Za-z0-9_\-]", "_", channel["id"])
    return safe


def _vault_episode_id(episode: PodcastEpisode) -> str:
    """Filename-safe episode id for the transcript cache."""
    import re
    safe = re.sub(r"[^A-Za-z0-9_\-]", "_", episode.id)
    return safe[:80] or "unknown_episode"


def main() -> int:
    """Shim: forward podcast channels into ``youtube_daily.main``.

    Cron ``b4af9754c582`` still invokes this script. We resolve the
    podcast-only channel set, then rewrite ``sys.argv`` so youtube_daily
    runs the shared digest + Discord path.
    """
    ap = argparse.ArgumentParser(
        description="Phase 2 shim — podcast channels via youtube_daily "
                    "(LLM digest + Discord). Filters config/channels.json "
                    "for source_type='podcast'.",
    )
    ap.add_argument("--dry-run", action="store_true",
                    help="Skip vault writes and ledger marks (report only).")
    ap.add_argument("--n-channels", type=int, default=1,
                    help="How many podcast channels to process per run "
                         "(default 1; we currently only have l'Immo).")
    ap.add_argument("--channels", default="",
                    help="Comma-separated channel IDs to override round-robin.")
    ap.add_argument("--pipeline-run-id", default="",
                    help="Pipeline run id (default: auto-generated).")
    ap.add_argument("--skip-store", action="store_true",
                    help="Bypass ProcessedStore (for local debugging).")
    ap.add_argument("--mode", choices=("short", "long"), default="short",
                    help="Forwarded to youtube_daily (default short).")
    args = ap.parse_args()

    channels = _load_podcast_channels()
    if not channels:
        logger.warning("no podcast channels enabled in channels.json — nothing to do")
        print("[podcast_daily] no podcast channels enabled — exit 0")
        return 0

    if args.channels:
        wanted = set(args.channels.split(","))
        channels = [c for c in channels if c["id"] in wanted]
    else:
        channels = pick_channels(channels, n=min(args.n_channels, len(channels)))

    if not channels:
        print("[podcast_daily] no channels matched — exit 0")
        return 0

    ids = ",".join(c["id"] for c in channels)
    run_id = args.pipeline_run_id or datetime.now(timezone.utc).strftime(
        "pod-%Y%m%d-%H%M%S"
    )
    print(f"[podcast_daily] shim → youtube_daily channels={ids} "
          f"mode={args.mode} run_id={run_id}")

    yt_argv = [
        "youtube_daily.py",
        "--channels", ids,
        "--mode", args.mode,
        "--pipeline-run-id", run_id,
        "--n-channels", str(max(len(channels), 1)),
    ]
    if args.dry_run:
        yt_argv.append("--dry-run")
    if args.skip_store:
        yt_argv.append("--skip-store")

    old_argv = sys.argv
    try:
        sys.argv = yt_argv
        return int(youtube_daily_main() or 0)
    finally:
        sys.argv = old_argv


if __name__ == "__main__":
    sys.exit(main())
