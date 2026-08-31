#!/usr/bin/env python3
"""Daily podcast-only pipeline (Plan 10, 2026-08-31).

Run by cron (currently the youtube_daily cron; you can split into its own
cron if the cadence diverges). This script:

  1. Loads ``config/channels.json`` — same config as YouTube.
  2. Filters for ``source_type == "podcast"`` channels (currently l'Immo).
  3. Round-robin picks N channels per day (doy % len(channels)) — same
     selection logic as :func:`youtube_daily.pick_channels`, so a YouTube
     cron run and a podcast cron run land on the same channel on the same
     UTC date. Mixing the two source types in one ``pick_channels`` call
     would have created cross-source scheduling dependencies this script
     sidesteps.
  4. For each picked channel: fetch RSS → dedup against ProcessedStore
     → fetch transcripts (Podigee JSON > VTT > RSS description) →
     write vault ``immobilien-kb/vault/YouTube/<channel>/_transcripts/
     <episode_guid>.md`` (same layout as YouTube transcripts so the
     cleanup cron and Obsidian reader work unchanged).
  5. Marks each new episode in ProcessedStore (source_type="podcast")
     so we don't re-fetch on the next run.

No LLM calls here. Transcript ingestion + vault persistence only.
Translation + digest live in youtube_daily.

Why a separate script (instead of extending youtube_daily)
----------------------------------------------------------
youtube_daily.py is 1006 lines with deep YouTube-specific call chains
(Invidious → kome.ai → Google Translate → Map-Reduce digest). Bolting
on a podcast fetcher at line 459 would force every call site to handle
two source types. Cleaner to keep youtube_daily pure-YouTube and let
this script own the podcast half. They share config, dedup store, and
cron, but their fetch/translate stages are independent.
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

    The RSS <guid> is a Podigee hex hash — already unique — but we salt
    it with the channel id so two podcast feeds can never collide on the
    same hash row.
    """
    return make_hash("podcast", f"{channel_id}|{episode.id}")


def step_fetch_unprocessed_episodes(
    channel: dict,
    store: Optional[ProcessedStore],
    look_back: int = 5,
) -> list[PodcastEpisode]:
    """List recent episodes and filter out anything already in the ledger.

    Returns newest-first list of unprocessed candidates. Empty list means
    there's nothing new today for this channel — caller should treat as
    a no-op.

    With ``store=None`` (e.g. ``--skip-store`` mode), skip the dedup
    check entirely and return every episode. The caller is responsible
    for not marking anything in the ledger in that mode.
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
    """Fetch transcript, write vault, mark in ledger.

    Returns a summary dict so the caller can aggregate a run report.
    """
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

    # Empty description floor — no transcript AND no description means
    # the episode is unusable for any downstream stage. Skip it but log
    # so the user can decide whether to backfill manually.
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
    """Channel folder name in ``immobilien-kb/vault/YouTube/<name>/``.

    Podcast channels need a vault folder; we use a sanitised version of
    the channel id. Keeps the existing cleanup cron / Obsidian readers
    happy — they iterate channel folders regardless of source type.
    """
    import re
    safe = re.sub(r"[^A-Za-z0-9_\-]", "_", channel["id"])
    return safe


def _vault_episode_id(episode: PodcastEpisode) -> str:
    """Filename-safe episode id for the transcript cache.

    RSS <guid> is usually a Podigee hex hash (already safe) but some
    podcasts use full URLs. Strip non-alphanumerics to stay compatible
    with the YouTube-side ``_transcripts/<video_id>.md`` layout.
    """
    import re
    safe = re.sub(r"[^A-Za-z0-9_\-]", "_", episode.id)
    return safe[:80] or "unknown_episode"


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Daily podcast pipeline. Filters config/channels.json "
                    "for source_type='podcast' channels, picks via round-"
                    "robin, persists transcripts to vault.",
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
    args = ap.parse_args()

    run_id = args.pipeline_run_id or datetime.now(timezone.utc).strftime(
        "pod-%Y%m%d-%H%M%S"
    )

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
    print(f"[podcast_daily] picked {len(channels)}: {[c['id'] for c in channels]}")

    store = None
    if not args.skip_store:
        store = ProcessedStore(PROCESSED_BASE_ID, table_name=PROCESSED_TABLE)
        logger.info("ProcessedStore ready: base=%s table=%s",
                    PROCESSED_BASE_ID, PROCESSED_TABLE)

    run_start = datetime.now(timezone.utc)
    summaries = []
    for ch in channels:
        # In --skip-store mode we still want to see what would happen,
        # so fetch episodes and report — no ledger dedup, no vault write.
        if args.skip_store:
            eps = list_podcast_episodes(ch, limit=3)
            for ep in eps:
                summaries.append({
                    "channel_id": ch["id"],
                    "episode_id": ep.id,
                    "title": ep.title,
                    "duration": ep.duration,
                    "transcript_source": ep.transcript_type or "rss-description",
                    "transcript_chars": len(ep.description),
                    "wrote_vault": False,
                    "marked_processed": False,
                    "skipped_reason": "skip-store mode",
                })
            continue

        candidates = step_fetch_unprocessed_episodes(ch, store, look_back=5)
        if not candidates:
            print(f"[{ch['id']}] no new episodes (ledger clean)")
            continue
        # Only ingest the newest unprocessed episode to keep run bounded
        # and avoid hammering Podigee when there's a multi-episode backlog.
        # Cleanup cron will eventually drain the rest as old transcripts
        # expire from the 30-day cache.
        ep = candidates[0]
        summary = step_persist_episode(
            ch, ep, store, run_id=run_id, dry_run=args.dry_run,
        )
        summaries.append(summary)

    # Print run report so cron output is greppable.
    print("\n=== podcast_daily run report ===")
    print(f"run_id: {run_id}")
    print(f"channels: {[c['id'] for c in channels]}")
    for s in summaries:
        line = (
            f"  • [{s.get('channel_id','?')}] {s.get('title','?')[:60]}"
            f"  src={s.get('transcript_source','?')}"
            f"  chars={s.get('transcript_chars','?')}"
        )
        if s.get("skipped_reason"):
            line += f"  SKIP={s['skipped_reason']}"
        else:
            line += f"  wrote={s.get('wrote_vault')} marked={s.get('marked_processed')}"
        print(line)
    elapsed = (datetime.now(timezone.utc) - run_start).total_seconds()
    print(f"elapsed: {elapsed:.2f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())