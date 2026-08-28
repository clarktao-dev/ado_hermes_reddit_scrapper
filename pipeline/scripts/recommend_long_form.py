#!/usr/bin/env python3
"""Recommend which processed YouTube video to upgrade into a long-form article.

Subcommands
-----------

``candidates``   (debug / unchanged): list past N days of YouTube records that
                 have NOT yet been promoted to ``article_type ∈
                 {long-form, pending-long-form, skipped-long-form}``.
                 Sorts by ``duration`` ASC for historical reasons — this is
                 a raw DB listing, **not** the recommendation list. Use
                 ``run`` for the curated output.

``run``          (cron entry, 2026-08-14 rewrite): apply the
                user-validated long-form criteria (prefilter keywords +
                LLM editorial scoring) to the past 3 days, write the
                markdown digest, optionally push to Discord ``#長文推薦``.
                Honors ``--days-back``, ``--output``, ``--no-discord``.

``confirm``      (unchanged): mark a record ``pending-long-form`` and shell
                 out to ``youtube_daily.py --mode long``.

``skip``         (unchanged): mark a record ``skipped-long-form``.

The ``confirm`` / ``skip`` subcommands still PATCH Airtable's
``article_type`` field — that part already works. ``run`` is read-only on
Airtable (ProcessedStore._list_all_records only).
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# Make the package importable when run as a script
ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

import requests  # noqa: E402  (project depends on requests)

from pipeline.lib.long_form_editor import (  # noqa: E402
    load_criteria,
    prefilter,
    render_markdown,
    score_candidates,
    _extract_record,
)
from pipeline.lib.processed_store import (  # noqa: E402
    DEFAULT_TABLE,
    ProcessedStore,
    make_hash,
)
from pipeline.lib import longform_vault  # noqa: E402  — vault cache + path helpers

ARTICLE_TYPE_FIELD = "article_type"
DEFAULT_OUTPUT_DIR = os.path.expanduser("~/.hermes/cron/output/recommend_long_form")
DEFAULT_DISCORD_CHANNEL = "longform"  # → see discord_sender.py aliases dict


# ---------------------------------------------------------------------------
# Shared helpers (used by confirm/skip — keep them exactly as they were)
# ---------------------------------------------------------------------------

def _get_store() -> ProcessedStore:
    return ProcessedStore()


def _patch_record(store: ProcessedStore, record_id: str, source_hash: str,
                  fields: Dict[str, Any]) -> str:
    """Wrap the private _patch_record. Caller is responsible for cache coherence."""
    return store._patch_record(record_id, fields, source_hash=source_hash)  # noqa: SLF001


def _list_all(store: ProcessedStore, formula: str) -> List[Dict[str, Any]]:
    """Direct list via _list_all_records (private but stable)."""
    return store._list_all_records(filter_formula=formula)  # noqa: SLF001


# ---------------------------------------------------------------------------
# candidates (debug-only listing — no LLM, no Discord, no editorial sort)
# ---------------------------------------------------------------------------

def cmd_candidates(args: argparse.Namespace) -> int:
    """Print raw Airtable YouTube records from the last N days, sorted by
    recency (first_seen_at DESC). For editorial recommendations + Discord
    push, use ``run`` instead.

    This is a debug listing only. It does NOT:
    - Call the LLM
    - Apply editorial scoring
    - Push to Discord
    - Sort by video duration (the old buggy behavior — user rejected 2026-08-14)
    """
    store = _get_store()
    criteria = load_criteria()
    formula = (
        f"AND({{source_type}}='youtube',"
        f"DATETIME_DIFF(NOW(), {{first_seen_at}}, 'days') <= {args.days_back})"
    )
    records = _list_all(store, formula)
    excluded = set(criteria.get("excluded_article_types", []))
    candidates = []
    for r in records:
        f = r.get("fields", {})
        at = (f.get(ARTICLE_TYPE_FIELD) or "").strip()
        if at in excluded:
            continue
        candidates.append({"id": r["id"], **f})

    # Sort by first_seen_at DESC (most recent first).
    # We deliberately do NOT sort by duration — the user explicitly rejected
    # that on 2026-08-14 ("影片長度根本不該是首先拿來做判斷的標準,而是內容").
    candidates.sort(
        key=lambda c: c.get("first_seen_at") or "",
        reverse=True,
    )
    candidates = candidates[: args.limit]

    print(f"過去 {args.days_back} 天 youtube 記錄(按 first_seen_at 降冪,{len(candidates)} 筆):\n")
    if not candidates:
        print("  (沒有 — Pipeline 還沒跑、或全部都做過/跳過了)")
        return 0

    for c in candidates:
        # Defensive metadata parse (the field is a JSON string in Airtable)
        meta_raw = c.get("metadata")
        meta = {}
        if isinstance(meta_raw, str) and meta_raw.strip():
            try:
                meta = json.loads(meta_raw)
            except Exception:
                pass
        elif isinstance(meta_raw, dict):
            meta = meta_raw
        duration = meta.get("duration_sec") or meta.get("duration") or "?"
        channel = (c.get("channels") or ["?"])[0] if c.get("channels") else "?"
        print(f"  📹 {c.get('title', '?')}")
        print(f"     channel:    {channel}")
        print(f"     duration:   {duration}s (display only — NOT used for ranking)")
        print(f"     first_seen: {c.get('first_seen_at', '?')}")
        print(f"     source_id:  {c.get('source_id', '?')}")
        print(f"     record_id:  {c['id']}")
        print()

    return 0


# ---------------------------------------------------------------------------
# run — new cron entry (LLM-scored, externalized criteria)
# ---------------------------------------------------------------------------

def fetch_youtube_since(cutoff_iso: str,
                        excluded_article_types: List[str]) -> List[Dict[str, Any]]:
    """Fetch all youtube records with first_seen_at >= cutoff and not in
    the excluded article_type set. Read-only on Airtable.

    cutoff_iso: ISO-8601 with trailing Z (e.g. '2026-08-11T00:00:00.000Z').
    """
    store = _get_store()
    # NOT(OR({article_type}='long-form', {article_type}='pending-long-form', ...))
    if excluded_article_types:
        not_excluded = (
            f"NOT(OR({{article_type}}='{excluded_article_types[0]}'"
            + "".join(f",{{article_type}}='{t}'" for t in excluded_article_types[1:])
            + "))"
        )
    else:
        not_excluded = "TRUE()"
    formula = (
        f"AND({{source_type}}='youtube',"
        f"IS_AFTER({{first_seen_at}}, '{cutoff_iso}'),"
        f"{not_excluded})"
    )
    return _list_all(store, formula)


def _push_to_discord(md: str, top: List[Dict[str, Any]]) -> None:
    """Push the markdown digest to Discord #長文推薦 via the existing wrapper.

    The wrapper handles 40333 / IP-block at the discord_sender.py layer
    (3 retries × 2s between attempts; persistent failures surface to the
    caller).
    """
    from pipeline.lib.youtube_discord import _send  # local import — keep CLI fast
    today = datetime.now().strftime("%Y-%m-%d")
    title = f"📚 本週 Long-form 候選 ({today})"
    _send(channel=DEFAULT_DISCORD_CHANNEL, title=title, content=md)
    print(f"✅ Discord 推到 {DEFAULT_DISCORD_CHANNEL}: {len(top)} 部")


def cmd_run(args: argparse.Namespace) -> int:
    """Cron entry: fetch past N days YouTube, prefilter, LLM-score,
    write markdown, optionally push Discord."""
    criteria = load_criteria()
    days_back = args.days_back if args.days_back is not None else criteria.get("days_back", 3)
    cutoff_dt = datetime.now(timezone.utc) - timedelta(days=days_back)
    cutoff = cutoff_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    print(f"🔎 撈過去 {days_back} 天 YouTube records (cutoff={cutoff})...")
    raw_records = fetch_youtube_since(cutoff, criteria.get("excluded_article_types", []))
    print(f"   → {len(raw_records)} 筆")

    # Flatten to the shape long_form_editor expects
    flat: List[Dict[str, Any]] = []
    for r in raw_records:
        rec = _extract_record(r)
        if rec is None:
            continue
        flat.append(rec)

    # Cap to max_input_per_run (read-only preflight guard)
    max_in = criteria.get("max_input_per_run", 30)
    if len(flat) > max_in:
        flat = flat[:max_in]

    survivors, dropped = prefilter(flat, criteria)
    print(f"   → 預篩選後存活 {len(survivors)} 筆、剔除 {len(dropped)} 筆")

    scored = score_candidates(survivors, criteria)
    print(f"   → LLM 評分完成 {len(scored)} 筆")

    # Print per-record score + rationale to stdout so cron log shows what the
    # LLM decided without opening the .md (truncate rationale to 200 chars).
    print("\n--- per-record scores ---")
    for r in scored:
        rat = (r.get("rationale") or "").strip().replace("\n", " ")
        if len(rat) > 200:
            rat = rat[:197] + "..."
        print(f"[{r.get('score', 0)}/10] {r.get('title', '?')} ({r.get('channel_name', '?')})")
        print(f"  {rat}")
    print("--- end per-record ---\n")

    scored.sort(key=lambda r: r.get("score", 0), reverse=True)
    min_score = criteria.get("min_score", 6)
    top = [r for r in scored if r.get("score", 0) >= min_score]
    top = top[: criteria.get("max_recommendations", 3)]
    print(f"   → top {len(top)} (score >= {min_score})")

    md = render_markdown(top, dropped, criteria, prefilter_count=len(scored))

    # Resolve output path
    out_path = args.output
    if not out_path:
        out_path = os.path.join(
            DEFAULT_OUTPUT_DIR, f"{datetime.now().strftime('%Y-%m-%d')}.md"
        )
    out_dir = os.path.dirname(out_path) or "."
    os.makedirs(out_dir, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(md)
    print(f"📝 markdown 寫到 {out_path}")

    # Print summary
    print("\n=== 摘要 ===")
    for i, r in enumerate(top, 1):
        print(f"  {i}. [{r.get('score', 0)}/10] {r.get('title', '?')}"
              f"  ({r.get('channel_name', '?')})")
    if not top:
        print("  (本期 0 推薦)")

    if args.no_discord:
        print("ℹ️ --no-discord → skip Discord push")
    elif not top:
        print("ℹ️ 0 推薦 → skip Discord push (per design)")
    else:
        _push_to_discord(md, top)

    return 0


# ---------------------------------------------------------------------------
# confirm / skip — UNCHANGED (they patch Airtable article_type)
# ---------------------------------------------------------------------------

def _find_record_by_source_id(store: ProcessedStore, source_id: str) -> Optional[Dict[str, Any]]:
    formula = (
        f"AND({{source_type}}='youtube',"
        f"{{source_id}}='{source_id.replace(chr(39), chr(39) + chr(39))}')"
    )
    rows = _list_all(store, formula)
    return rows[0] if rows else None


def cmd_confirm(args: argparse.Namespace) -> int:
    """Mark a record pending-long-form and shell out to youtube_daily.py --mode long.

    Vault cache layer: if ``immobilien-kb/vault/Longform/<date>/<video_id>---<slug>.md``
    already exists, skip the LLM pipeline entirely and just push the cached file
    to Discord. This is the "Idea 1" cache — zero tokens for re-confirm.
    """
    store = _get_store()
    rec = _find_record_by_source_id(store, args.source_id)
    if not rec:
        print(f"❌ Source ID {args.source_id} 找不到 youtube record")
        return 1
    rec_id = rec["id"]
    source_hash = make_hash("youtube", args.source_id)

    # Cache check: if longform already written, skip the entire pipeline.
    if not args.force and longform_vault.longform_already_exists(args.source_id):
        cached = longform_vault.find_longform(args.source_id)
        if cached is None:
            # Defensive: race between exists check and find. Fall through to full path.
            pass
        else:
            rel_path = str(cached.relative_to(ROOT))
            print(f"♻️  vault cache hit: {rel_path}")
            print("✅ 0 LLM tokens — skip youtube_daily, just push to Discord")
            if not args.dry_run:
                _patch_record(store, rec_id, source_hash, {ARTICLE_TYPE_FIELD: "long-form"})
                _push_cached_to_discord(cached, args.source_id)
            else:
                print("🔇 --dry-run: skip Airtable + Discord")
            return 0

    if args.dry_run:
        print(f"🔇 --dry-run: skip Airtable write (would mark {args.source_id} pending-long-form)")
    else:
        _patch_record(store, rec_id, source_hash, {ARTICLE_TYPE_FIELD: "pending-long-form"})
        print(f"✅ {args.source_id} 標記為 pending-long-form")

    cmd = [
        "/root/.hermes/hermes-agent/venv/bin/python",
        str(ROOT / "pipeline" / "youtube_daily.py"),
        "--video-id", args.source_id,
        "--mode", "long",
    ]
    if args.dry_run:
        cmd.append("--dry-run")
    if args.from_summary:
        cmd.append("--from-summary")
    print(f"🚀 跑: {' '.join(cmd)}")
    rc = subprocess.call(cmd, cwd=str(ROOT))

    # After youtube_daily finishes, migrate the longform file from
    # podcast-kb/vault/Daily/.../  →  immobilien-kb/vault/Longform/<date>/...
    # so the next confirm hits the cache instead of re-running.
    if rc == 0 and not args.dry_run:
        _migrate_longform_artifacts(args.source_id, rec)

    return rc


def _migrate_longform_artifacts(source_id: str, rec: dict) -> None:
    """Move freshly-written _longform.md from podcast-kb → immobilien-kb.

    Best-effort: a missing source file is not fatal (the user may have
    manually deleted it). On success, prints the new path and updates
    Airtable ``vault_path`` so the next cron can find it.
    """
    pub_date = (
        rec.get("fields", {}).get("first_seen_at", "")[:10]
        or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    )
    channel_name = (
        (rec.get("fields", {}).get("metadata") or "")
        .split('"channel_name"')[1].split('"')[1]
        if '"channel_name"' in (rec.get("fields", {}).get("metadata") or "")
        else "unknown"
    )
    moved_paths = []
    # youtube_obsidian truncates the slug to 60 chars BEFORE appending
    # _longform.md, so the video_id may be cut off in the middle of the
    # slug. Use a permissive `*<video_id>*` glob and filter to end with
    # the correct suffix to avoid matching summary files.
    for date_dir in longform_vault.PODCAST_LONGFORM_ROOT.iterdir():
        if not date_dir.is_dir():
            continue
        for src in date_dir.glob(f"*_longform.md"):
            if source_id not in src.name:
                continue
            try:
                dest = longform_vault.migrate_podcast_longform(
                    video_id=source_id,
                    source_md=src,
                    date_str=pub_date,
                    slug_hint=channel_name,
                )
                moved_paths.append(dest)
            except Exception as e:  # noqa: BLE001
                print(f"⚠️  migrate 失敗: {src} → {e}")

    if moved_paths:
        rel = str(moved_paths[0].relative_to(ROOT))
        print(f"📦 已搬到: {rel}")
        try:
            store = _get_store()
            _patch_record(
                store,
                rec["id"],
                make_hash("youtube", source_id),
                {ARTICLE_TYPE_FIELD: "long-form", "vault_path": rel},
            )
            print(f"✅ Airtable 標 long-form + vault_path={rel}")
        except Exception as e:  # noqa: BLE001
            print(f"⚠️  Airtable 更新失敗: {e}")


def _push_cached_to_discord(cached_md: Path, source_id: str) -> None:
    """Best-effort Discord push for the cache-hit branch.

    Reuses the same wrapper as ``cmd_run`` to get the same embed shape.
    Failures here don't downgrade the cache-hit success — the .md is the
    source of truth, Discord is the convenience surface.
    """
    try:
        from pipeline.lib.youtube_discord import _send  # type: ignore
        rel = str(cached_md.relative_to(ROOT))
        body = (
            f"♻️ **長文 cache hit — {source_id}**\n\n"
            f"📄 `{rel}`\n"
            f"從 vault 直接推送,0 LLM token。\n\n"
            + cached_md.read_text(encoding="utf-8")[:1500]
        )
        _send(channel="longform", content=body)
        print("📨 已推 Discord #長文推薦")
    except Exception as e:  # noqa: BLE001
        print(f"⚠️  Discord 推送失敗(不影響 cache 結果): {e}")


def cmd_skip(args: argparse.Namespace) -> int:
    """Mark a record as skipped-long-form so it stops appearing in candidates."""
    store = _get_store()
    rec = _find_record_by_source_id(store, args.source_id)
    if not rec:
        print(f"❌ Source ID {args.source_id} 找不到 youtube record")
        return 1
    rec_id = rec["id"]
    source_hash = make_hash("youtube", args.source_id)
    _patch_record(store, rec_id, source_hash, {ARTICLE_TYPE_FIELD: "skipped-long-form"})
    print(f"✅ {args.source_id} 標記為 skipped-long-form")
    return 0


# ---------------------------------------------------------------------------
# argparse
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="recommend_long_form.py",
        description="Recommend processed YouTube videos that are good candidates for long-form articles.",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    p_cand = sub.add_parser("candidates", help="List raw YouTube records (debug only — use 'run' for editorial recommendations)")
    p_cand.add_argument("--days-back", type=int, default=7)
    p_cand.add_argument("--limit", type=int, default=5)
    p_cand.set_defaults(func=cmd_candidates)

    p_run = sub.add_parser("run", help="Cron entry: LLM-scored long-form recommendations")
    p_run.add_argument("--days-back", type=int, default=None,
                       help="Lookback window (default = criteria.days_back = 3)")
    p_run.add_argument("--output", type=str, default=None,
                       help="Override output markdown path "
                            "(default: ~/.hermes/cron/output/recommend_long_form/<date>.md)")
    p_run.add_argument("--no-discord", action="store_true",
                       help="Skip Discord push (default: push if 1+ recommendations)")
    p_run.set_defaults(func=cmd_run)

    p_conf = sub.add_parser("confirm", help="Trigger long-form generation for a video")
    p_conf.add_argument("source_id")
    p_conf.add_argument("--dry-run", action="store_true")
    p_conf.add_argument("--force", action="store_true",
                        help="Bypass vault cache — re-run the LLM pipeline even "
                             "if immobilien-kb/vault/Longform/<date>/<video_id>---<slug>.md "
                             "already exists (default: cache hit → 0 tokens).")
    p_conf.add_argument("--from-summary", action="store_true",
                        help="Pass --from-summary to youtube_daily so long-form is "
                             "expanded from the existing short-summary vault file "
                             "(Idea 2: skips translation, ~33%% fewer LLM tokens).")
    p_conf.set_defaults(func=cmd_confirm)

    p_skip = sub.add_parser("skip", help="Mark a video as skipped-long-form")
    p_skip.add_argument("source_id")
    p_skip.set_defaults(func=cmd_skip)

    return p


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
