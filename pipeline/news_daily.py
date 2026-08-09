#!/usr/bin/env python3
"""Daily German real-estate news pipeline (9 steps).

Steps:
  1. load_config
  2. fetch_rss
  3. filter_keywords
  3b. quick_score   — title-only LLM pre-filter (cheap)
  4. dedup_cross_source  — Airtable ``ProcessedContent`` is the single source of truth
     (via :func:`filter_processed`). state.json is **deprecated** — kept only as a
     in-process fallback when ``--skip-store`` is set.
  4b. age filter (≤ max_days)
  4c. fetch_full_text (only for survivors)
  5. translate + analyse
  5b. relevance filter (≥ min_relevance)
  6. rank_by_relevance + source quota
  7. write_vault         — wipe + write immobilien-kb/vault/Daily/<date>/
  8. send_discord        — push to channel `headlines` (alias)
  9. push_to_github      — git add + commit + push via paramiko
  10. mark_processed + update_side_effects — backfill into the Airtable ledger
      (best-effort; failure here doesn't lose the vault write).

Usage:
    python3 news_daily.py                 # full run (fetch → translate → vault → discord → github)
    python3 news_daily.py --dry-run       # steps 1-6 only (no vault / discord / github side effects)
    python3 news_daily.py --limit N       # only fetch first N sources (testing)
    python3 news_daily.py --skip-store    # bypass ProcessedStore (legacy in-process dedup)
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

# Ensure the pipeline package is importable when invoked as a script.
_PIPELINE_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_DIR = os.path.dirname(_PIPELINE_DIR)
if _PROJECT_DIR not in sys.path:
    sys.path.insert(0, _PROJECT_DIR)

from pipeline.lib import (  # noqa: E402
    config_loader,
    dedup,
    filter_news,
    obsidian,
    rss_fetch,
    translate,
)
from pipeline.lib.processed_store import (  # noqa: E402
    DEFAULT_TABLE,
    ProcessedStore,
    make_hash,
    normalize_url,
)

# discord_sender lives outside the package — load it by path.
_DISCORD_SENDER = os.path.join(_PROJECT_DIR, "immobilien-kb", "tools", "discord_sender.py")
_PUSH_TO_GITHUB = os.path.join(_PROJECT_DIR, "push_to_github.py")

# ProcessedContent Airtable config (Task 4 — single source of truth for dedup).
PROCESSED_BASE_ID = os.environ.get(
    "AIRTABLE_PROCESSED_CONTENT_BASE_ID", "appHilorcrC5T0p2u",
)
PROCESSED_TABLE = os.environ.get(
    "AIRTABLE_PROCESSED_CONTENT_TABLE", DEFAULT_TABLE,
)

logger = logging.getLogger("news_daily")
if not logger.handlers:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )


# --------------------------------------------------------------------------- #
# ProcessedStore helpers (Task 4 — Airtable as source of truth for news dedup).
# --------------------------------------------------------------------------- #

def _get_store() -> ProcessedStore:
    """Instantiate the module-level ProcessedStore (lazy, cached)."""
    global _PROCESSED_STORE
    if _PROCESSED_STORE is None:
        _PROCESSED_STORE = ProcessedStore(
            PROCESSED_BASE_ID, table_name=PROCESSED_TABLE,
        )
        logger.info(
            "ProcessedStore ready: base=%s table=%s",
            PROCESSED_BASE_ID, PROCESSED_TABLE,
        )
    return _PROCESSED_STORE


_PROCESSED_STORE: Optional[ProcessedStore] = None


def filter_processed(
    items: List[dict],
    store: Optional[ProcessedStore] = None,
    days_threshold: int = 3,
) -> List[dict]:
    """Drop items already in the Airtable ``ProcessedContent`` ledger.

    This is the news pipeline's new dedup gate (Task 4). The previous
    in-process URL + fuzzy-title matcher in ``pipeline.lib.dedup`` is
    kept only as a fallback when ``store`` is None.

    For each item:

    1. ``url_normalized`` is set (strip ``utm_*`` / ``fbclid`` / ``gclid``,
       lowercase host, sort query).
    2. If the normalized URL is already in the ledger → **skip** (exact
       dedup; this is the only gate — the previous "any news in past
       N days" batch gate was removed in Task 9).

    Items without a URL pass through unchanged.

    Returns the kept list. Mutates input dicts in place to add
    ``url_normalized`` on the kept items (so downstream code can hash
    the same value when calling ``mark_processed``).

    .. note::
       The ``days_threshold`` parameter is kept for backwards compatibility
       with callers; it is no longer used internally (no batch gate).
    """
    if store is None:
        store = _get_store()
    kept: List[dict] = []
    skipped_exact = 0
    skipped_no_url = 0

    # Dedup is now EXACT-URL-ONLY (Task 9, 2026-08-09).
    # The previous "any news in past N days → skip all" gate was removed
    # because it meant once the pipeline ran once, every subsequent run
    # in that window would skip all *new* items too — which defeats the
    # user's intent of "process new articles each run". The max_days gate
    # (Step 4b age filter) still drops old news; URL dedup blocks the
    # exact article from being processed twice.
    # `days_threshold` is kept in the signature for backwards compatibility
    # with existing callers; it is no longer used internally.
    _ = days_threshold  # intentionally unused

    for item in items:
        url = (item.get("url") or "").strip()
        if not url:
            skipped_no_url += 1
            kept.append(item)
            continue

        normalized = normalize_url(url)
        item["url_normalized"] = normalized

        # Exact-match dedup via Airtable ledger.
        if store.is_processed("news", normalized):
            skipped_exact += 1
            logger.info(
                "[skip already-processed] %s | %s",
                normalized, item.get("title", "")[:60],
            )
            continue

        kept.append(item)

    logger.info(
        "filter_processed: %d kept, %d skipped(exact), %d skipped(no-url)",
        len(kept), skipped_exact, skipped_no_url,
    )
    return kept


# --------------------------------------------------------------------------- #
# Step helpers (each prints progress, count, elapsed; swallows exceptions).
# --------------------------------------------------------------------------- #

def _step(name, fn):
    """Run one step with timing + error capture. Returns the result or None."""
    print(f"\n=== STEP: {name} ===", flush=True)
    t0 = time.time()
    try:
        result = fn()
        elapsed = time.time() - t0
        n = len(result) if hasattr(result, "__len__") and not isinstance(result, (str, dict)) else "—"
        print(f"[OK] {name} ({elapsed:.2f}s, count={n})", flush=True)
        return result
    except Exception as e:
        elapsed = time.time() - t0
        print(f"[ERROR] {name} failed after {elapsed:.2f}s: {type(e).__name__}: {e}", flush=True)
        return None


def _import_discord():
    """Import discord_sender.py from the tools/ dir without installing it."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("discord_sender", _DISCORD_SENDER)
    if spec is None or spec.loader is None:
        raise ImportError(f"could not load module spec for {_DISCORD_SENDER}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# --------------------------------------------------------------------------- #
# Step bodies (each is a no-arg callable for _step()).
# --------------------------------------------------------------------------- #

def step_load_config():
    cfg = config_loader.load_config()
    print(f"  vault={cfg.get('vault', {}).get('root')}")
    print(f"  sources enabled={len(config_loader.get_sources())}")
    return cfg


def step_fetch(sources):
    return rss_fetch.fetch_all_sources(sources)


def step_filter(items):
    return filter_news.filter_items(items)


def step_dedup(items, *, skip_store: bool = False, days_threshold: int = 3):
    """Drop items already in the Airtable ``ProcessedContent`` ledger.

    Two code paths (controlled by ``skip_store``):

    * **default** — delegates to :func:`filter_processed`, which uses
      ``store.is_processed('news', url_normalized)`` for exact match
      and ``store.get_recent('news', days=N)`` for the recent-window
      gate. This is the production path (Task 4).

    * **skip_store=True** — falls back to the legacy in-process URL +
      fuzzy-title matcher in :mod:`pipeline.lib.dedup`. Kept for
      ``--skip-store`` debugging only.
    """
    if skip_store:
        logger.warning(
            "step_dedup: --skip-store set → using legacy in-process dedup",
        )
        return dedup.dedup_items(items, fallback_factory=_get_store)
    return filter_processed(items, days_threshold=days_threshold)


def step_fetch_full_text(items, delay_sec=1.0):
    """Step 2.5: enrich surviving items with fetched article body.

    Runs AFTER dedup + age filter, BEFORE translate — so only the items that
    actually need translation (5-15 items) trigger an HTTP request, not the
    raw 150+ RSS entries.

    Items whose source declares ``"no_full_text": true`` (e.g. Google News,
    whose URLs are redirects that hit a consent page) skip the HTTP fetch
    and rely on RSS title + summary alone. If those two fields are both
    empty, the item is dropped — translation cannot work without content.
    """
    keep = []
    skipped = 0
    for it in items:
        if it.get("no_full_text"):
            # Use RSS title + summary as the entire content. Drop empty.
            html_or_text = " ".join([
                (it.get("title") or ""),
                (it.get("summary") or ""),
            ]).strip()
            if len(html_or_text) < 30:
                skipped += 1
                logger.info(
                    "[skip no-full-text empty] %s | %s",
                    it.get("source_name"), it.get("title", "")[:60],
                )
                continue
            it["full_text"] = html_or_text
            it["content_html"] = html_or_text
            it["_fetch_done"] = True  # mark so fetch_full_text_for_items skips
            keep.append(it)
        else:
            keep.append(it)
    if skipped:
        logger.info("[no_full_text] skipped %d item(s) with empty title+summary", skipped)
    return rss_fetch.fetch_full_text_for_items(keep, delay_sec=delay_sec)


def step_translate(items, chunk_size=2):
    """Translate+analyze each item.

    Per-item mode (2026-08-07): chunked batch (chunk_size=2/4/8) keeps hanging
    on ollama-cloud — large prompts (~10K+ tokens for 5K-char full_text × 2)
    cause the backend to silently drop requests after the first batch. Per-item
    calls keep each prompt under 5K tokens and have been verified to return
    reliably. The inter-item 3s cooldown prevents sequential LLM queue buildup.

    The `chunk_size` parameter is kept for CLI compatibility but ignored.
    """
    import time as _time
    out = []
    for i, it in enumerate(items):
        result = translate.analyze_item(it)
        out.append(result if isinstance(result, dict) else it)
        if i < len(items) - 1:
            _time.sleep(3.0)  # per-item cooldown
    return out


def step_rank(items):
    return translate.rank_by_relevance(items)


# --------------------------------------------------------------------------- #
# Source quota + date filter
# --------------------------------------------------------------------------- #

# Per-source cap. Handelsblatt is the specialist real-estate feed → higher
# quota; other Wirtschaft feeds → lower. Configurable via CLI.
_DEFAULT_QUOTAS = {
    "Handelsblatt Immobilien": 8,
}
_DEFAULT_OTHER_QUOTA = 3


def filter_by_age(items, max_days):
    """Keep only items whose pub_date is within the last `max_days` days.
    Items with no pub_date are kept (assumed recent)."""
    if max_days is None or max_days <= 0:
        return items
    cutoff = datetime.now(timezone.utc) - timedelta(days=max_days)
    out = []
    for it in items:
        pub = it.get("pub_date")
        if pub is None:
            out.append(it)  # no date → keep
            continue
        if isinstance(pub, str):
            # parse ISO string
            try:
                pub = datetime.fromisoformat(pub.replace("Z", "+00:00"))
            except Exception:
                out.append(it)
                continue
        if pub.tzinfo is None:
            pub = pub.replace(tzinfo=timezone.utc)
        if pub >= cutoff:
            out.append(it)
    return out


def apply_source_quota(items, primary_max=8, other_max=3):
    """Cap items per source to prevent any single feed from dominating.

    Rules (per user's 2026-08-07 spec):
      - "Primary" sources (Handelsblatt Immobilien — specialist feed) cap at
        `primary_max` items.
      - All other sources cap at `other_max` items.
      - No minimum floor: if a source has only 1 quality item, that 1 is kept.
        The final relevance judgement is left to the LLM (--min-relevance).
      - Items within each bucket are kept in their incoming (rank) order.

    Edge cases:
      - `primary_max` / `other_max` <= 0 → disable upper cap for that tier
        (not recommended: this can let a noisy source flood the digest).
    """
    if not items:
        return items
    buckets: dict[str, list] = {}
    for it in items:
        buckets.setdefault(it.get("source_name", "?"), []).append(it)
    out = []
    for src, src_items in buckets.items():
        upper = primary_max if (src in _DEFAULT_QUOTAS and primary_max > 0) \
                else (other_max if other_max > 0 else len(src_items))
        out.extend(src_items[:upper])
    return out


def filter_by_relevance(items, min_score=5):
    """Drop items whose LLM-assigned relevance_to_buyer is below min_score.

    Items without a relevance score (None) are kept (safer default).
    """
    if not items or min_score is None or min_score <= 0:
        return items
    out = []
    for it in items:
        score = it.get("relevance_to_buyer")
        if score is None:
            out.append(it)  # no score → keep
            continue
        try:
            if int(score) >= min_score:
                out.append(it)
        except (TypeError, ValueError):
            out.append(it)
    return out


def step_write_vault(items, cfg, content_kind: str = "longform"):
    """Write each item as a markdown file + the daily index file.

    Wipes the existing daily folder BEFORE writing so re-runs don't
    accumulate stale items. The wipe is opt-in: pass cfg.vault.wipe=False
    (or env VAULT_KEEP=1) to disable for debugging.

    ``content_kind`` (Task 7, 2026-08-09): ``"short-summary"`` or
    ``"longform"`` (default). Controls the file suffix in
    ``obsidian.write_news_item`` (``_summary.md`` vs ``_longform.md``).
    Wiping is scoped to ``*<suffix>.md`` so a short-summary run can't
    delete long-form artifacts (and vice versa) when both modes coexist
    in the same daily folder.

    Returns a dict mapping each item to its written output path (used by
    :func:`step_mark_processed` to record ``output_path`` in the ledger).
    """
    import shutil
    vault_cfg = cfg.get("vault", {})
    vault_root = vault_cfg.get("root")
    if not vault_root:
        raise ValueError("cfg.vault.root missing — check config/pipeline.json")
    github_url = f"{cfg.get('github', {}).get('owner', '')}/{cfg.get('github', {}).get('repo', '')}"
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    out_dir = os.path.join(vault_root, "Daily", date_str)
    # Wipe only files of *this* content_kind so short + long can coexist.
    # Disable with cfg.vault.wipe=False or VAULT_KEEP=1.
    wipe = vault_cfg.get("wipe", True) and os.environ.get("VAULT_KEEP") != "1"
    wiped = False
    if wipe and os.path.isdir(out_dir):
        suffix_glob = "_summary.md" if content_kind == "short-summary" else "_longform.md"
        for p in Path(out_dir).glob(f"*{suffix_glob}"):
            p.unlink()
        wiped = True
    item_paths: dict[int, str] = {}
    for i, it in enumerate(items):
        path = obsidian.write_news_item(it, vault_root, date_str,
                                        content_kind=content_kind)
        item_paths[i] = path
    # Index file is shared across both modes in the same daily folder —
    # only the first call writes it.
    index_path = obsidian.write_daily_index(items, vault_root, date_str, github_url)
    print(f"  wrote {len(item_paths) + 1} files under {vault_root}/Daily/{date_str}/"
          f" (kind={content_kind}{' wiped existing' if wiped else ''})")
    return {"items": items, "item_paths": item_paths, "index_path": index_path}


def step_send_discord(items, cfg, dry_run):
    """Send the daily digest to Discord. dry_run=True just prints the would-be payload length.

    Sends 1 header + N body embeds (configurable via discord.items_per_embed,
    default 3) so each embed stays well under Discord's 4096-char limit and
    the burst never triggers per-channel rate limits.

      - Header embed — index of all titles + source/date.
      - Body embeds — every `items_per_embed` items, with full Chinese
        summary + URL.

    `cfg.discord.per_item_summary_chars` (default 600) caps each item's
    summary so 3 items + headers + URLs comfortably fit in one embed.

    Returns a dict ``{"ok": bool, "per_item": {item_index: [message_ids]}}``
    so :func:`step_update_side_effects` can backfill the ledger with the
    Discord message ids. ``per_item`` is keyed by the item's position in
    the input list.
    """
    result: dict = {"ok": False, "per_item": {}}
    discord_cfg = cfg.get("discord", {})
    alias = discord_cfg.get("channel_alias", "headlines")
    summary_max = int(discord_cfg.get("summary_chars_per_embed", 3800))
    per_item_max = int(discord_cfg.get("per_item_summary_chars", 600))
    items_per_embed = int(discord_cfg.get("items_per_embed", 3))
    if not items:
        print("  no items to send")
        return result
    mod = _import_discord()
    channel_id = mod._resolve_channel(alias)  # noqa: SLF001 (intentional; alias→id mapping lives there)

    if dry_run:
        body_count = (len(items) + items_per_embed - 1) // items_per_embed
        print(f"  [dry-run] would send 1 header + {body_count} body embeds to "
              f"channel '{alias}' ({len(items)} items, {items_per_embed}/embed)")
        return result

    # ---- Header embed ----
    header_lines = [f"📰 德國房地產每日頭條 — {datetime.utcnow().strftime('%Y-%m-%d')} ({len(items)} 則)"]
    for i, it in enumerate(items, 1):
        title = it.get("title_zh") or it.get("title", "")
        src = it.get("source_name", "?")
        header_lines.append(f"{i}. [{src}] {title}")
    header_body = "\n".join(header_lines)[:3800]
    header_resp = mod.send_to_channel(channel_id, header_body, as_embed=True,
                                      title="📰 德國房地產每日頭條",
                                      color=0x3498db)
    header_msg_id = None
    if isinstance(header_resp, dict):
        header_msg_id = header_resp.get("id")
    elif isinstance(header_resp, str):
        header_msg_id = header_resp
    if header_msg_id:
        # Attribute header to index 0 if available, else drop it
        if 0 in result["per_item"]:
            result["per_item"][0].append(header_msg_id)
        else:
            result["per_item"][0] = [header_msg_id]

    # ---- Body embeds: split into chunks of items_per_embed ----
    ok = True
    body_index = 0
    for start in range(0, len(items), items_per_embed):
        body_index += 1
        batch = items[start:start + items_per_embed]
        body_lines = []
        for j, it in enumerate(batch, 1):
            global_i = start + j
            title = it.get("title_zh") or it.get("title", "")
            summary = it.get("summary_zh") or ""
            if len(summary) > per_item_max:
                summary = summary[:per_item_max] + "…(截斷)"
            url = it.get("url", "")
            src = it.get("source_name", "?")
            body_lines.append(f"### {global_i}. {title}\n*來源：{src}*")
            if summary:
                body_lines.append(summary)
            if url:
                body_lines.append(f"🔗 {url}")
            body_lines.append("")  # blank line between items
        body = "\n".join(body_lines)[:summary_max]
        # Color gradient blue → green → teal by batch index.
        color = 0x2ecc71 + (body_index * 0x050505)
        resp = mod.send_to_channel(
            channel_id,
            body,
            as_embed=True,
            title=f"📖 第 {body_index} 場摘要",
            color=color,
        )
        msg_id = None
        if isinstance(resp, dict):
            msg_id = resp.get("id")
        elif isinstance(resp, str):
            msg_id = resp
        if msg_id:
            # Attribute this embed to the first item in the batch (good-enough
            # backref; the daily index already lists every title).
            first_idx = start
            result["per_item"].setdefault(first_idx, []).append(msg_id)
        if not resp:
            ok = False
    result["ok"] = ok
    return result


def step_push_github(dry_run):
    """Run push_to_github.py from the project root.

    Returns ``{"pushed": bool, "commit_sha": str|None}`` so
    :func:`step_update_side_effects` can backfill the ledger with the
    GitHub commit SHA.
    """
    out: dict = {"pushed": False, "commit_sha": None}
    if dry_run:
        print("  [dry-run] would run push_to_github.py")
        return out
    print(f"  exec: python3 {_PUSH_TO_GITHUB}")
    res = subprocess.run([sys.executable, _PUSH_TO_GITHUB], cwd=_PROJECT_DIR)
    out["pushed"] = res.returncode == 0
    if out["pushed"]:
        sha_proc = subprocess.run(
            ["git", "-C", _PROJECT_DIR, "rev-parse", "HEAD"],
            capture_output=True, text=True, timeout=10,
        )
        if sha_proc.returncode == 0:
            out["commit_sha"] = sha_proc.stdout.strip()
    return out


# --------------------------------------------------------------------------- #
# Step 10: ledger writes (Task 4)
# --------------------------------------------------------------------------- #

def _build_news_metadata(item: dict, date_str: str) -> dict:
    """Build the metadata JSON for :func:`ProcessedStore.mark_processed`."""
    epoch = item.get("pub_date_epoch")
    if epoch is None and isinstance(item.get("pub_date"), str):
        try:
            epoch = int(datetime.fromisoformat(
                item["pub_date"].replace("Z", "+00:00")
            ).timestamp())
        except Exception:  # noqa: BLE001
            epoch = None
    return {
        "epoch": epoch,
        "source": item.get("source_name", "?"),
        "lang": item.get("source_language", "de"),
        "relevance_rank": item.get("relevance_rank"),
        "relevance_to_buyer": item.get("relevance_to_buyer"),
        "date": date_str,
    }


def step_mark_processed(
    items: List[dict],
    item_paths: dict,
    *,
    store: Optional[ProcessedStore] = None,
    run_id: str = "",
    date_str: str = "",
    article_type: Optional[str] = None,
) -> dict:
    """Mark every vault-written item as processed in the Airtable ledger.

    Failure of any single ``mark_processed`` is logged but does **not**
    raise — the vault write already succeeded, and the next run will
    simply re-process the item. Worst case is a duplicate vault write,
    not a missed dedup.

    ``article_type`` (Task 7, 2026-08-09): optional. When provided, every
    record written here is tagged with the same value (``short-summary``
    or ``long-form``). When None, the field is omitted entirely so older
    callers keep working unchanged.

    Returns ``{"ok": int, "errors": [(idx, item, exc_str), ...], "record_ids": [...]}``.
    """
    if store is None:
        store = _get_store()
    errors: list = []
    record_ids: list = []
    for idx, item in enumerate(items):
        url_norm = item.get("url_normalized") or normalize_url(item.get("url", ""))
        if not url_norm:
            logger.warning(
                "mark_processed: skipping item idx=%d (no url_normalized)",
                idx,
            )
            continue
        title = item.get("title_zh") or item.get("title", "")
        try:
            record_id = store.mark_processed(
                source_type="news",
                source_id=url_norm,
                title=title,
                channels=["news.daily_top3"],
                pipeline_run_id=run_id,
                output_path=item_paths.get(idx),
                metadata=_build_news_metadata(item, date_str),
                tags=[],
                article_type=article_type,
            )
            record_ids.append(record_id)
            logger.info(
                "marked processed: news | %s -> %s (article_type=%s)",
                url_norm, record_id, article_type,
            )
        except Exception as e:  # noqa: BLE001
            logger.error(
                "mark_processed failed for idx=%d url=%s: %s",
                idx, url_norm, e,
            )
            errors.append((idx, item, str(e)))
    return {"ok": len(record_ids), "errors": errors, "record_ids": record_ids}


def step_update_side_effects(
    items: List[dict],
    *,
    discord_summary: Optional[dict] = None,
    github_summary: Optional[dict] = None,
    store: Optional[ProcessedStore] = None,
) -> dict:
    """Backfill discord_message_id + github_commit_sha into the ledger.

    Best-effort: any per-item failure is logged but never raised.
    Returns ``{"ok": int, "errors": [...]}``.
    """
    if store is None:
        store = _get_store()
    errors: list = []
    ok_count = 0
    per_item = (discord_summary or {}).get("per_item", {}) if discord_summary else {}
    commit_sha = (github_summary or {}).get("commit_sha") if github_summary else None

    for idx, item in enumerate(items):
        url_norm = item.get("url_normalized") or normalize_url(item.get("url", ""))
        if not url_norm:
            continue
        msg_ids = per_item.get(idx, []) if per_item else []
        # Flatten list-of-lists / single str / None into a comma-separated str
        if isinstance(msg_ids, list):
            msg_ids_flat: list = []
            for m in msg_ids:
                if isinstance(m, list):
                    msg_ids_flat.extend(m)
                else:
                    msg_ids_flat.append(m)
            msg_ids_str = ",".join(str(m) for m in msg_ids_flat if m) or None
        else:
            msg_ids_str = str(msg_ids) if msg_ids else None
        if not msg_ids_str and not commit_sha:
            continue
        try:
            store.update_side_effects(
                source_hash=make_hash("news", url_norm),
                discord_message_id=msg_ids_str,
                github_commit_sha=commit_sha,
            )
            ok_count += 1
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "update_side_effects failed for %s: %s", url_norm, e,
            )
            errors.append((idx, item, str(e)))
    return {"ok": ok_count, "errors": errors}


# --------------------------------------------------------------------------- #
# Main orchestration.
# --------------------------------------------------------------------------- #

def run_pipeline(dry_run=False, source_limit=None, chunk_size=8,
                 max_days=7, quota_primary=8, quota_other=3,
                 min_relevance=3, min_quick_score=4,
                 skip_store=False, pipeline_run_id="",
                 mode: str = "short"):
    t_start = time.time()
    run_id = pipeline_run_id or datetime.now(timezone.utc).strftime(
        "news-%Y%m%d-%H%M%S"
    )
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    content_kind = "short-summary" if mode == "short" else "longform"
    article_type = content_kind
    print(f"[news_daily] starting at {datetime.utcnow().isoformat()}Z "
          f"(dry_run={dry_run}, source_limit={source_limit}, "
          f"mode={mode}, run_id={run_id}, skip_store={skip_store})")

    # Step 1: load config
    cfg = _step("1. load_config", step_load_config)
    if cfg is None:
        print("Aborting — config load failed.")
        return 1

    sources = config_loader.get_sources()
    if source_limit is not None and source_limit > 0:
        sources = sources[:source_limit]
        print(f"  --limit {source_limit}: using {len(sources)} sources")

    # Step 2: fetch RSS
    items = _step("2. fetch_rss", lambda: step_fetch(sources))
    if items is None:
        items = []
    print(f"  raw fetched: {len(items)} items")

    # Step 3: filter
    items = _step("3. filter_keywords", lambda: step_filter(items)) or []

    # Step 3b: quick relevance score (title-only, cheap LLM pre-filter)
    if min_quick_score is not None and min_quick_score > 0:
        def _do_quick():
            translate.quick_score_items(items, min_score=min_quick_score, chunk_size=12)
            return [it for it in items if it.get("quick_score") is None or it.get("quick_score", 0) >= min_quick_score]
        before = len(items)
        result = _step("3b. quick_score", _do_quick)
        items = result if result is not None else []
        print(f"  quick filter (≥ {min_quick_score}/10): {before} → {len(items)} items", flush=True)

    # Step 4: dedup (Airtable-backed, see filter_processed)
    items = _step("4. dedup_cross_source",
                  lambda: step_dedup(items, skip_store=skip_store,
                                     days_threshold=max_days or 3)) or []

    # Step 4b: age filter (keep only items within max_days)
    if max_days is not None and max_days > 0:
        before = len(items)
        items = filter_by_age(items, max_days)
        print(f"  age filter (≤ {max_days}d): {before} → {len(items)} items", flush=True)

    # Step 4b': apply source quota EARLY (Task 10, 2026-08-09) — without this,
    # Google News alone can flood the pipeline with 30+ items and balloon
    # translate time / token usage. Cap per-source BEFORE full-text fetch
    # and translation so we don't waste work on items that will be dropped.
    if quota_primary > 0 or quota_other > 0:
        before = len(items)
        items = apply_source_quota(
            items,
            primary_max=quota_primary,
            other_max=quota_other,
        )
        from collections import Counter as _C
        dist = _C(it.get("source_name", "?") for it in items)
        print(f"  source quota (primary≤{quota_primary}, other≤{quota_other}): "
              f"{before} → {len(items)} items | {dict(dist)}", flush=True)

    # Step 4c: fetch full article body (only for items that survived every
    # gate so far — typically 5-15 items, not the raw 150+ RSS entries).
    items = _step("4c. fetch_full_text",
                  lambda: step_fetch_full_text(items, delay_sec=1.0)) or []

    # Step 5: translate (batched LLM)
    items = _step("5. translate_batch",
                  lambda: step_translate(items, chunk_size=chunk_size)) or []

    # Step 5b: relevance filter (drop low LLM-scored items)
    if min_relevance is not None and min_relevance > 0:
        before = len(items)
        items = filter_by_relevance(items, min_score=min_relevance)
        print(f"  relevance filter (≥ {min_relevance}/10): {before} → {len(items)} items", flush=True)

    # Step 6: rank
    items = _step("6. rank_by_relevance", lambda: step_rank(items)) or []
    # Tag each item with its position so obsidian frontmatter knows the rank.
    for i, it in enumerate(items):
        it["relevance_rank"] = i + 1

    # Step 6b (legacy): keep a no-op quota pass for callers that disabled
    # the Step 4b' early quota. Step 4b' already enforces the cap; this
    # pass is a no-op when items are already within quota.
    if quota_primary > 0 or quota_other > 0:
        before = len(items)
        items = apply_source_quota(
            items,
            primary_max=quota_primary,
            other_max=quota_other,
        )
        from collections import Counter as _C
        dist = _C(it.get("source_name", "?") for it in items)
        print(f"  source quota re-check (no-op expected): "
              f"{before} → {len(items)} items | {dict(dist)}", flush=True)

    # Print a dry-run summary so --dry-run is verifiable without side effects.
    if dry_run:
        print("\n=== DRY-RUN SUMMARY ===")
        print(f"total items after dedup: {len(items)}")
        for i, it in enumerate(items[:10], 1):
            print(f"  {i}. [{it.get('source_name')}] {it.get('title_zh') or it.get('title')}")
        if len(items) > 10:
            print(f"  … (+{len(items) - 10} more)")
        print(f"\ntotal elapsed: {time.time() - t_start:.2f}s")
        return 0

    # Step 7: write vault
    vault_summary = _step("7. write_vault",
                          lambda: step_write_vault(items, cfg, content_kind=content_kind))
    vault_items: List[dict] = list(items) if isinstance(items, list) else []
    item_paths: dict = {}
    if isinstance(vault_summary, dict):
        _vi = vault_summary.get("items", items)
        vault_items = list(_vi) if isinstance(_vi, list) else []
        item_paths = vault_summary.get("item_paths", {}) or {}

    # Step 8: send discord (capture message_ids per item)
    discord_summary = _step("8. send_discord",
                            lambda: step_send_discord(vault_items, cfg,
                                                      dry_run=False))
    if not isinstance(discord_summary, dict):
        discord_summary = None

    # Step 9: push to github (capture commit_sha)
    github_summary = _step("9. push_to_github",
                           lambda: step_push_github(dry_run=False))
    if not isinstance(github_summary, dict):
        github_summary = None

    # Step 10: ledger writes (Task 4)
    if not skip_store and vault_items:
        _step("10a. mark_processed",
              lambda: step_mark_processed(
                  vault_items, item_paths,
                  run_id=run_id, date_str=date_str,
                  article_type=article_type,
              ))
        _step("10b. update_side_effects",
              lambda: step_update_side_effects(
                  vault_items,
                  discord_summary=discord_summary,
                  github_summary=github_summary,
              ))
    elif skip_store:
        print("\n[Step 10] skipped (--skip-store)")

    print(f"\n[news_daily] done — total {time.time() - t_start:.2f}s")
    return 0


def main():
    p = argparse.ArgumentParser(description="Daily German real-estate news pipeline.")
    p.add_argument("--dry-run", action="store_true",
                   help="Run steps 1-6 only (no vault / discord / github side effects).")
    p.add_argument("--mode", choices=("short", "long"), default="short",
                   help="Pipeline output mode. ``short`` (default) is the "
                        "default lightweight short-summary; ``long`` runs "
                        "the full editorial commentary pass. Both modes "
                        "produce a vault file with a mode-appropriate "
                        "suffix (``_summary.md`` vs ``_longform.md``) and "
                        "write ``article_type`` to the Airtable ledger.")
    p.add_argument("--limit", type=int, default=None,
                   help="Limit the number of RSS sources fetched (for testing).")
    p.add_argument("--chunk-size", type=int, default=2,
                   help="Batch size for the LLM translation step. chunk=2 keeps each "
                        "prompt small (~10K tokens total for 5K-char full_text × 2) "
                        "which avoids ollama-cloud hangs. chunk=4/8 produced prompts "
                        "large enough to silently hang. Slower per-batch but reliable.")
    p.add_argument("--max-days", type=int, default=7,
                   help="Only include items published within the last N days. "
                        "0 disables age filtering. Also drives filter_processed's "
                        "recent-news window.")
    p.add_argument("--quota-primary", type=int, default=8,
                   help="Max items per run from primary source (Handelsblatt). "
                        "0 disables source quota.")
    p.add_argument("--quota-other", type=int, default=3,
                   help="Max items per run from each other source.")
    p.add_argument("--min-relevance", type=int, default=3,
                   help="Drop items whose LLM-assigned relevance_to_buyer is "
                        "below this score (0-10). 0 disables the filter.")
    p.add_argument("--min-quick-score", type=int, default=4,
                   help="Title-only pre-filter (cheaper than --min-relevance). "
                        "Drops items whose quick_score is below this. 0 disables.")
    p.add_argument("--skip-store", action="store_true",
                   help="Bypass ProcessedStore (fall back to legacy in-process "
                        "dedup). Useful for local debugging when the Airtable "
                        "PAT is unavailable.")
    p.add_argument("--pipeline-run-id", default="",
                   help="Override the auto-generated pipeline run id "
                        "(default: news-YYYYMMDD-HHMMSS).")
    args = p.parse_args()
    return run_pipeline(
        dry_run=args.dry_run,
        source_limit=args.limit,
        chunk_size=args.chunk_size,
        max_days=args.max_days,
        quota_primary=args.quota_primary,
        quota_other=args.quota_other,
        min_relevance=args.min_relevance,
        min_quick_score=args.min_quick_score,
        skip_store=args.skip_store,
        mode=args.mode,
        pipeline_run_id=args.pipeline_run_id,
    )


if __name__ == "__main__":
    sys.exit(main())
