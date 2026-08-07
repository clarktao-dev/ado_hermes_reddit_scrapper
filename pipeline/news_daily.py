#!/usr/bin/env python3
"""Daily German real-estate news pipeline (9 steps).

Usage:
    python3 news_daily.py                 # full run (fetch → translate → vault → discord → github)
    python3 news_daily.py --dry-run       # steps 1-6 only (no vault / discord / github side effects)
    python3 news_daily.py --limit N       # only fetch first N sources (testing)
"""
import argparse
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

# Ensure the pipeline package is importable when invoked as a script.
_PIPELINE_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_DIR = os.path.dirname(_PIPELINE_DIR)
if _PROJECT_DIR not in sys.path:
    sys.path.insert(0, _PROJECT_DIR)

from pipeline.lib import config_loader, dedup, filter_news, obsidian, rss_fetch, translate  # noqa: E402

# discord_sender lives outside the package — load it by path.
_DISCORD_SENDER = os.path.join(_PROJECT_DIR, "immobilien-kb", "tools", "discord_sender.py")
_PUSH_TO_GITHUB = os.path.join(_PROJECT_DIR, "push_to_github.py")


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


def step_dedup(items):
    return dedup.dedup_items(items)


def step_fetch_full_text(items, delay_sec=1.0):
    """Step 2.5: enrich surviving items with fetched article body.

    Runs AFTER dedup + age filter, BEFORE translate — so only the items that
    actually need translation (5-15 items) trigger an HTTP request, not the
    raw 150+ RSS entries.
    """
    return rss_fetch.fetch_full_text_for_items(items, delay_sec=delay_sec)


def step_translate(items, chunk_size=8):
    return translate.analyze_items_batch(items, chunk_size=chunk_size)


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


def step_write_vault(items, cfg):
    """Write each item as a markdown file + the daily index file."""
    vault_cfg = cfg.get("vault", {})
    vault_root = vault_cfg.get("root")
    if not vault_root:
        raise ValueError("cfg.vault.root missing — check config/pipeline.json")
    github_url = f"{cfg.get('github', {}).get('owner', '')}/{cfg.get('github', {}).get('repo', '')}"
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    written = []
    for it in items:
        path = obsidian.write_news_item(it, vault_root, date_str)
        written.append(path)
    index_path = obsidian.write_daily_index(items, vault_root, date_str, github_url)
    written.append(index_path)
    print(f"  wrote {len(written)} files under {vault_root}/Daily/{date_str}/")
    return items  # return items so next step has data


def step_send_discord(items, cfg, dry_run):
    """Send the daily digest to Discord. dry_run=True just prints the would-be payload length."""
    discord_cfg = cfg.get("discord", {})
    alias = discord_cfg.get("channel_alias", "headlines")
    max_chars = int(discord_cfg.get("max_chars_per_message", 1900))
    if not items:
        print("  no items to send")
        return False
    # Build a short text digest (top N items, title_zh + URL).
    lines = [f"📰 德國房地產每日頭條 — {datetime.utcnow().strftime('%Y-%m-%d')} ({len(items)} 則)"]
    for i, it in enumerate(items, 1):
        title = it.get("title_zh") or it.get("title", "")
        url = it.get("url", "")
        lines.append(f"{i}. {title}\n   {url}")
    body = "\n".join(lines)
    if len(body) > max_chars:
        body = body[: max_chars - 30] + "\n…(截斷)"
    if dry_run:
        print(f"  [dry-run] would send {len(body)} chars to channel '{alias}'")
        return False
    mod = _import_discord()
    # Resolve alias to a channel id via the sender's own helper.
    channel_id = mod._resolve_channel(alias)  # noqa: SLF001 (intentional; alias→id mapping lives there)
    return mod.send_to_channel(channel_id, body)


def step_push_github(dry_run):
    """Run push_to_github.py from the project root."""
    if dry_run:
        print("  [dry-run] would run push_to_github.py")
        return False
    print(f"  exec: python3 {_PUSH_TO_GITHUB}")
    res = subprocess.run([sys.executable, _PUSH_TO_GITHUB], cwd=_PROJECT_DIR)
    return res.returncode == 0


# --------------------------------------------------------------------------- #
# Main orchestration.
# --------------------------------------------------------------------------- #

def run_pipeline(dry_run=False, source_limit=None, chunk_size=8,
                 max_days=3, quota_primary=8, quota_other=3,
                 min_relevance=5, min_quick_score=6):
    t_start = time.time()
    print(f"[news_daily] starting at {datetime.utcnow().isoformat()}Z "
          f"(dry_run={dry_run}, source_limit={source_limit})")

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

    # Step 4: dedup
    items = _step("4. dedup_cross_source", lambda: step_dedup(items)) or []

    # Step 4b: age filter (keep only items within max_days)
    if max_days is not None and max_days > 0:
        before = len(items)
        items = filter_by_age(items, max_days)
        print(f"  age filter (≤ {max_days}d): {before} → {len(items)} items", flush=True)

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

    # Step 6b: apply source quota (per-source upper cap; final relevance
    # judgement is the LLM's --min-relevance, not a forced minimum floor).
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
    _step("7. write_vault", lambda: step_write_vault(items, cfg))

    # Step 8: send discord
    _step("8. send_discord", lambda: step_send_discord(items, cfg, dry_run=False))

    # Step 9: push to github
    _step("9. push_to_github", lambda: step_push_github(dry_run=False))

    print(f"\n[news_daily] done — total {time.time() - t_start:.2f}s")
    return 0


def main():
    p = argparse.ArgumentParser(description="Daily German real-estate news pipeline.")
    p.add_argument("--dry-run", action="store_true",
                   help="Run steps 1-6 only (no vault / discord / github side effects).")
    p.add_argument("--limit", type=int, default=None,
                   help="Limit the number of RSS sources fetched (for testing).")
    p.add_argument("--chunk-size", type=int, default=8,
                   help="Batch size for the LLM translation step.")
    p.add_argument("--max-days", type=int, default=3,
                   help="Only include items published within the last N days. "
                        "0 disables age filtering.")
    p.add_argument("--quota-primary", type=int, default=8,
                   help="Max items per run from primary source (Handelsblatt). "
                        "0 disables source quota.")
    p.add_argument("--quota-other", type=int, default=3,
                   help="Max items per run from each other source.")
    p.add_argument("--min-relevance", type=int, default=5,
                   help="Drop items whose LLM-assigned relevance_to_buyer is "
                        "below this score (0-10). 0 disables the filter.")
    p.add_argument("--min-quick-score", type=int, default=6,
                   help="Title-only pre-filter (cheaper than --min-relevance). "
                        "Drops items whose quick_score is below this. 0 disables.")
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
    )


if __name__ == "__main__":
    sys.exit(main())
