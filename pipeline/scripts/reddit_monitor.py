#!/usr/bin/env python3
"""Reddit monitor: fetch 3 subreddits via RSS, score, translate, push Discord.

Usage:
    /root/.hermes/hermes-agent/venv/bin/python pipeline/scripts/reddit_monitor.py [--dry-run]

Outputs:
    - vault: immobilien-kb/vault/Reddit/<date>/_index.md + per-post files
    - Discord: #reddit-insights channel (skipped with --dry-run or --no-discord)

Design notes:
    - RSS via old.reddit.com (per Reddit's docs; www.reddit.com gets bot-detected
      from this VPS).
    - 12s delay between subreddit requests to dodge Reddit's burst rate limit.
    - Same scoring + translation helpers as recommend_long_form, but with a
      Reddit-flavored criteria (min_score=4; no long-form keyword blacklist).
    - URLs in Discord embeds are real reddit permalinks, not placeholders.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

REPO = Path(__root__ if (__root__ := "/root/projects/ado_hermes_reddit_scrapper") else __root__)
VAULT_ROOT = REPO / "immobilien-kb" / "vault" / "Reddit"
sys.path.insert(0, str(REPO))

from pipeline.lib.long_form_editor import score_candidates, load_criteria  # noqa: E402
from pipeline.lib.processed_store import (  # noqa: E402
    ProcessedStore,
    ProcessedStoreError,
    DEFAULT_TABLE,
)
from pipeline.lib.translate import analyze_items_batch  # noqa: E402
from pipeline.lib.youtube_discord import _send  # noqa: E402  # re-use existing Discord sender

PROCESSED_BASE_ID = os.environ.get(
    "AIRTABLE_PROCESSED_CONTENT_BASE_ID", "appHilorcrC5T0p2u"
)

logger = logging.getLogger("reddit_monitor")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s")
    )
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# ----------------------------------------------------------------------- #
# Config
# ----------------------------------------------------------------------- #

SUBREDDITS = [
    "Immobilieninvestments",
    "Finanzen",
    "Immoscoutwildgeworden",
    "wohnen",
]

PER_SUB_LIMIT = 10
PER_SUB_KEEP = 3

DISCORD_CHANNEL_REDDIT = "reddit"  # add this alias to discord_sender channel map

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0"
)

# Reddit-tuned criteria (looser than long-form podcast):
#   - min_score=4 (reddit is noisier than curated podcast transcripts)
#   - drop only the truly irrelevant keywords for a real-estate/finance feed
REDDIT_CRITERIA = {
    "version": "reddit-2026-08-14",
    "min_score": 4,
    "max_recommendations": PER_SUB_KEEP,
    "days_back": 1,
    "max_input_per_run": PER_SUB_LIMIT,
    "prefilter_keywords_drop": [
        "pizza", "ofen", "ferrari", "tefal", "unold", "rezept", "küche",
        "perfume", "düfte", "luxury", "schmuck",
        "fachtagung", "messe", "kongress", "tagung",
    ],
    "excluded_article_types": [],
}


# ----------------------------------------------------------------------- #
# Fetch
# ----------------------------------------------------------------------- #


def fetch_subreddit(subreddit: str, today_only: bool = True, max_retries: int = 2) -> list[dict]:
    """Pull up to 25 newest entries from r/<sub>/new/.rss via old.reddit.com.

    On 429, wait 60s and retry up to `max_retries` times — Reddit's burst
    rate limit can take 30-90s to reset on this VPS.
    """
    url = f"https://old.reddit.com/r/{subreddit}/new/.rss"
    for attempt in range(max_retries + 1):
        r = requests.get(url, timeout=15, headers={"User-Agent": USER_AGENT})
        if r.status_code == 200:
            return parse_atom(r.text, subreddit)
        if r.status_code == 429 and attempt < max_retries:
            wait = 60 * (attempt + 1)
            print(f"[fetch] r/{subreddit} → 429, sleeping {wait}s (retry {attempt+1}/{max_retries})", file=sys.stderr)
            time.sleep(wait)
            continue
        print(f"[fetch] r/{subreddit} → HTTP {r.status_code}", file=sys.stderr)
        return []
    return []


def parse_atom(xml: str, subreddit: str) -> list[dict]:
    entries = re.findall(r"<entry>(.*?)</entry>", xml, re.DOTALL)
    today = datetime.now(timezone.utc).date()
    out = []
    for ex in entries:
        title_m = re.search(
            r"<title[^>]*>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>", ex, re.DOTALL
        )
        link_m = re.search(r'<link[^>]+href="([^"]+)"', ex)
        updated_m = re.search(r"<updated>(.*?)</updated>", ex)
        content_m = re.search(r"<content[^>]*>(.*?)</content>", ex, re.DOTALL)
        author_m = re.search(r"<author>.*?<name>(.*?)</name>", ex, re.DOTALL)
        id_m = re.search(r"<id>(.*?)</id>", ex)
        try:
            dt = datetime.fromisoformat(updated_m.group(1)) if updated_m else None
        except Exception:
            dt = None
        out.append({
            "id": id_m.group(1) if id_m else None,
            "title": (title_m.group(1) if title_m else "")
            .replace("&quot;", '"')
            .replace("&amp;", "&"),
            "url": link_m.group(1) if link_m else "",
            "updated": updated_m.group(1) if updated_m else None,
            "date": dt.date().isoformat() if dt else None,
            "is_today": bool(dt and dt.date() == today),
            "author": author_m.group(1) if author_m else None,
            "content_html": (content_m.group(1) if content_m else "")[:6000],
            "subreddit": subreddit,
        })
    return out


def choose_today_or_fallback(entries: list[dict], want: int) -> list[dict]:
    """Today first; if fewer than `want`, walk backward in time."""
    today = [e for e in entries if e["is_today"]]
    older = [e for e in entries if not e["is_today"]]
    if len(today) >= want:
        return today[:want]
    return (today + older)[:want]


# ----------------------------------------------------------------------- #
# Score + translate
# ----------------------------------------------------------------------- #


def build_records(entries: list[dict]) -> list[dict]:
    return [
        {
            "source_id": e["id"],
            "title": e["title"],
            "channel_name": f"r/{e['subreddit']}",
            "first_seen_at": e["updated"],
            "record_id": e["id"],
            "_url": e["url"],
            "_author": e["author"],
            "_content_html": e["content_html"],
            "_date": e["date"],
            # _reddit_id 保留原 reddit atom id,給後面 write_vault → mark_processed 用
            "_reddit_id": e["id"],
        }
        for e in entries
    ]


def build_translate_items(records: list[dict]) -> list[dict]:
    items = []
    for s in records:
        body = re.sub(r"<[^>]+>", " ", s["_content_html"] or "")
        body = re.sub(r"\s+", " ", body).strip()
        items.append({
            "title": s["title"],
            "source_name": s["channel_name"],
            "url": s["_url"],  # ← real permalink, not placeholder
            "pub_date": s["_date"],
            # Batch translate reads `summary`; put the post body there so the
            # LLM can summarize the actual content instead of "title only".
            "summary": body[:1500],
            "full_text": body,
            "content_html": s["_content_html"],
        })
    return items


# ----------------------------------------------------------------------- #
# Vault + Discord output
# ----------------------------------------------------------------------- #


def write_vault(per_sub_picks: dict[str, list[dict]], dry_run: bool) -> tuple[Path, dict[str, Path]]:
    """寫 vault + 回傳每篇貼文對應的 vault 路徑。

    回傳:
        (index_path, post_paths): index_path 是 _index.md 路徑,
        post_paths 是 ``(subreddit, source_id) -> Path`` 的 mapping,
        給 ``mark_processed`` 用 ``output_path`` 欄位。

    命名:``<subreddit>-<slug>.md``,subreddit 用 ``_`` 取代 ``/``,
    slug 取 title 前 60 字,只留 ``[\\w\\-一-鿿]``(中文/英文/數字/底線/連字號)。
    """
    today = datetime.now(timezone.utc).date().isoformat()
    out_dir = VAULT_ROOT / today
    if not dry_run:
        out_dir.mkdir(parents=True, exist_ok=True)

    post_paths: dict[str, Path] = {}
    # Per-post files
    for sub, picks in per_sub_picks.items():
        for it in picks:
            # sub is like "r/Immobilieninvestments"; flatten to safe filename.
            safe_sub = sub.replace("/", "_")
            slug = re.sub(r"[^\w\-一-鿿]+", "-", (it.get("title_zh") or it.get("title", "untitled"))[:60]).strip("-")
            if not slug:
                slug = "untitled"
            post_path = out_dir / f"{safe_sub}-{slug}.md"
            body = render_post_md(it)
            if not dry_run:
                post_path.write_text(body, encoding="utf-8")
            # 用 reddit post id 當 key(每篇貼文唯一)
            reddit_id = it.get("_reddit_id") or it.get("source_id") or it.get("id") or ""
            if reddit_id:
                post_paths[reddit_id] = post_path

    # Index
    index_lines = [
        f"# Reddit Daily — {today}",
        "",
        f"{len(SUBREDDITS)} 個德國房地產 / 財經 subreddit,各取 top 3 LLM 評分最高的貼文,",
        "翻譯 + 摘要後推到 Discord #reddit-insights。",
        "",
    ]
    for sub, picks in per_sub_picks.items():
        # sub is "r/Immobilieninvestments"; render as "## r/..." for display
        display_sub = sub if sub.startswith("r/") else f"r/{sub}"
        index_lines.append(f"## {display_sub}")
        index_lines.append("")
        for it in picks:
            title = it.get("title_zh") or it.get("title", "")
            score = it.get("score", 0)
            url = it.get("url", "")
            index_lines.append(f"- [{score:>2}] [{title}]({url})")
        index_lines.append("")

    index_path = out_dir / "_index.md"
    if not dry_run:
        index_path.write_text("\n".join(index_lines), encoding="utf-8")
    return index_path, post_paths


def render_post_md(it: dict) -> str:
    title = it.get("title_zh") or it.get("title", "")
    summary = it.get("summary_zh", "")
    score = it.get("score", 0)
    rationale = it.get("rationale", "")
    url = it.get("url", "")
    sub = it.get("source_name", "")
    date = it.get("pub_date", "")
    author = (it.get("full_text") or "")[:200]  # we don't carry author through translate; keep stub

    return (
        f"# {title}\n\n"
        f"- **Subreddit**: {sub}\n"
        f"- **Date**: {date}\n"
        f"- **Score (LLM)**: {score}\n"
        f"- **URL**: {url}\n"
        f"- **Rationale**: {rationale}\n\n"
        f"## 摘要\n\n{summary}\n"
    )


def push_discord(per_sub_picks: dict[str, list[dict]], dry_run: bool) -> None:
    """Push one Discord embed per subreddit with all picks + real URLs."""
    for sub, picks in per_sub_picks.items():
        if not picks:
            continue
        # sub is "r/Immobilieninvestments"; display as "r/..." (no doubling).
        display_sub = sub if sub.startswith("r/") else f"r/{sub}"
        lines = [f"# {display_sub} — Reddit 每日精選", ""]
        for it in picks:
            title = it.get("title_zh") or it.get("title", "")
            score = it.get("score", 0)
            summary = it.get("summary_zh", "")
            url = it.get("url", "")
            lines.append(f"## [{score:>2}] {title}")
            lines.append(f"<{url}>")
            lines.append(summary[:500])
            lines.append("")
        text = "\n".join(lines)
        if dry_run:
            print(f"[discord dry-run] {sub}: {len(text)} chars")
            continue
        _send(channel=DISCORD_CHANNEL_REDDIT, content=text[:1900])  # 1900 safe cap
        time.sleep(1)


def _build_reddit_metadata(item: dict, today: str) -> dict:
    """組出 ProcessedContent.metadata 用的 JSON blob。"""
    return {
        "source": "reddit",
        "subreddit": item.get("source_name", ""),  # e.g. "r/Finanzen"
        "url": item.get("_url") or item.get("url", ""),
        "author": item.get("_author", ""),
        "pub_date": item.get("_date") or item.get("pub_date", ""),
        "score": item.get("score", 0),
        "rationale": item.get("rationale", ""),
        "digest_date": today,
    }


def mark_reddit_processed(
    picks: dict[str, list[dict]],
    post_paths: dict[str, Path],
    *,
    dry_run: bool,
) -> None:
    """對每篇上榜貼文呼叫 ``ProcessedStore.mark_processed``。

    ``source_id`` 用 reddit atom id(像 ``t3_xxxxx``)— 這是 Reddit 唯一
    識別,當成 ProcessedContent 的 dedup key(配合 source_type='reddit'
    即可用 ``make_hash(source_type, source_id)`` 算 source_hash)。

    Failure of any single mark is logged but does not raise — vault 寫入
    已經成功,下次 run 會自動重試(``mark_processed`` 是 idempotent)。

    ``--dry-run`` 不寫 Airtable,只 print 預期行為。
    """
    if dry_run:
        n = sum(len(v) for v in picks.values())
        logger.info(
            "[dry-run] 預期標記 %d 個 reddit record 到 ProcessedContent",
            n,
        )
        return

    store = ProcessedStore(PROCESSED_BASE_ID, DEFAULT_TABLE)
    today = datetime.now(timezone.utc).date().isoformat()
    ok = 0
    errs: list[tuple[str, str]] = []
    for sub, items in picks.items():
        for it in items:
            reddit_id = it.get("_reddit_id") or it.get("source_id") or ""
            if not reddit_id:
                logger.warning("skip: 沒有 reddit_id (%s)", it.get("title", "")[:40])
                continue
            output_path = post_paths.get(reddit_id)
            if output_path is not None:
                output_path = str(output_path)
            title = it.get("title_zh") or it.get("title", "")
            channel_value = f"reddit.{sub.replace('/', '_')}"
            first_seen = it.get("_date") or it.get("pub_date")
            # first_seen 必須是 ISO8601 datetime,不是 date。pub_date 已經是 YYYY-MM-DD 字串。
            first_seen_at = None
            if first_seen:
                try:
                    first_seen_at = datetime.fromisoformat(first_seen)
                except (TypeError, ValueError):
                    first_seen_at = None
            try:
                record_id = store.mark_processed(
                    source_type="reddit",
                    source_id=reddit_id,
                    title=title,
                    channels=[channel_value],
                    output_path=output_path,
                    metadata=_build_reddit_metadata(it, today),
                    tags=["short"],
                    article_type="short-summary",
                    first_seen_at=first_seen_at,
                )
                ok += 1
                logger.info(
                    "marked processed: reddit | %s -> %s", reddit_id, record_id,
                )
            except ProcessedStoreError as e:
                logger.error(
                    "mark_processed failed for reddit | %s: %s", reddit_id, e,
                )
                errs.append((reddit_id, str(e)))
            except Exception as e:  # noqa: BLE001
                logger.error(
                    "mark_processed crashed for reddit | %s: %s", reddit_id, e,
                )
                errs.append((reddit_id, str(e)))
    logger.info(
        "[reddit ledger] ok=%d errors=%d total=%d",
        ok, len(errs), sum(len(v) for v in picks.values()),
    )


# ----------------------------------------------------------------------- #
# Main
# ----------------------------------------------------------------------- #


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="skip vault + Discord writes")
    ap.add_argument("--no-discord", action="store_true", help="write vault but skip Discord")
    args = ap.parse_args()

    print(f"[reddit_monitor] {datetime.now(timezone.utc).isoformat()} — fetching {len(SUBREDDITS)} subs…")

    # 1) Fetch
    raw: dict[str, list[dict]] = {}
    for sub in SUBREDDITS:
        raw[sub] = fetch_subreddit(sub)
        n_today = sum(1 for e in raw[sub] if e["is_today"])
        print(f"  r/{sub}: {len(raw[sub])} entries ({n_today} today)")
        time.sleep(12)  # Reddit burst-protection

    # 2) Pick today (or fallback) per sub
    chosen: dict[str, list[dict]] = {
        sub: choose_today_or_fallback(entries, PER_SUB_LIMIT)
        for sub, entries in raw.items()
    }
    total = sum(len(v) for v in chosen.values())
    print(f"[reddit_monitor] chosen {total} entries (dedup on reddit post id)")
    # dedup by reddit post id across subs (rare but possible cross-post)
    seen = set()
    for sub in chosen:
        deduped = []
        for e in chosen[sub]:
            if e["id"] in seen:
                continue
            seen.add(e["id"])
            deduped.append(e)
        chosen[sub] = deduped[:PER_SUB_LIMIT]

    # 3) Score
    all_records: list[dict] = []
    for sub in SUBREDDITS:
        all_records.extend(build_records(chosen[sub]))
    print(f"[reddit_monitor] scoring {len(all_records)} records…")
    scored = score_candidates(all_records, REDDIT_CRITERIA)
    by_sub: dict[str, list[dict]] = {f"r/{s}": [] for s in SUBREDDITS}
    for r in scored:
        by_sub[r["channel_name"]].append(r)
    top_picks: dict[str, list[dict]] = {}
    for sub_name, items in by_sub.items():
        items.sort(key=lambda x: -x.get("score", 0))
        top_picks[sub_name] = items[:PER_SUB_KEEP]
        print(f"  {sub_name}: top score = {top_picks[sub_name][0]['score'] if top_picks[sub_name] else 'n/a'}")

    # 4) Translate
    print(f"[reddit_monitor] translating top {PER_SUB_KEEP}/sub…")
    flat = []
    # Carry forward the private fields (url, content_html, ...) so
    # build_translate_items can populate the batch payload.
    for sub in SUBREDDITS:
        sub_records = build_records(chosen[sub])
        rec_by_id = {r["source_id"]: r for r in sub_records}
        for p in top_picks[f"r/{sub}"]:
            orig = rec_by_id.get(p["source_id"], {})
            p["_url"] = orig.get("_url", "")
            p["_content_html"] = orig.get("_content_html", "")
            p["_author"] = orig.get("_author", "")
            p["_date"] = orig.get("_date", "")
        flat.extend(build_translate_items(top_picks[f"r/{sub}"]))
    translated = analyze_items_batch(flat, chunk_size=4)

    # Merge translation back into picks by source_name + url
    by_key = {(t.get("source_name"), t.get("url")): t for t in translated}
    final: dict[str, list[dict]] = {}
    for sub_name, picks in top_picks.items():
        merged = []
        for p in picks:
            url = p.get("_url", "")
            t = by_key.get((sub_name, url), {})
            merged.append({**p, **t})
        final[sub_name] = merged

    # 5) Write vault
    idx, post_paths = write_vault(final, dry_run=args.dry_run)
    print(f"[reddit_monitor] vault → {idx}")

    # 5b) Mark ProcessedContent — Plan 3 Round 3 補的,
    # 之前完全沒寫,導致 daily_digest 拉不到 reddit record。
    mark_reddit_processed(final, post_paths, dry_run=args.dry_run)

    # 6) Push Discord
    if not args.no_discord:
        push_discord(final, dry_run=args.dry_run)
        print("[reddit_monitor] discord pushed")
    else:
        print("[reddit_monitor] discord skipped (--no-discord)")

    print("[reddit_monitor] done.")


if __name__ == "__main__":
    main()
