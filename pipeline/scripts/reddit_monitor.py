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
from pipeline.lib.translate import analyze_items_batch  # noqa: E402
from pipeline.lib.youtube_discord import _send  # noqa: E402  # re-use existing Discord sender

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


def write_vault(per_sub_picks: dict[str, list[dict]], dry_run: bool) -> Path:
    """Write immobilien-kb/vault/Reddit/<date>/_index.md + per-post .md files."""
    today = datetime.now(timezone.utc).date().isoformat()
    out_dir = VAULT_ROOT / today
    if not dry_run:
        out_dir.mkdir(parents=True, exist_ok=True)

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
    return index_path


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
    idx = write_vault(final, dry_run=args.dry_run)
    print(f"[reddit_monitor] vault → {idx}")

    # 6) Push Discord
    if not args.no_discord:
        push_discord(final, dry_run=args.dry_run)
        print("[reddit_monitor] discord pushed")
    else:
        print("[reddit_monitor] discord skipped (--no-discord)")

    print("[reddit_monitor] done.")


if __name__ == "__main__":
    main()
