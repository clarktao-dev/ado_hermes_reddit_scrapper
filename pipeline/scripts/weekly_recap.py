#!/usr/bin/env python3
"""Weekly recap (Plan 4 part 2).

Pulls ReactionPicks from the past N days (default 3) and pushes a header +
one message per item to channel 1539010288026779688 (#挑文區).

Each item message asks the user to react with:
- ✅ = 寫長文
- 🟡 = Podcast 主題
- 📝 = 其他
- ❌ = 跳過

The user can react normally in Discord; we don't capture these decisions
into Firestore in this round — that's a follow-up.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests

from pipeline.lib.reaction_store import ReactionStoreError, get_reaction_store

logger = logging.getLogger("weekly_recap")

DISCORD_BOT_TOKEN = os.environ.get("DISCORD_BOT_TOKEN", "")
TARGET_CHANNEL_ID = "1539010288026779688"  # #挑文區
DISCORD_GUILD_ID = os.environ.get("DISCORD_GUILD_ID", "1306402830617616485")
DEFAULT_DAYS = 3
PROCESS_DECISION_LABEL = "✅=寫長文 · 🟡=Podcast 主題 · 📝=其他 · ❌=跳過"


def query_recent_reactions(days: int) -> list[dict[str, Any]]:
    """Fetch ReactionPicks records newer than ``days`` ago."""
    try:
        return get_reaction_store().query_recent(days)
    except ReactionStoreError as e:
        raise RuntimeError(str(e)) from e


def render_header(n: int, days: int) -> str:
    return (
        f"📚 **週回顧 — 過去 {days} 天共 {n} 篇 ✅**\n\n"
        f"以下是你最近 {days} 天在 #每日頭條 / #每日podcast / #每日reddit "
        f"按過 ✅ 的項目。\n\n"
        f"看完請針對每則訊息按:\n"
        f"• ✅ = 寫長文\n"
        f"• 🟡 = Podcast 主題\n"
        f"• 📝 = 其他(請在 thread 補充說明)\n"
        f"• ❌ = 跳過\n\n"
        f"{n} 則訊息如下,各自按 emoji。"
    )


def render_message(idx: int, total: int, rec: dict[str, Any]) -> str:
    fields = rec.get("fields", {})
    kind = fields.get("message_kind", "other")
    title = fields.get("title") or "(無標題)"
    snippet = (fields.get("snippet") or "")[:200]
    author = fields.get("channel_name") or "unknown"
    reaction_date = fields.get("reaction_date", "")
    message_id = fields.get("message_id", "")
    embed_url = fields.get("embed_url")

    lines = [
        f"[{idx}/{total}] [{kind}] {title}",
        f"頻道: {author} · {reaction_date[:16]}",
        f"連結: https://discord.com/channels/{DISCORD_GUILD_ID}/{fields.get('channel_id')}/{message_id}",
    ]
    if embed_url:
        lines.append(f"Embed: {embed_url}")
    if snippet:
        lines.append(f"\n> {snippet}")
    lines.append("")
    lines.append(PROCESS_DECISION_LABEL)
    return "\n".join(lines)


def push_message(content: str) -> str | None:
    if not DISCORD_BOT_TOKEN:
        raise RuntimeError("DISCORD_BOT_TOKEN not set in env")
    url = f"https://discord.com/api/v10/channels/{TARGET_CHANNEL_ID}/messages"
    r = requests.post(
        url,
        headers={"Authorization": f"Bot {DISCORD_BOT_TOKEN}", "Content-Type": "application/json"},
        json={"content": content[:2000]},  # Discord limit
        timeout=20,
    )
    if r.status_code in (200, 201):
        return r.json().get("id")
    raise RuntimeError(f"Discord HTTP {r.status_code}: {r.text[:200]}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
    logger.info("fetching ReactionPicks past %s days", args.days)

    try:
        records = query_recent_reactions(args.days)
    except Exception as e:
        logger.error("firestore fetch failed: %s", e)
        return 1

    logger.info("found %d records", len(records))

    if not records:
        print("(no records in window — nothing to recap)")
        return 0

    header = render_header(len(records), args.days)
    if args.dry_run:
        print("=== HEADER ===")
        print(header)
        for i, rec in enumerate(records, 1):
            print()
            print(f"=== MSG {i}/{len(records)} ===")
            print(render_message(i, len(records), rec))
        return 0

    try:
        header_id = push_message(header)
        logger.info("header pushed (id=%s)", header_id)
    except Exception as e:
        logger.error("header push failed: %s", e)
        return 2

    pushed = 0
    for i, rec in enumerate(records, 1):
        content = render_message(i, len(records), rec)
        try:
            msg_id = push_message(content)
            logger.info("msg %d/%d pushed (id=%s)", i, len(records), msg_id)
            pushed += 1
        except Exception as e:
            logger.warning("msg %d/%d push failed: %s", i, len(records), e)

    logger.info("done. pushed %d/%d", pushed, len(records))
    return 0


if __name__ == "__main__":
    sys.exit(main())
