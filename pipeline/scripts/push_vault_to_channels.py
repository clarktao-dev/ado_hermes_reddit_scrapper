#!/usr/bin/env python3
"""Plan 4 測試 — 從 vault 把今天內容用個別訊息推到 3 channel,讓 user 測試 ✅ reaction。

Usage:
  python3 push_vault_to_channels.py           # 實際推送
  python3 push_vault_to_channels.py --dry-run # 只 print 不推
"""
from __future__ import annotations

import argparse
import logging
import os
import pathlib
import re
import sys
import time
from datetime import date
from typing import Optional

import requests

VAULT_ROOT = pathlib.Path(
    "/root/projects/ado_hermes_reddit_scrapper/immobilien-kb/vault"
)
DISCORD_BOT_TOKEN = os.environ.get("DISCORD_BOT_TOKEN", "")

CHANNEL_MAP = {
    "Daily": "1520791894995501106",  # #每日頭條
    "Reddit": "1537907956132089976",  # #每日reddit
    # YouTube 暫時不處理 (vault 結構跟 Daily/Reddit 不同,在 channel subdirs)
}

# 訊息長度上限 — Discord message hard limit 2000,留 buffer
TITLE_MAX = 120
SNIPPET_MAX = 600

logger = logging.getLogger("push_vault")


def parse_frontmatter_and_body(text: str) -> tuple[str, str, Optional[str]]:
    """從 vault .md 抽出 (title, snippet, url)。"""
    # 1) frontmatter ---
    m = re.match(r"^---\s*\n(.*?)\n---\s*\n(.*)$", text, re.DOTALL)
    fm = {}
    body = text
    if m:
        fm_block = m.group(1)
        body = m.group(2)
        for line in fm_block.splitlines():
            if ":" in line:
                k, v = line.split(":", 1)
                fm[k.strip()] = v.strip().strip('"').strip("'")
    title = fm.get("title") or body.splitlines()[0].lstrip("# ").strip()[:TITLE_MAX]
    url = fm.get("url") or fm.get("source_url")

    # 2) snippet — body 前幾個有意義的句子
    snippet = ""
    for line in body.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # 跳過 **URL:** 等 metadata 行
        if line.startswith("**") and ":**" in line:
            continue
        snippet += line + " "
        if len(snippet) > SNIPPET_MAX:
            break
    snippet = snippet.strip()[:SNIPPET_MAX]
    return title, snippet, url


def push_message(channel_id: str, content: str) -> Optional[str]:
    if not DISCORD_BOT_TOKEN:
        raise RuntimeError("DISCORD_BOT_TOKEN not set in env")
    url = f"https://discord.com/api/v10/channels/{channel_id}/messages"
    for attempt in range(4):
        r = requests.post(
            url,
            headers={"Authorization": f"Bot {DISCORD_BOT_TOKEN}", "Content-Type": "application/json"},
            json={"content": content[:2000]},
            timeout=20,
        )
        if r.status_code in (200, 201):
            return r.json().get("id")
        if r.status_code == 429:
            retry_after = r.json().get("retry_after", 0.5) if r.headers.get("content-type", "").startswith("application/json") else 0.5
            time.sleep(float(retry_after) + 0.2)
            continue
        raise RuntimeError(f"Discord HTTP {r.status_code}: {r.text[:200]}")
    raise RuntimeError("Discord 429 retry exhausted (4 attempts)")


def render_message(kind: str, title: str, snippet: str, url: Optional[str], vault_path: str) -> str:
    lines = [f"📰 **[{kind}] {title}**"]
    if url:
        lines.append(f"🔗 {url}")
    if snippet:
        lines.append(f"\n> {snippet}")
    lines.append(f"\n_(vault: `{pathlib.Path(vault_path).name}`)_")
    return "\n".join(lines)


def scan_today_vault(day_str: Optional[str] = None) -> list[dict]:
    """掃今天(或指定日期)的 vault,回傳所有可推送項目。"""
    if day_str is None:
        day_str = date.today().isoformat()
    items = []
    for vault_subdir, channel_id in CHANNEL_MAP.items():
        day_dir = VAULT_ROOT / vault_subdir / day_str
        if not day_dir.exists():
            logger.warning("dir not found: %s", day_dir)
            continue
        for f in sorted(day_dir.glob("*.md")):
            if f.name == "_index.md":
                continue
            try:
                text = f.read_text(encoding="utf-8")
            except Exception as e:
                logger.warning("read failed %s: %s", f, e)
                continue
            try:
                title, snippet, url = parse_frontmatter_and_body(text)
            except Exception as e:
                logger.warning("parse failed %s: %s", f, e)
                continue
            items.append({
                "kind": vault_subdir.lower(),
                "title": title,
                "snippet": snippet,
                "url": url,
                "vault_path": str(f),
                "channel_id": channel_id,
            })
    return items


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--day", default=None, help="override date (YYYY-MM-DD)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
    items = scan_today_vault(args.day)
    logger.info("found %d items", len(items))

    if not items:
        print("(no items to push)")
        return 0

    pushed = 0
    for i, it in enumerate(items, 1):
        content = render_message(it["kind"], it["title"], it["snippet"], it["url"], it["vault_path"])
        if args.dry_run:
            print(f"=== [{i}/{len(items)}] → channel {it['channel_id']} ===")
            print(content)
            print()
            continue
        try:
            mid = push_message(it["channel_id"], content)
            logger.info("pushed %d/%d (msg_id=%s channel=%s)", i, len(items), mid, it["channel_id"])
            pushed += 1
        except Exception as e:
            logger.error("push failed %d/%d: %s", i, len(items), e)

    if not args.dry_run:
        logger.info("done. pushed %d/%d", pushed, len(items))
    return 0


if __name__ == "__main__":
    sys.exit(main())