"""Push VideoDigest summaries to Discord channel `podcast` (alias).

Re-uses discord_sender.py + sends via subprocess (learned from news pipeline).
Format: 1 header embed + N body embeds (3 items/embed to avoid rate limits).
"""
from __future__ import annotations
import json
import os
import subprocess
from pathlib import Path
from typing import List


DISCORD_SENDER = "/root/projects/ado_hermes_reddit_scrapper/immobilien-kb/tools/discord_sender.py"


def _send(channel: str, content: str, as_embed: bool = True, title: str = "") -> bool:
    """Wrapper around discord_sender.py — returns True on success."""
    cmd = ["python3", DISCORD_SENDER, channel]
    if as_embed and title:
        cmd.extend(["--title", title])
    cmd.append(content)
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    return proc.returncode == 0


def step_send_discord(digests, channel: str = "podcast",
                      per_embed: int = 3, dry_run: bool = False) -> dict:
    """Push digests to Discord. Returns summary.

    Layout:
      embed 1 (header): index of all videos for the day
      embed 2..N (body): 1 video per body, with all 4 sections packed in
    """
    if not digests:
        return {"n_embeds": 0, "errors": ["no digests to send"]}

    summary = {"n_embeds": 0, "errors": [], "channel": channel}

    # Header embed: index
    header_lines = [f"📺 今日 Podcast 摘要 — {len(digests)} 部影片", ""]
    for d in digests:
        title_short = d.title[:50]
        header_lines.append(f"• **{d.channel_name}** — [{title_short}]({d.url})")
    header_text = "\n".join(header_lines)
    if dry_run:
        summary["header_preview"] = header_text
    else:
        if _send(channel, header_text, title="📺 每日 Podcast 摘要"):
            summary["n_embeds"] += 1
        else:
            summary["errors"].append("header send failed")

    # Body embeds: 1 per digest (kept short, summary + analyst key points)
    for d in digests:
        body_parts = [
            f"**{d.title}**",
            f"頻道：{d.channel_name}",
            f"影片時長：{d.duration_sec // 60} 分 {d.duration_sec % 60} 秒",
            f"連結：{d.url}",
            "",
            "**摘要**",
            d.summary_zh[:600] if d.summary_zh else "（無）",
            "",
            "**房地產分析師視角**",
            d.analyst_zh[:800] if d.analyst_zh else "（無）",
            "",
            "**內容製作人視角**",
            d.producer_zh[:600] if d.producer_zh else "（無）",
        ]
        body = "\n".join(body_parts)
        # Discord embed limit 4096 chars; cap at 3800
        if len(body) > 3800:
            body = body[:3800] + "\n\n_（內容截斷，完整版見 vault）_"
        if dry_run:
            summary.setdefault("body_previews", []).append({
                "video_id": d.video_id,
                "title": d.title,
                "preview": body[:300] + "...",
            })
        else:
            if _send(channel, body, title=d.title[:80]):
                summary["n_embeds"] += 1
            else:
                summary["errors"].append(f"body send failed: {d.video_id}")

    return summary
