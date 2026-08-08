"""Push VideoDigest summaries to Discord channel `podcast` (alias).

One embed per video — no header, no body. The 4096-char Discord limit is
respected by splitting the digest into up to N embeds per video ONLY if it
truly exceeds the limit. No artificial 3800-char cap.
"""
from __future__ import annotations
import os
import subprocess
from typing import List


DISCORD_SENDER = "/root/projects/ado_hermes_reddit_scrapper/immobilien-kb/tools/discord_sender.py"
EMBED_MAX_CHARS = 4000  # Discord hard cap is 4096; leave a small safety margin


def _send(channel: str, content: str, as_embed: bool = True, title: str = "",
          retries: int = 3) -> bool:
    """Wrapper around discord_sender.py — returns True on success.
    Retries up to `retries` times on transient failures (Discord API can
    return 5xx or hang under VPS IP blocks)."""
    cmd = ["python3", DISCORD_SENDER, channel]
    if as_embed and title:
        cmd.extend(["--title", title])
    cmd.append(content)
    for attempt in range(retries):
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=45)
            if proc.returncode == 0:
                return True
            print(f"    [discord] attempt {attempt + 1}/{retries} failed: rc={proc.returncode}")
        except subprocess.TimeoutExpired:
            print(f"    [discord] attempt {attempt + 1}/{retries} timeout")
        if attempt < retries - 1:
            import time
            time.sleep(2)
    return False


def _build_embed_body(d) -> str:
    """Build the body of one embed from one digest. No truncation."""
    parts = [
        f"**頻道**：{d.channel_name}",
        f"**影片時長**：{d.duration_sec // 60} 分 {d.duration_sec % 60} 秒",
        f"**連結**：{d.url}",
        "",
        "**摘要**",
        d.summary_zh or "（無）",
        "",
        "**房地產分析師視角**",
        d.analyst_zh or "（無）",
        "",
        "**內容製作人視角**",
        d.producer_zh or "（無）",
        "",
        "**重點詞彙**",
        d.vocab_zh or "（無）",
    ]
    return "\n".join(parts)


def _split_chunks(text: str, max_chars: int = EMBED_MAX_CHARS) -> List[str]:
    """Split text into chunks under max_chars. Each chunk is one embed.
    Splits at paragraph boundaries to avoid mid-sentence truncation."""
    if len(text) <= max_chars:
        return [text]
    chunks = []
    while text:
        if len(text) <= max_chars:
            chunks.append(text)
            break
        # find last double-newline within max_chars
        cut = text.rfind("\n\n", 0, max_chars)
        if cut < max_chars // 2:
            cut = text.rfind("\n", 0, max_chars)
        if cut < max_chars // 2:
            cut = max_chars
        chunks.append(text[:cut])
        text = text[cut:].lstrip("\n")
    return chunks


def step_send_discord(digests, channel: str = "podcast",
                      dry_run: bool = False) -> dict:
    """Push digests to Discord. Returns summary.

    One embed per chunk of a digest. No header embed. No artificial
    truncation; if a digest is too long for one embed, it's split at
    paragraph boundaries across multiple embeds with the same title.
    """
    if not digests:
        return {"n_embeds": 0, "errors": ["no digests to send"]}

    summary = {"n_embeds": 0, "errors": [], "channel": channel, "per_video": []}

    for d in digests:
        body = _build_embed_body(d)
        chunks = _split_chunks(body)
        sent_for_video = 0
        for idx, chunk in enumerate(chunks):
            # Same title for all chunks of one video so they group in Discord
            title = d.title[:80] if idx == 0 else f"{d.title[:60]}（續 {idx + 1}/{len(chunks)}）"
            if dry_run:
                summary.setdefault("previews", []).append({
                    "video_id": d.video_id,
                    "title": title,
                    "len_chars": len(chunk),
                })
            else:
                if _send(channel, chunk, title=title):
                    sent_for_video += 1
                else:
                    summary["errors"].append(f"send failed: {d.video_id} chunk {idx + 1}")
        summary["per_video"].append({
            "video_id": d.video_id,
            "title": d.title,
            "n_embeds": sent_for_video,
        })
        summary["n_embeds"] += sent_for_video

    return summary