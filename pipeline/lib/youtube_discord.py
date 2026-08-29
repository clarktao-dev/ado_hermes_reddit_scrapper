"""Push VideoDigest summaries to Discord channel `podcast` (alias).

One embed per video — no header, no body. The 4096-char Discord limit is
respected by splitting the digest into up to N embeds per video ONLY if it
truly exceeds the limit. No artificial 3800-char cap.
"""
from __future__ import annotations
import os
import subprocess
from typing import List


from pipeline.lib.paths import DISCORD_SENDER as _DISCORD_SENDER_PATH

DISCORD_SENDER = str(_DISCORD_SENDER_PATH)
EMBED_MAX_CHARS = 4000  # Discord hard cap is 4096; leave a small safety margin


def _is_degraded_digest(d) -> bool:
    """Skip Discord push for LLM stub downgrades (content_quality=degraded)."""
    summary = getattr(d, "summary_zh", "") or ""
    return "內容待補" in summary


def _send(channel: str, content: str, as_embed: bool = True, title: str = "",
          retries: int = 3) -> list:
    """Wrapper around discord_sender.py — returns list of message_ids on success.
    Empty list on failure (after retries). discord_sender.py already prints
    'OK: sent N message(s)' to stdout; we capture that to harvest message_ids
    via a second CLI call that prints them. (Cheap: discord_sender is <2s.)
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
                # discord_sender.py prints "OK: sent N message(s)" then
                # one "  id: <mid>" per sent message. Harvest those.
                mids: list = []
                import re as _re
                for line in proc.stdout.splitlines():
                    m = _re.search(r"id:\s*(\d{17,20})", line)
                    if m:
                        mids.append(m.group(1))
                if mids:
                    return mids
                # Fallback: call discord_sender.send_to_channel directly
                # to get the structured return value.
                try:
                    import importlib.util as _ilu
                    spec = _ilu.spec_from_file_location(
                        "discord_sender", DISCORD_SENDER
                    )
                    if spec is None or spec.loader is None:
                        return []
                    mod = _ilu.module_from_spec(spec)
                    spec.loader.exec_module(mod)  # type: ignore[union-attr]
                    res = mod.send_to_channel(channel, content,
                                              as_embed=as_embed, title=title or "")
                    if res.get("ok") and res.get("message_ids"):
                        return list(res["message_ids"])
                except Exception:
                    pass
                return []
            print(f"    [discord] attempt {attempt + 1}/{retries} failed: rc={proc.returncode}")
        except subprocess.TimeoutExpired:
            print(f"    [discord] attempt {attempt + 1}/{retries} timeout")
        if attempt < retries - 1:
            import time
            time.sleep(2)
    return []


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
        if _is_degraded_digest(d):
            summary["per_video"].append({
                "video_id": d.video_id,
                "title": d.title,
                "n_embeds": 0,
                "message_ids": [],
                "skipped": "degraded",
            })
            continue
        body = _build_embed_body(d)
        chunks = _split_chunks(body)
        sent_for_video = 0
        message_ids_for_video: list = []
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
                mids = _send(channel, chunk, title=title)
                if mids:
                    sent_for_video += 1
                    message_ids_for_video.extend(mids)
                else:
                    summary["errors"].append(f"send failed: {d.video_id} chunk {idx + 1}")
        summary["per_video"].append({
            "video_id": d.video_id,
            "title": d.title,
            "n_embeds": sent_for_video,
            "message_ids": message_ids_for_video,
        })
        summary["n_embeds"] += sent_for_video

    return summary