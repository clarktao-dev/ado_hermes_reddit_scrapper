#!/usr/bin/env python3
"""
discord_sender.py — 發送訊息到 Discord channel (走 bot token)

設計原則：
- Token 永遠只從 .env 讀，絕不 print / log / echo
- 任何錯誤訊息只回報「失敗原因」，不附帶 token 或環境變數
- 自動分段處理 2000 字上限
- 支援純文字與 embed 兩種模式

使用：
    from discord_sender import send_to_channel
    send_to_channel("1520791894995501106", "Hello!")
    send_to_channel("1520791894995501106", "Long text...", as_embed=True, title="標題")
    send_to_channel("1520791894995501106", md_content, from_file=True)

CLI：
    python3 discord_sender.py <channel_id> <message_file_or_text> [--title TITLE] [--embed]
"""
import os
import sys
import json
import argparse
import requests
from pathlib import Path
from typing import Optional, List


# Discord API 字數限制
DISCORD_MAX_CHARS = 2000
SEGMENT_SAFE = 1900  # 留 buffer


def _load_token() -> str:
    """從 /root/.hermes/.env 讀 DISCORD_BOT_TOKEN。讀不到就 raise。
    
    不論成功失敗，絕不把 token 內容回傳到 stdout/stderr。
    """
    env_path = Path("/root/.hermes/.env")
    if not env_path.exists():
        raise FileNotFoundError(f"env file not found: {env_path}")
    
    token = None
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line.startswith("DISCORD_BOT_TOKEN="):
                token = line.split("=", 1)[1].strip()
                break
    
    if not token:
        raise ValueError("DISCORD_BOT_TOKEN not set in env file")
    
    return token


def _resolve_channel(channel: str) -> str:
    """接受 channel ID 或別名 ('home', 'headlines', '每日頭條')"""
    aliases = {
        "home": "1495548848183967916",      # main channel id
        "headlines": "1520791894995501106",  # #每日頭條
        "每日頭條": "1520791894995501106",
        "podcast": "1535461574460968960",    # #每日podcast (YouTube transcript digest)
        "每日podcast": "1535461574460968960",
        "tao": "1495562787685011616",        # #tao (Destatis official stats digest)
        "longform": "1537705289367953408",  # #長文推薦 (long-form YouTube recs) — created 2026-08-14, manual via Discord UI
    }
    if channel in aliases:
        return aliases[channel]
    return channel


def _split_message(text: str, max_size: int = SEGMENT_SAFE) -> List[str]:
    """依行切分，避免把段落切一半。"""
    segments = []
    current = ""
    for line in text.split("\n"):
        if len(current) + len(line) + 1 > max_size:
            if current.strip():
                segments.append(current.rstrip())
            current = line + "\n"
        else:
            current += line + "\n"
    if current.strip():
        segments.append(current.rstrip())
    return segments


def send_to_channel(
    channel: str,
    text: str,
    *,
    as_embed: bool = False,
    title: Optional[str] = None,
    color: int = 0x3498db,  # blue
) -> dict:
    """發送訊息到 Discord channel。回傳 {'ok': bool, 'message_ids': [...], 'error': str|None}
    
    Args:
        channel: channel ID 或別名
        text: 訊息內容
        as_embed: 用 Discord embed 格式（標題/顏色/縮圖）
        title: embed 標題（as_embed=True 時使用）
        color: embed 顏色（int hex，預設藍）
    """
    try:
        token = _load_token()
    except Exception as e:
        return {"ok": False, "error": f"token load failed: {type(e).__name__}"}
    
    channel_id = _resolve_channel(channel)
    url = f"https://discord.com/api/v10/channels/{channel_id}/messages"
    headers = {
        "Authorization": f"Bot {token}",
        "Content-Type": "application/json",
    }
    
    message_ids = []
    try:
        if as_embed:
            # embed 模式：完整內容放一個 embed (上限 4096 chars)
            payload = {
                "embeds": [{
                    "title": title or "Message",
                    "description": text[:4090] + ("..." if len(text) > 4090 else ""),
                    "color": color,
                }]
            }
            r = requests.post(url, headers=headers, json=payload, timeout=15)
            if r.status_code == 200:
                message_ids.append(r.json()["id"])
            else:
                return {"ok": False, "error": f"http {r.status_code}: {r.text[:200]}"}
        else:
            # 純文字模式：自動分段
            for segment in _split_message(text):
                r = requests.post(url, headers=headers, json={"content": segment}, timeout=15)
                if r.status_code == 200:
                    message_ids.append(r.json()["id"])
                else:
                    return {"ok": False, "error": f"http {r.status_code}: {r.text[:200]}"}
        
        return {"ok": True, "message_ids": message_ids, "error": None}
    
    except requests.RequestException as e:
        return {"ok": False, "error": f"network error: {type(e).__name__}"}
    except Exception as e:
        return {"ok": False, "error": f"unexpected: {type(e).__name__}"}


def main():
    parser = argparse.ArgumentParser(description="Send message to Discord channel")
    parser.add_argument("channel", help="Channel ID or alias (home/headlines/每日頭條)")
    parser.add_argument("content", help="Message text OR path to file containing message")
    parser.add_argument("--from-file", action="store_true", help="Treat content as file path")
    parser.add_argument("--embed", action="store_true", help="Send as embed (with title)")
    parser.add_argument("--title", help="Embed title")
    parser.add_argument("--color", type=lambda x: int(x, 0), default=0x3498db, help="Embed color (hex)")
    
    args = parser.parse_args()
    
    if args.from_file:
        path = Path(args.content)
        if not path.exists():
            print(f"FAIL: file not found: {args.content}", file=sys.stderr)
            sys.exit(1)
        text = path.read_text()
    else:
        text = args.content
    
    result = send_to_channel(
        args.channel, text,
        as_embed=args.embed, title=args.title, color=args.color,
    )
    
    if result["ok"]:
        print(f"OK: sent {len(result['message_ids'])} message(s)")
        for mid in result["message_ids"]:
            print(f"  id: {mid}")
    else:
        print(f"FAIL: {result['error']}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
