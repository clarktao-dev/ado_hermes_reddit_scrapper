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
import time
import argparse
import requests
from pathlib import Path
from typing import Optional, List


# Discord API 字數限制
DISCORD_MAX_CHARS = 2000
SEGMENT_SAFE = 1900  # 留 buffer

# ── Discord 429 rate-limit retry (2026-10-21) ────────────────────────────
# 為什麼需要：news_daily 2026-10-20 一次送 15 個 embed 觸發 per-channel
# 429（5 msg / 5s），6/14 items 靜默掉在這裡 —— send_to_channel 原本
# 只回 {"ok": False, "error": "http 429: ..."}，pipeline 就把它當成
# 「已送出但沒回 id」繼續走，Airtable 記 discord=False，讀者永遠收不到。
# 規則：讀 Discord 回的 retry_after（秒，通常 0.3–0.6）+ 0.15s jitter，
# 最多試 4 次；非 429 的 4xx/5xx 不重試（重試沒意義，只會延長）。
MAX_429_RETRIES = 4
_429_BASE_SLEEP = 0.15


def _retry_after_seconds(resp, attempt: int) -> float:
    """從 Discord 429 body 的 retry_after 決定這次要睡多久。

    拿不到就 exponential fallback（0.4 * 2**attempt），並額外加 jitter
    避免多個 sender 同時醒來再撞一次。
    """
    delay = 0.4 * (2 ** attempt)
    try:
        body = resp.json()
        if isinstance(body, dict) and "retry_after" in body:
            delay = float(body["retry_after"])
    except Exception:  # noqa: BLE001 — body 不是 JSON 時走 fallback
        pass
    if body_is_global(resp):
        delay = max(delay, 1.0)
    return delay + _429_BASE_SLEEP


def body_is_global(resp) -> bool:
    """Discord 的 global rate limit 會讓 retry_after 長很多。"""
    try:
        body = resp.json()
        return bool(isinstance(body, dict) and body.get("global"))
    except Exception:  # noqa: BLE001
        return False


def _post_with_429_retry(url: str, headers: dict, payload: dict) -> requests.Response:
    """POST 並在遇到 429 時依 retry_after 重試。回傳最後一次 response。

    這層放在 transport，所以所有 caller（news_daily / youtube / podcast）
    都自動獲得 rate-limit 保護，不只是 news pipeline。
    """
    resp: Optional[requests.Response] = None
    for attempt in range(MAX_429_RETRIES + 1):
        resp = requests.post(url, headers=headers, json=payload, timeout=15)
        if resp.status_code != 429:
            return resp
        if attempt >= MAX_429_RETRIES:
            break
        sleep_s = _retry_after_seconds(resp, attempt)
        print(f"[discord_sender] 429 rate limited, sleeping {sleep_s:.2f}s "
              f"(retry {attempt + 1}/{MAX_429_RETRIES})", file=sys.stderr, flush=True)
        time.sleep(sleep_s)
    return resp  # type: ignore[return-value]


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
    # 別名表(channel id 寫死,別名只是給 call site 用)。
    # 新增 digest_candidates / digest_recap(Plan 3,2026-08-17)對應:
    #   - digest_candidates = 每日 06:30 CEST 推候選清單
    #   - digest_recap      = 週二/五 07:00 CEST 推週回顧(同一 channel
    #                         也服務 long-form YouTube recs,因此保留
    #                         ``longform`` alias 給舊 caller 用)
    aliases = {
        "home": "1495548848183967916",      # main channel id
        "headlines": "1520791894995501106",  # #每日頭條
        "每日頭條": "1520791894995501106",
        "podcast": "1535461574460968960",    # #每日podcast (YouTube transcript digest)
        "每日podcast": "1535461574460968960",
        "tao": "1495562787685011616",        # #tao (Destatis official stats digest)
        "longform": "1537705289367953408",  # #長文推薦 (long-form YouTube recs) — created 2026-08-14, manual via Discord UI
        "reddit": "1537907956132089976",   # #Reddit (Reddit 房地產/財經每日精選) — created 2026-08-14
        "digest_candidates": "1539010288026779688",  # 每日候選清單 (Plan 3)
        "digest_recap": "1537705289367953408",        # 週回顧(同 longform channel;Plan 3)
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
            r = _post_with_429_retry(url, headers, payload)
            if r.status_code == 200:
                message_ids.append(r.json()["id"])
            else:
                return {"ok": False, "error": f"http {r.status_code}: {r.text[:200]}"}
        else:
            # 純文字模式：自動分段
            for segment in _split_message(text):
                r = _post_with_429_retry(url, headers, {"content": segment})
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
