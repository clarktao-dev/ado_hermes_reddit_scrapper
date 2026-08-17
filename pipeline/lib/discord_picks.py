#!/usr/bin/env python3
"""Discord emoji reaction listener — Plan 3 (2026-08-17)

用途
----
``daily_digest.py`` 每日推送候選清單到 Discord channel 後,使用者會在每則
候選訊息上按 ✅ / ❌ / 🟡 emoji。本 daemon 訂閱 ``MESSAGE_REACTION_ADD``
event,過濾出對應的 channel + emoji,把使用者選擇寫進 Airtable
``DailyDigestPicks.picked / picked_at / discord_user_id``。

設計
----
- **長跑 daemon**(systemd 或 nohup),不是 cron 一次性 — reaction 隨時
  可能發生,daemon 需持續在線。
- 用 ``discord.py``(已裝)而不是裸 websocket:code 短、可讀,並由 library
  處理 heartbeat / reconnection / session resume。
- Token 來源:跟 ``immobilien-kb/tools/discord_sender.py`` 一致,從
  ``~/.hermes/.env`` 讀 ``DISCORD_BOT_TOKEN``(單一 bot 帳號)。
- Airtable 寫入:複製 ``daily_digest.py`` 的 ``_airtable_request`` +
  ``update_pick_by_message`` 邏輯,避免 cross-module refactor(Plan 3 只
  求可運作;若日後 reuse 量大再抽到 ``pipeline/lib`` 共用 module)。
- 過濾條件:
  - channel id = ``TARGET_CHANNEL_ID``
  - emoji 在 ``EMOJI_TO_PICKED`` 內(✅/❌/🟡)
  - 對應 ``message_id`` 在 ``DailyDigestPicks`` 找得到 row
- 找不到 row 時靜默 log warning(可能是 user 在舊 digest 訊息上按,或
  push 時 message_id 沒寫進去),不拋例外。

啟動
----
- 直接跑(背景):
  ``nohup /root/.hermes/hermes-agent/venv/bin/python3 \\
      pipeline/lib/discord_picks.py >> /var/log/discord_picks.log 2>&1 &``
- 或加 systemd unit(見 plan 註記)。
- 環境需求:DISCORD_BOT_TOKEN(同 discord_sender.py)、AIRTABLE_API_KEY
  (同 daily_digest.py)。

停
----
- SIGINT / SIGTERM 觸發 graceful close(client.close 後退出)。

退出碼
------
- 0  — 正常關機(SIGINT / SIGTERM)
- 1  — 啟動失敗(token 缺、intents 缺、其他 fatal)
"""
from __future__ import annotations

import asyncio
import importlib.util
import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from urllib import error, parse, request

# ---------------------------------------------------------------------------
# 路徑 + import(把 ``pipeline/lib`` 加進 sys.path 讓 daily_digest 共用函式
# 在需要時可 import;目前為獨立 daemon,只載入 ``_airtable_request`` 區塊。
# ---------------------------------------------------------------------------

_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parent.parent
sys.path.insert(0, str(_REPO))

logger = logging.getLogger("discord_picks")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s")
    )
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# 設定(Plan 3 規格)
# ---------------------------------------------------------------------------

# digest_candidates channel id(每日 06:30 CEST 推候選清單的 channel)
TARGET_CHANNEL_ID = 1539010288026779688

# emoji → DailyDigestPicks.picked(單選 yes / no / maybe)
EMOJI_TO_PICKED: Dict[str, str] = {
    "\u2705": "yes",   # ✅
    "\u274C": "no",    # ❌
    "\U0001F7E1": "maybe",  # �
}

# Airtable(同 daily_digest.py 的 BASE_ID / TABLE)
BASE_ID = "appHilorcrC5T0p2u"
DIGEST_TABLE = "DailyDigestPicks"


# ---------------------------------------------------------------------------
# Token 載入(跟 discord_sender.py 同來源,~/.hermes/.env 內 DISCORD_BOT_TOKEN)
# ---------------------------------------------------------------------------

_ENV_PATH = Path("/root/.hermes/.env")


def _load_bot_token() -> str:
    """讀 ``~/.hermes/.env`` 的 ``DISCORD_BOT_TOKEN=...``。"""
    if not _ENV_PATH.exists():
        raise RuntimeError(f"env file not found: {_ENV_PATH}")
    for raw in _ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("DISCORD_BOT_TOKEN="):
            return line.split("=", 1)[1].strip()
    raise RuntimeError("DISCORD_BOT_TOKEN not set in env file")


def _load_allowed_users() -> Optional[set]:
    """讀 ``DISCORD_ALLOWED_USERS=alice,bob``。沒設就 None(放行全部)。"""
    if not _ENV_PATH.exists():
        return None
    for raw in _ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line.startswith("DISCORD_ALLOWED_USERS="):
            val = line.split("=", 1)[1].strip()
            users = {u.strip() for u in val.split(",") if u.strip()}
            return users if users else None
    return None


# ---------------------------------------------------------------------------
# Airtable I/O(複製自 daily_digest.py,本檔獨立 daemon,不 cross-import)
# ---------------------------------------------------------------------------


class DailyDigestStoreError(RuntimeError):
    pass


def _airtable_request(
    method: str,
    path: str,
    *,
    body: Optional[Dict[str, Any]] = None,
    timeout: float = 30.0,
) -> Dict[str, Any]:
    token = os.environ.get("AIRTABLE_API_KEY", "")
    if not token:
        raise DailyDigestStoreError("AIRTABLE_API_KEY env var not set")
    url = f"https://api.airtable.com/v0{path}"
    data: Optional[bytes] = None
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = request.Request(url, data=data, method=method, headers=headers)
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            payload = resp.read().decode("utf-8")
            return json.loads(payload) if payload else {}
    except error.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace")
        raise DailyDigestStoreError(
            f"{method} {path} -> {e.code}: {err_body}"
        ) from e


def update_pick_by_message(message_id: str, fields: Dict[str, Any]) -> int:
    """用 ``message_id`` 找 DailyDigestPicks row 並 PATCH。回傳更新筆數。"""
    formula = f"{{message_id}}='{message_id}'"
    path = f"/{BASE_ID}/{parse.quote(DIGEST_TABLE, safe='')}"
    params = {"filterByFormula": formula}
    full_path = f"{path}?{parse.urlencode(params)}"
    resp = _airtable_request("GET", full_path)
    recs = resp.get("records", [])
    if not recs:
        return 0
    rec_id = recs[0]["id"]
    body = {"typecast": True, "fields": fields}
    patch_path = f"{path}/{rec_id}"
    _airtable_request("PATCH", patch_path, body=body)
    return 1


# ---------------------------------------------------------------------------
# Discord client
# ---------------------------------------------------------------------------

# discord.py 2.x lazy import(模組載入時就檢查)
try:
    import discord
except ImportError as e:
    sys.stderr.write(
        "FATAL: discord.py not installed. "
        "Install: /root/.hermes/hermes-agent/venv/bin/pip install discord.py\n"
    )
    raise


class PickBot(discord.Client):
    """訂閱 MESSAGE_REACTION_ADD,把 emoji 反應寫進 Airtable。"""

    def __init__(
        self,
        *,
        target_channel_id: int,
        allowed_users: Optional[set],
    ) -> None:
        # guild reactions 不需要 message_content intent
        intents = discord.Intents.default()
        intents.guilds = True
        intents.guild_reactions = True
        super().__init__(intents=intents)
        self.target_channel_id = target_channel_id
        self.allowed_users = allowed_users

    async def on_ready(self) -> None:
        logger.info(
            "logged in as %s (id=%s); watching channel=%s",
            self.user,
            self.user.id if self.user else "?",
            self.target_channel_id,
        )

    async def on_raw_reaction_add(
        self, payload: discord.RawReactionActionEvent
    ) -> None:
        # 只在意 add(MESSAGE_REACTION_ADD)
        if payload.event_type != "REACTION_ADD":
            return
        # 頻道過濾
        if payload.channel_id != self.target_channel_id:
            return
        # emoji 過濾
        emoji_name = payload.emoji.name
        picked = EMOJI_TO_PICKED.get(emoji_name)
        if picked is None:
            return  # 非 ✅/❌/🟡,略過
        # 使用者過濾
        if self.allowed_users is not None:
            user = payload.member or await self.fetch_user(payload.user_id)
            user_key = (user.name if user else str(payload.user_id)).lower()
            if user_key not in {u.lower() for u in self.allowed_users}:
                logger.warning(
                    "blocked reaction from non-allowed user id=%s name=%s",
                    payload.user_id,
                    user,
                )
                return
        # 寫 Airtable
        msg_id = str(payload.message_id)
        user_id = str(payload.user_id)
        picked_at = datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%S.000Z"
        )
        fields = {
            "picked": picked,
            "picked_at": picked_at,
            "discord_user_id": user_id,
        }
        try:
            n = update_pick_by_message(msg_id, fields)
        except DailyDigestStoreError as e:
            logger.error("airtable update failed (msg=%s): %s", msg_id, e)
            return
        if n == 0:
            logger.warning(
                "no DailyDigestPicks row for message_id=%s "
                "(digest 訊息太舊?或 daily_digest push 時沒寫進去?)",
                msg_id,
            )
            return
        logger.info(
            "recorded: msg=%s user=%s emoji=%s -> picked=%s",
            msg_id, user_id, emoji_name, picked,
        )


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


async def _run() -> int:
    token = _load_bot_token()
    allowed = _load_allowed_users()
    if allowed:
        logger.info("allowed_users filter active: %s", sorted(allowed))
    else:
        logger.info("no DISCORD_ALLOWED_USERS set; all reactions accepted")

    bot = PickBot(
        target_channel_id=TARGET_CHANNEL_ID,
        allowed_users=allowed,
    )

    # graceful shutdown: discord.py 2.x 在 close 會自動處理
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _stop(*_: Any) -> None:
        logger.info("shutdown signal received; closing...")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _stop)

    # 啟動 client
    runner = asyncio.create_task(bot.start(token, reconnect=True))
    await stop_event.wait()
    await bot.close()
    try:
        await runner
    except asyncio.CancelledError:
        pass
    return 0


def main() -> int:
    try:
        return asyncio.run(_run())
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
