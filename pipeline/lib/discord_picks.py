#!/usr/bin/env python3
"""Discord emoji reaction listener — Plan 4 被動模式 (2026-08-17)

用途
----
Plan 3 原本行為:``daily_digest.py`` 每日推送候選清單到 Discord channel,
使用者按 ✅ / ❌ / 🟡 emoji → daemon 寫進 Airtable ``DailyDigestPicks``。

Plan 4 (2026-08-17) 改成**完全被動**:

- 不再主動推 15 候選到 ``#挑文區``(Plan 3 的 digest_candidates channel)。
- User 在自己本來就會逛的 channel ``#每日頭條`` / ``#每日podcast`` /
  ``#每日reddit`` 看到任何訊息,直接按 ✅。
- 本 daemon 訂閱 ``MESSAGE_REACTION_ADD`` event,過濾出對應的 channel +
  ✅ emoji + allowlist user,把使用者按過的訊息寫進 Airtable 新表
  ``ReactionPicks``(取代 Plan 3 的 ``DailyDigestPicks``)。
- 寫進 ``ReactionPicks`` 的記錄會被 ``weekly_recap.py`` 週二/五 07:00
  CEST 拉出來,推摘要回 ``#挑文區`` 問 user 怎麼處理每則 ✅。

設計
----
- **長跑 daemon**(systemd 或 nohup),不是 cron 一次性 — reaction 隨時
  可能發生,daemon 需持續在線。
- 用 ``discord.py``(已裝)而不是裸 websocket:code 短、可讀,並由 library
  處理 heartbeat / reconnection / session resume。
- Token 來源:跟 ``immobilien-kb/tools/discord_sender.py`` 一致,從
  ``~/.hermes/.env`` 讀 ``DISCORD_BOT_TOKEN``(單一 bot 帳號)。
- Airtable 寫入:複製 ``daily_digest.py`` 的 ``_airtable_request`` +
  ``create_pick`` 邏輯,避免 cross-module refactor;若日後 reuse 量大
  再抽到 ``pipeline/lib`` 共用 module。
- 過濾條件(Plan 4):
  - channel id ∈ ``LISTEN_CHANNELS``(3 個 user 逛的 channel)
  - emoji name == ``PICKED_EMOJI``(只收 ✅)
  - user id ∈ ``ALLOWED_USERS``(讀 ``DISCORD_ALLOWED_USERS``)
- channel → message_kind 自動判定(``CHANNEL_KIND_MAP``)。
- ``reaction_id`` 為 dedup key,格式
  ``f"{user_id}-{channel_id}-{message_id}-{emoji}"``,確保 retry / 重啟
  不會重複寫入。

啟動
----
- 直接跑(背景):
  ``nohup /root/.hermes/hermes-agent/venv/bin/python3 \\
      pipeline/lib/discord_picks.py >> /var/log/discord_picks.log 2>&1 &``
- 或加 systemd unit(見 plan 註記)。
- 環境需求:
  - ``DISCORD_BOT_TOKEN``(同 ``discord_sender.py``)
  - ``DISCORD_ALLOWED_USERS``(逗號分隔)
  - ``AIRTABLE_API_KEY``
  - ``AIRTABLE_REACTION_PICKS_TABLE_ID``(由
    ``setup_airtable_reaction_picks.py`` 設定)

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
import json
import logging
import os
import re
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from urllib import error, parse, request

import requests

# ---------------------------------------------------------------------------
# 路徑 + import
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
# 設定(Plan 4 spec 2026-08-17)
# ---------------------------------------------------------------------------

# Plan 4 監聽 3 個 channel:user 逛的 #每日頭條 / #每日podcast / #每日reddit
LISTEN_CHANNELS: List[str] = [
    "1520791894995501106",  # 每日頭條
    "1535461574460968960",  # 每日podcast
    "1537907956132089976",  # 每日reddit
]

# channel_id → message_kind(單獨欄位存 Airtable 給 weekly_recap 用)
CHANNEL_KIND_MAP: Dict[str, str] = {
    "1520791894995501106": "news",
    "1535461574460968960": "podcast",
    "1537907956132089976": "reddit",
}

# 頻道 ID → 人類可讀名稱(寫進 Airtable channel_name 欄位)
CHANNEL_NAME_MAP: Dict[str, str] = {
    "1520791894995501106": "每日頭條",
    "1535461574460968960": "每日podcast",
    "1537907956132089976": "每日reddit",
}

# Plan 4 只收 ✅(Plan 3 是 ✅/❌/🟡 三種,但 Plan 4 �/🟡 改為 weekly recap
# 自己處理 — daemon 這層只挑 ✅)
PICKED_EMOJI: str = "\u2705"  # ✅

# 訊息內容截斷長度(避免 Airtable multilineText 爆 + Discord 400)
TITLE_MAX = 200
SNIPPET_MAX = 500

# Airtable(同 daily_digest.py 的 BASE_ID;TABLE id 從 env 讀,Plan 4 新表)
BASE_ID = "appHilorcrC5T0p2u"
DEFAULT_REACTION_TABLE = "ReactionPicks"

# Discord guild id(從 token 反查不到;從 env 讀 fallback 寫 message_url)
DEFAULT_GUILD_ID = "1306402830617616485"  # immobilien-kb Discord guild


# ---------------------------------------------------------------------------
# Token / Config 載入(跟 discord_sender.py 同來源)
# ---------------------------------------------------------------------------

_ENV_PATH = Path("/root/.hermes/.env")


def _load_env_value(key: str) -> Optional[str]:
    """讀 ``~/.hermes/.env`` 的 ``KEY=value`` 設定。

    優先順序: ``os.environ[key]`` > ``~/.hermes/.env`` 內容。
    這讓 systemd / nohup / 子 shell export 的 env var 可以覆寫 .env 預設值
    (例如 daemon 啟動時可以 override `DISCORD_ALLOWED_USERS` 把 username 換
    成 user_id,而不必修改 protected .env)。
    """
    val = os.environ.get(key)
    if val:
        return val
    if not _ENV_PATH.exists():
        return None
    for raw in _ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith(f"{key}="):
            return line.split("=", 1)[1].strip()
    return None


def _load_bot_token() -> str:
    """讀 ``~/.hermes/.env`` 的 ``DISCORD_BOT_TOKEN=...``。"""
    token = _load_env_value("DISCORD_BOT_TOKEN")
    if not token:
        raise RuntimeError(
            f"DISCORD_BOT_TOKEN not set in {_ENV_PATH}"
        )
    return token


def _load_allowed_users() -> Set[str]:
    """讀 ``DISCORD_ALLOWED_USERS=alice,bob``。

    支援兩種格式:
    1. 直接 user_id(像 ``918464660024459304``)— 立刻收
    2. username(像 ``smilefacetao``)— 用 bot token + Discord API
       查 guild member,resolve 成 user_id

    Plan 4 daemon 強制要求設好(不像 Plan 3 沒設就放行全部) — 防止 bot 收到
    任何 user 在 3 個 channel 按 ✅ 都默默寫進 Airtable。
    """
    val = _load_env_value("DISCORD_ALLOWED_USERS")
    if not val:
        logger.warning(
            "DISCORD_ALLOWED_USERS not set; daemon will reject ALL reactions "
            "(safety default — set DISCORD_ALLOWED_USERS in %s to enable)",
            _ENV_PATH,
        )
        return set()
    raw = [u.strip() for u in val.split(",") if u.strip()]
    # 全部都已是 user_id(>=17 位純數字)?
    if all(re.fullmatch(r"\d{17,20}", x) for x in raw):
        return set(raw)

    # 否則試著 resolve username → user_id
    token = _load_env_value("DISCORD_BOT_TOKEN")
    guild_id = _load_guild_id()
    if not token or not guild_id:
        logger.warning(
            "DISCORD_ALLOWED_USERS contains username(s) but DISCORD_BOT_TOKEN or "
            "DISCORD_GUILD_ID missing — cannot resolve. Using raw values "
            "(reactions will likely be blocked)."
        )
        return set(raw)

    resolved: set[str] = set()
    for name in raw:
        if re.fullmatch(r"\d{17,20}", name):
            resolved.add(name)
            continue
        try:
            url = (
                f"https://discord.com/api/v10/guilds/{guild_id}/members/search"
                f"?query={name}&limit=5"
            )
            r = requests.get(
                url, headers={"Authorization": f"Bot {token}"}, timeout=10
            )
            if r.status_code != 200:
                logger.warning(
                    "username resolve HTTP %s for '%s' — keeping raw", r.status_code, name
                )
                resolved.add(name)
                continue
            for m in r.json():
                u = m.get("user", {})
                if u.get("username") == name or u.get("global_name") == name:
                    resolved.add(str(u["id"]))
                    break
            else:
                logger.warning(
                    "username '%s' not found in guild %s — keeping raw", name, guild_id
                )
                resolved.add(name)
        except Exception as e:
            logger.warning("username resolve failed for '%s': %s", name, e)
            resolved.add(name)
    logger.info(
        "allowed_users resolved: %s → %s", ", ".join(sorted(raw)), ", ".join(sorted(resolved))
    )
    return resolved


def _load_reaction_table_id() -> str:
    """讀 ``AIRTABLE_REACTION_PICKS_TABLE_ID=tbl...``。"""
    val = _load_env_value("AIRTABLE_REACTION_PICKS_TABLE_ID")
    if not val:
        # Fallback 預設名稱;真正建立請用 setup_airtable_reaction_picks.py
        logger.warning(
            "AIRTABLE_REACTION_PICKS_TABLE_ID not set; using default name '%s'. "
            "Set the env var after running setup_airtable_reaction_picks.py.",
            DEFAULT_REACTION_TABLE,
        )
        return DEFAULT_REACTION_TABLE
    return val


def _load_guild_id() -> str:
    """讀 ``DISCORD_GUILD_ID=...``(寫 message_url 用)。"""
    val = _load_env_value("DISCORD_GUILD_ID")
    return val or DEFAULT_GUILD_ID


# ---------------------------------------------------------------------------
from pipeline.lib.reaction_store import (  # noqa: E402
    ReactionStore,
    ReactionStoreError,
    get_reaction_store,
)

_REACTION_STORE: Optional[ReactionStore] = None


def _get_reaction_store() -> ReactionStore:
    global _REACTION_STORE
    if _REACTION_STORE is None:
        _REACTION_STORE = get_reaction_store()
    return _REACTION_STORE


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


def _extract_content(message: discord.Message) -> Dict[str, Any]:
    """從 Discord message 解析出 title / snippet / embed_url / embed_image。

    Plan 4 spec:
    - 有 embed:用 ``embed.title`` + ``embed.description`` + ``embed.url``
      + ``embed.image.url``
    - 無 embed:用 ``message.content`` 前 TITLE_MAX / SNIPPET_MAX 字

    任一欄位缺 fallback 到另一個來源,確保寫進 Airtable 的 4 個欄位都有
    東西(避免 weekly_recap render 變空)。
    """
    title = ""
    snippet = ""
    embed_url: Optional[str] = None
    embed_image: Optional[str] = None

    content = message.content or ""
    content_first_line = content.split("\n", 1)[0] if content else ""

    if message.embeds:
        e = message.embeds[0]
        # title: embed.title → 第一行 content
        title = (e.title or content_first_line or "(無標題)")[:TITLE_MAX]
        # description: embed.description → content 前 500 字
        if e.description:
            snippet = e.description[:SNIPPET_MAX]
        elif content:
            snippet = content[:SNIPPET_MAX]
        else:
            snippet = ""
        # embed url: e.url(若是 article embed 通常有)
        embed_url = getattr(e, "url", None)
        # embed image: e.image.url
        if getattr(e, "image", None) and getattr(e.image, "url", None):
            embed_image = e.image.url
        elif getattr(e, "thumbnail", None) and getattr(e.thumbnail, "url", None):
            embed_image = e.thumbnail.url
    else:
        title = (content_first_line or "(無標題)")[:TITLE_MAX]
        snippet = content[:SNIPPET_MAX]

    return {
        "title": title,
        "snippet": snippet,
        "embed_url": embed_url,
        "embed_image": embed_image,
    }


def _build_message_url(guild_id: str, channel_id: int, message_id: int) -> str:
    return f"https://discord.com/channels/{guild_id}/{channel_id}/{message_id}"


def _iso_now_utc() -> str:
    """UTC ISO 字串(給 Airtable dateTime 用,daemon 端寫入用 UTC)。

    Airtable dateTime 設 ``timeZone: Europe/Berlin``,我們直接傳 ISO + Z,
    Airtable 會自動轉成 Europe/Berlin 顯示。
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


class PickBot(discord.Client):
    """Plan 4 被動 reaction listener。

    訂閱 ``MESSAGE_REACTION_ADD``,過濾:
    - channel ∈ LISTEN_CHANNELS
    - emoji == PICKED_EMOJI
    - user ∈ ALLOWED_USERS

    通過後抓 message 內容 → 寫進 ReactionPicks(用 ``reaction_id`` dedup)。
    """

    def __init__(
        self,
        *,
        listen_channels: Set[str],
        channel_kind_map: Dict[str, str],
        channel_name_map: Dict[str, str],
        picked_emoji: str,
        allowed_users: Set[str],
        guild_id: str,
    ) -> None:
        # guild reactions 不需要 message_content intent
        intents = discord.Intents.default()
        intents.guilds = True
        intents.guild_reactions = True
        super().__init__(intents=intents)
        self.listen_channels = listen_channels
        self.channel_kind_map = channel_kind_map
        self.channel_name_map = channel_name_map
        self.picked_emoji = picked_emoji
        self.allowed_users = allowed_users
        self.guild_id = guild_id

    async def on_ready(self) -> None:
        ch_list = ", ".join(sorted(self.listen_channels))
        logger.info(
            "logged in as %s (id=%s); watching channels=[%s]; emoji=%s; "
            "allowed_users=%d",
            self.user,
            self.user.id if self.user else "?",
            ch_list,
            self.picked_emoji,
            len(self.allowed_users),
        )

    async def on_raw_reaction_add(
        self, payload: discord.RawReactionActionEvent
    ) -> None:
        # 只在意 add(MESSAGE_REACTION_ADD)
        if payload.event_type != "REACTION_ADD":
            return

        channel_id_str = str(payload.channel_id)
        # 1. channel 過濾
        if channel_id_str not in self.listen_channels:
            return

        # 2. emoji 過濾(只收 ✅)
        emoji_name = payload.emoji.name
        if emoji_name != self.picked_emoji:
            return

        # 3. user 過濾
        user_id = str(payload.user_id)
        if not self.allowed_users:
            logger.warning(
                "reaction blocked (no DISCORD_ALLOWED_USERS): user=%s channel=%s",
                user_id, channel_id_str,
            )
            return
        if user_id not in self.allowed_users:
            # Plan 4 user 過濾是 user_id 字串比對(不要 fallback 到 name — 防止同名 user)
            logger.warning(
                "reaction blocked (user not in allowlist): user=%s channel=%s",
                user_id, channel_id_str,
            )
            return

        # 4. dedup key(同一 user / channel / message / emoji 重試不重複寫)
        reaction_id = f"{user_id}-{channel_id_str}-{payload.message_id}-{emoji_name}"
        try:
            existing = _get_reaction_store().find_by_reaction_id(reaction_id)
        except ReactionStoreError as e:
            logger.error("find_reaction_pick failed (%s): %s", reaction_id, e)
            return
        if existing:
            logger.info(
                "skip (already recorded): reaction_id=%s rec=%s",
                reaction_id, existing,
            )
            return

        # 5. 抓 message 完整內容
        try:
            channel = self.get_channel(payload.channel_id) or await self.fetch_channel(
                payload.channel_id
            )
            # 限定「可 fetch_message 的 channel type」(TextChannel /
            # Thread / VoiceChannel 等)— ForumChannel / CategoryChannel /
            # PrivateChannel 沒有 fetch_message,要 fallback 到別的方法
            # 或直接略過(這裡直接 log warning 跳過,因為我們的 LISTEN_CHANNELS
            # 都是 text channel)。
            if not hasattr(channel, "fetch_message"):
                logger.warning(
                    "channel type=%s does not support fetch_message; skip "
                    "(channel=%s message=%s)",
                    type(channel).__name__, channel_id_str, payload.message_id,
                )
                return
            message = await channel.fetch_message(payload.message_id)
        except (discord.NotFound, discord.HTTPException) as e:
            logger.warning(
                "fetch_message failed (msg may be deleted or no access): "
                "channel=%s message=%s err=%s",
                channel_id_str, payload.message_id, e,
            )
            return

        # 6. 解析 embed / content
        extracted = _extract_content(message)
        title = extracted["title"]
        snippet = extracted["snippet"]
        embed_url = extracted["embed_url"]
        embed_image = extracted["embed_image"]

        # 7. channel → kind / name
        kind = self.channel_kind_map.get(channel_id_str, "other")
        name = self.channel_name_map.get(channel_id_str, f"channel-{channel_id_str}")
        message_url = _build_message_url(
            self.guild_id, payload.channel_id, payload.message_id
        )

        # 8. 寫 Airtable
        fields: Dict[str, Any] = {
            "reaction_date": _iso_now_utc(),
            "channel_id": channel_id_str,
            "channel_name": name,
            "message_id": str(payload.message_id),
            "message_url": message_url,
            "message_kind": kind,
            "title": title,
            "snippet": snippet,
            "discord_user_id": user_id,
            "reaction_id": reaction_id,
        }
        # 兩個 url 欄位只在有值時寫(null 在 Airtable 視為空字串,
        # 寫空 url 會被 typecast 拒絕 → 用 None 跳過)
        if embed_url:
            fields["embed_url"] = embed_url
        if embed_image:
            fields["embed_image"] = embed_image

        try:
            rec_id = _get_reaction_store().create_reaction_pick(fields)
        except ReactionStoreError as e:
            err = str(e)
            # 422 通常是 reaction_id 已存在(極短時間內兩次 reaction)。
            # 或者是欄位值不合 — 都 log 詳細,不拋例外,讓 daemon 繼續跑。
            logger.error(
                "create_reaction_pick failed (msg=%s user=%s): %s",
                payload.message_id, user_id, err,
            )
            return

        logger.info(
            "recorded: reaction_id=%s kind=%s user=%s msg=%s title=%s rec=%s",
            reaction_id, kind, user_id, payload.message_id,
            (title[:50] + "...") if len(title) > 50 else title,
            rec_id,
        )


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

async def _run() -> int:
    _get_reaction_store()

    token = _load_bot_token()
    allowed = _load_allowed_users()
    guild_id = _load_guild_id()

    if allowed:
        logger.info(
            "allowed_users filter active: %d users", len(allowed),
        )
    else:
        logger.warning(
            "no DISCORD_ALLOWED_USERS set; daemon will REJECT all reactions "
            "(set DISCORD_ALLOWED_USERS in %s to enable listening)",
            _ENV_PATH,
        )

    logger.info(
        "Firestore reactions collection=%s",
        os.environ.get("FIRESTORE_REACTIONS_COLLECTION", "reactions"),
    )
    logger.info(
        "Listen channels: %s (Plan 4 spec — #每日頭條 / #每日podcast / #每日reddit)",
        LISTEN_CHANNELS,
    )

    bot = PickBot(
        listen_channels=set(LISTEN_CHANNELS),
        channel_kind_map=CHANNEL_KIND_MAP,
        channel_name_map=CHANNEL_NAME_MAP,
        picked_emoji=PICKED_EMOJI,
        allowed_users=allowed,
        guild_id=guild_id,
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
