#!/usr/bin/env python3
"""Daily Digest — Plan 3 (2026-08-17)

跨 pipeline 統一產出「昨日候選清單」推到 Discord ``digest_candidates``
頻道,使用者用 emoji reaction(✅/❌/🟡)挑選 → 由 ``discord_picks.py``
回寫 Airtable ``DailyDigestPicks``。

設計
----
- 來源:Airtable ``ProcessedContent`` 過去 24-48 小時寫入的 record
  (``ProcessedStore.get_recent`` 對每個 source_type 各 call 一次,
  再合併 + source quota 5 / 類型)。
- 排序優先序:``paywall_preview_kept DESC``(Plan 1 救回來的優先),
  再 ``processed_at DESC``(Plan 3 spec 寫的 ``quality_score DESC``
  在 ProcessedContent 不存在,所以退回 ``processed_at``)。
- 每日預估 8-15 篇,header 訊息 + N 則候選(每則獨立 message)。
- 候選推到 Discord → 同步寫進 ``DailyDigestPicks``(message_id 留空,
  等 ``discord_picks.py`` 在 user 按 reaction 時補上)。

CLI
----
- (無參數):跑正常 push(header + 候選 + 寫 Airtable)。
- ``--dry-run``:只 print 訊息內容,不推 Discord,不寫 Airtable。
- ``--test-pick MESSAGE_ID EMOJI``:模擬一次 emoji reaction —
  在 ``DailyDigestPicks`` 找對應 message_id 的 row 寫入
  ``picked / picked_at / discord_user_id``。測試流程用。
- ``--days N``:覆寫預設 2 天的回看窗口。
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib import error, parse, request

# 讓 ``daily_digest.py`` 直接被跑時找得到 ``lib``
_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parent.parent
sys.path.insert(0, str(_REPO))
sys.path.insert(0, str(_REPO / "pipeline"))

from pipeline.lib.processed_store import (  # noqa: E402
    ProcessedStore,
    ProcessedStoreError,
)

logger = logging.getLogger("daily_digest")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s")
    )
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# 設定
# ---------------------------------------------------------------------------

BASE_ID = "appHilorcrC5T0p2u"
PROCESSED_TABLE = "ProcessedContent"
DIGEST_TABLE = "DailyDigestPicks"

# Discord
DISCORD_DIGEST_CHANNEL_ALIAS = "digest_candidates"
DISCORD_DIGEST_CHANNEL_ID = "1539010288026779688"

# Quota
SOURCE_QUOTA = 5  # 每個 source_type 最多推幾篇

# 預設回看窗口(昨日 = 過去 2 天內)
DEFAULT_DAYS = 2
DEFAULT_LIMIT = 100

# 候選推送的 emoji 對應(給 header 訊息顯示用)
REACTION_LEGEND = "✅ = 想寫 · ❌ = 跳過 · 🟡 = 之後再看"

# Vault root for fallback scanning (used when ProcessedContent has 0 rows
# for a source_type within the lookback window — typically the first day
# of a new pipeline, or after a backfill gap).
VAULT_ROOT = _REPO / "immobilien-kb" / "vault"

# Filename regex used to parse Reddit vault markdown filenames:
#   ``r_<subreddit>-<title-slug>.md``
_VAULT_REDDIT_RE = re.compile(r"^r_([^-]+)-(.+)$")
# Frontmatter-style URL extraction for Reddit files:
#   ``**URL**: https://old.reddit.com/r/...``
_VAULT_URL_RE = re.compile(r"\*\*URL\*\*:\s*(https?://\S+)")
# YAML frontmatter parser for YouTube transcripts:
#   ``video_id: ...``, ``channel: ...``, ``fetched_at: ...``
_YAML_FRONT_RE = re.compile(r"^---\n(.*?)\n---\n", re.DOTALL)

# article_type → content_kind(Plan 3 DailyDigestPicks.content_kind 欄位)
# ProcessedContent.article_type 是 short-summary / long-form / pending-long-form /
# skipped-long-form / stat-table。Plan 3 DailyDigestPicks.content_kind 是
# short-summary / longform / paywall-preview / short-paywall-preview /
# full-article — 兩邊 enum 不一致,這裡做保守 mapping:
#   - long-form      → longform
#   - short-summary  → short-summary
#   - stat-table     → full-article(destatis 整份 CSV/table 視為 full)
#   - 其餘           → short-summary(安全降級)
#   - paywall-preview_kept=True 時:用 paywall_preview_kind 直接覆寫
_ARTICLE_TYPE_TO_CONTENT_KIND = {
    "long-form": "longform",
    "short-summary": "short-summary",
    "stat-table": "full-article",
    "pending-long-form": "short-summary",
    "skipped-long-form": "short-summary",
}


# ---------------------------------------------------------------------------
# Airtable 直接 I/O(寫 DailyDigestPicks 用 — 沒有現成 store 類別)
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


def create_pick(record_fields: Dict[str, Any]) -> str:
    """寫一筆 DailyDigestPicks record。回傳 record id。

    Airtable POST /<base>/<table> with a single ``fields`` body returns
    ``{"id": "...", "fields": {...}}`` at the top level (not wrapped in a
    ``records`` array — that's only the batch shape). We accept both.
    """
    body = {"typecast": True, "fields": record_fields}
    path = f"/{BASE_ID}/{parse.quote(DIGEST_TABLE, safe='')}"
    resp = _airtable_request("POST", path, body=body)
    recs = resp.get("records") or []
    if recs:
        return recs[0]["id"]
    # Single-record response shape.
    if resp.get("id"):
        return resp["id"]
    raise DailyDigestStoreError(f"create returned no records: {resp}")


def update_pick_by_message(message_id: str, fields: Dict[str, Any]) -> int:
    """用 ``message_id`` 找到 DailyDigestPicks 對應 record 並 PATCH。

    ``message_id`` 是 digest 推送後 user 按 emoji 對應的 Discord message id。
    """
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
# Vault folder fallback (when ProcessedContent is empty for a source_type)
# ---------------------------------------------------------------------------
#
# Plan 3 Part 2 (2026-08-17): when ``store.get_recent`` returns 0 rows for a
# source_type inside the lookback window (e.g. the day after a new pipeline
# starts writing vault files but before ``mark_processed`` runs, or after a
# backfill that has not yet landed), fall back to scanning the on-disk
# vault. The vault files are the ground truth for "what we have right now";
# Airtable is the dedup ledger.
#
# Each fallback builder returns a list of Airtable-shaped record dicts that
# can be merged into the pool. ``source_hash`` is synthesised from the
# vault path so dedup by hash still works when a record IS later written
# via ``mark_processed``.

def _synth_record(
    *,
    source_type: str,
    source_id: str,
    source_hash: str,
    title: str,
    url: str,
    vault_path: str,
    channels: List[str],
    day: str,
    article_type: str = "short-summary",
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a dict that mimics the shape of an Airtable ProcessedContent
    record so downstream ``_source_label`` / ``render_candidate`` /
    ``derive_content_kind`` work unchanged."""
    return {
        "id": f"vault:{source_id}",
        "fields": {
            "source_type": source_type,
            "source_id": source_id,
            "source_hash": source_hash,
            "title": title,
            "processed_at": f"{day}T00:00:00.000Z",
            "output_path": vault_path,
            "channels": channels,
            "article_type": article_type,
            "paywall_preview_kept": False,
            "paywall_preview_kind": "",
            "metadata": json.dumps(metadata or {"url": url}, ensure_ascii=False),
        },
    }


def _vault_reddit_fallback(days: int) -> List[Dict[str, Any]]:
    """Scan ``vault/Reddit/YYYY-MM-DD/*.md`` for the lookback window.

    Filename pattern: ``r_<subreddit>-<title-slug>.md``. URL comes from
    the ``**URL**:`` frontmatter-style line. ``source_hash`` is the
    canonical ``make_hash('reddit', 'vault:<stem>')`` — matches what the
    backfill script writes so that a subsequent ``mark_processed`` will
    PATCH the same record (not create a duplicate).
    """
    from datetime import date, timedelta

    from pipeline.lib.processed_store import make_hash

    items: List[Dict[str, Any]] = []
    today = date.today()
    base = VAULT_ROOT / "Reddit"
    if not base.exists():
        return items
    for d in range(days):
        day = (today - timedelta(days=d)).isoformat()
        day_dir = base / day
        if not day_dir.exists():
            continue
        for f in sorted(day_dir.glob("*.md")):
            if f.name == "_index.md":
                continue
            m = _VAULT_REDDIT_RE.match(f.stem)
            if not m:
                continue
            subreddit = m.group(1)
            title_slug = m.group(2).replace("-", " ").strip()
            if not title_slug:
                continue
            try:
                content = f.read_text(encoding="utf-8")
            except OSError:
                continue
            url_m = _VAULT_URL_RE.search(content)
            url = url_m.group(1) if url_m else ""
            source_id = f"vault:{f.stem}"
            items.append(
                _synth_record(
                    source_type="reddit",
                    source_id=source_id,
                    source_hash=make_hash("reddit", source_id),
                    title=title_slug,
                    url=url,
                    vault_path=str(f),
                    channels=[f"reddit.r_{subreddit}"],
                    day=day,
                    article_type="short-summary",
                    metadata={
                        "subreddit": subreddit,
                        "url": url,
                        "vault_only": True,
                    },
                )
            )
    return items


def _vault_youtube_fallback(days: int) -> List[Dict[str, Any]]:
    """Scan ``vault/YouTube/<Channel>/_transcripts/*.md`` for the lookback window.

    Pulls ``video_id``, ``channel`` from the YAML frontmatter. ``fetched_at``
    is used to compute the "day" bucket. ``source_id`` is
    ``vault:youtube:<video_id>``; ``url`` is the standard watch URL.
    """
    from datetime import date, timedelta, datetime as _dt

    from pipeline.lib.processed_store import make_hash

    items: List[Dict[str, Any]] = []
    base = VAULT_ROOT / "YouTube"
    if not base.exists():
        return items
    today = date.today()
    cutoff = today - timedelta(days=days)
    for channel_dir in sorted(base.iterdir()):
        if not channel_dir.is_dir():
            continue
        transcripts = channel_dir / "_transcripts"
        if not transcripts.exists():
            continue
        channel_name = channel_dir.name.replace("__", ".").replace("_", " ")
        for f in sorted(transcripts.glob("*.md")):
            try:
                content = f.read_text(encoding="utf-8")
            except OSError:
                continue
            yfm = _YAML_FRONT_RE.match(content)
            if not yfm:
                continue
            yaml_body = yfm.group(1)
            video_id = ""
            fetched_at = ""
            for line in yaml_body.splitlines():
                if line.startswith("video_id:"):
                    video_id = line.split(":", 1)[1].strip()
                elif line.startswith("fetched_at:"):
                    fetched_at = line.split(":", 1)[1].strip()
            if not video_id:
                continue
            # Day bucket: derive from fetched_at if available, else file mtime
            day = ""
            if fetched_at:
                try:
                    day = _dt.fromisoformat(fetched_at.replace("Z", "+00:00")).date().isoformat()
                except ValueError:
                    day = ""
            if not day:
                try:
                    mtime = date.fromtimestamp(f.stat().st_mtime)
                    day = mtime.isoformat()
                except OSError:
                    continue
            if _dt.fromisoformat(day).date() < cutoff:
                continue
            url = f"https://www.youtube.com/watch?v={video_id}"
            # First line of the body (after frontmatter) is the title — we
            # do a best-effort pull by looking for the first markdown H1.
            body = content[yfm.end():]
            title = ""
            for line in body.splitlines():
                line = line.strip()
                if line.startswith("# "):
                    title = line[2:].strip()
                    break
            if not title:
                title = video_id
            source_id = f"vault:youtube:{video_id}"
            items.append(
                _synth_record(
                    source_type="youtube",
                    source_id=source_id,
                    source_hash=make_hash("youtube", source_id),
                    title=title,
                    url=url,
                    vault_path=str(f),
                    channels=[f"youtube.{channel_dir.name}"],
                    day=day,
                    article_type="long-form",
                    metadata={
                        "channel": channel_name,
                        "video_id": video_id,
                        "url": url,
                        "vault_only": True,
                    },
                )
            )
    return items


_VAULT_FALLBACKS = {
    "reddit": _vault_reddit_fallback,
    "youtube": _vault_youtube_fallback,
}


# ---------------------------------------------------------------------------
# 候選資料挑選
# ---------------------------------------------------------------------------

def fetch_candidates(
    store: ProcessedStore,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    source_types: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """從 ProcessedContent 讀取過去 N 天所有 source 的 record。

    ProcessedStore.get_recent 對 ``source_type`` 是必填,所以這裡
    對每個 source 各 call 一次,合併 + dedup by ``source_hash``。

    Plan 3 Part 2 (2026-08-17): 如果某 source_type 在 ProcessedContent
    撈出 0 筆,fallback 掃 vault folder(Reddit / YouTube)。Synth record
    的 ``source_hash`` 跟 ``backfill_reddit_to_airtable.py`` 寫進去的
    一致,所以即使 vault 是 ground truth、後續 mark_processed 跑下去
    也不會產生 duplicate row。
    """
    if source_types is None:
        source_types = ["news", "youtube", "reddit"]

    seen: Dict[str, Dict[str, Any]] = {}
    per_source_count: Dict[str, int] = {}
    for st in source_types:
        try:
            rows = store.get_recent(st, days=days, limit=limit)
        except ProcessedStoreError as e:
            logger.warning("get_recent(%s) failed: %s", st, e)
            rows = []
        per_source_count[st] = len(rows)
        for r in rows:
            h = r.get("fields", {}).get("source_hash", "")
            if h and h not in seen:
                seen[h] = r

    # Vault fallback: only fill source_types that came back empty from
    # Airtable AND have a registered fallback builder.
    fallback_added = 0
    for st, count in per_source_count.items():
        if count > 0:
            continue
        builder = _VAULT_FALLBACKS.get(st)
        if builder is None:
            continue
        try:
            synth = builder(days)
        except Exception as e:  # defensive — never let a fallback break the digest
            logger.warning("vault fallback %s failed: %s", st, e)
            continue
        added_for_st = 0
        for r in synth:
            h = r.get("fields", {}).get("source_hash", "")
            if h and h not in seen:
                seen[h] = r
                added_for_st += 1
        if added_for_st:
            logger.info(
                "vault fallback: %s -> +%d synth records (ProcessedContent was empty)",
                st,
                added_for_st,
            )
            fallback_added += added_for_st

    if fallback_added:
        logger.info(
            "fetch_candidates total=%d (incl. %d vault-fallback synth records)",
            len(seen),
            fallback_added,
        )
    return list(seen.values())


def apply_quota(
    candidates: List[Dict[str, Any]], quota: int = SOURCE_QUOTA
) -> List[Dict[str, Any]]:
    """每個 source_type 最多 quota 筆。

    先依 (paywall_preview_kept DESC, processed_at DESC) 排序再分組。
    """
    def sort_key(r: Dict[str, Any]) -> tuple:
        f = r.get("fields", {})
        ppk = bool(f.get("paywall_preview_kept"))
        # processed_at 是 ISO str,可直接當字串比遞減
        ts = f.get("processed_at", "")
        return (0 if ppk else 1, ts)

    sorted_recs = sorted(candidates, key=sort_key, reverse=False)
    # tuple 第一項是 0/1,reverse=False 讓 0(paywall_kept)排前面 — 對
    # 第二項(processed_at)字串 reverse=True 則會顛倒,改成手動:
    sorted_recs = sorted(
        candidates, key=lambda r: (
            0 if r.get("fields", {}).get("paywall_preview_kept") else 1,
            r.get("fields", {}).get("processed_at", ""),
        ), reverse=False)
    # 上面這版兩段排序都是 asc,所以 paywall_kept=False 排後面、
    # 同 group 內 processed_at 較早者排前面 — 顛倒。修成單一 sorted:
    def key(r: Dict[str, Any]) -> tuple:
        f = r.get("fields", {})
        ppk = 1 if f.get("paywall_preview_kept") else 0
        ts = f.get("processed_at", "")
        return (ppk, ts)

    sorted_recs = sorted(candidates, key=key, reverse=True)

    out: List[Dict[str, Any]] = []
    per_source: Dict[str, int] = {}
    for r in sorted_recs:
        st = r.get("fields", {}).get("source_type", "")
        if per_source.get(st, 0) >= quota:
            continue
        per_source[st] = per_source.get(st, 0) + 1
        out.append(r)
    return out


# ---------------------------------------------------------------------------
# 訊息格式
# ---------------------------------------------------------------------------

def _source_label(record: Dict[str, Any]) -> str:
    """新聞 · Heise / YouTube · Mr. Steuer / Reddit · r/Hausbau。"""
    f = record.get("fields", {})
    st = f.get("source_type", "?")
    channels = f.get("channels") or []
    name = ""
    if channels:
        # channels 通常是 "youtube.ex_makler" / "news.daily_top3" / "reddit.r_immobilien"
        ch = channels[0]
        if "." in ch:
            name = ch.split(".", 1)[1]
        else:
            name = ch
    type_zh = {
        "news": "新聞",
        "youtube": "YouTube",
        "reddit": "Reddit",
        "podcast": "Podcast",
    }.get(st, st)
    return f"{type_zh} · {name}" if name else type_zh


def render_header(n_candidates: int, n_total_pool: int, digest_date: str) -> str:
    return (
        f"📋 **每日候選 — {digest_date}**\n"
        f"共 {n_candidates} 篇,從昨日 {n_total_pool} 篇 ProcessedContent 挑出。\n"
        f"\n"
        f"• ✅ = 想寫這篇\n"
        f"• ❌ = 跳過\n"
        f"• 🟡 = 之後再看\n"
        f"\n"
        f"{n_candidates} 則訊息如下,各自按 emoji 反應。"
    )


def render_candidate(
    idx: int, total: int, record: Dict[str, Any], digest_date: str
) -> str:
    f = record.get("fields", {})
    label = _source_label(record)
    title = f.get("title", "(無標題)")
    url = f.get("source_id", "")  # 用 source_id 當顯示 — 真 URL 在 metadata
    # 真 URL:Plan 3 spec 範例顯示完整 https://...。試著從 metadata JSON 拿。
    meta_raw = f.get("metadata", "")
    real_url = ""
    if isinstance(meta_raw, str) and meta_raw.strip():
        try:
            meta = json.loads(meta_raw)
            real_url = meta.get("url") or meta.get("source_url") or ""
        except Exception:
            real_url = ""
    url_line = real_url if real_url else f.get("source_id", "")
    vault_path = f.get("output_path", "")
    ppk = f.get("paywall_preview_kept")
    ppk_marker = " 💰(paywall preview)" if ppk else ""

    return (
        f"[{idx}/{total}] [{label}] {title}{ppk_marker}\n"
        f"\n"
        f"來源: {url_line}\n"
        f"Vault: {vault_path or '(尚未寫入)'}\n"
        f"Push time: 06:30 CEST · Digest date: {digest_date}\n"
        f"\n"
        f"✅  ❌  🟡  ← 按 emoji 標記"
    )


# ---------------------------------------------------------------------------
# Discord push
# ---------------------------------------------------------------------------

def _import_discord_sender():
    """``immobilien-kb/tools/discord_sender.py`` 帶 dash,需要 importlib。"""
    import importlib.util

    sender_path = (
        Path(__file__).resolve().parents[2]
        / "immobilien-kb"
        / "tools"
        / "discord_sender.py"
    )
    spec = importlib.util.spec_from_file_location(
        "_discord_sender_runtime", str(sender_path)
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load discord_sender from {sender_path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def push_to_discord(text: str) -> Dict[str, Any]:
    """Push one message to the digest channel with a small retry loop.

    Discord webhook sends return ``{"ok": True, "message_ids": [...]}``
    on success and ``{"ok": False, "error": ...}`` on failure (including
    HTTP 429 rate-limit responses). We retry up to 3 times with linear
    backoff so a momentary rate-limit hiccup doesn't leave a candidate
    without a ``message_id`` — that id is what ``discord_picks.py`` uses
    to look up the DailyDigestPicks row when the user clicks an emoji.
    """
    sender = _import_discord_sender()
    last_err = ""
    for attempt in range(3):
        try:
            res = sender.send_to_channel(DISCORD_DIGEST_CHANNEL_ALIAS, text)
        except Exception as e:  # pragma: no cover — defensive
            res = {"ok": False, "error": f"send_to_channel raised: {e}"}
        if res.get("ok"):
            return res
        last_err = res.get("error", "unknown")
        logger.warning(
            "discord push attempt %d/3 failed: %s", attempt + 1, last_err
        )
        time.sleep(0.6 * (attempt + 1))
    return {"ok": False, "error": last_err}


# ---------------------------------------------------------------------------
# content_kind mapping
# ---------------------------------------------------------------------------

def derive_content_kind(record: Dict[str, Any]) -> str:
    """從 ProcessedContent record 推出 DailyDigestPicks.content_kind。"""
    f = record.get("fields", {})
    # paywall-preview 優先(Plan 1 flag)
    if f.get("paywall_preview_kept"):
        kind = f.get("paywall_preview_kind") or "paywall-preview"
        if kind in ("paywall-preview", "short-paywall-preview"):
            return kind
        return "paywall-preview"
    article_type = f.get("article_type") or "short-summary"
    return _ARTICLE_TYPE_TO_CONTENT_KIND.get(article_type, "short-summary")


# ---------------------------------------------------------------------------
# 寫 DailyDigestPicks
# ---------------------------------------------------------------------------

def write_pick_rows(
    candidates: List[Dict[str, Any]],
    digest_date: str,
    message_ids: List[Optional[str]],
) -> List[str]:
    """寫 N 筆 DailyDigestPicks rows。回傳 record id list(供 debug)。

    ``message_ids[i]`` 對應 ``candidates[i]``,``None`` 表示那則
    推送失敗(仍寫 row,message_id 留空,等使用者事後補資料)。
    """
    out: List[str] = []
    for i, rec in enumerate(candidates):
        f = rec.get("fields", {})
        msg_id = message_ids[i] if i < len(message_ids) else None
        record_fields: Dict[str, Any] = {
            "digest_date": digest_date,
            "source_type": f.get("source_type"),
            "source_name": (f.get("channels") or [""])[0],
            "title": f.get("title", ""),
            "vault_path": f.get("output_path", ""),
            "content_kind": derive_content_kind(rec),
        }
        if msg_id:
            record_fields["message_id"] = msg_id
        # url 從 metadata 推
        meta_raw = f.get("metadata", "")
        if isinstance(meta_raw, str) and meta_raw.strip():
            try:
                meta = json.loads(meta_raw)
                url = meta.get("url") or meta.get("source_url")
                if url:
                    record_fields["url"] = url
            except Exception:
                pass
        try:
            rid = create_pick(record_fields)
            out.append(rid)
        except DailyDigestStoreError as e:
            logger.warning("create_pick failed (%s): %s", f.get("source_hash", "")[:12], e)
            out.append("")
    return out


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def _digest_date_str(now: Optional[datetime] = None) -> str:
    """今日 digest_date = UTC today。Plan 3 06:30 CEST 對 UTC 是 04:30。"""
    return (now or datetime.now(timezone.utc)).strftime("%Y-%m-%d")


def run_push(days: int = DEFAULT_DAYS, dry_run: bool = False) -> int:
    store = ProcessedStore(BASE_ID, PROCESSED_TABLE)
    pool = fetch_candidates(store, days=days)
    selected = apply_quota(pool)
    digest_date = _digest_date_str()
    header = render_header(len(selected), len(pool), digest_date)

    if dry_run:
        print("=" * 60)
        print("[header]")
        print(header)
        print()
        for i, rec in enumerate(selected, 1):
            print(f"[candidate {i}/{len(selected)}]")
            print(render_candidate(i, len(selected), rec, digest_date))
            print()
        print("=" * 60)
        print(f"DRY-RUN: would push 1 header + {len(selected)} candidates")
        return 0

    # 推 header
    hdr_res = push_to_discord(header)
    if not hdr_res.get("ok"):
        logger.error("header push failed: %s", hdr_res.get("error"))
        return 1
    logger.info("header pushed")

    # 逐則候選推送
    message_ids: List[Optional[str]] = []
    for i, rec in enumerate(selected, 1):
        text = render_candidate(i, len(selected), rec, digest_date)
        res = push_to_discord(text)
        if res.get("ok"):
            # send_to_channel 回 {'ok': True, 'message_ids': [...]}
            ids = res.get("message_ids") or []
            message_ids.append(ids[0] if ids else None)
            logger.info("candidate %d pushed (msg_id=%s)", i, message_ids[-1])
        else:
            logger.warning("candidate %d push failed: %s", i, res.get("error"))
            message_ids.append(None)

    # 寫 DailyDigestPicks
    if selected:
        write_pick_rows(selected, digest_date, message_ids)
        logger.info("DailyDigestPicks rows written: %d", len(selected))

    logger.info("done. pushed %d candidates + 1 header", len(selected))
    return 0


def run_test_pick(message_id: str, emoji: str) -> int:
    """模擬一次 emoji reaction — 寫進 DailyDigestPicks。"""
    emoji_to_picked = {"✅": "yes", "❌": "no", "🟡": "maybe"}
    picked = emoji_to_picked.get(emoji)
    if not picked:
        print(f"unsupported emoji: {emoji!r} (✅/❌/🟡 only)", file=sys.stderr)
        return 2
    picked_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    fields = {
        "picked": picked,
        "picked_at": picked_at,
        "discord_user_id": "TEST_USER_0",  # 標記為測試,不要污染真資料
    }
    n = update_pick_by_message(message_id, fields)
    if n == 0:
        print(
            f"no DailyDigestPicks row with message_id={message_id}",
            file=sys.stderr,
        )
        return 3
    print(
        f"OK: message_id={message_id} picked={picked} "
        f"picked_at={picked_at} rows_updated={n}"
    )
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="只 print,不推")
    ap.add_argument(
        "--test-pick",
        nargs=2,
        metavar=("MESSAGE_ID", "EMOJI"),
        help="模擬一次 emoji reaction(MESSAGE_ID + ✅/❌/🟡)",
    )
    ap.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"回看窗口天數(預設 {DEFAULT_DAYS})",
    )
    args = ap.parse_args(argv)

    if args.test_pick:
        msg_id, emoji = args.test_pick
        return run_test_pick(msg_id, emoji)

    return run_push(days=args.days, dry_run=args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
