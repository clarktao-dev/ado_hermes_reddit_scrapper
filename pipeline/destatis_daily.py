#!/usr/bin/env python3
"""Destatis daily pipeline — 抓德國官方統計 CSV → 寫 vault → 推 Discord + GitHub。

Steps
-----
  1. 載入 ``pipeline/config/destatis_sources.json``。
  2. 篩選 ``enabled: true`` 的 source。
  3. 對每個 source 跑 :func:`pipeline.lib.destatis_csv.fetch_and_parse` → ``DestatisDataset``。
  4. 對每個 dataset:
     a. **Airtable dedup gate** — ``source_id = "destatis:{id}:{reference_period}"``,
        若 ledger 已存在 → 跳過(印 ``[destatis] 跳過已處理: {id}``)。
     b. **Wipe** — ``immobilien-kb/vault/Stat/{YYYY-MM-DD}/`` 整個砍掉再重建。
     c. 渲染 vault markdown(每個 dataset 一個 .md,含 YAML frontmatter + 摘要 + 表格)。
     d. **Discord push** — 透過 ``discord_sender.send_to_channel`` 發 embed
        (標題前綴 ``🏗️ [Destatis 官方數據]``)。
     e. **Airtable mark_processed** — 記 vault: True / discord: True / github: False。
  5. **GitHub push** — ``git add immobilien-kb/`` + commit + push(用既有 ``push_to_github.py``)。
  6. 印 summary。

CLI
---
- ``python -m pipeline.destatis_daily``         — 預設跑全部 enabled,只做本地 vault。
- ``python -m pipeline.destatis_daily --push``  — 推 Discord + GitHub + 寫 Airtable。
- ``python -m pipeline.destatis_daily --dry-run``— 只印出會做什麼,不實際執行。
- ``python -m pipeline.destatis_daily --source <id>`` — 只跑單一 source。
- ``python -m pipeline.destatis_daily --channel <alias>`` — 覆寫預設 Discord channel。

設計細節
--------
- **Vault 路徑** 是 ``immobilien-kb/vault/Stat/{date}/``(新資料夾,與 ``Daily/`` / ``YouTube/`` 並列)。
  每日重跑前 ``shutil.rmtree`` 整個資料夾,確保乾淨。
- **Source id 格式** 為 ``destatis:{source_id}:{reference_period}``。當同一份
  reference_period 內重跑(例如 cron 每小時)會被 Airtable dedup 擋下;
  reference_period 變動(下個月資料更新)才會再次寫 vault + 推播。
- **Wipe 範圍** 是整個 ``Stat/{date}/`` 而非逐檔刪除,因為每天的 stat 集合理論上
  是一個完整的「快照」,不需要保留前一次的混合。
- **Vault 內容**:每個 dataset 一個 .md,內含 YAML frontmatter(8 個欄位)、
  最新月份摘要、資料範圍、最後 12 個月 markdown 表格、來源 URL 連結;
  ``_index.md`` 列出本日所有 dataset 摘要。
- **錯誤隔離**:單一 source 失敗不中斷整個 run(每個 source 都有 try/except);
  Airtable 寫入失敗 log 不 crash。
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import logging
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE.parent))

from pipeline.lib.destatis_csv import (  # noqa: E402
    DestatisDataset,
    fetch_and_parse,
)
from pipeline.lib.processed_store import (  # noqa: E402
    DEFAULT_TABLE,
    ProcessedStore,
    make_hash,
)


# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

REPO_ROOT = "/root/projects/ado_hermes_reddit_scrapper"
CONFIG_PATH = os.path.join(REPO_ROOT, "pipeline/config/destatis_sources.json")
VAULT_ROOT = os.path.join(REPO_ROOT, "immobilien-kb/vault")
STAT_SUBDIR = "Stat"
PUSH_SCRIPT = os.path.join(REPO_ROOT, "push_to_github.py")
DISCORD_SENDER = os.path.join(
    REPO_ROOT, "immobilien-kb", "tools", "discord_sender.py"
)

# Airtable ledger — same base as youtube_daily / news_daily, see
# pipeline.lib.processed_store. Override via env var if needed.
PROCESSED_BASE_ID = os.environ.get(
    "AIRTABLE_PROCESSED_CONTENT_BASE_ID", "appHilorcrC5T0p2u"
)
PROCESSED_TABLE = os.environ.get(
    "AIRTABLE_PROCESSED_CONTENT_TABLE", DEFAULT_TABLE
)

# Discord default — Stage 1 T3.1 fix: switch from the old "home" fallback
# to the dedicated ``tao`` alias (channel id 1495562787685011616) that
# the user created for Destatis official-stats digests. ``tao`` was
# added to ``discord_sender.py`` aliases at the same time. Callers can
# still override with ``--channel`` for ad-hoc runs.
DEFAULT_DISCORD_CHANNEL = "tao"

# Module-level logger
logger = logging.getLogger("destatis_daily")
if not logger.handlers:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )


# --------------------------------------------------------------------------- #
# Config loading
# --------------------------------------------------------------------------- #

def load_sources(only_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """Load enabled sources from ``pipeline/config/destatis_sources.json``.

    Args:
        only_id: If given, filter to the single source with this id
            (CLI --source override). The source must still be enabled.

    Returns:
        List of source config dicts (each contains id, name_de, name_zh,
        url, vault_filename, etc.).
    """
    cfg_path = Path(CONFIG_PATH)
    if not cfg_path.exists():
        raise FileNotFoundError(f"destatis config not found: {cfg_path}")
    data = json.loads(cfg_path.read_text(encoding="utf-8"))
    sources = [
        s for s in data.get("sources", []) if s.get("enabled", False)
    ]
    if only_id:
        sources = [s for s in sources if s["id"] == only_id]
        if not sources:
            available = [s["id"] for s in data.get("sources", [])]
            raise ValueError(
                f"--source {only_id!r} not found or not enabled; "
                f"available: {available}"
            )
    return sources


# --------------------------------------------------------------------------- #
# Discord import (mirrors news_daily._import_discord)
# --------------------------------------------------------------------------- #

def _import_discord():
    """Import ``discord_sender.py`` from the tools/ dir without installing."""
    spec = importlib.util.spec_from_file_location("discord_sender", DISCORD_SENDER)
    if spec is None or spec.loader is None:
        raise ImportError(f"could not load module spec for {DISCORD_SENDER}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


# --------------------------------------------------------------------------- #
# Vault rendering (per-dataset .md + _index.md)
# --------------------------------------------------------------------------- #

def _format_value_for_table(v: str) -> str:
    """Strip CSV quoting for display, replace commas with dots where appropriate.

    Destatis CSV uses ``;`` delimiter and ``,`` as decimal mark (German
    convention). We leave the value as-is for the markdown table — numbers
    in German format (``65,4``) are kept verbatim because that's what the
    source emits. Just strip surrounding double-quotes if csv reader kept
    them (it doesn't with QUOTE_ALL off — but be defensive).
    """
    v = (v or "").strip()
    if len(v) >= 2 and v.startswith('"') and v.endswith('"'):
        v = v[1:-1]
    return v


# Column names that signal a time-series index column. We only check the
# FIRST column (header[0]) because Destatis time-series put the period
# there ("Monat", "Jahr", "Zeit", "Datum", "Period", etc). Cross-section
# tables (e.g. investments_construction) put "Kategorie" there and
# should NOT be treated as time series.
_TIME_SERIES_FIRST_COL_KEYWORDS = (
    "monat", "month", "jahr", "year", "zeit", "time",
    "datum", "date", "period", "periode", "quartal", "quarter",
    "stichtag", "berichtsmonat",
)


def _is_time_series_first_col(header: List[str]) -> bool:
    """Return True if the first header column is a time/period index.

    A column is considered a time-series index if its (lowercased) name
    contains any of :data:`_TIME_SERIES_FIRST_COL_KEYWORDS` as a
    standalone token or a clear prefix. We deliberately look at the
    *first* column only because the highchart CSVs always lead with the
    period column; cross-section tables lead with the dimension
    column (e.g. ``Kategorie``).
    """
    if not header:
        return False
    first = (header[0] or "").strip().lower()
    if not first:
        return False
    # Tokenize on common delimiters so "Berichtsmonat (YYYY-MM)" still
    # matches "monat" and "Jahr/Quartal" matches "jahr".
    for tok in first.replace("(", " ").replace(")", " ").replace("/", " ").split():
        tok = tok.strip(",;:")
        for kw in _TIME_SERIES_FIRST_COL_KEYWORDS:
            if tok == kw or tok.startswith(kw):
                return True
    return False


def _render_dataset_md(
    ds: DestatisDataset,
    date_str: str,
    source_page: str = "",
) -> str:
    """Render a single DestatisDataset as a Markdown file.

    Layout:
      - YAML frontmatter (8 fields: source_type, source_id, name_de, name_zh,
        reference_period, fetched_at, url, encoding, n_rows, n_cols).
      - H1: ``# {name_zh} ({name_de})``
      - 最新資料 paragraph.
      - 資料範圍 paragraph.
      - Last-12-rows table (header + last 12 data rows, markdown pipe table).
      - 來源 URL link.

    Behaviour for non-time-series CSVs (e.g. ``investments_construction``,
    where the first column is a category, not a date): we render
    ``最新資料`` as a cross-section summary
    ("非時間序列資料,共 N 筆橫斷面資料") instead of mis-labelling the
    last row as the "latest month".
    """
    header = ds.header
    n_rows = len(ds.rows)
    n_cols = len(header)
    has_data = n_rows > 1
    is_time_series = has_data and _is_time_series_first_col(header)
    first_data = ds.rows[1] if has_data else []
    last_data = ds.rows[-1] if has_data else []
    first_period = first_data[0] if first_data else "—"
    last_period = last_data[0] if last_data else "—"

    # Last 12 data rows for the markdown table (newest at top via reverse).
    table_rows = ds.rows[1:] if has_data else []
    last_12 = list(reversed(table_rows[-12:]))

    # Build markdown table
    table_lines: List[str] = []
    if has_data and header:
        # Header row
        clean_header = [_format_value_for_table(h) for h in header]
        table_lines.append("| " + " | ".join(clean_header) + " |")
        table_lines.append("|" + "|".join(["---"] * len(clean_header)) + "|")
        for row in last_12:
            cells = [_format_value_for_table(c) for c in row]
            # Pad short rows so the table is well-formed
            while len(cells) < len(clean_header):
                cells.append("")
            table_lines.append("| " + " | ".join(cells[: len(clean_header)]) + " |")
    else:
        table_lines.append("_（無資料）_")
    table_md = "\n".join(table_lines)

    n_data_rows = max(0, n_rows - 1)

    if is_time_series:
        # Time-series: the first column is the period. Show last row's
        # non-period columns as the "本期數值".
        if has_data and len(last_data) > 1:
            latest_pairs: List[str] = []
            for i, val in enumerate(last_data[1:], start=1):
                col_name = header[i] if i < len(header) else f"col{i}"
                latest_pairs.append(
                    f"{_format_value_for_table(col_name)}="
                    f"{_format_value_for_table(val)}"
                )
            latest_summary = ", ".join(latest_pairs) or "—"
        else:
            latest_summary = "（無）"
        latest_label = "**最新月份**"
        latest_value = last_period
        range_label_first = "**起**"
        range_label_last = "**迄**"
        count_label = f"**月資料筆數**：{n_data_rows} 筆"
        count_suffix = f"（共 {n_rows} 列含表頭）" if has_data else ""
        table_section_title = "## 完整資料表（前 12 個月，最新在上）"
    else:
        # Cross-section (non-time-series). Don't pretend the first column
        # is a date; just say so and summarise what's in the table.
        latest_summary = (
            f"非時間序列資料,共 {n_data_rows} 筆橫斷面資料"
        )
        latest_label = "**資料類型**"
        latest_value = "橫斷面（cross-section）"
        range_label_first = "**首筆**"
        range_label_last = "**末筆**"
        count_label = f"**橫斷面資料筆數**：{n_data_rows} 筆"
        count_suffix = f"（共 {n_rows} 列含表頭）" if has_data else ""
        table_section_title = "## 完整資料表（最多顯示 12 筆）"

    frontmatter = "\n".join([
        "---",
        f"source_type: destatis_csv",
        f"source_id: {ds.source_id}",
        f"reference_period: {ds.reference_period}",
        f"name_de: \"{ds.name_de.replace(chr(34), '')}\"",
        f"name_zh: \"{ds.name_zh.replace(chr(34), '')}\"",
        f"url: {ds.url}",
        f"fetched_at: {ds.fetched_at}",
        f"date: {date_str}",
        f"encoding: {ds.encoding}",
        f"n_rows: {n_rows}",
        f"n_cols: {n_cols}",
        "---",
    ])

    body = "\n".join([
        frontmatter,
        "",
        f"# {ds.name_zh}（{ds.name_de}）",
        "",
        "## 最新資料",
        "",
        f"- **資料期間**：{ds.reference_period}",
        f"- {latest_label}：{latest_value}",
        f"- **本期數值**：{latest_summary}",
        "",
        "## 資料範圍",
        "",
        f"- {range_label_first}：{first_period}",
        f"- {range_label_last}：{last_period}",
        f"- {count_label}{count_suffix}",
        f"- **欄位數**：{n_cols} 個",
        "",
        table_section_title,
        "",
        table_md,
        "",
        "## 來源",
        "",
        f"- 原始 URL：[{ds.url}]({ds.url})",
        f"- 來源網頁：{source_page or '（見 config 內 `_source_page`）'}",
        "",
        f"_本檔案由 destatis-daily pipeline 自動產生於 "
        f"{datetime.now(timezone.utc).isoformat()}_",
    ])
    return body


def _render_index_md(
    datasets: List[DestatisDataset],
    date_str: str,
    vault_subdir: str = STAT_SUBDIR,
) -> str:
    """Render the per-day ``_index.md`` with all datasets of the day.

    Args:
        datasets: All datasets successfully fetched today (skipped ones
            are excluded — they'd confuse the "本日彙整" count).
        date_str: ``YYYY-MM-DD``.
        vault_subdir: The vault subdir name (default ``"Stat"``). Used in
            the GitHub link footer.
    """
    total = len(datasets)
    frontmatter = "\n".join([
        "---",
        "type: destatis_daily_digest",
        f"date: {date_str}",
        f"total_datasets: {total}",
        "---",
    ])

    lines: List[str] = [
        frontmatter,
        "",
        f"# 🏗️ Destatis 官方數據 — {date_str}",
        "",
        f"共 **{total} 個資料集** | 來源：德國聯邦統計局 (Destatis)",
        "",
        "## 本日彙整",
        "",
    ]

    if not datasets:
        lines.append("_（本日沒有新資料）_")
        lines.append("")
    else:
        for i, ds in enumerate(datasets, 1):
            n_data = max(0, len(ds.rows) - 1)
            last_row = ds.rows[-1] if len(ds.rows) > 1 else []
            latest_period = last_row[0] if last_row else "—"
            is_ts = _is_time_series_first_col(ds.header)
            if is_ts:
                # Time-series: first column IS a period — show latest month.
                latest_label = "**最新月份**"
                latest_value = latest_period
                count_label = "**資料筆數**"
                count_value = f"{n_data} 個月"
            else:
                # Cross-section: first column is a category, NOT a period.
                # Don't pretend the last row is the "latest category" —
                # just say it's cross-section and report the row count,
                # matching the single-dataset .md wording
                # ("資料類型:橫斷面").
                latest_label = "**資料類型**"
                latest_value = "橫斷面（cross-section）"
                count_label = "**資料筆數**"
                count_value = f"{n_data} 筆（橫斷面）"
            lines.append(f"### {i}. {ds.name_zh}（{ds.name_de}）")
            lines.append("")
            lines.append(f"- **資料期間**：{ds.reference_period}")
            lines.append(f"- {latest_label}：{latest_value}")
            lines.append(f"- {count_label}：{count_value}")
            lines.append(f"- **欄位數**：{len(ds.header)} 個")
            lines.append(f"- **原始 CSV**：[連結]({ds.url})")
            lines.append("")

    lines.extend([
        "## GitHub",
        "",
        f"https://github.com/clarktao-dev/ado_hermes_reddit_scrapper/"
        f"blob/main/immobilien-kb/vault/{vault_subdir}/{date_str}/",
    ])

    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# Vault writer
# --------------------------------------------------------------------------- #

@dataclass
class VaultWriteResult:
    """Per-dataset vault-write outcome."""
    source_id: str
    vault_path: str  # relative to repo
    reference_period: str
    n_data_rows: int
    n_cols: int
    encoding: str


def step_write_vault(
    datasets: List[DestatisDataset],
    sources: List[Dict[str, Any]],
    repo_root: str = REPO_ROOT,
    date_str: Optional[str] = None,
    wipe: bool = True,
) -> Tuple[List[VaultWriteResult], Path]:
    """Wipe the day's vault dir + write one .md per dataset + _index.md.

    Args:
        datasets: All datasets to render.
        sources: The corresponding source configs (used to pick
            ``vault_filename`` if a dataset config has a custom one).
        repo_root: Absolute path to the repository root.
        date_str: ``YYYY-MM-DD`` (defaults to today UTC).
        wipe: If True (default), ``shutil.rmtree`` the day's folder before
            writing so re-runs don't accumulate stale files.

    Returns:
        ``(per_dataset_results, index_path)`` — list of per-dataset write
        results (one per successfully-written dataset) and the absolute
        path of the ``_index.md``.
    """
    repo = Path(repo_root)
    if date_str is None:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = repo / "immobilien-kb" / "vault" / STAT_SUBDIR / date_str

    if wipe and out_dir.exists():
        logger.info("[destatis] wipe %s", out_dir)
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Map source_id → source config (for vault_filename override).
    cfg_by_id: Dict[str, Dict[str, Any]] = {s["id"]: s for s in sources}

    results: List[VaultWriteResult] = []
    for ds in datasets:
        cfg = cfg_by_id.get(ds.source_id, {})
        # Per-source custom filename if provided; else default to source_id.md.
        fname = cfg.get("vault_filename") or f"{ds.source_id}.md"
        path = out_dir / fname
        # Pull the human-readable source page from the config (named
        # ``_source_page`` to keep it out of the public API surface) so
        # the rendered .md can show "來源網頁: <real HTML page URL>"
        # instead of the placeholder.
        source_page = cfg.get("_source_page", "") or cfg.get("source_page", "")
        content = _render_dataset_md(ds, date_str, source_page=source_page)
        try:
            path.write_text(content, encoding="utf-8")
        except Exception as e:  # noqa: BLE001
            logger.error("[destatis] vault write failed for %s: %s",
                         ds.source_id, e)
            continue
        rel = str(path.relative_to(repo))
        logger.info("[destatis] wrote %s (%d bytes)", rel, len(content))
        results.append(VaultWriteResult(
            source_id=ds.source_id,
            vault_path=rel,
            reference_period=ds.reference_period,
            n_data_rows=max(0, len(ds.rows) - 1),
            n_cols=len(ds.header),
            encoding=ds.encoding,
        ))

    # Index
    index_path = out_dir / "_index.md"
    index_md = _render_index_md(datasets, date_str)
    index_path.write_text(index_md, encoding="utf-8")
    logger.info("[destatis] wrote %s (%d bytes)",
                str(index_path.relative_to(repo)), len(index_md))

    return results, index_path


# --------------------------------------------------------------------------- #
# Discord push
# --------------------------------------------------------------------------- #

def _build_embed_for_dataset(ds: DestatisDataset) -> Tuple[str, str]:
    """Build (title, body) for one dataset's Discord embed.

    Title is ``🏗️ [Destatis 官方數據] {name_zh}`` (truncated to 80 chars to
    stay safely under Discord's limits). Body is plain-text-friendly: the
    latest month, the values, and a link.
    """
    title = f"🏗️ [Destatis 官方數據] {ds.name_zh}"
    if len(title) > 80:
        title = title[:77] + "…"

    last_row = ds.rows[-1] if len(ds.rows) > 1 else []
    if last_row and len(last_row) > 1 and len(ds.header) > 1:
        pairs: List[str] = []
        for i, val in enumerate(last_row[1:], start=1):
            col_name = ds.header[i] if i < len(ds.header) else f"col{i}"
            pairs.append(f"  • {col_name} = {val}")
        latest_block = "\n".join(pairs)
        latest_period = last_row[0]
    else:
        latest_block = "（無）"
        latest_period = "—"
    n_data = max(0, len(ds.rows) - 1)
    body = (
        f"**{ds.name_zh}**（{ds.name_de}）\n\n"
        f"**資料期間**：`{ds.reference_period}`\n"
        f"**最新月份**：`{latest_period}`\n"
        f"**本期數值**：\n{latest_block}\n\n"
        f"**資料範圍**：{n_data} 個月（欄位數 {len(ds.header)}）\n"
        f"**編碼**：{ds.encoding}\n\n"
        f"🔗 原始 CSV：{ds.url}"
    )
    return title, body


def step_send_discord(
    datasets: List[DestatisDataset],
    channel_alias: str = DEFAULT_DISCORD_CHANNEL,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Push one Discord embed per dataset.

    Each dataset → one embed. No header embed, no aggregation — Destatis
    is low-frequency (3 datasets, monthly data) so a single embed per
    dataset is the right granularity.

    Returns a dict shaped like youtube_daily's ``step_send_discord`` so
    the caller can backfill ``discord_message_id``:
      ``{"n_embeds": int, "errors": [...], "per_dataset": [...]}``
    """
    summary: Dict[str, Any] = {
        "n_embeds": 0,
        "errors": [],
        "channel": channel_alias,
        "per_dataset": [],
    }
    if not datasets:
        summary["errors"].append("no datasets to send")
        return summary

    if dry_run:
        for ds in datasets:
            title, body = _build_embed_for_dataset(ds)
            summary["per_dataset"].append({
                "source_id": ds.source_id,
                "title": title,
                "body_chars": len(body),
                "message_ids": [],
            })
            summary["n_embeds"] += 1
        return summary

    try:
        mod = _import_discord()
    except Exception as e:  # noqa: BLE001
        summary["errors"].append(f"discord import failed: {e}")
        return summary

    for ds in datasets:
        title, body = _build_embed_for_dataset(ds)
        try:
            resp = mod.send_to_channel(
                channel_alias,
                body,
                as_embed=True,
                title=title,
            )
            ok = bool(resp and resp.get("ok"))
            mids = list(resp.get("message_ids", [])) if resp else []
            if not ok:
                summary["errors"].append(
                    f"send failed: {ds.source_id} (resp={resp})"
                )
            else:
                summary["n_embeds"] += 1
            summary["per_dataset"].append({
                "source_id": ds.source_id,
                "title": title,
                "message_ids": mids,
                "ok": ok,
            })
        except Exception as e:  # noqa: BLE001
            summary["errors"].append(f"send exception for {ds.source_id}: {e}")
            summary["per_dataset"].append({
                "source_id": ds.source_id,
                "title": title,
                "message_ids": [],
                "ok": False,
            })
    return summary


# --------------------------------------------------------------------------- #
# GitHub push (mirrors youtube_daily.push_to_github, scoped to immobilien-kb/)
# --------------------------------------------------------------------------- #

def step_push_github(
    repo_root: str = REPO_ROOT,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Stage + commit + push immobilien-kb/ to main.

    Same pattern as :func:`youtube_daily.push_to_github` but scoped to
    ``immobilien-kb/`` so the destatis run doesn't accidentally sweep
    ``podcast-kb/`` writes from a parallel youtube run.

    Returns ``{"pushed": bool, "commit_sha": str|None, "error": str|None,
    "commands": [...]}`` for logging / backfill.
    """
    out: Dict[str, Any] = {
        "pushed": False,
        "commit_sha": None,
        "commands": [],
        "dry_run": dry_run,
    }
    if dry_run:
        return out
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    msg = f"immobilien-kb: {today} destatis daily digest"

    cmds = [
        ["git", "-C", repo_root, "add", "immobilien-kb/"],
        ["git", "-C", repo_root, "-c", "user.email=ado@hermes.local", "-c",
         "user.name=Ado", "commit", "-m", msg],
    ]
    for cmd in cmds:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        out["commands"].append({
            "cmd": " ".join(cmd),
            "rc": proc.returncode,
            "stdout_tail": proc.stdout[-200:],
            "stderr_tail": proc.stderr[-200:],
        })
        if proc.returncode != 0 and "nothing to commit" not in proc.stderr:
            out["error"] = proc.stderr[-300:]
            return out

    sha_proc = subprocess.run(
        ["git", "-C", repo_root, "rev-parse", "HEAD"],
        capture_output=True, text=True, timeout=10,
    )
    if sha_proc.returncode == 0:
        out["commit_sha"] = sha_proc.stdout.strip()

    push_proc = subprocess.run(
        ["python3", PUSH_SCRIPT], capture_output=True, text=True, timeout=60,
    )
    out["push"] = {
        "rc": push_proc.returncode,
        "stdout_tail": push_proc.stdout[-200:],
        "stderr_tail": push_proc.stderr[-200:],
    }
    out["pushed"] = push_proc.returncode == 0
    return out


# --------------------------------------------------------------------------- #
# Per-dataset orchestration
# --------------------------------------------------------------------------- #

def process_one_source(
    source: Dict[str, Any],
    *,
    store: Optional[ProcessedStore],
    push_discord: bool,
    push_github_after: bool,
    dry_run: bool,
) -> Dict[str, Any]:
    """Fetch + dedup + write vault + push Discord + mark_processed for one source.

    Args:
        source: Source config dict from destatis_sources.json.
        store: Airtable ProcessedStore (or None if --skip-store).
        push_discord: If True, send the Discord embed.
        push_github_after: If True, the caller will push to GitHub after
            this returns (so we can collect all per-dataset results
            before committing).
        dry_run: If True, fetch + render locally, but don't push to
            Discord / write to Airtable.

    Returns:
        A dict shaped like::
            {
              "source_id": "auftragseingang_bauhauptgewerbe",
              "status": "ok" | "skipped" | "failed" | "dry-run",
              "dataset": DestatisDataset or None,
              "vault_result": VaultWriteResult or None,
              "discord_message_id": str or None,
              "error": str or None,
            }
    """
    source_id = source["id"]
    log_prefix = f"[destatis] [{source_id}]"
    print(f"\n{log_prefix} 開始處理 — {source.get('name_zh', '?')}", flush=True)
    out: Dict[str, Any] = {
        "source_id": source_id,
        "status": "pending",
        "dataset": None,
        "vault_result": None,
        "discord_message_id": None,
        "error": None,
    }

    # ----- Fetch + parse -----
    t0 = time.time()
    try:
        ds = fetch_and_parse(source)
    except Exception as e:  # noqa: BLE001
        msg = f"fetch_and_parse failed: {e}"
        logger.error("%s %s", log_prefix, msg)
        out["status"] = "failed"
        out["error"] = msg
        return out
    print(f"{log_prefix} 抓取完成: {len(ds.rows)} rows × {len(ds.header)} cols "
          f"in {time.time() - t0:.1f}s", flush=True)
    out["dataset"] = ds

    # ----- Compose the dedup key -----
    # The "stable" key is the source id + reference_period. When
    # Destatis releases a new month, reference_period changes, and a
    # fresh vault write / Discord push fires. Within the same period
    # (e.g. cron every hour), dedup blocks the redundant run.
    dedup_source_id = f"destatis:{ds.source_id}:{ds.reference_period}"

    # ----- Airtable dedup gate -----
    if store is not None and not dry_run:
        try:
            # source_type is "destatis_csv" to match the per-dataset
            # YAML frontmatter ``source_type: destatis_csv`` and the
            # plan's canonical naming for this pipeline.
            already = store.is_processed("destatis_csv", dedup_source_id)
        except Exception as e:  # noqa: BLE001
            logger.warning("%s is_processed failed (%s); continuing",
                           log_prefix, e)
            already = False
        if already:
            print(f"[destatis] 跳過已處理: {ds.source_id} "
                  f"(period={ds.reference_period})", flush=True)
            out["status"] = "skipped"
            return out

    # ----- Dry-run: stop here, report what would happen -----
    if dry_run:
        print(f"{log_prefix} [dry-run] would write vault + push Discord + "
              f"mark_processed (dedup_source_id={dedup_source_id})",
              flush=True)
        out["status"] = "dry-run"
        return out

    # ----- Vault write happens at the day-level in main(), not here -----
    # We just return the dataset so the caller can batch the day's datasets.
    out["status"] = "ok"
    return out


# --------------------------------------------------------------------------- #
# Main orchestration
# --------------------------------------------------------------------------- #

def run_pipeline(
    *,
    only_source: Optional[str] = None,
    push: bool = False,
    dry_run: bool = False,
    channel: str = DEFAULT_DISCORD_CHANNEL,
    skip_store: bool = False,
    pipeline_run_id: str = "",
) -> int:
    """End-to-end pipeline.

    Args:
        only_source: If set, only process this source id.
        push: If True, send Discord + push GitHub + write Airtable.
            If False (default), only write the local vault.
        dry_run: If True, fetch + render + print plan, no side effects.
        channel: Discord channel alias (default: 'tao').
        skip_store: Bypass ProcessedStore entirely (for local debugging).
        pipeline_run_id: Override the auto-generated run id.

    Returns:
        Process exit code (0 on success, non-zero on fatal error).
    """
    t_start = time.time()
    run_id = pipeline_run_id or datetime.now(timezone.utc).strftime(
        "destatis-%Y%m%d-%H%M%S"
    )
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    print(f"[destatis] starting at {datetime.now(timezone.utc).isoformat()}Z "
          f"(push={push}, dry_run={dry_run}, only_source={only_source}, "
          f"channel={channel}, run_id={run_id})")

    # ----- Step 1: load config -----
    try:
        sources = load_sources(only_id=only_source)
    except Exception as e:  # noqa: BLE001
        logger.error("[destatis] load_sources failed: %s", e)
        return 2
    print(f"[destatis] loaded {len(sources)} enabled source(s): "
          f"{[s['id'] for s in sources]}")

    # ----- Step 2: ProcessedStore -----
    store: Optional[ProcessedStore] = None
    if not skip_store and not dry_run:
        try:
            store = ProcessedStore(PROCESSED_BASE_ID,
                                   table_name=PROCESSED_TABLE)
            logger.info("[destatis] ProcessedStore ready: base=%s table=%s "
                        "run_id=%s", PROCESSED_BASE_ID, PROCESSED_TABLE, run_id)
        except Exception as e:  # noqa: BLE001
            logger.warning("[destatis] ProcessedStore init failed (%s); "
                           "running without dedup ledger", e)
            store = None

    # ----- Step 3: fetch + per-source dedup gate -----
    fetched: List[Tuple[Dict[str, Any], DestatisDataset]] = []
    skipped: List[Dict[str, Any]] = []
    failed: List[Dict[str, Any]] = []
    for src in sources:
        result = process_one_source(
            src,
            store=store,
            push_discord=False,  # pushed in step 5 after vault
            push_github_after=False,  # pushed in step 6 after vault
            dry_run=dry_run,
        )
        if result["status"] == "ok" or result["status"] == "dry-run":
            if result["dataset"] is not None:
                fetched.append((src, result["dataset"]))
        elif result["status"] == "skipped":
            skipped.append(result)
        else:  # failed
            failed.append(result)

    print(f"\n[destatis] fetch summary: {len(fetched)} new, "
          f"{len(skipped)} skipped, {len(failed)} failed")

    if dry_run:
        if fetched:
            print("\n[dry-run] would write these to vault:")
            for src, ds in fetched:
                print(f"  - {src['id']} ({len(ds.rows)} rows) "
                      f"→ immobilien-kb/vault/Stat/{date_str}/"
                      f"{src.get('vault_filename') or src['id'] + '.md'}")
            print(f"\n[dry-run] would write _index.md "
                  f"→ immobilien-kb/vault/Stat/{date_str}/_index.md")
            print(f"[dry-run] would push {len(fetched)} Discord embed(s) to "
                  f"channel '{channel}' (prefix '🏗️ [Destatis 官方數據]')")
            print(f"[dry-run] would git add immobilien-kb/ + commit + push")
            print(f"[dry-run] would mark_processed {len(fetched)} record(s) "
                  f"in Airtable")
        else:
            print("[dry-run] no new data to process")
        print(f"\n[destatis] dry-run done in {time.time() - t_start:.1f}s")
        return 0

    if not fetched and not failed:
        # Everything was skipped (e.g. all sources already in ledger).
        print("\n[destatis] nothing to process — all sources skipped")
        return 0

    # ----- Step 4: write vault (wipe + write per-dataset .md + _index.md) -----
    datasets_for_vault = [ds for _src, ds in fetched]
    sources_for_vault = [src for src, _ds in fetched]
    if datasets_for_vault:
        try:
            vault_results, index_path = step_write_vault(
                datasets_for_vault,
                sources_for_vault,
                repo_root=REPO_ROOT,
                date_str=date_str,
                wipe=True,
            )
            print(f"\n[destatis] vault: wrote {len(vault_results)} per-dataset "
                  f"file(s) + _index.md under "
                  f"immobilien-kb/vault/Stat/{date_str}/")
        except Exception as e:  # noqa: BLE001
            logger.error("[destatis] step_write_vault failed: %s", e)
            return 3
    else:
        vault_results = []
        index_path = None

    # ----- Step 5: send Discord (only if --push) -----
    discord_summary: Dict[str, Any] = {"n_embeds": 0, "errors": [],
                                       "per_dataset": []}
    if push and datasets_for_vault:
        try:
            discord_summary = step_send_discord(
                datasets_for_vault,
                channel_alias=channel,
                dry_run=False,
            )
            print(f"\n[destatis] discord: sent {discord_summary['n_embeds']} "
                  f"embed(s) to '{channel}' "
                  f"(errors={len(discord_summary['errors'])})")
            for e in discord_summary.get("errors", []):
                print(f"  - {e}")
        except Exception as e:  # noqa: BLE001
            logger.error("[destatis] step_send_discord failed: %s", e)
            # Don't return — vault write already succeeded; we can still
            # try GitHub + Airtable.

    # ----- Step 6: push GitHub (only if --push and vault changed) -----
    github_summary: Dict[str, Any] = {"pushed": False, "commit_sha": None}
    if push and vault_results:
        try:
            github_summary = step_push_github(
                repo_root=REPO_ROOT, dry_run=False,
            )
            print(f"\n[destatis] github: pushed={github_summary.get('pushed')}, "
                  f"commit_sha={github_summary.get('commit_sha')}")
            if github_summary.get("error"):
                print(f"  error: {github_summary['error']}")
        except Exception as e:  # noqa: BLE001
            logger.error("[destatis] step_push_github failed: %s", e)

    # ----- Step 7: mark_processed + update_side_effects (Airtable ledger) -----
    if store is not None and vault_results:
        # Map per-dataset vault result to its source for the ledger.
        cfg_by_id: Dict[str, Dict[str, Any]] = {
            s["id"]: s for s in sources_for_vault
        }
        # Map source_id → discord message_id (if any)
        discord_msg_ids: Dict[str, str] = {}
        for entry in discord_summary.get("per_dataset", []):
            mids = entry.get("message_ids") or []
            if mids:
                discord_msg_ids[entry["source_id"]] = ",".join(mids)

        commit_sha = github_summary.get("commit_sha")

        for vr in vault_results:
            src_cfg = cfg_by_id.get(vr.source_id, {})
            ds = next(
                (d for d in datasets_for_vault if d.source_id == vr.source_id),
                None,
            )
            if ds is None:
                continue
            dedup_source_id = f"destatis:{ds.source_id}:{ds.reference_period}"
            metadata = {
                "encoding": vr.encoding,
                "n_data_rows": vr.n_data_rows,
                "n_cols": vr.n_cols,
                "reference_period": ds.reference_period,
                "url": ds.url,
                "source_page": src_cfg.get("_source_page", ""),
                "category": src_cfg.get("category", ""),
            }
            try:
                record_id = store.mark_processed(
                    source_type="destatis_csv",
                    source_id=dedup_source_id,
                    title=ds.name_zh,
                    channels=[f"destatis.{ds.source_id}"],
                    pipeline_run_id=run_id,
                    output_path=vr.vault_path,
                    metadata=metadata,
                    tags=["destatis", src_cfg.get("category", "construction")],
                    article_type="stat-table",
                )
                logger.info("[destatis] marked processed: %s -> %s",
                            dedup_source_id, record_id)
            except Exception as e:  # noqa: BLE001
                logger.error("[destatis] mark_processed failed for %s: %s",
                             dedup_source_id, e)
                continue

            # update_side_effects — best effort
            msg_id = discord_msg_ids.get(vr.source_id)
            if msg_id or commit_sha:
                try:
                    store.update_side_effects(
                        source_hash=make_hash("destatis_csv", dedup_source_id),
                        discord_message_id=msg_id,
                        github_commit_sha=commit_sha,
                    )
                except Exception as e:  # noqa: BLE001
                    logger.warning(
                        "[destatis] update_side_effects failed for %s: %s",
                        dedup_source_id, e,
                    )

    # ----- Step 8: final summary -----
    elapsed = time.time() - t_start
    print(f"\n[destatis] done in {elapsed:.1f}s")
    print(f"  sources processed: {len(sources)}")
    print(f"  new (fetched + vault-written): {len(vault_results)}")
    print(f"  skipped (already in ledger):   {len(skipped)}")
    print(f"  failed:                        {len(failed)}")
    print(f"  discord embeds sent:           {discord_summary.get('n_embeds', 0)}")
    print(f"  github pushed:                 {github_summary.get('pushed', False)}")
    if failed:
        print("  failures:")
        for f in failed:
            print(f"    - {f['source_id']}: {f['error']}")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(
        description="Destatis 官方統計 daily pipeline。"
                    "預設只做本地 vault；用 --push 啟用 Discord + GitHub + Airtable。",
    )
    p.add_argument("--push", action="store_true",
                   help="推 Discord + GitHub + 寫 Airtable(預設關閉,只做 vault)。")
    p.add_argument("--dry-run", action="store_true",
                   help="只列印會做什麼,不實際執行(給 cron pre-flight)。")
    p.add_argument("--source", default=None,
                   help="只跑單一 source(用 source id,例如 'auftragseingang_bauhauptgewerbe')。")
    p.add_argument("--channel", default=DEFAULT_DISCORD_CHANNEL,
                   help=f"Discord channel 別名(預設 '{DEFAULT_DISCORD_CHANNEL}')。"
                        f"可用值:tao / home / headlines / 每日頭條 / podcast / 每日podcast"
                        f"(見 discord_sender.py)。")
    p.add_argument("--skip-store", action="store_true",
                   help="跳過 Airtable ProcessedStore(本地除錯用)。")
    p.add_argument("--pipeline-run-id", default="",
                   help="覆寫自動產生的 run id(預設 destatis-YYYYMMDD-HHMMSS)。")
    args = p.parse_args()

    if args.dry_run and args.push:
        print("[destatis] --dry-run 與 --push 互斥;忽略 --push", flush=True)
        args.push = False

    return run_pipeline(
        only_source=args.source,
        push=args.push,
        dry_run=args.dry_run,
        channel=args.channel,
        skip_store=args.skip_store,
        pipeline_run_id=args.pipeline_run_id,
    )


if __name__ == "__main__":
    sys.exit(main())
