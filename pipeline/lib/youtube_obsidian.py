"""Write one VideoDigest as a Markdown file under podcast-kb/vault/Daily/YYYY-MM-DD/.

Re-uses the wipe-on-rerun + 0-simplified gate from news pipeline's obsidian.py.
"""
from __future__ import annotations
import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from pipeline.lib.translate import force_traditional, has_simplified  # noqa: E402


def _slug(text: str, max_len: int = 60) -> str:
    s = re.sub(r"[^\w\-]+", "-", text.lower()).strip("-")
    return s[:max_len] or "untitled"


def step_write_vault(digests, repo_root: str, date_str: str | None = None,
                     wipe: bool = True) -> dict:
    """Wipe + write all digests as Markdown under podcast-kb/vault/Daily/<date>/.

    Returns a summary dict with counts and any failed files.
    """
    repo = Path(repo_root)
    if date_str is None:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = repo / "podcast-kb" / "vault" / "Daily" / date_str
    if wipe and out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    summary = {"date": date_str, "written": [], "skipped": [], "errors": []}

    for d in digests:
        try:
            slug = _slug(f"{d.channel_name}_{d.title}_{d.video_id}")[:80]
            path = out_dir / f"{slug}.md"
            content = _render_digest_md(d)
            # Defense in depth: force_traditional on the full content
            if has_simplified(content):
                fixed = force_traditional(content)
                content = fixed[0] if isinstance(fixed, tuple) else fixed
            path.write_text(content, encoding="utf-8")
            summary["written"].append(str(path.relative_to(repo)))
        except Exception as e:  # noqa: BLE001
            summary["errors"].append({"digest": d.video_id, "error": str(e)})

    # Index file
    index_path = out_dir / "_index.md"
    index_path.write_text(_render_index(digests, date_str), encoding="utf-8")
    summary["written"].append(str(index_path.relative_to(repo)))

    summary["n_files"] = len(summary["written"])
    summary["n_errors"] = len(summary["errors"])
    return summary


def _render_digest_md(d) -> str:
    duration_min = d.duration_sec // 60
    duration_sec = d.duration_sec % 60
    # `d.published_epoch` is now sourced from YouTube's RSS feed when available
    # (see youtube_fetch._fetch_published_from_rss). That gives us the real
    # publish date — yt-dlp's flat-playlist epoch is the playlist-add time and
    # is unreliable.
    pub = (
        datetime.fromtimestamp(d.published_epoch, timezone.utc).strftime("%Y-%m-%d")
        if d.published_epoch else "（未抓到、請見 YouTube 連結）"
    )
    parts = [
        f"# {d.title}",
        "",
        f"- **頻道**：{d.channel_name}",
        f"- **影片 ID**：`{d.video_id}`",
        f"- **發布日期**：{pub}",
        f"- **長度**：{duration_min} 分 {duration_sec} 秒",
        f"- **YouTube 連結**：[觀看影片]({d.url})",
        f"- **原文長度**：{d.n_chars:,} 字",
        "",
        "---",
        "",
        "## 摘要",
        "",
        d.summary_zh or "（無）",
        "",
        "## 房地產分析師視角",
        "",
        d.analyst_zh or "（無）",
        "",
        "## 內容製作人視角",
        "",
        d.producer_zh or "（無）",
        "",
        "## 重點詞彙",
        "",
        d.vocab_zh or "（無）",
        "",
        "---",
        "",
        f"_本檔案由 podcast-kb pipeline 自動產生於 {datetime.now(timezone.utc).isoformat()}_",
    ]
    return "\n".join(parts)


def _render_index(digests, date_str: str) -> str:
    lines = [
        f"# Podcast 摘要索引 — {date_str}",
        "",
        f"- 影片數量：{len(digests)}",
        "",
        "| 頻道 | 標題 | 摘要 |",
        "| --- | --- | --- |",
    ]
    for d in digests:
        title_link = f"[{d.title}]({d.url})"
        summary_short = (d.summary_zh or "（無）").split("\n")[0][:80]
        lines.append(f"| {d.channel_name} | {title_link} | {summary_short} |")
    return "\n".join(lines)
