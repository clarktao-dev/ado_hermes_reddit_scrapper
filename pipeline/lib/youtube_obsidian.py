"""Write one VideoDigest as a Markdown file under podcast-kb/vault/Daily/YYYY-MM-DD/.

Re-uses the wipe-on-rerun + 0-simplified gate from news pipeline's obsidian.py.

Plan 5 (2026-08-18): filename schema switched to the new format
    ``{YYYY-MM-DD}_{channel_short}_{kind}_{video_id}.md``
e.g. ``2026-08-18_finanzfluss_summary_zkW9KjyCTEc.md``.
The ``channel_name`` field in the body is preserved unchanged so any
downstream reader still sees the human-readable channel name.
"""
from __future__ import annotations
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from pipeline.lib.translate import force_traditional, has_simplified  # noqa: E402


# Channel human name → short code (mirrors PODCAST_KB_CHANNELS in
# /tmp/vault_path_map_dryrun.py v3).
CHANNEL_NAME_SHORT = {
    "1aLAGE Immobilienpodcast": "1alage",
    "1aLage - Der Immobilienpodcast": "1alage",
    "1aLage - Der Immobilienpodcast (alter Name)": "1alage",
    "Alexander Schmid Podcast": "alexander-schmid",
    "Der Ex-Makler": "ex-makler",
    "Finanzfluss": "finanzfluss",
    "Immocation": "immocation",
    "Insights Immo": "insights-immo",
    "Mr. Steuer": "mr-steuer",
    "Mr Steuer": "mr-steuer",
    "So geht Brandschutz": "so-geht-brandschutz",
    "Finanztip": "finanztip",
    "Marktcheck": "marktcheck",
}


def _resolve_channel_short(channel_name: str) -> str:
    """Map a YouTube channel's display name → the short code used in the
    filename. Falls back to a slugified lower-case version of the name."""
    if channel_name in CHANNEL_NAME_SHORT:
        return CHANNEL_NAME_SHORT[channel_name]
    return re.sub(r"[^a-z0-9-]+", "-", channel_name.lower()).strip("-") or "unknown"


def _slug(text: str, max_len: int = 60) -> str:
    s = re.sub(r"[^\w\-]+", "-", text.lower()).strip("-")
    return s[:max_len] or "untitled"


def step_write_vault(digests, repo_root: str, date_str: str | None = None,
                     wipe: bool = True,
                     content_kind: str = "longform") -> dict:
    """Wipe + write all digests as Markdown under podcast-kb/vault/Daily/<date>/.

    Args:
        digests: Iterable of VideoDigest (or anything with the same fields).
        repo_root: Repository root.
        date_str: Daily folder name (``YYYY-MM-DD``); defaults to today UTC.
        wipe: If True (default), remove the daily folder before writing so
            re-runs don't accumulate stale files.
        content_kind: ``"short-summary"`` or ``"longform"`` (default). The
            value is appended to the file slug (``_summary.md`` vs
            ``_longform.md``) so the same date folder can hold both modes
            side-by-side — daily ``--mode short`` writes ``_summary.md``,
            while on-demand ``--mode long`` (via
            ``pipeline/scripts/recommend_long_form.py confirm``) writes
            ``_longform.md``. (Task 7, 2026-08-09.)

    Returns a summary dict with counts and any failed files.
    """
    repo = Path(repo_root)
    if date_str is None:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = repo / "podcast-kb" / "vault" / "Daily" / date_str
    if wipe and out_dir.exists():
        # Task 7: only wipe files of *this* content_kind, so a short-summary
        # run can't accidentally delete a long-form artifact (and vice
        # versa). When the folder is brand-new we still wipe it whole to
        # preserve the legacy behavior of clearing stale daily folders.
        suffix = "_summary.md" if content_kind == "short-summary" else "_longform.md"
        for p in out_dir.glob(f"*{suffix}"):
            p.unlink()
        # Task 14 (2026-08-10): also wipe _index.md so it always reflects the
        # current run's digests. Previously the `if not index_path.exists()`
        # guard below meant a stale _index.md from an earlier run (e.g. a
        # different channel mix, or a previous day's leftover) would survive
        # the wipe and silently keep pointing at old content even after
        # `*_summary.md` / `*_longform.md` had been replaced.
        index_path = out_dir / "_index.md"
        if index_path.exists():
            index_path.unlink()
    out_dir.mkdir(parents=True, exist_ok=True)

    summary = {"date": date_str, "written": [], "skipped": [], "errors": []}

    suffix = "_summary" if content_kind == "short-summary" else "_longform"
    render_fn = _render_short_md if content_kind == "short-summary" else _render_digest_md
    for d in digests:
        try:
            channel_short = _resolve_channel_short(d.channel_name)
            # Plan 5 (2026-08-18): new filename schema —
            # {date}_{channel_short}_{kind}_{video_id}.md
            fname = f"{date_str}_{channel_short}_{suffix}_{d.video_id}.md"
            path = out_dir / fname
            content = render_fn(d)
            # Defense in depth: force_traditional on the full content
            if has_simplified(content):
                fixed = force_traditional(content)
                content = fixed[0] if isinstance(fixed, tuple) else fixed
            path.write_text(content, encoding="utf-8")
            summary["written"].append(str(path.relative_to(repo)))
        except Exception as e:  # noqa: BLE001
            summary["errors"].append({"digest": d.video_id, "error": str(e)})

    # Index file (shared across short + long summaries in the same folder).
    # Task 14: always rewrite so the index stays consistent with whatever
    # `*_summary.md` / `*_longform.md` files actually exist. The wipe step
    # above already removed any stale _index.md.
    index_path = out_dir / "_index.md"
    index_path.write_text(_render_index(digests, date_str), encoding="utf-8")
    summary["written"].append(str(index_path.relative_to(repo)))

    summary["n_files"] = len(summary["written"])
    summary["n_errors"] = len(summary["errors"])
    summary["content_kind"] = content_kind
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


def _render_short_md(d) -> str:
    """Compact summary + bullets + view + vocab (Task 9 short-summary).

    The daily pipeline runs in ``--mode short`` by default to save LLM
    tokens. The structure prompt now also asks for 3-5 個 vocab (Task 9),
    so we render the vocabulary section too. The section is OPTIONAL —
    if the LLM left it empty (e.g. non-real-estate video), the vault
    shows ``(無)``.

    The LLM call lives in :func:`pipeline.youtube_daily.step_structure_short`
    and writes back into ``d.summary_zh`` (200-char summary), ``d.analyst_zh``
    (3-5 bullets), ``d.producer_zh`` (1-2 sentences), and ``d.vocab_zh``
    (3-5 德文術語 with 中文 translation).
    """
    duration_min = d.duration_sec // 60
    duration_sec = d.duration_sec % 60
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
        "## 一句話摘要",
        "",
        d.summary_zh or "（無）",
        "",
        "## 重點 bullets",
        "",
        d.analyst_zh or "（無）",
        "",
        "## 觀點",
        "",
        d.producer_zh or "（無）",
        "",
        "## 重點詞彙",
        "",
        d.vocab_zh or "（無）",
        "",
        "---",
        "",
        "_本檔案由 podcast-kb pipeline（short 模式）自動產生於 "
        f"{datetime.now(timezone.utc).isoformat()}_",
    ]
    return "\n".join(parts)
