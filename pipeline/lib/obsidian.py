"""Obsidian vault writer.

Produces markdown files in vault/Daily/YYYY-MM-DD/ and vault/YouTube/{Channel}/
with YAML frontmatter so Dataview queries and Obsidian plugins can read them.
"""
import os
import shutil
import re
import sys
from datetime import datetime
from urllib.parse import urlparse


def _check_traditional(item, strict=True):
    """DEPRECATED — superseded by _validate_against_system_prompt below which
    runs the full SYSTEM-prompt rule set (including simplified Chinese)."""
    raise NotImplementedError("Use _validate_against_system_prompt instead.")


def _validate_against_system_prompt(item, strict=True):
    """Run the full SYSTEM-prompt validation gate. Raises ValueError on any
    'error'-level issue, logs warnings, when strict=True.
    """
    from pipeline.lib.translate import validate_zh_item, has_errors, filter_errors
    issues = validate_zh_item(item)
    errors = filter_errors(issues)
    warnings = [i for i in issues if i[1] == "warn"]
    for rule, sev, msg in warnings:
        print(f"[obsidian.gate] WARN: {rule}: {msg}", file=sys.stderr)
    if errors and strict:
        lines = "\n".join(f"  - {r}: {m}" for r, _s, m in errors)
        src = item.get("source_name", "?")
        title = (item.get("title") or "")[:60]
        raise ValueError(
            f"[obsidian.gate] REJECT item from {src} ({title!r}):\n{lines}"
        )


def _slugify(text, max_len=80):
    """ASCII-safe slug for filenames."""
    s = re.sub(r"[^a-zA-Z0-9äöüÄÖÜß-]+", "-", text)
    s = re.sub(r"-+", "-", s).strip("-").lower()
    return s[:max_len].rstrip("-")


# Map of content_kind → filename suffix. Module-level so other modules
# (e.g. ``news_daily.step_write_vault``) can look up the suffix for a
# batch wipe without duplicating the mapping.
#
# Plan 1 (2026-08-17) added:
#   - ``paywall-preview``         → ``_paywallpreview``
#   - ``short-paywall-preview``   → ``_shortpaywallpreview``
# These coexist with the legacy ``_summary`` and ``_longform`` suffixes;
# all four can live side-by-side in the same daily folder.
_CONTENT_KIND_SUFFIX = {
    "short-summary":          "_summary",
    "longform":               "_longform",
    "paywall-preview":        "_paywallpreview",
    "short-paywall-preview":  "_shortpaywallpreview",
}


def write_news_item(item, vault_root, date_str, strict_traditional=True,
                     content_kind: str = "longform"):
    """Write a single news item as a markdown file in vault/Daily/{date}/.
    Returns the absolute path written.

    Raises ValueError if the item fails the SYSTEM-prompt validation gate
    (unless `strict_traditional=False`, in which case it logs and continues).

    ``content_kind`` (Task 7, 2026-08-09): when ``"short-summary"`` (or
    anything other than the legacy ``"longform"``), the file is suffixed
    ``_summary.md`` instead of the legacy ``.md``/``_longform.md``. This
    lets a daily ``--mode short`` news run coexist with on-demand
    ``--mode long`` outputs in the same date folder.

    Plan 1 (2026-08-17): ``content_kind`` accepts two new values for
    paywall-preview items so the suffix explicitly tags the kind:
      - ``"paywall-preview"``       → ``_paywallpreview.md``
      - ``"short-paywall-preview"`` → ``_shortpaywallpreview.md``
    Both fall back to ``_longform`` for unknown content_kind values.
    """
    # Gate: run every SYSTEM-prompt rule. Reject if any error-level fails.
    _validate_against_system_prompt(item, strict=strict_traditional)

    # SKILL MODE (2026-08-07): no force_traditional fallback here. The LLM is
    # responsible for emitting Taiwan Traditional Chinese directly (constrained
    # by the SYSTEM prompt + temperature=0.1). If a simplified character
    # sneaks through, validate_zh_item() above will catch it via has_simplified
    # and raise, prompting re-translation. See batch-llm-translate-short-items
    # skill: rely on prompt discipline, not post-hoc patching.

    out_dir = os.path.join(vault_root, "Daily", date_str)
    # NOTE: We do NOT wipe the daily folder here — that would destroy items
    # written by previous calls in the same run. The wipe is done ONCE by
    # news_daily.step_write_vault() before the loop begins.
    os.makedirs(out_dir, exist_ok=True)

    src_id = item.get("source_id", "unknown")
    title_slug = _slugify(item.get("title", "untitled"), max_len=50)
    suffix = _CONTENT_KIND_SUFFIX.get(content_kind, "_longform")
    fname = f"{src_id}-{title_slug}{suffix}.md"
    path = os.path.join(out_dir, fname)

    # YAML frontmatter
    pub = item.get("pub_date")
    pub_str = pub.isoformat() if pub else ""
    tags_yaml = ", ".join(item.get("tags", []))
    entities = item.get("entities", {}) or {}
    ent_lines = []
    for k, vs in entities.items():
        if vs:
            ent_lines.append(f"- **{k}**: {', '.join(vs) if isinstance(vs, list) else vs}")
    ent_block = "\n".join(ent_lines) if ent_lines else "_（無顯著實體）_"

    body = f"""---
type: news
source: {item.get('source_name', '')}
source_id: {src_id}
url: {item.get('url', '')}
date: {date_str}
fetched: {datetime.utcnow().strftime('%Y-%m-%d')}
title_de: "{item.get('title', '').replace(chr(34), '')}"
title_zh: "{item.get('title_zh', '').replace(chr(34), '')}"
content_kind: {content_kind}
tags: [{tags_yaml}]
priority: {item.get('priority', 99)}
relevance_rank: {item.get('relevance_rank', 0)}
{f'''paywall_preview_kept: {str(bool(item.get('_paywall_preview_kept'))).lower()}
paywall_preview_kind: "{item.get('_paywall_preview_kind', '')}"
''' if item.get('_paywall_preview_kept') else ''}---

# {item.get('title', '')}

## 摘要

{item.get('summary_zh', '')}

## 德文原文摘要

> {item.get('summary', '')}

## 關鍵實體

{ent_block}

## 原文連結

{item.get('url', '')}
"""
    with open(path, "w", encoding="utf-8") as f:
        f.write(body)
    return path


def write_daily_index(items, vault_root, date_str, github_url):
    """Write a daily summary file: vault/Daily/YYYY-MM-DD/_index.md
    This is the file the GitHub push and Discord digest will reference.
    """
    out_dir = os.path.join(vault_root, "Daily", date_str)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "_index.md")

    by_source = {}
    for it in items:
        s = it.get("source_name", "?")
        by_source.setdefault(s, []).append(it)

    body = f"""---
type: daily_digest
date: {date_str}
total_items: {len(items)}
sources_count: {len(by_source)}
---

# 德國房地產每日頭條 — {date_str}

共 **{len(items)} 則新聞** | 來源 **{len(by_source)} 家媒體** | 去重後

## 本日彙整

"""
    for i, it in enumerate(items, 1):
        body += f"### {i}. {it.get('title_zh') or it.get('title', '')}\n\n"
        body += f"- **來源**: {it.get('source_name', '')}\n"
        body += f"- **德文標題**: {it.get('title', '')}\n"
        if it.get('summary_zh'):
            body += f"- **摘要**: {it.get('summary_zh', '')[:200]}\n"
        body += f"- **連結**: {it.get('url', '')}\n\n"

    body += "\n## 媒體分布\n\n"
    for s, lst in sorted(by_source.items(), key=lambda kv: -len(kv[1])):
        body += f"- **{s}**: {len(lst)} 則\n"

    body += f"\n## GitHub\n\nhttps://github.com/{github_url}/blob/main/immobilien-kb/vault/Daily/{date_str}/\n"

    with open(path, "w", encoding="utf-8") as f:
        f.write(body)
    return path
