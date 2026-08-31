"""Obsidian vault writer.

Produces markdown files in vault/Daily/YYYY-MM-DD/ with YAML frontmatter so
Dataview queries and Obsidian plugins can read them.

Plan 5 (2026-08-18): filename schema switched to the new format
    ``{YYYY-MM-DD}_{src_short}_{kind_token}_{slug}.md``
e.g. ``2026-08-18_hbl_longform_rueckgang-im-sueden-preise-fuer.md``.

The ``source_id`` field in the frontmatter is preserved unchanged so downstream
readers (ProcessedContent ledger, daily_digest) keep working.
"""
import os
import re
import sys
from datetime import datetime


# Source ID (canonical id used by RSS fetchers / ProcessedContent ledger) →
# short code used inside the vault filename. Mirrors the constants in
# ``/tmp/vault_path_map_dryrun.py`` v3 so a newly-written Daily file can be
# read back by ``push_vault_to_channels`` / ``daily_digest`` without a
# second translation pass.
#
# Adding a new source?  Add the long id here AND in vault_path_map_dryrun.SOURCE_SHORT.
SOURCE_SHORT = {
    "handelsblatt_immobilien": "hbl", "handelsblatt": "hbl",
    "faz_immobilien": "faz", "faz": "faz",
    "ntv_wirtschaft": "ntv", "ntv": "ntv",
    "spiegel_immobilien": "spiegel", "spiegel": "spiegel",
    "zeit_wirtschaft": "zeit", "zeit": "zeit",
    "google_news_immobilien": "gn",
    "wohnen": "r-wohnen",
    "finanzen": "r-finanzen",
    "immoscoutwildgeworden": "r-immoscout",
    "immobilieninvestments": "r-immobinv",
    "1alage-immobilienpodcast": "1alage",
    "alexander-schmid-podcast": "alexander-schmid",
    "der-ex-makler": "ex-makler",
    "finanzfluss": "finanzfluss",
    "immocation": "immocation",
    "insights-immo": "insights-immo",
    "mr-steuer": "mr-steuer",
    "so-geht-brandschutz": "so-geht-brandschutz",
    "auftragseingang_bauhauptgewerbe": "destatis",
    "genehmigte_wohnungen_monat": "destatis",
    "investments_construction": "destatis",
    "destatis_csv": "destatis",
}


def _resolve_src_short(item: dict) -> str:
    """Map a news item dict → the short source code used in filenames.

    Order of resolution:
      1. ``item["source_id"]``  (canonical id from RSS fetchers)
      2. ``item["source"]`` / ``item["source_name"]``  (human name, lowercased)
      3. ``"unknown"``

    The short code is also written into the YAML frontmatter as
    ``src_short`` so the pusher can read it back without re-resolving.
    """
    sid = (item.get("source_id") or "").lower()
    if sid in SOURCE_SHORT:
        return SOURCE_SHORT[sid]
    sname = (item.get("source") or item.get("source_name") or "").lower()
    if sname in SOURCE_SHORT:
        return SOURCE_SHORT[sname]
    return "unknown"


# Plan 5 (2026-08-18): kind tokens that go inside the filename. There is no
# default fallback — ``write_news_item`` raises if you pass an unknown kind.
_KIND_TOKEN = {
    "longform":               "longform",
    "short-summary":          "summary",
    "paywall-preview":        "paywallpreview",
    "short-paywall-preview":  "shortpaywallpreview",
}


# Plan 1 / Plan 10 (2026-08-19): the wipe logic in
# ``news_daily.step_write_vault`` uses this map to glob ``*{suffix}.md``
# files under ``vault/Daily/<date>/``. Each token is the Plan-5
# ``_KIND_TOKEN`` value prefixed with ``_`` so the glob does not match
# the date prefix in the filename (e.g. ``2026-08-19_hbl_longform_*.md``).
# Adding a new ``_KIND_TOKEN`` value requires adding a matching suffix
# here.
_CONTENT_KIND_SUFFIX = {
    "longform":              "_longform",
    "short-summary":         "_summary",
    "paywall-preview":       "_paywallpreview",
    "short-paywall-preview": "_shortpaywallpreview",
}


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


def _slugify(text, max_len=40):
    """ASCII-safe slug for filenames.

    Plan 5 (2026-08-18): German umlauts are transliterated BEFORE
    lowercasing so titles like ``Rückgang`` become ``rueckgang`` and
    not ``rückgang`` (which the regex below would have collapsed to
    ``rückgang`` and then lost in the second ``[^a-z0-9-]`` pass).

    If the slug still exceeds ``max_len`` after stripping punctuation,
    we rsplit on the last ``-`` so the cut lands on a word boundary.
    """
    if not text:
        return "untitled"
    t = text
    t = t.replace("Ä", "Ae").replace("Ö", "Oe").replace("Ü", "Ue")
    t = t.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue")
    t = t.replace("ß", "ss")
    s = re.sub(r"[^a-zA-Z0-9-]+", "-", t)
    s = re.sub(r"-+", "-", s).strip("-").lower()
    if len(s) > max_len:
        s = s[:max_len].rsplit("-", 1)[0].rstrip("-")
    return s or "untitled"


def write_news_item(item, vault_root, date_str, strict_traditional=True,
                     content_kind: str = "longform"):
    """Write a single news item as a markdown file in vault/Daily/{date}/.

    Plan 5 (2026-08-18): filename schema is
        ``{YYYY-MM-DD}_{src_short}_{kind_token}_{slug}.md``
    e.g. ``2026-08-18_hbl_longform_rueckgang-im-sueden-preise-fuer.md``.

    The YAML frontmatter still carries ``source_id`` (canonical id) and
    gains a new ``src_short`` field so downstream readers can round-trip
    the short code without re-resolving.

    Returns the absolute path written.

    Raises ValueError if the item fails the SYSTEM-prompt validation gate
    (unless ``strict_traditional=False``, in which case it logs and continues).

    ``content_kind`` accepts the four Plan-5 tokens:
      - ``"longform"``               → ``_longform.md``
      - ``"short-summary"``          → ``_summary.md``
      - ``"paywall-preview"``        → ``_paywallpreview.md``
      - ``"short-paywall-preview"``  → ``_shortpaywallpreview.md``
    Unknown kinds raise ``ValueError`` (no silent fallback — silent
    fallbacks were the source of the legacy ``_longform`` confusion).
    """
    # Gate: run every SYSTEM-prompt rule. Reject if any error-level fails.
    _validate_against_system_prompt(item, strict=strict_traditional)

    if content_kind not in _KIND_TOKEN:
        raise ValueError(
            f"[obsidian] unknown content_kind={content_kind!r} "
            f"(expected one of {sorted(_KIND_TOKEN)})"
        )
    kind_token = _KIND_TOKEN[content_kind]

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
    src_short = _resolve_src_short(item)
    title_slug = _slugify(item.get("title", "untitled"), max_len=40)
    fname = f"{date_str}_{src_short}_{kind_token}_{title_slug}.md"
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
src_short: {src_short}
url: {item.get('url', '')}
date: {date_str}
fetched: {datetime.utcnow().strftime('%Y-%m-%d')}
title_de: "{item.get('title', '').replace(chr(34), '')}"
title_zh: "{item.get('title_zh', '').replace(chr(34), '')}"
content_kind: {content_kind}
kind_token: {kind_token}
tags: [{tags_yaml}]
priority: {item.get('priority', 99)}
relevance_rank: {item.get('relevance_rank', 0)}
{f'''paywall_preview_kept: {str(bool(item.get('_paywall_preview_kept'))).lower()}
paywall_preview_kind: "{item.get('_paywall_preview_kind', '')}"
|''' if item.get('_paywall_preview_kept') else ''}---

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

    Plan 9 (2026-08-31): defensive URL dedup before counting. The earlier
    ``filter_processed`` step already enforces intra-batch dedup, but
    this is a cheap belt-and-braces guarantee that the index's
    ``total_items`` always matches the number of files written by
    ``write_news_item`` (which deduplicates by slug and would otherwise
    overwrite — leaving index saying 5 but vault only having 3).
    """
    out_dir = os.path.join(vault_root, "Daily", date_str)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "_index.md")

    # Plan 9: collapse any same-URL items before counting + listing.
    # We normalise the URL the same way filter_processed does so a
    # utm/fragment variant doesn't sneak in twice. First-occurrence wins.
    from pipeline.lib.processed_store import normalize_url
    seen_urls: set = set()
    unique_items: list = []
    for it in items:
        norm = normalize_url((it.get("url") or "").strip())
        key = norm or f"__no_url__{id(it)}"
        if key in seen_urls:
            continue
        seen_urls.add(key)
        unique_items.append(it)
    items = unique_items

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
