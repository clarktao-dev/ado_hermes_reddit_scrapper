"""LLM scoring module for the long-form YouTube recommender.

Pulls the past N days of YouTube records from ProcessedContent, runs them
through a title-level rule-based pre-filter, then asks the LLM to score
each surviving candidate 1-10 on how much a German Immo reader would
benefit from a long-form analysis.

Follows the batch-llm-agent-step pattern from translate.py:
  - module-level cooldown limiter (3s min, 8s after 8 calls)
  - 3 retries with exponential backoff 5s / 10s / 20s
  - except Exception in retry (don't drop items on transient LLM errors)
  - per-item fallback when batch fails
  - never drop a record — failed chunks emit safe-default entries
"""
from __future__ import annotations

import json
import os
import sys
import time
from typing import Any

# Make reddit-safe importable (same path translate.py uses).
_REDDIT_SAFE_SRC = "/root/reddit-safe/src"
if _REDDIT_SAFE_SRC not in sys.path:
    sys.path.insert(0, _REDDIT_SAFE_SRC)

from reddit_safe.pipeline.llm_client import call_json  # noqa: E402

from pipeline.lib.llm_cooldown import LLMCooldown  # noqa: E402
from pipeline.lib.processed_store import parse_metadata  # noqa: E402


# ---------------------------------------------------------------------------
# Criteria
# ---------------------------------------------------------------------------

CRITERIA_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "config", "long_form_criteria.json"
)

# Defaults applied before reading the JSON — JSON overrides any of these
# that it cares to set. Single source of truth for the four tunables.
DEFAULTS: dict = {
    "version": "embedded-defaults",
    "min_score": 6,
    "max_recommendations": 3,
    "days_back": 3,
    "max_input_per_run": 30,
    "prefilter_keywords_drop": [],
    "excluded_article_types": [
        "long-form",
        "pending-long-form",
        "skipped-long-form",
    ],
}


def load_criteria() -> dict:
    """Load pipeline/lib/config/long_form_criteria.json with defaults.

    Returns a fresh dict per call (so callers can mutate safely), seeded
    from DEFAULTS and overridden by values in the JSON file.
    """
    try:
        with open(CRITERIA_PATH, encoding="utf-8") as f:
            from_disk = json.load(f)
    except FileNotFoundError:
        from_disk = {}
    return {**DEFAULTS, **from_disk}


# ---------------------------------------------------------------------------
# Record extraction
# ---------------------------------------------------------------------------


def _parse_metadata(raw: Any) -> dict:
    """Backward-compat alias for ``processed_store.parse_metadata``."""
    return parse_metadata(raw)


def _extract_record(rec: dict) -> dict | None:
    """Flatten an Airtable record → {source_id, title, channel_name, first_seen_at}.

    Field fallback rules (from real-estate-intelligence-pipeline skill):
      - title: required, drop record if empty
      - source_id: required
      - first_seen_at: ISO with Z
      - channel_name: try metadata.channel_name; else channels[0].split('.',1)[1]
    """
    f = rec.get("fields", {}) or {}
    title = (f.get("title") or "").strip()
    if not title:
        return None
    source_id = (f.get("source_id") or "").strip()
    first_seen_at = (f.get("first_seen_at") or "").strip()

    meta = _parse_metadata(f.get("metadata"))
    channel_name = (meta.get("channel_name") or "").strip()

    if not channel_name:
        channels = f.get("channels") or []
        if channels:
            first = str(channels[0])
            if "." in first:
                channel_name = first.split(".", 1)[1]
            else:
                channel_name = first

    return {
        "source_id": source_id,
        "title": title,
        "channel_name": channel_name or "?",
        "first_seen_at": first_seen_at,
        "record_id": rec.get("id", ""),
    }


# ---------------------------------------------------------------------------
# Pre-filter
# ---------------------------------------------------------------------------


def prefilter(records: list[dict], criteria: dict) -> tuple[list[dict], list[dict]]:
    """Apply ``prefilter_keywords_drop`` to title (case-insensitive substring).

    Accepts either the flattened shape (top-level ``title``) or the raw
    Airtable record (nested ``fields.title``). Always returns the
    flattened shape in both branches so the caller can hand survivors
    straight to ``score_candidates``.

    Returns (survivors, dropped_with_reason). Reason is a short string
    naming the keyword that triggered the drop.
    """
    keywords = [k.lower() for k in (criteria.get("prefilter_keywords_drop") or [])]
    survivors: list[dict] = []
    dropped: list[dict] = []

    for r in records or []:
        # Normalize to flat shape (top-level title / source_id / etc).
        if "fields" in r and "title" not in r:
            flat = _extract_record(r)
            if flat is None:
                continue
            title_lower = flat["title"].lower()
            base = flat
        else:
            title_lower = (r.get("title") or "").lower()
            base = {
                "source_id": r.get("source_id", ""),
                "title": r.get("title", ""),
                "channel_name": r.get("channel_name", "?"),
                "first_seen_at": r.get("first_seen_at", ""),
                "record_id": r.get("record_id", ""),
            }

        hit = next((k for k in keywords if k in title_lower), None)
        if hit:
            dropped.append({**base, "reason": f"prefilter keyword: {hit}"})
        else:
            survivors.append(base)
    return survivors, dropped


# ---------------------------------------------------------------------------
# LLM scoring
# ---------------------------------------------------------------------------

SCORING_SYSTEM_PROMPT = """You are scouting for a German-language real-estate newsletter editor.
Score each candidate 1-10 on how much a real-estate-curious German reader (looking to buy, sell, rent, or invest) would benefit from a long-form analysis of this video.

High signal (7-10):
- Interest rate changes, ECB / Bundesbank policy
- BImA, Grundsteuer, GEG, Energieausweis, Mietrecht, WEG legal changes
- Regional market reports (Stuttgart, München, Berlin, Frankfurt, Hamburg, Köln, Düsseldorf)
- Baukosten, Baupreise, Baugenehmigungen shifts
- Sanierung, energetische Sanierung, Energieeffizienz
- Klumpenrisiko, Portfolio diversification across asset classes
- Wirtschaftsweise / Sachverständigenrat reports with Immo implication
- 1aLAGE, ex_makler, matic, Immo-spezifische Makler / Analysten

Medium signal (4-6):
- Macro economics with Immo spillover (7-year stagnation, recession)
- Retirement savings that mention Immobilie as alternative
- Generic investment advice with property mentioned

Low signal (1-3):
- Pure stock picking, ETF allocation, Scalable Capital
- Pizza ovens, kitchen gadgets, perfumes, lifestyle
- Trade-fair recordings, conference vlogs
- US-centric real estate (English-language, USD, US tax)

Write the rationale in **Traditional Chinese (Taiwan)** — 1-2 sentences explaining the value to a German real estate reader. Avoid English unless using a technical term that has no good Chinese equivalent.

Return JSON array, one object per input in order:
[{"source_id": "...", "title": "...", "score": N, "rationale": "1-2 sentences on why this is / isn't valuable to a German Immo reader"}, ...]
Sort by score desc internally but return in INPUT order (we'll sort in Python)."""


def _build_scoring_messages(chunk: list[dict]) -> list[dict]:
    """Build messages for one chunk. Number items [1]...[N]."""
    payload = [
        {
            "n": i + 1,
            "source_id": r.get("source_id", ""),
            "title": r.get("title", ""),
            "channel": r.get("channel_name", "?"),
        }
        for i, r in enumerate(chunk)
    ]
    user = (
        "Score each of the following YouTube candidates 1-10 for a German "
        "real-estate newsletter. Return a JSON array of length "
        f"{len(chunk)} in INPUT order. Each object needs source_id, title, "
        "score, rationale.\n\n"
        + json.dumps(payload, ensure_ascii=False)
    )
    return [
        {"role": "system", "content": SCORING_SYSTEM_PROMPT},
        {"role": "user", "content": user},
    ]


def _parse_scoring_response(text: str, expected_len: int) -> list[dict] | None:
    """Parse LLM JSON response. Strip ```json fences first.

    Returns list[dict] on success, None on parse error or length mismatch.
    """
    if not text:
        return None
    cleaned = text.strip()
    # Strip ```json ... ``` fences (LLMs sometimes wrap the array).
    if cleaned.startswith("```"):
        first_nl = cleaned.find("\n")
        if first_nl != -1:
            cleaned = cleaned[first_nl + 1:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        cleaned = cleaned.strip()
    try:
        parsed = json.loads(cleaned)
    except Exception:
        return None
    if not isinstance(parsed, list) or len(parsed) != expected_len:
        return None
    return parsed


def _safe_default_for(r: dict, reason: str) -> dict:
    """Return a safe-default scored record (score=0, no real signal)."""
    return {
        "source_id": r.get("source_id", ""),
        "title": r.get("title", ""),
        "channel_name": r.get("channel_name", "?"),
        "first_seen_at": r.get("first_seen_at", ""),
        "record_id": r.get("record_id", ""),
        "score": 0,
        "rationale": reason,
    }


def _call_with_retry(messages: list[dict]) -> Any:
    """Call call_json with cooldown + exponential backoff. Raises on final failure.

    Pacing: the LLMCooldown context manager (pipeline.lib.llm_cooldown)
    enforces the inter-call gap (3s pre-call #8, 8s after). Retries on
    failure sleep 5s then 10s between attempts.
    """
    last_err: Exception | None = None
    for attempt in range(3):
        try:
            with LLMCooldown():
                parsed, _usage = call_json(messages, temperature=0.0)
            return parsed
        except Exception as e:
            last_err = e
        if attempt < 2:
            time.sleep(5 * (2 ** attempt))  # 5s, 10s
    assert last_err is not None
    raise last_err


def _score_one(item_messages: list[dict], record: dict) -> dict:
    """Per-item fallback: single-record LLM call wrapped in retry."""
    try:
        parsed = _call_with_retry(item_messages)
    except Exception as e:
        return _safe_default_for(record, f"LLM unavailable: {type(e).__name__}")
    if not isinstance(parsed, list) or len(parsed) != 1:
        return _safe_default_for(record, "LLM returned unexpected shape")
    obj = parsed[0]
    try:
        score = int(obj.get("score", 0))
    except (TypeError, ValueError):
        score = 0
    return {
        "source_id": record.get("source_id", ""),
        "title": record.get("title", ""),
        "channel_name": record.get("channel_name", "?"),
        "first_seen_at": record.get("first_seen_at", ""),
        "record_id": record.get("record_id", ""),
        "score": score,
        "rationale": (obj.get("rationale") or "").strip() or "（無）",
    }


def score_candidates(records: list[dict], criteria: dict) -> list[dict]:
    """Score each record via LLM batch (chunk_size=4).

    Returns list of dicts with original record fields merged in:
      [{source_id, title, channel_name, first_seen_at, score, rationale, record_id}, ...]

    Failed chunks → safe-default entries (score=0, rationale="LLM unavailable").
    Per-item fallback when batch fails.
    Never drop a record.
    """
    if not records:
        return []
    chunk_size = 4  # per real-estate-intelligence-pipeline skill pitfall 7
    out: list[dict] = []
    for chunk in (records[i:i + chunk_size] for i in range(0, len(records), chunk_size)):
        messages = _build_scoring_messages(chunk)
        parsed: Any | None = None
        try:
            parsed = _call_with_retry(messages)
        except Exception:
            parsed = None

        # shape check + parse defensively
        results: list[dict] | None = None
        if isinstance(parsed, list) and len(parsed) == len(chunk):
            results = []
            for rec, obj in zip(chunk, parsed):
                try:
                    score = int(obj.get("score", 0))
                except (TypeError, ValueError):
                    score = 0
                results.append({
                    "source_id": rec.get("source_id", ""),
                    "title": rec.get("title", ""),
                    "channel_name": rec.get("channel_name", "?"),
                    "first_seen_at": rec.get("first_seen_at", ""),
                    "record_id": rec.get("record_id", ""),
                    "score": score,
                    "rationale": (obj.get("rationale") or "").strip() or "（無）",
                })
        if results is None:
            # batch failed entirely → per-item fallback (preserves count)
            results = []
            for rec in chunk:
                item_messages = _build_scoring_messages([rec])
                results.append(_score_one(item_messages, rec))
        out.extend(results)
        # Cooldown between chunks is handled by LLMCooldown inside _call_with_retry.
    return out


# ---------------------------------------------------------------------------
# Markdown rendering
# ---------------------------------------------------------------------------


def _youtube_url(source_id: str) -> str:
    if not source_id:
        return ""
    return f"https://www.youtube.com/watch?v={source_id}"


def render_markdown(recommendations: list[dict], excluded: list[dict],
                    criteria: dict, prefilter_count: int = 0) -> str:
    """Build the output markdown report (Traditional Chinese).

    - If recommendations empty: write "本期 0 推薦" with excluded list
    - Else: per-recommendation sections (rationale, score, URL, channel, date)
    - Always include a generation footer with criteria version + date

    Args:
        recommendations: top-scoring records that met the threshold.
        excluded: records dropped by the title pre-filter (prefilter keywords).
        criteria: loaded criteria dict.
        prefilter_count: total records that survived the pre-filter (i.e. were
            fed to the LLM). Use this for the "共 N 筆" header so the count
            reflects what was actually evaluated, not just the recommended
            subset. Falls back to len(excluded)+len(recommendations) for
            backwards compatibility.
    """
    today = time.strftime("%Y-%m-%d")
    version = criteria.get("version", "?")
    min_score = criteria.get("min_score", 6)

    # Fall back to the old (undercounting) sum if caller didn't pass it.
    total = prefilter_count if prefilter_count else (len(excluded) + len(recommendations))

    lines: list[str] = []
    if not recommendations:
        lines.append(f"# 📚 本週 Long-form 候選清單 ({today})")
        lines.append("")
        lines.append(
            f"> 過去 3 天 YouTube 共 {total} 筆,"
            f"預篩選後 {total} 筆給 LLM 評分,"
            f"0 筆達到推薦門檻(>= {min_score}/10)。"
        )
        lines.append("")
        lines.append("**本期 0 推薦**。原因:過去 3 天新上影片中沒有符合房地產讀者"
                     "長期價值的內容(利率 / 政策 / 區域行情 / 法律變動 / 能源法規 /"
                     "專業裝修知識)。")
        lines.append("")
        if excluded:
            lines.append("預篩選掉的清單(供 user 過目):")
            for d in excluded:
                lines.append(f"- {d.get('title', '?')} — `{d.get('reason', '?')}`")
            lines.append("")
    else:
        lines.append(f"# 📚 本週 Long-form 候選清單 ({today})")
        lines.append("")
        lines.append(
            f"> 過去 3 天 YouTube 共 {total} 筆,"
            f"預篩選後 {total} 筆給 LLM 評分,"
            f"{len(recommendations)} 筆達到推薦門檻(>= {min_score}/10)。"
        )
        lines.append("")
        for i, rec in enumerate(recommendations, 1):
            url = _youtube_url(rec.get("source_id", ""))
            lines.append(f"## {i}. {rec.get('title', '?')}")
            lines.append(f"- **Score**: {rec.get('score', 0)}/10")
            lines.append(f"- **Channel**: {rec.get('channel_name', '?')}")
            lines.append(f"- **First seen**: {rec.get('first_seen_at', '?')}")
            if url:
                lines.append(f"- **URL**: {url}")
            lines.append(f"- **Rationale**: {rec.get('rationale', '（無）')}")
            lines.append(f"- **Record ID**: `{rec.get('record_id', '?')}`")
            lines.append("")

    lines.append("---")
    lines.append("")
    lines.append(f"_Generated: {today} · criteria version: {version} · "
                 f"min_score={min_score} · max_recommendations="
                 f"{criteria.get('max_recommendations', 3)} · "
                 f"days_back={criteria.get('days_back', 3)}_")
    return "\n".join(lines) + "\n"
