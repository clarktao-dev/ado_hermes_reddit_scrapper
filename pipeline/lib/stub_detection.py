"""Detect "stub" content in transcripts and LLM-generated summaries.

A "stub" is text that looks like a legitimate response but contains no real
content — instead it carries an error message (HTML error page, server error
banner, "video unavailable" placeholder, etc.).

Two kinds of stubs exist in the YouTube daily pipeline:

1. **Transcript stub** — kome.ai / Invidious returned an Error 500 HTML page
   instead of a transcript. The transcript cache stores this as if it were
   real text. (Historical: 8/25-8/27 cron runs flagged these.)

2. **LLM summary stub** — the LLM receives a perfectly good transcript,
   Google-translates it to zh, but the LLM *hallucinates* a placeholder like
   "影片因伺服器錯誤無法載入" as its one-sentence summary. Discovered
   2026-08-27: cache was healthy, but LLM still produced a stub summary.

Both kinds must be detected so the pipeline can short-circuit (skip video)
or re-prompt (force LLM to summarise the actual content). This module is
the single source of truth for the regex set — callers should never inline
their own "Error 500" / "伺服器錯誤" checks.

Usage::

    from pipeline.lib import stub_detection

    if stub_detection.is_stub_transcript(de_text):
        # treat as empty transcript, move on
        ...

    if stub_detection.is_stub_summary(zh_summary):
        # re-prompt or downgrade to (no content)
        ...
"""
from __future__ import annotations

import re
from typing import Final


# --------------------------------------------------------------------------- #
# Transcript stubs (raw / pre-translation text — likely German or English)
# --------------------------------------------------------------------------- #

# HTML / server error pages masquerading as transcripts
_TRANSCRIPT_STUB_PATTERNS: Final[tuple[str, ...]] = (
    r"Error\s*500",
    r"500\s+Internal\s+Server\s+Error",
    r"Internal\s+Server\s+Error",
    r"<title>\s*500\b",          # ErrorPage HTML titles
    r"Server\s+Error",            # generic
    r"invidious.*unavailable",
    r"this video is unavailable",
    r"video\s+(is|not)\s+available",
    r"watch\s+on\s+youtube",      # Invidious placeholder footer
)


# --------------------------------------------------------------------------- #
# Summary stubs (zh-TW one-sentence summaries from LLM)
# --------------------------------------------------------------------------- #

# Phrases the LLM uses when it *thinks* it has no content.
# Each is intentionally broad — better to flag a borderline case than ship a stub.
_SUMMARY_STUB_PATTERNS: Final[tuple[str, ...]] = (
    r"伺服器錯誤",
    r"伺服器\s*錯誤",
    r"無法載入",
    r"無法取得",
    r"無法讀取",
    r"無法提供",
    r"無法(?:[一-龥]){1,3}取得",   # 「無法...取得」 with 1-3 CJK chars between
    r"影片(?:因|內容)(?:[一-龥]){1,5}無法",  # 「影片因...無法」 / 「影片內容...無法」
    r"影片僅顯示",
    r"內容無法",
    r"無法觀看",
    r"Error\s*500",
    r"placeholder",
    r"影片不在(?:可|在)",
    r"已(?:下架|刪除|移除)",
    r"字幕(?:檔|內容)?(?:無法|不(?:可|在))",
    r"目前無法(?:取得|觀看|載入)",
)


def _compile(patterns: tuple[str, ...]) -> re.Pattern[str]:
    return re.compile("|".join(patterns), re.IGNORECASE | re.DOTALL)


_TRANSCRIPT_STUB_RE: Final[re.Pattern[str]] = _compile(_TRANSCRIPT_STUB_PATTERNS)
_SUMMARY_STUB_RE: Final[re.Pattern[str]] = _compile(_SUMMARY_STUB_PATTERNS)


def is_stub_transcript(text: str, head_chars: int = 2000) -> bool:
    """Return True if ``text`` looks like a server error page, not a real
    transcript. Only inspects the first ``head_chars`` chars — error pages
    are usually tagged at the top (HTML <title>, error banner, etc.).

    Empty / whitespace-only text is also considered a stub — kome.ai returns
    ``text=''`` when the API call fails, and the empty transcript still
    produces a "no content" vault file downstream.
    """
    if not text or not text.strip():
        return True
    head = text[:head_chars]
    return bool(_TRANSCRIPT_STUB_RE.search(head))


def is_stub_summary(text: str, head_chars: int = 600) -> bool:
    """Return True if ``text`` looks like an LLM placeholder ("無法載入",
    "Error 500", ...) rather than an actual one-sentence summary.

    Only inspects the first ``head_chars`` chars. We use the head because the
    one-sentence summary lives in the very first paragraph.
    """
    if not text or not text.strip():
        return True
    head = text[:head_chars]
    return bool(_SUMMARY_STUB_RE.search(head))


def stub_reason(text: str, head_chars: int = 600) -> str:
    """For diagnostic logging: return the matched stub phrase, or empty str."""
    if not text:
        return ""
    head = text[:head_chars]
    m = _SUMMARY_STUB_RE.search(head) or _TRANSCRIPT_STUB_RE.search(head)
    return m.group(0) if m else ""
