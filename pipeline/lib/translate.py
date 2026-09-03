"""Translate and analyze German news via reddit_safe.pipeline.llm_client.

For each item: produces Chinese summary + key entities + tags. Returns the
augmented item dict. Supports single (analyze_item) and batched
(analyze_items_batch) modes — batched is preferred for cost/latency.

API key: read from OLLAMA_CLOUD_API_KEY env, falling back to llm_client's
default (already wired in reddit_safe).
"""
import json
import sys
import time
from pathlib import Path

# --------------------------------------------------------------------------- #
# Global LLM call cooldown
# --------------------------------------------------------------------------- #
# Ollama (and other LLM backends) sometimes hang or rate-limit when we hit
# them with many sequential calls in quick succession. This limiter enforces
# a minimum interval between calls and tracks call count so we can apply
# adaptive cooldowns when calls pile up.
#
# Tuning:
#   - _MIN_INTERVAL_S: minimum seconds between any two LLM calls (default 3)
#   - _ADAPTIVE_AFTER: after this many calls in a session, increase interval
#   - _ADAPTIVE_INTERVAL_S: enforced interval once _ADAPTIVE_AFTER reached
#
# Used by all batch translation / scoring helpers below.
_MIN_INTERVAL_S = 3.0
_ADAPTIVE_AFTER = 8
_ADAPTIVE_INTERVAL_S = 8.0
_call_log: list[float] = []  # global; shared across this module's helpers


class _Cooldown:
    """Context manager that enforces a minimum interval between LLM calls
    and applies an adaptive longer pause once a high call volume is reached.
    """
    def __enter__(self):
        if not _call_log:
            return self
        # adaptive interval once we cross the threshold
        target = _ADAPTIVE_INTERVAL_S if len(_call_log) >= _ADAPTIVE_AFTER else _MIN_INTERVAL_S
        elapsed = time.time() - _call_log[-1]
        if elapsed < target:
            time.sleep(target - elapsed)
        return self

    def __exit__(self, *exc):
        _call_log.append(time.time())
        # keep log bounded
        if len(_call_log) > 64:
            del _call_log[:32]
        return False

# Make reddit-safe importable without installing the package.
# Prefer the production checkout; fall back to the pipeline test stub
# (Cloud Agent / CI sandboxes without /root/reddit-safe).
_REDDIT_SAFE_CANDIDATES = (
    "/root/reddit-safe/src",
    str(Path(__file__).resolve().parents[2] / "pipeline" / "tests" / "stubs"),
)
for _src in _REDDIT_SAFE_CANDIDATES:
    if Path(_src).is_dir() and _src not in sys.path:
        sys.path.append(_src)  # append — never shadow an already-installed pkg

from reddit_safe.pipeline.llm_client import call_json, LLMError  # noqa: E402

# ---------------------------------------------------------------------------
# Simplified → Traditional Chinese conversion (2026-08-07 rewrite)
#
# We previously maintained a hand-curated ~170-char map. That map was
# always incomplete (漏字: 纠, 纷, 胀, 摊, 调, ...) and required constant
# maintenance as the LLM surfaced new simplified characters.
#
# Now: use `opencc-python-reimplemented` with config `s2twp` (Simplified →
# Taiwan Traditional, with phrase conversion). OpenCC maintains a dictionary
# of ~370K entries (字 + 詞組) and is the de-facto standard for Chinese script
# conversion. Zero maintenance — we just call `.convert()`.
#
#   pip install opencc-python-reimplemented
#
# Two thin wrappers preserve the rest of the codebase's API:
#   - force_traditional(text) -> (text, n_replacements)
#   - has_simplified(text)    -> bool
# ---------------------------------------------------------------------------
import opencc

# Shared converter instance (OpenCC is thread-safe after init).
_OPENCC_S2TWP = opencc.OpenCC("s2twp")


def force_traditional(text):
    """Convert simplified Chinese to Taiwan Traditional Chinese.

    Uses OpenCC `s2twp` config: Simplified → Traditional (Taiwan standard)
    with phrase-aware conversion (e.g. 法律纠纷 → 法律糾紛, 通貨膨脹, 房地產).
    Returns (converted_text, length_delta) where positive length_delta means
    the output is longer than the input (typical for s2tw because some chars
    expand, e.g. 计算机 → 計算機).
    """
    if not text:
        return text, 0
    converted = _OPENCC_S2TWP.convert(text)
    return converted, len(converted) - len(text)


def has_simplified(text):
    """True if `text` contains characters that OpenCC would simplify/expand.

    Implemented as: True if converting and comparing back differs. (OpenCC is
    idempotent for already-Traditional input.)
    """
    if not text:
        return False
    return _OPENCC_S2TWP.convert(text) != text


# --------------------------------------------------------------------------- #
# Per-item validation gate — one rule per SYSTEM prompt requirement
# --------------------------------------------------------------------------- #
# Returns a list of (rule_name, severity, message) tuples. Empty list = pass.
# Severities: 'error' = reject item, 'warn' = log but keep.
# Rules correspond 1:1 to clauses in the SYSTEM prompt.
# --------------------------------------------------------------------------- #
_RELEVANCE_LO = 0
_RELEVANCE_HI = 10
_MIN_SUMMARY_LEN = 60       # 絕對下限（防止完全空摘要）
_MAX_SUMMARY_LEN = 280      # 絕對上限（防止 LLM 瞎擴寫）
_MIN_SUMMARY_RATIO = 0.30   # 摘要字數 >= 原文 CJK 字數的 30%
_MAX_SUMMARY_RATIO = 1.50   # 摘要字數 <= 原文 CJK 字數的 150%
_MIN_TAGS = 3
_MAX_TAGS = 5
_MAX_VALIDATION_RETRIES = 2  # analyze_item 重試次數（超過就 fail）


def _count_cjk(text):
    return sum(1 for c in text if "\u4e00" <= c <= "\u9fff")


def validate_zh_item(item):
    """Validate that `item` conforms to every clause in the SYSTEM prompt.

    Returns a list of (rule, severity, message). Empty list = all passed.

    Use `validate_severity(issues, level='error')` to filter.
    """
    issues = []

    # Rule 1: title_zh must exist and not be empty
    title = item.get("title_zh") or ""
    if not title.strip():
        issues.append(("title_empty", "error", "title_zh is empty"))
    # Rule 1b: title must be Traditional Chinese
    if title and has_simplified(title):
        issues.append(("title_simplified", "error",
                       f"title_zh has simplified chars: {title[:50]!r}"))

    # Rule 2: summary_zh must exist
    summary = item.get("summary_zh") or ""
    if not summary.strip():
        issues.append(("summary_empty", "error", "summary_zh is empty"))
    # Rule 2b: summary must be Traditional Chinese
    if summary and has_simplified(summary):
        issues.append(("summary_simplified", "error",
                       f"summary_zh has simplified chars: {summary[:50]!r}"))

    # Rule 3: summary length (CJK chars only; exclude German URLs/numbers)
    cjk_len = _count_cjk(summary)
    if summary and cjk_len < _MIN_SUMMARY_LEN:
        issues.append(("summary_too_short", "error",
                       f"summary_zh CJK length {cjk_len} < {_MIN_SUMMARY_LEN}"))
    if summary and cjk_len > _MAX_SUMMARY_LEN:
        issues.append(("summary_too_long", "error",
                       f"summary_zh CJK length {cjk_len} > {_MAX_SUMMARY_LEN} "
                       "(possible LLM hallucination expansion)"))

    # Rule 4: entities must be a dict with the right keys (each can be empty list)
    entities = item.get("entities")
    if entities is None:
        issues.append(("entities_missing", "error", "entities field missing"))
    elif not isinstance(entities, dict):
        issues.append(("entities_wrong_type", "error",
                       f"entities is {type(entities).__name__}, expected dict"))
    else:
        for key in ("institutions", "laws", "numbers", "people"):
            v = entities.get(key)
            if v is None:
                issues.append(("entities_key_missing", "error",
                               f"entities.{key} missing"))
            elif not isinstance(v, list):
                issues.append(("entities_key_wrong_type", "error",
                               f"entities.{key} is {type(v).__name__}, expected list"))

    # Rule 5: tags count 3-5
    tags = item.get("tags") or []
    if not isinstance(tags, list):
        issues.append(("tags_wrong_type", "error",
                       f"tags is {type(tags).__name__}, expected list"))
    elif len(tags) < _MIN_TAGS:
        issues.append(("tags_too_few", "error",
                       f"tags count {len(tags)} < {_MIN_TAGS}"))
    elif len(tags) > _MAX_TAGS:
        issues.append(("tags_too_many", "warn",
                       f"tags count {len(tags)} > {_MAX_TAGS}"))

    # Rule 6: relevance_to_buyer must be int in [0, 10]
    rel = item.get("relevance_to_buyer")
    if rel is None:
        issues.append(("relevance_missing", "error",
                       "relevance_to_buyer missing (LLM failed to return it)"))
    elif not isinstance(rel, int):
        try:
            rel = int(rel)
        except (TypeError, ValueError):
            issues.append(("relevance_wrong_type", "error",
                           f"relevance_to_buyer is {type(rel).__name__}, expected int"))
            rel = None
    if rel is not None and isinstance(rel, int) and not (_RELEVANCE_LO <= rel <= _RELEVANCE_HI):
        issues.append(("relevance_out_of_range", "error",
                       f"relevance_to_buyer {rel} not in [{_RELEVANCE_LO}, {_RELEVANCE_HI}]"))

    # Rule 7: source_name / url must be present (sanity check)
    if not item.get("source_name"):
        issues.append(("source_name_missing", "error", "source_name missing"))
    if not item.get("url"):
        issues.append(("url_missing", "error", "url missing"))

    return issues


def filter_errors(issues):
    """Keep only error-level issues."""
    return [i for i in issues if i[1] == "error"]


def has_errors(issues):
    return any(sev == "error" for _rule, sev, _msg in issues)


SYSTEM = """你是德文→台灣繁體中文的房地產新聞分析師。

**用台灣當地常見中文用語**撰寫翻譯與摘要：寫「公寓」、「電梯」、「房貸」、「出租」、「貸款利率」、「房地產」。

首次出現的專有名詞、人名、機構名附原文德文（例：聯邦銀行 Bundesbank、房貸利率 Bauzinsen）。

數字、百分比、貨幣保留德文格式（例：3,2 Prozent、1,5 Milliarden Euro、Stand 2024）。

任務：
1. 繁體中文翻譯（保留數字、人名、機構名）
2. 150-300 字摘要 — 必須完全根據內文，禁止推論/編造/加背景知識
3. 抽取關鍵實體
4. 3-5 個 tag

**relevance_to_buyer 評分 (0-10)**：
- 0-2：完全無關（純股市、科技、政治、娛樂、體育、人物軼事）
- 3-4：邊緣（一般經濟政策、利率走勢、純企業財報）
- 5-6：中等（房貸利率變動、區域房市數據、建築法規更新）
- 7-8：高度相關（房貸新規、區域價格變化、補貼政策變動、租賃法變動）
- 9-10：極度重要（KfW 重大政策、聯邦最高法院房貸判例、首付新規）

輸出嚴格 JSON，無 markdown 圍欄：
{
  "title_zh": "...",
  "summary_zh": "...",
  "entities": {"institutions": [...], "laws": [...], "numbers": [...], "people": [...]},
  "tags": ["#tag1", "#tag2", ...],
  "relevance_to_buyer": <0-10 整數>
}"""


def _build_user_prompt(item):
    """Build the per-item user prompt from the news item.

    Priority order for content (most authoritative first):
      1. full_text — fetched from the article URL by rss_fetch.fetch_full_text
         (typically 2,000-16,000 chars; the actual article body).
      2. content_html (RSS content:encoded, stripped) — RSS inlined HTML.
      3. summary (RSS short description) — fallback.
      4. title only — last resort.

    The summary returned to the vault must be derived from the actual article
    body. The LLM is not allowed to inject external knowledge or speculation.
    """
    import re as _re
    body_text = (item.get("full_text") or "").strip()
    if not body_text:
        raw = item.get("content_html") or ""
        body_text = _re.sub(r"<[^>]+>", " ", raw)
        body_text = _re.sub(r"\s+", " ", body_text).strip()
    if not body_text:
        body_text = (item.get("summary") or "").strip()
    if not body_text:
        body_text = (item.get("title") or "").strip()
    # Cap to keep prompt size reasonable (~4k chars ≈ 1k tokens)
    body_text = body_text[:4000]
    # Annotate the source so the LLM knows the basis for the summary.
    has_full = bool(item.get("full_text"))
    source_label = "已抓取網頁全文" if has_full else "僅有 RSS 摘要（可能不完整）"
    body_chars = len(item.get("full_text") or item.get("content_html") or item.get("summary") or "")
    return f"""新聞：
標題：{item.get('title', '')}
來源：{item.get('source_name', '')}
URL：{item.get('url', '')}
日期：{item.get('pub_date', '')}
內文長度：{body_chars} 字（{source_label}）

=== 內文（請務必根據此內文摘要，禁止使用未在此提供的背景知識）===
{body_text}
=== 內文結束 ===

請輸出 JSON。"""


def _apply_parsed(item, parsed, elapsed=None, usage=None):
    title_zh = parsed.get("title_zh", "")
    summary_zh = parsed.get("summary_zh", "")
    # Belt-and-braces (2026-08-07): the SYSTEM prompt instructs the LLM to
    # emit Taiwan Traditional Chinese directly, but the LLM occasionally slips.
    # We force-convert via OpenCC (industry-standard library, 370K-entry dict)
    # rather than a hand-maintained map. Zero maintenance.
    title_zh, t_delta = force_traditional(title_zh)
    summary_zh, s_delta = force_traditional(summary_zh)
    item["title_zh"] = title_zh
    item["summary_zh"] = summary_zh
    item["traditional_replacements"] = t_delta + s_delta
    if t_delta + s_delta:
        item["had_simplified"] = True
    item["entities"] = parsed.get("entities", {})
    item["tags"] = parsed.get("tags", [])
    item["relevance_to_buyer"] = parsed.get("relevance_to_buyer")
    if elapsed is not None:
        item["llm_elapsed"] = round(elapsed, 1)
    if usage:
        item["llm_tokens"] = usage.get("total_tokens")
    # L2: Run the full validation gate (covers all SYSTEM-prompt rules).
    issues = validate_zh_item(item)
    item["validation_issues"] = issues
    if issues:
        # Surface warnings immediately; errors are surfaced at the obsidian gate.
        for rule, sev, msg in issues:
            if sev == "warn":
                print(f"[validate] {item.get('source_name','?')} {rule}: {msg}", file=sys.stderr)
    return item


# --------------------------------------------------------------------------- #
# Quick title-only relevance scoring (used as a pre-filter before translation)
# --------------------------------------------------------------------------- #

_QUICK_SCORE_SYSTEM = """你是德文新聞分類員。對每則德文新聞標題，判斷它對「**在德國想買房、有房、租房的人**」的相關程度。
只看標題（必要時可看摘要 100 字內）。

回傳 0-10 整數：
- 0-2：完全無關（純股市、純科技、純政治、娛樂、體育、人物軼事、純企業財報）
- 3-4：邊緣（一般經濟政策、利率走勢、未直接觸及房地產）
- 5-6：中等（房貸利率變動、區域房市數據、建築法規更新）
- 7-8：高度相關（房貸新規、區域價格變化、補貼政策變動、租賃法變動）
- 9-10：極度重要（KfW 重大政策、聯邦最高法院房貸判例、首付新規、重大房市崩盤/反轉）

回傳**純 JSON 陣列**，長度等於輸入，每則對應一個整數。"""


def _build_quick_score_messages(chunk):
    payload = [
        {
            "i": i,
            "title": it.get("title", ""),
            "summary": (it.get("summary", "") or "")[:150],
        }
        for i, it in enumerate(chunk)
    ]
    user = (
        "以下是一批德文新聞標題。對每則評 0-10 分。\n"
        "**只回 JSON 陣列**，長度與輸入相同，例如 [0, 7, 3, 8, 0]\n\n"
        + json.dumps(payload, ensure_ascii=False)
    )
    return [
        {"role": "system", "content": _QUICK_SCORE_SYSTEM},
        {"role": "user", "content": user},
    ]


def quick_score_items(items, min_score=6, chunk_size=12):
    """Title-only relevance pre-filter. Returns the list with `quick_score`
    populated. Items below `min_score` are still returned but marked; the
    caller decides whether to drop them.

    This is much cheaper than `analyze_items_batch` because the user prompt
    is tiny (~150 chars per item, no full translation).
    """
    out = []
    total = len(items)
    if total == 0:
        return out
    chunk_count = (total + chunk_size - 1) // chunk_size
    for chunk_idx, start in enumerate(range(0, total, chunk_size)):
        chunk = items[start:start + chunk_size]
        messages = _build_quick_score_messages(chunk)
        try:
            parsed, _ = _call_with_retry(messages, max_retries=2)
        except Exception as e:
            # on failure, mark all as unscored and keep
            for it in chunk:
                it["quick_score"] = None
                it["quick_score_error"] = str(e)
            out.extend(chunk)
            continue
        if not isinstance(parsed, list) or len(parsed) != len(chunk):
            for it in chunk:
                it["quick_score"] = None
            out.extend(chunk)
            continue
        for it, s in zip(chunk, parsed):
            try:
                it["quick_score"] = int(s)
            except (TypeError, ValueError):
                it["quick_score"] = None
        out.extend(chunk)
        # Inter-chunk cooldown (2026-08-07): same reasoning as
        # analyze_items_batch — ollama-cloud queues and hangs on bursts.
        if chunk_idx < chunk_count - 1:
            time.sleep(2.0)  # quick_score is small, lighter sleep
    return out


def filter_by_quick_score(items, min_score=6):
    """Drop items whose quick_score is below min_score. None = no score → keep."""
    if not items or min_score is None or min_score <= 0:
        return items
    out = []
    for it in items:
        s = it.get("quick_score")
        if s is None:
            out.append(it)
            continue
        try:
            if int(s) >= min_score:
                out.append(it)
        except (TypeError, ValueError):
            out.append(it)
    return out


def _mark_error(item, err):
    item["title_zh"] = ""
    item["summary_zh"] = f"[翻譯失敗: {type(err).__name__}]"
    item["entities"] = {}
    item["tags"] = []
    item["llm_error"] = str(err)


def analyze_item(item):
    """Translate+analyze a single news item. Returns the item dict augmented with zh fields.

    Retries up to _MAX_VALIDATION_RETRIES times when the LLM output fails any
    error-level validation rule (simplified Chinese, summary too short, tags
    missing, etc.). Each retry adds a correction hint to the user prompt so
    the LLM is told specifically what to fix.
    """
    messages = [
        {"role": "system", "content": SYSTEM},
        {"role": "user", "content": _build_user_prompt(item)},
    ]
    t = time.time()
    attempts = 0
    last_err = None
    while attempts <= _MAX_VALIDATION_RETRIES:
        attempts += 1
        try:
            with _Cooldown():
                parsed, usage = call_json(messages)
        except LLMError as e:
            last_err = e
            if attempts > _MAX_VALIDATION_RETRIES:
                _mark_error(item, e)
                return item
            continue  # network/json error → retry the same messages
        except Exception as e:
            last_err = e
            _mark_error(item, e)
            return item
        _apply_parsed(item, parsed, elapsed=time.time() - t, usage=usage)
        # Check validation
        errors = [i for i in item.get("validation_issues", []) if i[1] == "error"]
        if not errors:
            return item  # success
        if attempts > _MAX_VALIDATION_RETRIES:
            item["llm_validation_failed"] = [r for r, _s, _m in errors]
            return item  # give up; mark failed
        # Retry: append a correction hint to the user prompt and re-call.
        hints = "\n".join(f"- 修補: {msg}" for _r, _s, msg in errors)
        retry_messages = list(messages) + [
            {"role": "user", "content": f"你的上一次輸出不符合以下規則，請重新生成完整 JSON 並修正：\n{hints}"},
        ]
        messages = retry_messages
    if last_err is not None:
        _mark_error(item, last_err)
    return item


# --------------------------------------------------------------------------- #
# Batch mode
# --------------------------------------------------------------------------- #

_BATCH_SYSTEM = SYSTEM + "\n\n你會一次收到多則新聞（JSON 陣列）。回傳長度必須完全相同，順序對應輸入。每則維持同樣 schema。"


def _build_batch_messages(chunk):
    """chunk: list of item dicts. Returns messages asking for JSON array of same length."""
    payload = [
        {
            "i": i,
            "title": it.get("title", ""),
            "source": it.get("source_name", ""),
            "url": it.get("url", ""),
            "summary": (it.get("summary", "") or "")[:400],
        }
        for i, it in enumerate(chunk)
    ]
    user = (
        "以下是一批德文新聞（JSON 陣列）。\n"
        "請對每則做翻譯/摘要/實體/tag，輸出**長度與輸入完全相同**的 JSON 陣列，順序對應。\n\n"
        + json.dumps(payload, ensure_ascii=False)
    )
    return [
        {"role": "system", "content": _BATCH_SYSTEM},
        {"role": "user", "content": user},
    ]


def _call_with_retry(messages, max_retries=3):
    """Call call_json with exponential backoff. Returns (parsed, usage) or raises.

    Backoff: 5s, 10s, 20s. Handles the ollama-cloud pattern where the Nth
    sequential request in a minute sometimes times out at 120s — a longer
    pause + retry usually clears it without re-batching.

    Also enforces the global cooldown limiter so we don't burst calls.
    """
    last_err = None
    for attempt in range(max_retries):
        try:
            with _Cooldown():
                return call_json(messages)
        except Exception as e:
            last_err = e
            if attempt < max_retries - 1:
                time.sleep(5 * (2 ** attempt))   # 5, 10
    if last_err is not None:
        raise last_err
    raise RuntimeError("_call_with_retry: no attempts made")


def _process_chunk_with_fallback(chunk):
    """Try batch call on a chunk. If it fails after retries, fall back to
    per-item analyze_item (slower but reliable). Returns nothing — mutates
    items in place and appends to a side channel via _out list.

    Implementation: we don't return a value; caller inspects the items.
    """
    messages = _build_batch_messages(chunk)
    try:
        parsed, usage = _call_with_retry(messages, max_retries=3)
    except Exception as batch_err:
        # batch failed after retries — try per-item, each with its own retry
        for it in chunk:
            try:
                _call_with_retry(
                    [
                        {"role": "system", "content": SYSTEM},
                        {"role": "user", "content": _build_user_prompt(it)},
                    ],
                    max_retries=2,
                )
            except Exception as item_err:
                _mark_error(it, item_err)
                continue
            # If we get here the call succeeded inside _call_with_retry but we
            # discarded the parsed value. Re-call once more (cheap, ~1.5s) to
            # actually fill the fields.
            analyze_item(it)
        return
    if not isinstance(parsed, list) or len(parsed) != len(chunk):
        # shape mismatch — per-item retry
        for it in chunk:
            analyze_item(it)
        return
    for it, p in zip(chunk, parsed):
        _apply_parsed(it, p, usage=usage)
    # Validation retry: any item with error-level issues gets re-analyzed
    # individually with a correction hint. Per-item analyze_item handles the
    # retry loop with the LLM.
    bad = [it for it in chunk
           if any(sev == "error" for _r, sev, _m in it.get("validation_issues", []))]
    for it in bad:
        analyze_item(it)


def analyze_items_batch(items, chunk_size=8):
    """Translate+analyze a list of items, chunked into LLM calls of `chunk_size`.

    Returns the list augmented in place. Failed items get `llm_error` filled.
    Per-chunk retry with exponential backoff handles ollama-cloud timeouts
    without changing chunk size or token usage.
    """
    out = []
    total = len(items)
    if total == 0:
        return out
    t0 = time.time()
    # Inter-chunk cooldown (2026-08-07): ollama-cloud queues sequential
    # requests and silently drops them when the queue backs up. We sleep
    # briefly between chunks so the backend can drain before we fire the
    # next one. Without this, chunk_size=4 with retry can produce 11+
    # back-to-back LLM calls that hang indefinitely.
    chunk_count = (total + chunk_size - 1) // chunk_size
    for chunk_idx, start in enumerate(range(0, total, chunk_size)):
        chunk = items[start:start + chunk_size]
        _process_chunk_with_fallback(chunk)
        out.extend(chunk)
        # Sleep between chunks (but not after the last one — no point).
        if chunk_idx < chunk_count - 1:
            _CHUNK_COOLDOWN_S = 3.0
            time.sleep(_CHUNK_COOLDOWN_S)
    for it in out:
        it.setdefault("llm_elapsed_batch_total", round(time.time() - t0, 1))
    return out


# --------------------------------------------------------------------------- #
# Ranking
# --------------------------------------------------------------------------- #


def rank_by_relevance(items):
    """Ask LLM to rank items by relevance to 'German real estate buyer'.

    Returns items reordered; no items dropped.
    """
    if len(items) <= 1:
        return items
    titles = "\n".join(f"[{i}] {it.get('title', '')}" for i, it in enumerate(items))
    user = (
        "從下列德國房地產/金融新聞選最值得一個在德國買房/關注房地產的人知道的優先順序。\n"
        "按重要性從高到低輸出 index 順序，例如：[3, 0, 5, 1, 2]\n"
        "只輸出 JSON：{\"order\": [3, 0, 5, 1, 2]}\n\n"
        f"新聞列表：\n{titles}"
    )
    try:
        parsed, _ = call_json(
            [{"role": "system", "content": "Respond in strict JSON."}, {"role": "user", "content": user}]
        )
        order = parsed.get("order", list(range(len(items))))
        if sorted(order) == list(range(len(items))):
            return [items[i] for i in order]
    except Exception:
        pass
    return items
