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
_REDDIT_SAFE_SRC = "/root/reddit-safe/src"
if _REDDIT_SAFE_SRC not in sys.path:
    sys.path.insert(0, _REDDIT_SAFE_SRC)

from reddit_safe.pipeline.llm_client import call_json, LLMError  # noqa: E402

SYSTEM = """你是德文→**繁體中文 (台灣)** 的房地產/金融新聞分析師。**禁止使用簡體中文**。

任務：對輸入的德文新聞做：
1. 完整中文翻譯（保留所有數字、人名、機構名）
2. 200-300 字中文摘要
3. 抽取關鍵實體（機構/法案/數字/人物）
4. 3-5 個 tag

德文房地產/金融專有名詞首次出現時保留原文 + 中文括弧。

**強制用台灣繁體正體（不是中國大陸簡體）**。常見字對照：
- 軟體 ≠ 软件
- 資訊 ≠ 信息
- 網路 ≠ 网络
- 影片 ≠ 视频
- 資料庫 ≠ 数据库
- 建築 ≠ 建筑
- 市場 ≠ 市场
- 意味著 ≠ 意味着
- 資料 ≠ 资料
- 檔案 ≠ 文件
- 伺服器 ≠ 服务器
- 連結 ≠ 链接
- 預設 ≠ 默认
- 程式 ≠ 程序
- 視訊 ≠ 视频

輸出嚴格 JSON 物件，無 markdown 圍欄，無 commentary：
{
  "title_zh": "...",
  "summary_zh": "...",
  "entities": {"institutions": [...], "laws": [...], "numbers": [...], "people": [...]},
  "tags": ["#tag1", "#tag2", ...],
  "relevance_to_buyer": <0-10 整數, 對「在德國買房/有房者」的相關程度>
}

**relevance_to_buyer 評分標準**：
- 0-2：完全無關（純股市、純科技、純政治、娛樂、體育、人物軼事）
- 3-4：邊緣（一般經濟政策、利率走勢、純企業財報，未直接觸及房地產）
- 5-6：中等（房貸利率變動、區域房市數據、建築法規更新）
- 7-8：高度相關（房貸新規、區域價格變化、補貼政策變動、租賃法變動）
- 9-10：極度重要（KfW 重大政策、聯邦最高法院房貸判例、首付新規）

只看跟「在德國想買房/有房/租房的人」會關心的程度，不評文章品質。"""


def _build_user_prompt(item):
    return f"""新聞：
標題：{item.get('title', '')}
來源：{item.get('source_name', '')}
URL：{item.get('url', '')}
摘要：{item.get('summary', '')[:400]}

請輸出 JSON。"""


def _apply_parsed(item, parsed, elapsed=None, usage=None):
    item["title_zh"] = parsed.get("title_zh", "")
    item["summary_zh"] = parsed.get("summary_zh", "")
    item["entities"] = parsed.get("entities", {})
    item["tags"] = parsed.get("tags", [])
    item["relevance_to_buyer"] = parsed.get("relevance_to_buyer")  # 0-10 or None
    if elapsed is not None:
        item["llm_elapsed"] = round(elapsed, 1)
    if usage:
        item["llm_tokens"] = usage.get("total_tokens")


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
    for start in range(0, total, chunk_size):
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
    """Translate+analyze a single news item. Returns the item dict augmented with zh fields."""
    messages = [
        {"role": "system", "content": SYSTEM},
        {"role": "user", "content": _build_user_prompt(item)},
    ]
    t = time.time()
    try:
        with _Cooldown():
            parsed, usage = call_json(messages)
        _apply_parsed(item, parsed, elapsed=time.time() - t, usage=usage)
    except LLMError as e:
        _mark_error(item, e)
    except Exception as e:
        _mark_error(item, e)
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
    for start in range(0, total, chunk_size):
        chunk = items[start:start + chunk_size]
        _process_chunk_with_fallback(chunk)
        out.extend(chunk)
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
