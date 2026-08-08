"""v1: External translation (Google) + single LLM structuring.

The previous version used the LLM to both translate AND structure the digest
(8 map calls + 1 reduce). That pipeline hung on ollama-cloud after the 7th
sequential call, so the reduce step always timed out.

This v1 splits the work:
  1. Translate: split transcript into chunks → GoogleTranslator (deep-translator)
     translates each chunk to Traditional Chinese. Zero LLM cost.
  2. Structure: ONE LLM call takes the merged Chinese text and emits the
     dual-lens digest (analyst + producer + vocab).

Total LLM calls: 1 per video (down from 9). This dodges the ollama hang
entirely and finishes a 25-minute video in ~60 seconds instead of ~250.

Quality trade-off: Google Translate DE→zh-TW is grammatically stiff but
factually correct. The LLM structuring layer cleans up the output into
proper analyst/producer prose.
"""
from __future__ import annotations
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from typing import List, Optional

from deep_translator import GoogleTranslator
from deep_translator.exceptions import NotValidPayload, TranslationNotFound

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))

from pipeline.lib.translate import force_traditional, has_simplified  # noqa: E402

try:
    from reddit_safe.pipeline.llm_client import call_json, call  # type: ignore
except Exception:  # pragma: no cover
    call_json = None
    call = None


CHUNK_SIZE = 4500  # chars per Google Translate call (their soft limit ~5000)


@dataclass
class VideoDigest:
    video_id: str
    title: str
    channel_name: str
    url: str
    published_epoch: Optional[int]
    duration_sec: int
    source_language: str
    n_chars: int
    summary_zh: str
    analyst_zh: str
    producer_zh: str
    vocab_zh: str
    map_calls: int
    reduce_calls: int
    elapsed_sec: float


def _normalize(text) -> str:
    if isinstance(text, tuple):
        text = text[0]
    return text or ""


def _split_chunks(text: str, size: int = CHUNK_SIZE) -> List[str]:
    text = text.strip()
    if len(text) <= size:
        return [text]
    chunks: List[str] = []
    start = 0
    while start < len(text):
        end = min(start + size, len(text))
        if end < len(text):
            nl = text.rfind("\n", start, end)
            if nl > start + size // 2:
                end = nl
        chunks.append(text[start:end].strip())
        start = end
    return [c for c in chunks if c]


def _translate_chunk(chunk: str, source: str = "de", target: str = "zh-TW",
                     retries: int = 2) -> str:
    """Translate one chunk via Google Translate, with retry."""
    last_err: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            return GoogleTranslator(source=source, target=target).translate(chunk)
        except (NotValidPayload, TranslationNotFound) as e:
            last_err = e
            continue
        except Exception as e:  # network errors
            last_err = e
            if attempt < retries:
                time.sleep(2.0)
            continue
    raise RuntimeError(f"Google Translate failed after {retries + 1} attempts: {last_err}")


STRUCTURE_SYSTEM_PROMPT = """你是一位專精於德國房地產的資深編輯助理，幫台灣投資人整理 YouTube 影片的繁體中文深度整理。

**輸入**：影片的逐字稿全文（已是德文→繁體中文的機器翻譯結果、可能不夠通順）。
**輸出**：使用以下結構（純繁體中文、Markdown 格式）。

**規則**：
- 只能根據輸入的中文內容整理，禁止補充原文沒有的資料、數據、法條
- 專有名詞保留德文原文並用括號補充中文（例：Grunderwerbsteuer（房地產交易稅））
- 使用台灣在地表達（「公寓」「房貸」「貸款利率」「房地產」「稅務」）
- 數字、人名、公司名稱忠於原文
- **嚴格使用純 Markdown 格式**：用 `## 標題` 開頭、bullet 用 `- `、不要用 JSON、不要用 code block 包整個輸出
- **內容要有深度**：完整論述、引用影片中的具體數據與案例、展開分析「為什麼」與「怎麼辦」、不要只列標題
- **字數只是參考**：重點是內容和論點能否完整表達。**不要硬性設字數上限**。如果某個段落需要 1000 字才能完整論述、就寫 1000 字；反之 200 字就說完、就 200 字。唯一上限是 Discord embed 4000 字（單一 embed）。**不要為了字數而灌水、不要為了短而省略重點**。

**結構**：

## 摘要
（完整覆蓋影片所有關鍵主題、含背景/論點/方向/結論/影響。字數視內容深度而定、勿硬性限制。）

## 房地產分析師視角
（每個分段完整論述：成因、影響、實例、給出具體投資判斷建議）

- **市場趨勢**：（這個趨勢的成因、影響範圍、可持續性、對不同地區的差異化影響）
- **投資影響**：（對不同類型投資人、首購族、換屋族、投資族的具體策略建議、進場時機、標的選擇）
- **法規 / 稅務**：（涉及的法條或稅務議題細節、實際影響金額、合規注意事項；若無則明確寫「本集未涉及」並簡短說明）
- **風險與機會**：（明確指出值得關注的具體地區、價格區間、投資標的類型、進場風險指標）

## 內容製作人視角
（每個分段完整段落論述）

- **Hook**：（吸引人的開場金句）
- **Angle**：（從哪些角度引用這集內容、適合的目標受眾、預期效果）
- **Newsletter 摘要**：（可直接拿去當電子報內容、涵蓋背景+論點+結論+CTA）

## 重點詞彙
（10-15 個詞彙、每個含：德文術語 → 中文翻譯 → 完整用法說明、含使用情境與計算範例）
"""

STRUCTURE_USER_TEMPLATE = """以下是 YouTube 影片逐字稿的繁體中文機器翻譯結果。請整合並依結構整理。

## 影片資訊
- 標題：{title}
- 頻道：{channel}
- 影片時長：{duration} 秒

## 繁中逐字稿全文
{translated_text}

---

請按結構輸出（純繁體中文、Markdown）："""


def _structure_digest(video, translated_text: str, timeout: int = 180) -> str:
    """Single LLM call to structure the translated text into the dual-lens digest."""
    duration_min = video.duration_sec // 60
    duration_str = f"{duration_min} 分 {video.duration_sec % 60} 秒"
    user = STRUCTURE_USER_TEMPLATE.format(
        title=video.title,
        channel=video.channel_name,
        duration=duration_str,
        translated_text=translated_text,
    )
    if call is None:
        raise RuntimeError("reddit_safe.pipeline.llm_client.call not available")
    messages = [
        {"role": "system", "content": STRUCTURE_SYSTEM_PROMPT},
        {"role": "user", "content": user},
    ]
    # Use `call` (plain text) since we want Markdown, not JSON.
    # call_json insists on parseable JSON and rejects our structured digest.
    text, _usage = call(messages, timeout=timeout)
    return text.strip()


def _split_final_digest(text: str) -> dict:
    sections = {"summary_zh": "", "analyst_zh": "", "producer_zh": "", "vocab_zh": ""}
    if not text or text.startswith("[LLM_ERROR]"):
        sections["summary_zh"] = text
        return sections
    parts = re.split(r"^## (.+)$", text, flags=re.MULTILINE)
    if len(parts) < 3:
        sections["summary_zh"] = text.strip()
        return sections
    headers_bodies = []
    for i in range(1, len(parts), 2):
        h = parts[i].strip()
        b = parts[i + 1].strip() if i + 1 < len(parts) else ""
        headers_bodies.append((h, b))
    for h, b in headers_bodies:
        if "摘要" in h and "Newsletter" not in h and "電子報" not in h:
            if not sections["summary_zh"]:
                sections["summary_zh"] = b
        elif "分析師" in h or "分析" in h:
            sections["analyst_zh"] = b
        elif "製作" in h or "Producer" in h or "內容製作" in h:
            sections["producer_zh"] = b
        elif "詞彙" in h or "Vocabulary" in h:
            sections["vocab_zh"] = b
    return sections


def digest_video(video, transcript_text: str, cooldown_sec: float = 3.0,
                 translate_source: str = "de", translate_target: str = "zh-TW",
                 llm_timeout: int = 300) -> VideoDigest:
    """v1: Google translate chunks → single LLM structuring call."""
    t0 = time.time()
    chunks = _split_chunks(transcript_text)
    n_chunks = len(chunks)
    print(f"    [translate] {n_chunks} chunks via Google Translate ({translate_source}→{translate_target})")
    translated_chunks: List[str] = []
    for i, c in enumerate(chunks):
        zh = _translate_chunk(c, source=translate_source, target=translate_target)
        translated_chunks.append(zh)
        if i < n_chunks - 1:
            time.sleep(cooldown_sec)
    translated_text = "\n\n".join(translated_chunks)
    print(f"    [translate] done, {len(translated_text)} chars zh-TW")

    # OpenCC defense on the machine translation (belt-and-braces; Google usually
    # already outputs zh-TW but let's be safe)
    if has_simplified(translated_text):
        translated_text = _normalize(force_traditional(translated_text))
        print(f"    [OpenCC] cleaned simplified chars in translation")

    print(f"    [structure] 1 LLM call to format dual-lens digest")
    structure_text = _structure_digest(video, translated_text, timeout=llm_timeout)
    structure_text = _normalize(force_traditional(structure_text)) if has_simplified(structure_text) else structure_text
    sections = _split_final_digest(structure_text)

    return VideoDigest(
        video_id=video.id,
        title=video.title,
        channel_name=video.channel_name,
        url=video.url,
        published_epoch=video.epoch,
        duration_sec=video.duration_sec,
        source_language=translate_source,
        n_chars=len(transcript_text),
        summary_zh=sections["summary_zh"],
        analyst_zh=sections["analyst_zh"],
        producer_zh=sections["producer_zh"],
        vocab_zh=sections["vocab_zh"],
        map_calls=n_chunks,
        reduce_calls=1,
        elapsed_sec=time.time() - t0,
    )
