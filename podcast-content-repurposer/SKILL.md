---
name: podcast-content-repurposer
description: "Convert a podcast vault digest into 6 platform-native drafts (Facebook, Threads, X/Twitter, LinkedIn, Newsletter, Podcast outline). Use when user wants to cross-post a German-real-estate podcast episode to social media or repurpose vault content into different formats."
tags: ["podcast", "social-media", "newsletter", "content-repurposing", "facebook", "twitter", "threads", "linkedin"]
platforms: [linux, macos]
---

# Podcast Content Repurposer

Convert one VideoDigest (vault .md file from `podcast-kb/vault/Daily/YYYY-MM-DD/`)
into platform-native drafts for social media, newsletter, and podcast outlines.

## Use when

- User says: "把 podcast 整理成 FB/IG 文章"、"幫我寫電子報"、"Podcast 大綱"、"發 Threads / X 短文"
- After the daily podcast pipeline runs and you want to fan the content out to multiple platforms
- User wants to cross-post a podcast episode to social without rewriting from scratch

## Input

- One vault file at `podcast-kb/vault/Daily/YYYY-MM-DD/<channel>---<slug>.md`
- Already contains 4 sections: 摘要 / 房地產分析師視角 / 內容製作人視角 / 重點詞彙
- Already in Taiwan Traditional Chinese (zh-TW)

## Output

Six formats in one go, written to `podcast-kb/content/<date>/<channel>---<slug>/`:

| File | Platform | Length | Tone |
|---|---|---|---|
| `facebook.md` | Facebook 粉專 | 800-1500 字 | 友善 + 數據 + CTA |
| `threads.md` | Threads | 100-280 字 / 則 | 簡潔垂直思考 |
| `twitter.md` | X / Twitter | 5-8 推、每推 ≤280 字 | 專業 hook + 數據點 |
| `linkedin.md` | LinkedIn | 1500-2000 字 | 權威 + 業界 insider 觀點 |
| `newsletter.md` | 電子報 | 1500-2500 字 | 完整版、含背景+結論+CTA |
| `podcast-outline.md` | Podcast 大綱 | 結構化 intro+段落+outro | 口播稿格式 |

All in Taiwan Traditional Chinese (zh-TW), with German terms kept in parentheses
when they help (Grunderwerbsteuer, Bezahlbarkeitsindex).

## How to run

The repurposer uses `reddit_safe.pipeline.llm_client.call`, which is hermes-internal
and not available in standalone Python. There are two ways to run it:

### Via `execute_code` (recommended)

```python
from hermes_tools import execute_python  # not a real call — see below
```

Concretely: drop the script into a `from hermes_tools import ...` block and let
`execute_code` import it. See `templates/repurpose_executor.py` for a working
template.

### Manual per-format prompts

When you want to generate a single format quickly, paste this prompt into a
fresh LLM call (with the vault 4 sections + the format-specific rules below):

```
{USER_TEMPLATE_FOR_CHOSEN_FORMAT}
```

## Platform-specific rules

### Facebook 粉專
- Hook 第 1 段（問題 / 數據 / 反直覺觀點）、不要再寫「今天來談…」開頭
- 3-5 段正文、每段以關鍵字開頭
- 結尾 CTA（留言 / 分享 / 看影片）
- 1-2 個 emoji 點綴、不要裝飾性 emoji
- 字數：800-1500 字

### Threads（短文垂直思考）
- 1-3 則，每則 100-280 字
- 第 1 則：直接寫結論或最尖銳的觀點
- 後續則：解釋 + 個人觀點
- 最後一則：留問題給讀者回答
- 每則用 `===` 分隔、編號 `1/`, `2/`, `3/`

### X / Twitter（Thread）
- 5-8 推
- 第 1 推：Hook + `🧵1/N` 標記
- 第 2-7 推：每推一個數據點或論點、≤280 字
- 最後推：總結 + 影片連結 + vault URL
- 每推用 `---` 分隔、編號 `1/` → `N/`

### LinkedIn
- 標題用專業詞彙（insider / broker 角度）
- 第一段直接寫論點、不要自介
- 用 `-` 或數字清單
- 結尾 CTA：開放性問題
- **Zero emojis**（極簡）
- 字數：1500-2000 字

### Newsletter（電子報）
- 主旨（50 字內）
- 引言（破冰 + 為什麼讀者要看）
- 主體（用 vault 摘要 + 分析師視角重點）
- 個人觀點（編輯自己的聲音）
- 結尾 CTA（留言 / 轉發 / 訂閱）
- 「本期關鍵字」box（從 vault 詞彙抓 5-8 個）

### Podcast 大綱（給自己錄 Podcast 用）
- Intro（30 秒 hook + 自我介紹 + 3 個 takeaway）
- 主體（5-8 個段落、每段 60-90 秒口播稿）
- Outro（總結 + 呼籲行動 + 預告下集）
- 每段要有「該段重點」、「參考來源」引用 vault

## Pitfalls

- **LLM output drift on long prompts**: when you ask for "all 6 formats in one call" the
  LLM tends to truncate later formats. Always **one LLM call per format**.
- **Threads/X Twitter 字數**: LLM 常會超過、用 prompt 明確寫「嚴格 ≤280 字」並截斷 + 「…」
- **德文術語一致性**: vault 詞彙裡的德文必須在每個 format 中保持原文、不要強翻
- **`call_json` 拒絕 Markdown**: 一定要用 `call()`、timeout 180s 就夠
- **3 秒 inter-call cooldown**: 6 個 format 連發會 hang、每個 format 間隔 3s
- **不要幫使用者按發送**: skill 只生成內容、推播是 separate 動作

## Verification

After running, confirm:
1. `ls podcast-kb/content/<date>/<slug>/` shows 6 files (or however many formats requested)
2. Twitter thread line count = 5-8 推、每推 ≤280 字
3. Newsletter has 主旨 + 引言 + 主體 + CTA + 關鍵字 box
4. Podcast outline has Intro + 5-8 段落 + Outro
5. All files start with `# ` (markdown title) and contain at least 2 German terms
