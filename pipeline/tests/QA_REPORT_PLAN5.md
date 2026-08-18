# QA Report — Plan 5 (Vault Rename + Airtable 對齊 + Code Path 改動)

**驗證日期**: 2026-08-18
**驗證者**: QA Engineer (subagent)
**工作目錄**: `/root/projects/ado_hermes_reddit_scrapper`
**總結**: **8 / 8 PASS**

---

## 驗證結果一覽

| # | 項目 | 結果 |
|---|------|------|
| 1 | 所有 vault `.md` 都符合新格式 `{date}_{src}_{kind}_{slug}.md` | ✅ PASS |
| 2 | 0 個 unknown src | ✅ PASS |
| 3 | `obsidian.py` import + 德文 umlaut 處理正確 | ✅ PASS |
| 4 | `youtube_obsidian.py` import + channel 解析正確 | ✅ PASS |
| 5 | `reddit_monitor.py` import 正確 | ✅ PASS |
| 6 | `push_vault_to_channels.py` 三 channel dry-run | ✅ PASS |
| 7 | Airtable `ProcessedContent` 0 orphan,≥100 new format | ✅ PASS |
| 8 | 四個 `.py` 沒遺留舊 suffix logic | ✅ PASS (with caveat) |

---

## 詳細驗證結果

### 1. Vault `.md` 新格式檢查 — ✅ PASS

- 153 個 vault `.md` 檔案遍歷 (`find immobilien-kb/vault podcast-kb/vault -name "*.md" ! -name "_index.md"`)
- Daily / Reddit / Stat 三類:全部符合 `^\d{4}-\d{2}-\d{2}_[a-z0-9-]+_[a-z]+_.+\.md$`
- YouTube:位於三層 layout `{vault}/YouTube/{date}/{channel}/{file}`(新)或兩層 `{vault}/YouTube/{channel}/_transcripts/`(舊)
- **BAD 行數**: 0

### 2. 0 個 unknown src — ✅ PASS

- `grep -r "unknown_longform\|unknown_summary\|unknown_transcript\|unknown_dataset"` 命中: **0 行**
- 所有 vault 檔案的 src 欄位都已修正為正確頻道/來源

### 3. `obsidian.py` import + 德文 umlaut — ✅ PASS

```python
assert _slugify('Rückgang') == 'rueckgang'                       # ✓
assert _slugify('für Eigentumswohnungen') == 'fuer-eigentumswohnungen'  # ✓
assert _KIND_TOKEN['short-summary'] == 'summary'                 # ✓
assert _KIND_TOKEN['paywall-preview'] == 'paywallpreview'        # ✓
assert len(SOURCE_SHORT) >= 20                                   # ✓
```
輸出: `obsidian OK`

### 4. `youtube_obsidian.py` import + channel 解析 — ✅ PASS

```python
assert _resolve_channel_short('Finanzfluss') == 'finanzfluss'              # ✓
assert _resolve_channel_short('1aLAGE Immobilienpodcast') == '1alage'     # ✓
```
輸出: `youtube_obsidian OK`

### 5. `reddit_monitor.py` import — ✅ PASS

```python
assert 'r-wohnen' in rm.SUBREDDIT_SHORT.values()    # ✓
```
輸出: `reddit_monitor OK`

### 6. `push_vault_to_channels.py` 三 channel dry-run — ✅ PASS

- 指令: `--dry-run --day 2026-08-18`
- 總 item 數: **29**(預期 15-30 ✓)
- Channel 分佈:
  - `1520791894995501106` (Daily): item [1]–[14],共 **14 個**
  - `1537907956132089976` (Reddit): item [15]–[26],共 **12 個**
  - `1535461574460968960` (YouTube): item [27]–[29],共 **3 個**
- 三個 channel 都有出現 ✓
- vault 路徑正確指向新格式 `{date}_{src}_{kind}_{slug}.md`

### 7. Airtable `ProcessedContent` 對齊度 — ✅ PASS

- Airtable `output_path` 開頭為 `/root/` 的 record 總數: **115**
- `orphan` (路徑不存在): **0**
- 符合新格式 `^\d{4}-\d{2}-\d{2}_[a-z0-9-]+_[a-z]+_.+\.md$`: **115** (100%)
- 預期 ≥100: ✓ (115 ≥ 100)
- 輸出: `airtable OK`

### 8. 四個 `.py` 沒遺留舊 suffix logic — ✅ PASS (with caveat)

- `pipeline/lib/obsidian.py`:所有 `_longform` / `_paywallpreview` / `_shortpaywallpreview` 命中皆為 docstring 歷史說明(line 8, 137, 149–154),**無實際 runtime suffix 拼接邏輯** ✓
- `pipeline/lib/youtube_obsidian.py`:所有 `_transcripts` / `2026-08-` 命中皆為註解/docstring 歷史紀錄(line 5, 7, 70, 86, 104),**無實際 `_transcripts/` 路徑寫入邏輯** ✓
- **Caveat**:詳見下方「最重要風險」

---

## 最重要風險

**`daily_digest.py` 的 `_vault_youtube_fallback` (line 320–409) 還在讀舊 YouTube layout `vault/YouTube/<Channel>/_transcripts/*.md`,Plan 5 未處理這個 code path。**

證據:
- `daily_digest.py:340` — `transcripts = channel_dir / "_transcripts"`
- `daily_digest.py:412-415` — 此 fallback 在 vault-only / fetch_candidates 路徑會被呼叫(`_VAULT_FALLBACKS = {"reddit": ..., "youtube": _vault_youtube_fallback}`)
- Plan 5 將 YouTube 改為新 layout `vault/YouTube/{date}/{channel}/*.md`(flat、無 `_transcripts/` 子目錄)
- 當前 vault 內**兩套 layout 並存**:舊的 `_transcripts/` 子目錄還在(8 個頻道),新的 `{date}/{channel}/` 也已寫入(5 個日期)

**潛在影響**:
- 短期內舊 `_transcripts/` 子目錄的 transcript 仍可被 `daily_digest.py` 讀取 → 表面上看正常
- 但下次 `cleanup_transcripts.py` 或 youtube_obsidian 重新跑時,若新 writer 不再寫入 `_transcripts/`,舊 transcript 不會被更新 → `daily_digest.py` fallback 會讀到「過期/孤兒」的舊 transcript
- 真正危險:新 writer 寫入新 layout `vault/YouTube/{date}/{channel}/`,舊的 fallback 完全找不到這些新檔案 → **新 YouTube transcript 會在 `daily_digest` 路徑下被默默忽略**,只有 `push_vault_to_channels.py` 能讀到

**建議**:
1. Plan 5 應補改 `daily_digest.py:320–409`,改用 `{date}/{channel}/` 新 layout 掃描
2. 或在 Plan 6 清理時刪除舊 `_transcripts/` 子目錄並同步修正 fallback
3. 此風險不影響本次 8 項 PASS 結果(因為 `push_vault_to_channels.py` 已正確處理新 layout),但若未來 cron 走 `daily_digest` 路徑會 silently 漏掉新 YouTube 內容

---

## 其他觀察(非阻擋)

1. 兩套 YouTube layout 並存(舊 `_transcripts/` + 新 `{date}/{channel}/`)— 短期可接受,但應在 Plan 6 統一
2. Airtable `output_path` 100% (115/115) 符合新格式 → Plan 5 rename + Airtable 同步極為乾淨
3. Reddit `SUBREDDIT_SHORT` 包含 `r-wohnen` ✓,但 spec 未列出完整映射清單,建議日後補一份對照表

---

## 結論

Plan 5 的核心交付項目全部 PASS:
- 153 個 vault rename 完成
- 0 個 unknown src 殘留
- 4 個 Python code path 改動正確(德文 umlaut、channel 解析、subreddit 解析、suffix 重構)
- Airtable 對齊 115/115 (100%)
- push 3 channel dry-run 端到端正常

唯一待辦風險為 `daily_digest.py:_vault_youtube_fallback` 仍走舊 `_transcripts/` 路徑,**建議在 Plan 6 處理**。本次驗證未 commit、未 push。
