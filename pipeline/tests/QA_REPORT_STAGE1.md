# Stage 1 Destatis Pipeline QA Report

**Date**: 2026-08-13 (Thursday)
**QA Engineer**: independent subagent
**Verdict**: ⚠️ **APPROVED_WITH_FIXES**(可上 production,但 backend engineer 應在 commit 前修正 schema 同步 + docstring 過時)

---

## 1. pytest 結果

```
82 passed, 1 skipped in 5.53s
```

- ✅ 對齊 brief 預期(`82 passed, 1 skipped`)
- 唯一 skipped 的是 `test_fetch_and_parse_end_to_end_smoke`(線上 host 不可達時 skip — Scenario 1 設計如此)
- Scenario 2/3/4 全部 pass
- 沒有 `F` 失敗,沒有 `x` 預期失敗

---

## 2. 程式碼審查

### 2.1 風格

- ✅ `destatis_csv.py` 中文 log + retry 2/4/8s 指數 backoff + type hints + `from __future__ import annotations`,**與 `youtube_fetch.py` / `rss_fetch.py` 風格一致**
- ✅ `destatis_daily.py` logger 用 `logging.getLogger("destatis_daily")`,basicConfig 模式跟 `news_daily.py` / `youtube_daily.py` 一致
- ⚠️ **Minor — destatis_daily.py 沒用 `@dataclass` 風格的 step 函式**:每個 step (`step_write_vault`、`step_send_discord`、`step_push_github`) 各自有不同回傳 dict shape,缺統一介面。但跟 `news_daily.py` 既有寫法(那邊每個 step 也是各自 return)一致,故視為可接受

### 2.2 結構

- ✅ `destatis_daily.py` 單一職責,13 個 function,沒有 `>200 行的函式(僅 `run_pipeline` 239 行,但這是 main orchestration,合理)
- ✅ `destatis_csv.py` 7 個 function,全部 < 50 行
- ✅ 模組邊界清楚:CSV 下載/解析、vault render、Discord push、GitHub push、main orchestration 各自分檔或分段
- ✅ 沒有 TODO / FIXME / XXX / HACK
- ✅ 沒有未使用 import(逐個 grep 驗證)

### 2.3 錯誤處理

- ✅ 單一 source 失敗不中斷整個 run:`process_one_source` 的 `try/except` 確保抓取失敗只標記為 `failed`,其他 source 繼續
- ✅ Airtable `is_processed` 失敗 fallback 為 `False`(line 776-779)
- ✅ `step_send_discord` 失敗時 log 不 crash,繼續 GitHub push
- ✅ GitHub push 失敗時 log 不 crash,繼續 mark_processed
- ✅ `mark_processed` 失敗時 `continue` 該 dataset,不 crash 整個 run
- ✅ 單一 source vault 寫入失敗時 log error 並 continue,不 crash 整個 run(line 496-499)

### 2.4 程式碼細節

- ✅ `_is_time_series_first_col` 接受多種時間關鍵字(monat / jahr / zeit / datum / period / quartal / berichtsmonat / stichtag 等),對中文/德文混用友善
- ✅ `parse_csv` 用 `;` 分隔 + `QUOTE_ALL`(對齊 Destatis CSV 格式)
- ✅ `detect_encoding` 三層 fallback:`utf-8-sig` → `utf-8` → `latin-1`
- ✅ `_slug_from_url` 安全過濾 URL 內特殊字元

---

## 3. 動態驗證

### 3.1 Dry-run(實際跑的場景)

執行:
```bash
/root/.hermes/hermes-agent/venv/bin/python -m pipeline.destatis_daily --dry-run
```

- ✅ `[destatis] starting` 印出
- ✅ `channel=tao` 印出(預設,Stage 1 T3.1 fix)
- ✅ 3 個 source 全部載入:`['auftragseingang_bauhauptgewerbe', 'genehmigte_wohnungen_monat', 'investments_construction']`
- ✅ 3 個 source 全部抓到(`auftrag` 198 rows、`genehmigte` 198 rows + utf-8-sig、`investments` 7 rows)
- ✅ Dry-run 不寫 vault、不推 Discord、不寫 Airtable、不 push GitHub(只 log "would")
- ✅ `[dry-run] would write these to vault:` 列出 3 個 .md + `_index.md`
- ✅ `[dry-run] would push 3 Discord embed(s) to channel 'tao' (prefix '🏗️ [Destatis 官方數據]')`
- ✅ `[dry-run] would mark_processed 3 record(s) in Airtable`
- ✅ `[dry-run] would git add immobilien-kb/ + commit + push`
- ✅ 整個 dry-run 0.2 秒(包含 3 個 HTTP GET 到 destatis.de)

### 3.2 Vault 內容(純函式 render,寫到 `/tmp/qa_destatis_vault/`)

執行:
```python
ds = fetch_and_parse(src)
md = destatis_daily._render_dataset_md(ds, "2026-08-13", source_page=src.get("_source_page", ""))
```

- ✅ `auftragseingang_bauhauptgewerbe.md` (2093 bytes):
  - frontmatter 完整 8 欄(source_type/source_id/reference_period/name_de/name_zh/url/fetched_at/date/encoding/n_rows/n_cols)
  - `**最新月份**:2026/05/01` 對齊時間序列
  - `**起**:2010/01/01` / `**迄**:2026/05/01` / `**月資料筆數**:197 筆`
  - 4 欄數值對應 header:`Kalender- und saisonbereinigt (X13 JDemetra+)=95,3, Trend-Konjunktur-Komponente (Berliner Verfahren)=93,4, Kalender- und Preisbereinigt=97,4`
  - 來源網頁:對應到 `_source_page` URL ✅
  - markdown 表格顯示前 12 個月,`newest at top via reverse`(2026/05 在最上面,2025/06 在最下面)✅
- ✅ `genehmigte_wohnungen_monat.md` (1988 bytes):
  - 結構同上,`encoding: utf-8-sig`(對齊 `genehmigte` 有 BOM 的特性)✅
  - 資料範圍 2010/01 → 2026/05,共 197 月
- ✅ `investments_construction.md` (1619 bytes):
  - **`**資料類型**:橫斷面(cross-section)`** ✅ T3.1 修法生效
  - `**首筆**:Bau von Gebäuden` / `**末筆**:Übrige Wirtschaftszweige`(不再誤抓 Übrige 為「最新月份」)✅
  - `**橫斷面資料筆數**:6 筆(共 7 列含表頭)`
  - `**本期數值**:非時間序列資料,共 6 筆橫斷面資料`
  - 表格標題為「## 完整資料表(最多顯示 12 筆)」,不是「前 12 個月」✅
  - 來源網頁:對應到 `_source_page` URL ✅
- ✅ `_index.md` (1121 chars):
  - frontmatter:`type: destatis_daily_digest / date: 2026-08-13 / total_datasets: 3`
  - `# 🏗️ Destatis 官方數據 — 2026-08-13`
  - 三個 dataset 摘要,正確區分:
    - `auftragseingang`: `**最新月份**:2026/05/01` / `**資料筆數**:197 個月`
    - `genehmigte`: `**最新月份**:2026/05/01` / `**資料筆數**:197 個月`
    - `investments`: `**資料類型**:橫斷面(cross-section)` / `**資料筆數**:6 筆(橫斷面)` ✅

### 3.3 Airtable dedup 驗證

執行:
```python
ProcessedStore('appHilorcrC5T0p2u', 'ProcessedContent')
.is_processed('destatis_csv', 'destatis:auftragseingang_bauhauptgewerbe:latest')
```

- ✅ 3 個 source 全部回 `False`(之前 T3.2 清空過 ledger,新 run 會觸發 mark_processed)
- ✅ `dashboard_dedup.py recent destatis_csv` 回 `No destatis_csv records found`
- ✅ ProcessedContent table 內目前共 66 筆(41 news + 25 youtube),**0 筆 destatis_csv** — 對齊
- ✅ 沒有任何 record 的 `article_type` 是 `stat-table`(確認 T3.2 跑 --push 後 vault + ledger 都被清)
- ⚠️ **Important — `article_type="stat-table"` 不在 `airtable_processed_content_schema.json` 的 singleSelect 選項中**(該 schema 內只有 `short-summary / long-form / pending-long-form / skipped-long-form`)。程式碼內 `destatis_daily.py` line 1005 寫 `article_type="stat-table"`,雖然 `mark_processed` 有 `typecast: True`(line 417)會讓 Airtable 自動新增 singleSelect 選項,實務上會 work,但 **schema 檔沒跟著同步**。建議 backend engineer 補上 `stat-table` 選項到 `airtable_processed_content_schema.json`。

### 3.4 Cron 設定驗證

- ✅ `id: 2049d7a025c9`(對齊 brief)
- ✅ `name: destatis_daily_weekly`
- ✅ `enabled: true`、`state: scheduled`
- ✅ `schedule.expr: 0 1 * * 5` UTC(對齊 brief:Berlin 週五 03:00 CEST)
- ✅ `next_run_at: 2026-08-14T01:00:00+00:00`(今天是 2026-08-13 週四 08:24 UTC,下次觸發是週五 8/14 01:00 UTC = Berlin 03:00 CEST — **對齊**)
- ✅ `deliver: local`(對齊 brief)
- ✅ `skills: []`(對齊 brief)
- ✅ `repeat.completed: 0`(還沒跑過)
- ✅ `prompt` 包含:
  - `python -m pipeline.destatis_daily --push` ✅
  - `set -a && source /root/.hermes/.env && set +a`(`AIRTABLE_API_KEY` 注入)✅
  - `2>&1 | tee ~/.hermes/cron/output/destatis_daily/$(date -u +%Y-%m-%d).log` ✅
  - DST / 冬令時注意事項 ✅
  - 失敗不 retry(避免重複推 Discord)✅
  - 3 embed / 3 vault 檔 / 3 Airtable 紀錄 / 1 GitHub commit 預期 ✅

### 3.5 README 驗證

`pipeline/destatis_daily.README.md`(144 行,超過 QA 預期 > 50 行)

- ✅ 架構圖(ASCII art cron → pipeline → fetch → vault + Discord + Airtable + GitHub)
- ✅ 3 個 source 表格(對齊 `destatis_sources.json`)
- ✅ 新增 source 步驟(5 步驟 + dry-run 驗證)
- ✅ 跑指令(無 --push / --push / --dry-run / --source / --channel)
- ✅ 環境變數說明
- ✅ 回傳碼說明
- ✅ 測試指令
- ✅ 監控位置(Airtable / Discord / GitHub / cron log)
- ✅ 已知限制 5 條(BOM / DST / cron 不 retry / TZ=Europe/Berlin / mid-week)
- ✅ 相關檔案清單
- ⚠️ **Minor — README line 139 寫「pipeline 是 1081 行」但 brief 寫 750 行(這是 brief 的舊數字,實際已 1081),不影響功能**
- ⚠️ **Minor — README line 96 寫「Scenario 1 在 host 離線時跳過」,但實際 pytest 跑出 1 skipped,代表 host 在跑測試時 offline,這次是 1 個 skip,符合預期**

### 3.6 既有 pipeline 隔離

- ✅ `python -m pipeline.youtube_daily --help` 正常輸出(--dry-run / --mode / --channels / --n-channels / --skip-store / --pipeline-run-id / --video-id / --force)
- ✅ `python -m pipeline.news_daily --help` 正常輸出(--dry-run / --mode / --limit / --chunk-size / --max-days / --quota-primary / --quota-other / --min-relevance / --min-quick-score / --skip-store / --pipeline-run-id)
- ✅ 兩個 pipeline 的 arg 集合跟 destatis **不重疊也不衝突**
- ✅ destatis 用相同 `processed_store` 共用 Airtable ledger 但用不同 `source_type`(`destatis_csv` vs `youtube` vs `news`),互不干擾

---

## 4. Git 狀態

```
git status --short:
 M pipeline/scripts/dashboard_dedup.py          (殘留,非 destatis)
 M pipeline/scripts/recommend_long_form.py      (殘留,非 destatis)
 M podcast-kb/vault/Daily/2026-08-13/_index.md (殘留,非 destatis)
 D "podcast-kb/.../finanzfluss_..."            (殘留,非 destatis)
 ... 等 4 個殘留(都是其他 pipeline 的工作未 commit,非 destatis 範圍)
?? pipeline/config/destatis_sources.json        (T1 寫的)
?? pipeline/destatis_daily.README.md            (T5 寫的)
?? pipeline/destatis_daily.py                   (T3 寫的)
?? pipeline/lib/destatis_csv.py                 (T1 寫的)
?? pipeline/tests/test_destatis_csv.py          (T1 寫的)
?? pipeline/tests/test_destatis_daily_integration.py  (T4 寫的)
?? pipeline/tests/test_destatis_daily_render.py (T3.1 寫的)

git log --oneline -3:
6510464 podcast channels: drop marktcheck, add insightsimmo + sogehtbrandschutz + alexanderschmid_podcast
c0ca1ba immobilien-kb: 2026-08-13 destatis daily digest
5f27e47 podcast-kb: 2026-08-13 daily digest
```

- ✅ destatis 7 個檔案(6 個 source code + 1 個 README)全部 `??` untracked
- ✅ 沒有意外的 `??` 殘留 destatis 檔
- ✅ `channels.json` commit `6510464` 已在 main ✅
- ✅ 之前的 T3.2 commit `c0ca1ba` 也在 main(vault/Airtable 雖清掉但 commit SHA 留著)— 對齊 brief
- ✅ 其他 modification 都不是 destatis 範圍(屬於其他 pipeline 工作中),QA 不評估

---

## 5. 發現的問題清單

### Critical(必須修)

無。

### Important(應該修,在下次 commit 前)

1. **`article_type="stat-table"` 沒在 `airtable_processed_content_schema.json` 的 singleSelect 選項中**
   - 檔案:`pipeline/scripts/airtable_processed_content_schema.json` line 109-114
   - 問題:`destatis_daily.py` line 1005 寫 `article_type="stat-table"`,但 schema 只列 `short-summary / long-form / pending-long-form / skipped-long-form`
   - 影響:雖然 `typecast: True` 會讓 Airtable 在第一次寫入時自動新增 `stat-table` 選項,production cron 跑第一次時會成功(後續 record 也都會 work),但 **本地 schema 檔會跟 production Airtable state 脫鉤**
   - 修法:把 `"stat-table"` 補到 `airtable_processed_content_schema.json` 的 article_type options 內,或為 destatis 改用 `short-summary` 之類既有值

2. **`run_pipeline` docstring 過時**
   - 檔案:`pipeline/destatis_daily.py` line 820
   - 問題:docstring 寫 `channel: Discord channel alias (default: 'home').`,實際 default 是 `tao`(line 100 + 809)
   - 影響:不大(讀 code 的人會困惑,但 CLI 行為是對的)
   - 修法:line 820 改為 `channel: Discord channel alias (default: 'tao').`

### Minor(可選,非阻擋)

1. **README line 139 寫「pipeline 是 1081 行」但 brief 寫 750 行**
   - 實際檔案 1081 行 — 數字本身是對的,brief 是舊的
   - 不影響功能,只是 metadata 對齊問題

2. **`/tmp/destatis_csv/` 累積 CSV 暫存檔(目前 63 個)**
   - 每次 dry-run 或正式 run 都會新加 stamp 檔名,沒有 retention policy
   - 不影響 production(本機 /tmp 通常會被 OS 自動清),但若長時間跑多次會佔空間
   - 修法:在 `fetch_csv` 加 max-age cleanup,或寫到固定檔名(覆蓋)

3. **`_render_index_md` line 423-425 內 GitHub URL hardcode `clarktao-dev/ado_hermes_reddit_scrapper`**
   - 跟 `push_to_github.py` line 69-70 一致
   - 跟 youtube_daily / news_daily 風格一致(都 hardcode,沒 config 化)
   - 不是 destatis 特有的問題,不修也可以

4. **README line 96 寫「Scenario 1 在 host 離線時跳過」,但實際 1 個 skipped 代表這次 pytest 跑時 host offline**
   - 屬於設計意圖(host offline skip 避免 CI 紅),不是 bug
   - 但如果 production 跑前想 100% 確認 destatis CSV URL 都還活著,需要手動跑 Scenario 1 確認

---

## 6. 建議

1. **在 commit 進 git 索引前,backend engineer 應先修 2 個 Important issue**:
   - 補 `stat-table` 到 `airtable_processed_content_schema.json`
   - 修 `run_pipeline` docstring 的 default channel 字串
2. **production cron 第一次跑(8/14 週五 Berlin 03:00 CEST)後,應監控**:
   - Airtable ProcessedContent table 內有 3 筆 `destatis_csv` 紀錄
   - Discord `#tao` channel 收到 3 個 embed(標題前綴 `🏗️ [Destatis 官方數據]`)
   - GitHub main branch 有新 commit
   - `~/.hermes/cron/output/destatis_daily/2026-08-14.log` 沒錯誤
3. **下一階段可考慮**:
   - `/tmp/destatis_csv/` retention policy
   - `reference_period` 從 CSV 第一個資料行抽 year/month,組成 `destatis:{id}:{YYYY-MM}` 讓 dedup 更精準(目前固定 `:latest`,若同一 source 兩份 reference_period 同時存在會漏抓 — 但罕見,見 README line 124-125)

---

## 7. 結論

**Stage 1 整套是否可上 production?** ✅ **可上 production**(在補 2 個 Important fix 後更安心)

**理由**:
- pytest 82 passed / 1 skipped ✅
- Dry-run 行為完全符合預期,沒有意外 push ✅
- Vault 渲染 TS / cross-section 場景都正確(T3.1 修法已生效)✅
- Airtable dedup 閘門乾淨,新 run 不會被舊 ledger 阻擋 ✅
- Cron 設定完整(時區、DST、env source、log 位置、failure mode 都寫好)✅
- README 內容完整(架構、3 個 source、加 source 步驟、跑指令、測試、監控、限制)✅
- 沒有跟 youtube / news pipeline 衝突 ✅
- Git 狀態乾淨(只有預期內的 7 個 untracked 檔)✅

唯一要 backend engineer 注意的:`article_type="stat-table"` schema 同步(第一次跑時 `typecast: True` 會自動加新選項,所以不會壞,但建議主動補 schema 檔以保持文件跟 production state 一致)。
