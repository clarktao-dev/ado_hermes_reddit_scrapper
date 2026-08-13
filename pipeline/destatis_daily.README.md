# Destatis 每日數據 Pipeline

> 抓德國聯邦統計局 (Destatis) 官方 CSV → 寫 vault → 推 Discord + GitHub + 寫 Airtable ledger。

`pipeline.destatis_daily` 是 Stage 1 第四個子系統,跟 `youtube_daily`、`news_daily` 並列,共用同一份
Airtable ledger 與同一個 Discord sender,但只抓 **3 個低頻的官方統計 topic**(每月/每季更新)。

## 架構

```
       ┌────────────────────────────────────────────────────────┐
       │  cron: Europe/Berlin 週五 03:00 (CEST, UTC+2)          │
       │  Hermes cron table 內 id = 2049d7a025c9                │
       │  觸發 expression: 0 1 * * 5 (host system clock = UTC)  │
       └────────────────────┬───────────────────────────────────┘
                            ↓
                  destatis_daily.py (run_pipeline)
                            ↓
            ┌───────────────┴───────────────┐
            ↓                               ↓
  load destatis_sources.json      for each enabled source:
  (篩 enabled: true)                        ↓
                            ┌───────────────┴───────────────┐
                            ↓                               ↓
                  fetch_and_parse                  Airtable dedup gate
                  (3 retries, UTF-8)               source_id = "destatis:{id}:latest"
                            ↓                               ↓
                       DestatisDataset          already processed → skip
                            ↓                               ↓
                            └───────────────┬───────────────┘
                                            ↓
                            ┌───────────────┼───────────────┐
                            ↓               ↓               ↓
                       vault 寫入     Discord 推送      Airtable
                   Stat/{UTC 當天}/  每個 dataset      mark_processed
                    + _index.md     一個 embed         (channels=[destatis.{id}])
                                            ↓
                                     GitHub push
                                    immobilien-kb/
```

## 已啟用的 3 個 Source

| source_id | 中文名 | 主題 | CSV URL 端點 |
| --- | --- | --- | --- |
| `auftragseingang_bauhauptgewerbe` | 主承攬業新訂單(實質,原值) | 營建業訂單 | `…/auftragseingang-bauhauptgewerbe.csv?__blob=value&v=70` |
| `genehmigte_wohnungen_monat` | 許可住宅數(月報) | 建照 | `…/genehmigte-wohnungen-monat.csv?__blob=value&v=82` |
| `investments_construction` | 營建業投資 | 投資 | `…/investitionen-baugewerbe.csv?__blob=value&v=2` |

完整 metadata 見 [`pipeline/config/destatis_sources.json`](./config/destatis_sources.json)。

## 新增 Source 步驟

1. 在 [`pipeline/config/destatis_sources.json`](./config/destatis_sources.json) 的 `sources` 陣列內新增一筆,
   必填欄位:`id` (kebab 命名)、`name_de`、`name_zh`、`url` (從 Destatis 圖表頁右鍵 Inspect → Network → 找 `.csv` 端點)、
   `enabled: true`、`vault_filename`、`_source_page` (圖表頁 HTML URL,供 vault markdown footer 引用)。
2. 跑一次 dry-run 驗證 CSV 抓得到、欄位可解析:
   ```bash
   python -m pipeline.destatis_daily --source <new_id> --dry-run
   ```
3. 跑一次本地寫入(不推送),檢查 `immobilien-kb/vault/Stat/{當天}/<new_id>.md` 內容正確。
4. 確認無誤後正式 `--push` 一次。
5. 跑整合測試確保新 source 不破壞既有流程:
   ```bash
   pytest pipeline/tests/test_destatis_daily_integration.py -v
   ```

## 跑

工作目錄必須是 repo root(`/root/projects/ado_hermes_reddit_scrapper`),
Python 必須是 `/root/.hermes/hermes-agent/venv/bin/python` (有裝 `requests`、`pandas` 等依賴)。

| 指令 | 行為 |
| --- | --- |
| `python -m pipeline.destatis_daily` | 跑全部 enabled source,**只寫 vault + Airtable**,不推 Discord,不上 GitHub |
| `python -m pipeline.destatis_daily --push` | 同上 + **推 Discord + push GitHub** |
| `python -m pipeline.destatis_daily --dry-run` | 只印出會做什麼(列出 3 個 source 跟 vault 預計寫入),不實際 fetch、不寫任何東西 |
| `python -m pipeline.destatis_daily --source <id>` | 只跑單一 source(其他 source 跳過) |
| `python -m pipeline.destatis_daily --channel <alias>` | 覆寫預設 Discord channel(`tao`) |

### 環境變數

- `AIRTABLE_PROCESSED_CONTENT_BASE_ID`(預設 `appHilorcrC5T0p2u`):ledger base。
- `AIRTABLE_PROCESSED_CONTENT_TABLE`(預設 `ProcessedContent`):ledger table。
- 從 `/root/.hermes/.env` source。

### 回傳碼

- `0`:全部成功(或全部被 dedup 跳過,也是預期成功)。
- `1`:至少一個 source 失敗。詳細錯誤在 stdout / stderr,`last_error` 欄位會留 Airtable / Discord / GitHub 失敗原因。

## 測試

```bash
# 全部 82 passed, 1 skipped(Scenario 1 在 host 離線時跳過)
pytest pipeline/tests/test_destatis_csv.py \
         pipeline/tests/test_destatis_daily_render.py \
         pipeline/tests/test_destatis_daily_integration.py -v
```

- `test_destatis_csv.py` — CSV 解析、URL 編碼、retry 邏輯單元測試。
- `test_destatis_daily_render.py` — `_render_index_md` / `_render_dataset_md` 純函式測試。
- `test_destatis_daily_integration.py` — 4 個 end-to-end scenario:
  1. `TestFetchAndParseEndToEndSmoke` — 實際抓 Destatis CSV(離線時 skip)。
  2. `TestVaultWipeAndWriteIdempotent` — vault 寫入 + 重跑 byte-identical 驗證。
  3. `TestAirtableDedupBlocksRerun` — Airtable ledger 滿載時 3 個 source 全部 short-circuit。
  4. `TestDiscordPushMocked` — 3 個 embed 推到 `tao` channel、title 前綴 `🏗️ [Destatis 官方數據]`。

## 監控

| 來源 | 看什麼 |
| --- | --- |
| Airtable `ProcessedContent` table | `source_type = "destatis_csv"` 的紀錄(`source_id` 格式 `destatis:{source_id}:latest`,`channels` 含 `destatis.<id>`) |
| Discord `#tao` channel (id `1495562787685011616`) | 推送歷史,每週五早上 03:00 Berlin / 01:00 UTC 後應有 3 個 embed |
| GitHub `immobilien-kb/vault/Stat/` | vault 結構,當週日期資料夾內應有 3 個 .md + 1 個 `_index.md` |
| `~/.hermes/cron/output/destatis_daily/` | cron 執行 log 與 stderr |

## 已知限制

- **只能抓 Destatis 主站公開的 highchart CSV** — 目前 3 個 Bauen 主題(訂單 / 建照 / 投資)。
  GENESIS-Online 平台上千個 dataset 需要登入,未列入 plan(改寫需要 `GENESIS_API_KEY` + SOAP 解析)。
- **`reference_period` 固定 `latest`** — 當 Destatis 推出新月份時,我們無法從 CSV 自動偵測
  該月份是否已處理過;目前靠 Airtable ledger 阻擋「同 source_id 重跑」,
  但若同一 source 兩份 reference_period 同時存在(罕見),不會被 dedup 抓到。
  未來 plan:從 CSV 第一個資料行抽 year/month,組成 `destatis:{id}:{YYYY-MM}`。
- **Cron 表達式不感知 DST** — 觸發時間是 `0 1 * * 5` UTC = Berlin 03:00 CEST(夏令)。
  冬令時(CET, 11 月初-3 月底)會變成 Berlin 02:00。若需要嚴格 03:00 冬令,
  未來要拆成兩個 cron job(各自覆蓋 DST 段/月)或加自適應 wrapper。
- **Cron 跑失敗不會 retry** — prompt 內明確指示「失敗只 log,不 retry」,避免重複推 Discord。
  下次週五會自動再跑,失敗紀錄在 `~/.hermes/cron/output/destatis_daily/`。
- **`TZ=Europe/Berlin` 不影響 vault 日期** — `destatis_daily.py` 內部用 `datetime.now(timezone.utc)`
  決定 vault 資料夾名稱,所以週五 Berlin 03:00 觸發時,vault 會寫到 `Stat/{UTC 當天}/`
  (DST 期間恰好是同一天,冬令時 = Berlin 仍 02:xx → UTC 仍 01:xx → 寫到前一天 → 跟「週五 Berlin 03:00」邏輯上一致)。
- **不會自動跑 mid-week 更新** — 只有週五 03:00 一次。如果 Destatis 週一釋出新資料,
  要等週五才進 vault / Discord。緊急 ad-hoc 推送:用 `--source <id> --push` 手動跑。

## 相關檔案

- `pipeline/destatis_daily.py` — main pipeline (1081 行)。
- `pipeline/lib/destatis_csv.py` — CSV fetch + parse (retry / encoding / data rows)。
- `pipeline/lib/processed_store.py` — Airtable ledger helper (與 youtube_daily / news_daily 共用)。
- `pipeline/config/destatis_sources.json` — 來源清單 (3 個 enabled)。
- `pipeline/tests/test_destatis_daily_integration.py` — 整合測試 4 個 scenario。
- `~/.hermes/cron/jobs.json` — cron job 紀錄 (`name: destatis_daily_weekly`, `id: 2049d7a025c9`)。
