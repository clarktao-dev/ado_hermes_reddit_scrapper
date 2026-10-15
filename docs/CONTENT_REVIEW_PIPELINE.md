# 內容審核發布管線（Plan 6 設計）

目標：你平常只按 ✅；每兩天系統自動整理並寫初稿；**只有你審核通過才發布**。
你真正要做的事 = 最後看一眼、改幾個字、按通過。

---

## 0. 一句話結論

| 問題 | 答案 |
|------|------|
| 要不要新開一個 Agent？ | **要，但只開一個「草稿寫手 Automation」**；不要再複製一個完整 Ado。 |
| Ado（Hermes / VPS）負責什麼？ | **永遠在線的基礎設施**：抓文、Discord 推播、✅ 監聽、狀態機、發布閘門。 |
| Cursor Agent 負責什麼？ | **每兩天批次寫初稿 + 依審核結果改稿**；不碰自動發布。 |
| 你負責什麼？ | Discord `#審核區` 看草稿 → 改字 / 通過 / 退回。 |

---

## 1. 現況（已有）vs 缺口

### 已有（Plan 4）

```
每日管線 → Discord #每日頭條 / #每日podcast / #每日reddit
                ↓ 你按 ✅
         discord_picks.py daemon
                ↓
         Firestore reactions（ReactionPicks）
                ↓ 週二 / 五 07:00 CEST
         weekly_recap.py → #挑文區（問你怎麼處理）
```

### 缺口（本設計要補）

1. `#挑文區` 的二次 emoji（寫長文 / Podcast / 跳過）**還沒寫回 Firestore**
2. 沒有「依挑文自動寫初稿」步驟
3. 沒有 **draft → approved → published** 狀態機；沒有「未審核不得發布」閘門
4. `podcast-content-repurposer` 已能產 6 平台草稿，但沒接到挑文流程

---

## 2. 目標流程（你忙碌時也能跑）

```
[A 永遠在跑 · Ado/VPS]
  抓文 → Discord 推播
  你閒逛時按 ✅ → ReactionPicks(status=picked)

[B 每兩天 · Cron + Cursor Automation]
  撈 status=picked（過去 ~3 天、尚未進草稿佇列）
  → 寫 ContentJobs（一則挑文 = 一張工單）
  → 觸發「草稿寫手」寫初稿到 vault drafts/
  → Discord #審核區 推「待審核」卡片（含草稿連結）
  → ContentJobs.status = awaiting_review

[C 你有空時 · 只做這步]
  在 #審核區：
    ✅ = 通過（可小改後再按）
    ✏️ = 退回改稿（thread 寫修改意見）
    ❌ = 不要這篇

[D 通過後 · Ado 閘門腳本]
  只有 status=approved 才允許：
    - 推網站
    - 推社群
  否則永遠只是草稿
```

核心原則：**生成與發布分離。Agent 永遠只能寫 draft；發布權只在「審核通過」之後的腳本。**

---

## 3. 職責切分（方便維護）

### 3.1 留在 Ado / Hermes（VPS）— 不要換成 Cursor Agent

| 元件 | 原因 |
|------|------|
| `youtube_daily` / `news_daily` / `podcast_daily` / reddit | 定時、確定性、要本機憑證 |
| `discord_picks.py` daemon | 長連線 WebSocket，必須 24/7 |
| Firestore `reactions` / 新 `content_jobs` | 單一真相來源（SSOT） |
| `weekly_recap` → 升級為 `draft_batch` 排程 | Cron 可靠、兩天一次即可 |
| `publish_approved.py`（新） | **硬閘門**：讀 status，拒絕非 approved |

Ado 的人格 Prompt **不必重寫**；只加一條硬規則（見 §6.1）。

### 3.2 新建：一個 Cursor Automation「草稿寫手」

名稱建議：`content-draft-writer`（不要叫第二個 Ado）

| 觸發 | 行為 |
|------|------|
| 每兩天（或被 Ado cron webhook / 手動 `@`） | 讀 `content_jobs` 裡 `status=queued` 的工單 |
| | 讀 vault 原文 → 產網站長文 + 社群草稿 |
| | 寫入 `drafts/<job_id>/`，更新 job → `awaiting_review` |
| Discord thread 有 ✏️ 修改意見 | 依意見改稿，再推回 `#審核區` |

**不要**讓這個 Agent：發社群、改 production 網站、碰 `DISCORD_BOT_TOKEN` 以外的發布密鑰（若密鑰在 VPS，發布只留 VPS）。

### 3.3 你（人類）— 唯一審核者

- 平常：逛 channel 按 ✅（已在做）
- 每兩天後有空：打開 `#審核區`，每則花 1–3 分鐘
- 原則：**不通過 = 不發布**；沒按就是草稿

---

## 4. 資料模型（可擴充的關鍵）

### 4.1 Firestore `reactions`（既有，小擴）

保留現有欄位，新增：

| 欄位 | 說明 |
|------|------|
| `workflow_status` | `picked` → `queued` → `drafted` → `awaiting_review` → `approved` / `rejected` / `skipped` |
| `intent` | `long_form` / `podcast` / `social_only` / `other`（來自 #挑文區 二次反應；可先預設 `long_form`） |
| `job_id` | 對應 `content_jobs` 文件 id |

### 4.2 Firestore `content_jobs`（新集合 · SSOT）

一則挑文 → 一張工單。之後加平台、加審核人、加 A/B 文案都擴這張表，不要改 daemon。

```text
job_id            string   # 文件 id
reaction_id       string
source_type       youtube|news|reddit|podcast
title             string
source_url        string
vault_path        string   # 原文 digest 路徑
intent            long_form|podcast|social_only|other
status            queued|writing|awaiting_review|revising|approved|rejected|published|failed
artifacts         map      # { website, facebook, threads, twitter, linkedin, newsletter, ... }
review_channel_id string
review_message_id string
revision_notes    string   # 你在 thread 留下的修改意見
created_at        timestamp
updated_at        timestamp
published_at      timestamp|null
error             string|null
```

### 4.3 Vault 草稿目錄（檔案真相 + Git 可回顧）

```text
hermes_vault_collection/
  drafts/
    YYYY-MM-DD/
      <job_id>/
        META.json          # 對應 content_jobs 快照
        website.md         # 網站長文初稿
        facebook.md
        threads.md
        twitter.md
        linkedin.md
        newsletter.md      # 依 intent 決定產哪些
  published/               # 僅 approved 後由 publish 腳本搬入或標記
```

狀態以 Firestore 為準；vault 檔案是產物。Cursor Agent 只寫 `drafts/`，**禁止**寫入 `published/`。

---

## 5. 狀態機（硬規則）

```text
picked
  → queued          # cron 收進批次
  → writing         # Agent 開工
  → awaiting_review # 草稿推到 #審核區
  → revising        # 你按 ✏️ + 留言
  → awaiting_review # 改完再審
  → approved        # 你按 ✅
  → published       # 只有 publish_approved.py 能轉這步
  ↘ rejected / skipped
```

`publish_approved.py` 偽碼：

```python
assert job["status"] == "approved", "refuse: not approved"
# 然後才允許網站 / 社群 API
job["status"] = "published"
```

任何 Agent / cron / 手動腳本若跳過此檢查 = bug。

---

## 6. Prompt 與你要在 Ado 做的事

### 6.1 Ado（Hermes）系統規則 — 加這一段即可

```text
【內容發布硬規則 — 不可覆寫】
1. 你不得自行將任何內容標記為 published，也不得呼叫社群/網站發布 API，
   除非該 content_job 的 status 已是 approved。
2. 從 ReactionPicks / ✅ 產生的文案，預設只能寫入 drafts/，status=awaiting_review。
3. 使用者未在 #審核區明確通過前，一律視為草稿。
4. 若使用者說「直接發」「幫我發上去」但 job 非 approved：先提醒需審核，
   並把 job 留在 awaiting_review；不要擅自發布。
5. 你負責基礎設施（抓文、監聽 ✅、排程、閘門）；長文初稿交給
   content-draft-writer Automation。
```

### 6.2 Cursor Automation「草稿寫手」— 完整 Prompt

```text
你是 Hermes 內容管線的「草稿寫手」（content-draft-writer）。

## 任務
1. 讀取 Firestore content_jobs 中 status=queued（或 revising）的工單。
2. 依 vault_path 讀原文 digest（摘要 / 分析師視角 / 製作人視角 / 詞彙）。
3. 依 intent 產出對應草稿（預設 long_form）：
   - website.md（繁中長文，可上網站）
   - 社群：facebook / threads / twitter / linkedin（規則見 podcast-content-repurposer/SKILL.md）
   - intent=podcast 時加 podcast-outline.md
4. 寫入 hermes_vault 的 drafts/YYYY-MM-DD/<job_id>/，並更新 META.json。
5. 更新 job：artifacts 路徑、status=awaiting_review。
6. 在 Discord #審核區發一則審核卡（標題、來源、各草稿摘要前 200 字、job_id）。
7. 若 status=revising：必讀 revision_notes，只改被點名的問題，不要整篇重寫。

## 硬限制
- 禁止發布到網站或社群。
- 禁止修改 published/ 或把 status 設成 approved / published。
- 禁止刪除使用者原文 vault。
- 一則 job 一次處理；失敗寫 error 並 status=failed，不要卡住整批。
- 語言：台灣繁體中文；德文專有名詞保留原文並附中文。

## 輸出
每則 job 結束時回報：job_id、產了哪些檔、Discord message id、下一個等人做的動作（審核）。
```

### 6.3 Discord `#審核區` 操作約定（給你自己）

| Emoji | 意思 | 系統行為 |
|-------|------|----------|
| ✅ | 通過，可以發 | status→approved；之後由 publish cron/腳本發 |
| ✏️ | 要改 | 請在 thread 用一句話寫要改什麼；status→revising |
| ❌ | 不要 | status→rejected；不發 |

建議審核卡固定格式（方便 daemon 解析）：

```text
📝 待審核 | job_id=<id>
標題：...
來源：...
Intent：long_form
草稿：website · FB · Threads · X · LinkedIn
——
請按：✅通過 · ✏️改稿（thread 說明） · ❌不要
```

### 6.4 可選：二次意圖（#挑文區）

若你希望「先選長文還是 Podcast」再寫稿：

- 保留現有 `weekly_recap` 的 ✅🟡📝❌
- **補做** reaction → 寫回 `intent`（Plan 4 註記的 follow-up）
- 然後 draft_batch 只撈「已有 intent」的 picks

若你想再省一步：跳過二次選擇，**全部預設 long_form + 社群組**，審核時再 ❌ 掉不喜歡的。以「你很累」為前提，建議先用預設 long_form。

---

## 7. 排程建議

| 何時 | 誰 | 做什麼 |
|------|-----|--------|
| 全天 | `discord_picks` | 收 ✅ |
| 每兩天 07:00 CEST（可沿用週二/五，或改 `0 5 */2 * *`） | Ado cron `draft_batch.py` | picked→queued，觸發寫手 |
| 寫手跑完 | Automation | 初稿 + #審核區通知 |
| 你有空 | 你 | 審核 |
| 每小時或你按完後 | `publish_approved.py` | 只發 approved |

兩天一批 ≈ 現有週二/五節奏；若改成真正的 `*/2`，記得冬令時間註記（與 `cron/jobs.yaml` 相同）。

---

## 8. 實作順序（低風險、可擴充）

### Phase A — 狀態與閘門（先做，立刻有「不會誤發」）

1. `content_jobs` schema + `setup_firestore` 擴充
2. `reactions.workflow_status` 欄位
3. `publish_approved.py`（即使暫時 no-op 發布，也先有 assert）
4. Ado Prompt 加上 §6.1 硬規則

### Phase B — 自動草稿

1. `draft_batch.py`：撈 picked → 建 jobs
2. Cursor Automation + §6.2 Prompt
3. 接上 `podcast-content-repurposer` 模板
4. `#審核區` 推播格式固定

### Phase C — 審核閉環

1. 審核區 reaction listener（可併入 `discord_picks` 或獨立小 daemon）
2. ✏️ + thread 文字 → `revision_notes` → 再觸發寫手
3. approved → `publish_approved` 真的發網站/社群

### Phase D — 擴充點（之後再加，現在預留欄位即可）

- 多審核人 / 多品牌
- 新平台：只加 `artifacts` key + repurposer 模板
- A/B 標題、排程發布時間、`publish_at`
- 網站 CMS adapter（Ghost / WordPress / 靜態 repo）獨立成 plugin

---

## 9. 你現在要做的事 vs 我（這個 Agent）能做的事

### 你要做（Ado / Discord / Cursor 設定）

1. **Ado**：貼上 §6.1 硬規則（或確認 Hermes system prompt 可改）
2. **Discord**：建 `#審核區` channel，把 id 告訴管線設定
3. **Cursor**：新建 Automation `content-draft-writer`，貼 §6.2；綁這個 repo + vault repo 權限
4. **繼續**只按 ✅；審核時只進 `#審核區`
5. （可選）把現有週二/五 recap 改成「直接進草稿、不再二次選」，減少你點擊

### 我能做（程式實作，等你說開始）

1. Phase A/B/C 的 schema、腳本、cron 條目、測試
2. 審核卡格式與 reaction → Firestore 更新
3. 把 repurposer 接到 `content_jobs`
4. `publish_approved` 閘門 + dry-run
5. 更新 `cron/jobs.yaml`、`INTEGRATION.md`

### 不必做的

- 不要再養一個「全能第二 Ado」——職責會糊掉、難維護
- 不要讓寫手 Agent 擁有發布密鑰
- 不要在沒有 `approved` 前接正式社群 API

---

## 10. 成功標準

- 你連續一週只做「按 ✅ + 偶爾審核」，仍穩定產出草稿
- 任一非 `approved` 的 job 呼叫 publish → 被拒絕（有測試）
- 新增一個社群平台 = 加模板 + artifacts 欄位，不必改 daemon
- 草稿全在 Git/`drafts/`，審核紀錄全在 Firestore，出事可回放

---

## 附錄：與舊 Plan 的關係

| Plan | 內容 | 本設計 |
|------|------|--------|
| Plan 3 | 主動推 15 候選 | 已棄用 |
| Plan 4 | 被動 ✅ + 週回顧 | **保留**；recap 可簡化或升成 draft_batch |
| Plan 5 | Vault 路徑整理 | 草稿目錄遵守同一 vault 慣例 |
| Plan 6（本文件） | 自動草稿 + 人審 + 發布閘門 | 新增 |
