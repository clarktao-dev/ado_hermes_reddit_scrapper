# QA Report — Plan 3 (Daily Digest)

**QA Engineer:** Hermes Agent (delegated, 2026-08-17)
**Date:** 2026-08-17
**Backend:** uncommitted changes (working tree)
**Plan source:** `/root/.hermes/plans/plan-3-daily-digest.md`

## Summary

**PASS (7/7)** — 所有程式驗證通過。**唯一 blocker:** `discord_picks.py` daemon 必須先啟動才能讓 cron 推的 digest 訊息實際被記錄;jobs.yaml 已標 `depends_on: discord_picks_daemon`。

## Test Results

| # | Test | Status | Notes |
|---|---|---|---|
| 1 | 程式碼 import | ✅ PASS | All 7 symbols import cleanly; `EMOJI_TO_PICKED={'✅':'yes','❌':'no','🟡':'maybe'}`; `TARGET_CHANNEL_ID=1539010288026779688` |
| 2 | `push_to_discord` 不重複 | ✅ PASS | 1 definition at line 344; 0 `from immobilien_kb_tools_discord_sender` |
| 3 | dry-run 訊息格式 | ✅ PASS | 10 candidates (5 新聞 + 5 YouTube + 0 Reddit); 🟡 emoji renders correctly; banner present; final line `DRY-RUN: would push 1 header + 10 candidates` — no real push |
| 4 | Airtable schema | ✅ PASS | Table `DailyDigestPicks` (`tblYNBqwSb4a7tV8S`) exists with exactly 11 fields matching `FIELD_SPEC` |
| 5 | `discord_picks.py` 初始化 | ✅ PASS | `PickBot`, `EMOJI_TO_PICKED`, `TARGET_CHANNEL_ID`, `update_pick_by_message` all importable |
| 6 | `cron/jobs.yaml` 合法 | ✅ PASS | 2 jobs present, correct names, both `enabled=False` |
| 7 | `--test-pick` 行為 | ✅ PASS | Exit code = 3, error `no DailyDigestPicks row with message_id=FAKE_MSG_ID`, Airtable still 0 records (no pollution) |

## 觀察發現(低風險)

1. **YouTube URL 是 bare video id**(e.g. `來源: CCnWZ1IFLms`)— code path 嘗試 `meta.url || meta.source_url || source_id`,但 YouTube record 的 metadata JSON 不帶 URL,fallback 到 bare id。**UX 不優但 not a bug**;Plan author flagged。
2. **YouTube vault 全為 `(尚未寫入)`** — 5/5 YouTube candidate 沒有 `output_path`,正常因為這些 record 還沒進到 vault(只有新聞有 vault_path)。Daily digest 來源只看 `processed_at DESC`,沒 filter vault_path 存在與否。
3. **cron/jobs.yaml schema 是 dict-wrapped**(`{version, timezone, jobs: [...]}`),不是 top-level list — 任何 cron 執行器需走 `data["jobs"]`。Backend 確認這個 schema 是 hermes cron 的標準格式。
4. **Airtable 0 records 正常**:`--dry-run` 沒寫 row、`--test-pick FAKE_MSG_ID` 找不到 row 也沒寫 — 兩個測試後都還是 0 records,符合預期。

## 風險與 follow-up

### ✅ Plan 3 ready to merge,但 daemon 必須先啟動

`discord_picks.py` daemon 必須先在 VPS 上 run 起來(用 nohup / systemd / pm2 之類),cron 推的 digest 訊息才會被記錄成 labeled data。**沒起 daemon 之前不要開 cron**。

**啟動方式建議**:
- 簡單: `nohup /root/.hermes/hermes-agent/venv/bin/python3 pipeline/lib/discord_picks.py > /tmp/discord_picks.log 2>&1 &`
- 進階: systemd unit(後續可加)
- 注意: Bot 需要 `GUILD_MESSAGE_REACTIONS` intent(看 Discord Developer Portal 的 Bot settings)

### Part 2(產文流程)— 等累積 30 天 / 60 篇 labeled data 再啟動

每日 digest 跑 1 個月後,看 `DailyDigestPicks` 累積的 `picked=yes` 數量:
- ≥ 60 篇 → 跑一次 `recommend_long_form`-style 分析,推「你之前 ✅ 過類似主題」
- < 60 篇 → 累積更多

### 其他 follow-up

1. **YouTube URL 視覺優化**(nice-to-have):讓 `processed_store.get_recent` 回傳時把 `metadata.url` 拆成獨立欄位,或 `daily_digest.py` 補一個 `https://youtu.be/{video_id}` fallback。
2. **冬令時間**(`-1` 風險):目前 schedule `30 4 * * *` 是夏令基準;冬令需手動暫停 + 改 `30 5 * * *`。Backend 已寫在 yaml 註解。
3. **`--recap` flag**:週回顧推送的 flag 還沒寫,Job 2 enabled:false 是正確的 — Part 2 補 flag 後再開。
4. **digest_recap 與 longform 共 channel**:plan 已標記是暫時共存,如果之後週回顧訊息量變大,可以區隔到新 channel。

## Recommendation to PM

- **✅ Plan 3 可以 merge** — 7/7 程式驗證通過。
- 合併後用 nohup 啟動 daemon → 開 cron `digest_candidates_cest_0630` 的 `enabled: true`。
- **第一個 digest 推送前** 我會跑一次手動 `daily_digest.py`(無 dry-run) 推到 channel `1539010288026779688`,讓你看實際格式。你按 emoji 後 daemon 會自動寫進 Airtable。
- Plan 3 Part 2(產文流程) 等累積 30 天 labeled data 再啟動。