# Reddit RSS 抓取測試 — 待檢視清單

狀態：**暫停中** (2026-08-07)

## 已完成測試

| # | 測試 | 結果 | 備註 |
|---|---|---|---|
| 1 | Layer 1: `.rss` + Chrome UA, limit=25 | ✅ 200, 25 篇 | `layer1_house.xml` |
| 2 | Layer 3a: `/r/Hausbau/top.json?t=month` | ❌ 403 (HTML 阻擋頁) | JSON endpoint 在此 VPS 被擋 |
| 3 | Layer 3b: `/r/Hausbau/new.json` | ❌ 403 | 同上 |
| 4 | limit=50 (間隔 8s) | ❌ 429 | 觸發 rate limit |
| 5 | limit=100 (間隔 60s) | ✅ 200, 100 篇 | `limit_100.xml`, 時間範圍 2025-12-12 ~ 2026-07-29 |

## 結論摘要

- **RSS endpoint 通，JSON endpoint 不通** (跟預期相反)
- **rate limit 寬鬆**: 間隔 60 秒單請求 OK，間隔 8 秒連發觸發 429
- **單次最多 100 篇** (limit=100 確認有效)
- **Reddit RSS 不提供**: score、comments、upvotes 數字
- **時間覆蓋**: 100 篇跨度約 8 個月 (置頂公告 + 用戶文混雜)

## 待檢視 / 待測試項目

### A. RSS 行為
- [ ] 25 篇裡的時間分布 (確認「今天 2026-08-07 發文數 = 0」是否成立)
- [ ] limit=200/500 是否也通 (需間隔 60s+)
- [ ] 換 subreddit 驗證 RSS 通 (e.g. r/Finanzen, r/Immobilien)
- [ ] `.rss` vs `/comments/xxx/.rss` (單篇) 留言 RSS 是否提供 comments 數

### B. JSON 繞路
- [ ] 換 User-Agent 風格 (e.g. mobile Safari, Firefox) 試 JSON endpoint
- [ ] 加 `Accept-Language: en-US,en;q=0.9` 試
- [ ] JSON 真的死透 → 走 RSSHub 公共實例 (rsshub.app)
- [ ] JSON 死透 → Google fallback (`site:reddit.com/r/Hausbau`)

### C. 分析可行性
- [ ] 100 篇跑 deepseek-v4-flash 翻譯+主題分析
- [ ] 想辦法拿回覆數 (單篇 RSS / HTML scrape / 接受沒有)
- [ ] popularity ranking 替代方案 (用發布時間+字數權重?)

### D. last30days 對照
- [ ] 回查 reddit-safe pipeline 當時拿到的 56-57 則怎麼處理 popularity
- [ ] last30days 真正的實作是怎麼繞 Reddit 反爬

## 檔案位置

```
/tmp/reddit-test/
├── CHECKLIST.md         (本檔)
├── layer1_house.xml     (25 篇, limit=25 第一次測試)
└── limit_100.xml        (100 篇, 間隔 60s 第二次測試)
```
