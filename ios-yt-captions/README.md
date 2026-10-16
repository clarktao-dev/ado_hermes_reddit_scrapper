# iPhone 字幕 App（經 GitHub 信箱）

你要的流程：

1. **Agent / VPS** 把要抓的 YouTube 連結寫進 GitHub：`transcript-bridge/inbox/`
2. **你打開 iPhone App** → 按「同步佇列」
3. App 自己撈連結、用手機網路抓現成字幕、上傳到 `transcript-bridge/outbox/`
4. **VPS** 跑 import，把 outbox 存進 transcript cache，再做 AI 整理

**不必**一直開 Mac 終端，也**不必**為日常同步開 Tailscale（仍可用，但非必須）。

## 一次安裝 App（Mac Xcode → iPhone）

1. 安裝 Xcode，新建 iOS App（SwiftUI），名稱 `YTCaptions`
2. 把本目錄 `YTCaptions/*.swift` 拖進專案
3. Info：允許 Arbitrary Loads（若還要用 http webhook；純 GitHub HTTPS 可不用）
4. 選你的 iPhone → Signing 選 Apple ID → ▶ Run

## App 設定

| 欄位 | 值 |
|------|-----|
| Owner | `clarktao-dev` |
| Repo | `ado_hermes_reddit_scrapper` |
| Branch | 先用 `cursor/mac-transcript-bridge-7a35`（merge 後改 `main`） |
| Token | GitHub Fine-grained PAT，權限 **Contents: Read and write** |
| 語言 | `de,en` |

建立 Token：GitHub → Settings → Developer settings → Personal access tokens → Fine-grained  
只勾這個 repo 的 Contents 讀寫即可。

## Agent 怎麼丟連結

```bash
# 在 VPS / 本機 repo
PYTHONPATH=. python3 pipeline/scripts/transcript_bridge_enqueue.py \
  --url 'https://www.youtube.com/watch?v=VIDEO_ID' \
  --channel Finanztip \
  --push
```

或設 `YOUTUBE_TRANSCRIPT_SOURCE=mac` + `TRANSCRIPT_BRIDGE_PUSH=1`，  
`youtube_daily` 缺字幕時會自動寫 inbox 並 push。

## VPS 怎麼收結果

```bash
git pull
PYTHONPATH=. python3 pipeline/scripts/transcript_bridge_import.py --push
```

建議加一條 cron：每小時 `git pull && transcript_bridge_import.py --push`。

## 日常你只要

1. （可選）等 Agent 自動丟 inbox，或自己 enqueue  
2. 打開 iPhone App → **同步佇列**  
3. 完成

## 限制（誠實說）

- iOS **不能可靠 24h 背景自動跑**；需要你偶爾打開 App 按同步（或之後再加通知提醒）
- 影片沒有字幕就抓不到（不會用 Whisper）
- 雲端 Cursor **無法替你按 Xcode 安裝**；要在 Mac 裝一次到手機
