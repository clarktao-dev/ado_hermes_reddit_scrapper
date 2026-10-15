# YouTube Mac Transcript Worker

在家用 **MacBook** 抓 YouTube **既有字幕**（不重新語音辨識），再推給線上 Agent 做分析。  
VPS 只收文字，**永不直接打 YouTube**——這是避開資料中心 IP 被擋的關鍵。

## 為什麼這樣比較不會被擋

| 做法 | 結果 |
|------|------|
| VPS / 雲端 IP 抓字幕 | 很容易 429 / bot check |
| 付費 datacenter proxy | 一樣常被擋 |
| **家用住宅 IP（Mac）** | 成功率最高 |
| **再加瀏覽器登入 cookie**（Safari/Chrome） | 被擋時最有效的後備 |
| 慢速排隊 + 每日上限 + 退避 | 降低再被標記的機率 |

本工具抓的是 YouTube 已有的字幕軌（人工或自動產生），**不是 Whisper 重轉**。

## 架構

```
enqueue URL  →  Mac inbox  →  fetch captions  →  POST webhook  →  線上 AI 整理
                 (家裡)         (現成字幕)         (只傳文字)
```

## Mac 安裝（約 2 分鐘）

```bash
# 1) 把整個 youtube-mac-worker 資料夾拷到 Mac，例如 ~/youtube-mac-worker
cd ~/youtube-mac-worker
bash scripts/install_mac.sh

# 2) 編輯設定
nano config.toml
# 填 webhook.url = "https://你的VPS/hooks/transcript"
# cookies_from_browser 預設 safari（若你主要用 Chrome 就改 chrome）

# 3) 用瀏覽器登入一次 YouTube（Safari 或 Chrome，跟設定一致）

# 4) 先單支測試
.venv/bin/python -m worker fetch 'https://www.youtube.com/watch?v=VIDEO_ID'
```

若 `fetch` 成功，再掛背景：

```bash
launchctl load ~/Library/LaunchAgents/com.yt.mac.worker.plist
```

電源接上，並在系統設定關掉「接電源時自動睡眠」。

## 日常使用

```bash
# 丟進佇列（背景 worker 會自動抓並 POST）
.venv/bin/python -m worker enqueue 'https://www.youtube.com/watch?v=xxxx'

# 看佇列
.venv/bin/python -m worker status

# 手動處理一筆
.venv/bin/python -m worker once
```

可選：開本機 HTTP（只綁 `127.0.0.1`）：

```bash
.venv/bin/python -m worker serve
curl -X POST http://127.0.0.1:8787/enqueue \
  -H 'content-type: application/json' \
  -d '{"url":"https://www.youtube.com/watch?v=xxxx"}'
```

線上 Agent 若要遠端丟 URL：在 Mac 上另開 **Cloudflare Tunnel** 指到 `8787`（免費），VPS 只呼叫 `/enqueue`，仍然由 Mac 去抓 YouTube。

## Webhook 收到的 JSON

```json
{
  "video_id": "xxxx",
  "url": "https://www.youtube.com/watch?v=xxxx",
  "title": "...",
  "language": "zh-Hant",
  "is_generated": true,
  "source": "yt-dlp",
  "text": "完整逐字稿...",
  "segments": [{"start": 0.0, "duration": 2.1, "text": "..."}],
  "meta": {},
  "fetched_at": 1710000000.0
}
```

VPS 範例接收端：`examples/webhook_receiver.py`（把 `analyze()` 換成你現有 AI 流程）。

## 防擋設定（config.toml）

- `delay_min_sec` / `delay_max_sec`：任務間隔（預設 4–10 秒）
- `daily_limit`：每天最多幾支（預設 80）
- `cookies_from_browser`：務必設成你有登入 YouTube 的瀏覽器
- 被擋時會自動 cooldown，不要改成瘋狂重試

## 故障排除

1. **`Sign in to confirm you're not a bot`**  
   瀏覽器登入 YouTube → 確認 `cookies_from_browser` 正確 → `brew upgrade yt-dlp`

2. **transcript-api 失敗但 yt-dlp 成功**  
   正常。第一段輕量 API 失敗時會自動走 cookie 通道。

3. **沒有字幕**  
   該影片本身沒有字幕軌；這工具不會（也不該）用 Whisper 硬轉。

4. **Mac 睡覺就不跑**  
   接電、防止自動睡眠；`launchd` 無法對抗合上蓋子深度睡眠。

## 明確不要做的事

- 不要把這套抓稿邏輯搬回 VPS
- 不要開很多執行緒同時抓
- 不要用資料中心 proxy「繞過」——通常更糟
