# Mac 住宅 IP 抓字幕 → VPS 只做 AI 整理

目標架構（必須遵守）：

1. **Mac**（家用住宅 IP）：收 YouTube URL → 抓現成字幕 → webhook 推給 VPS  
2. **VPS / Hermes**：只收純文字 → 翻譯 / LLM digest / Discord / vault  
3. **VPS 永遠不要直接打 YouTube 抓字幕**

本 repo 已接好 VPS 端。Mac 端沿用既有套件 `youtube-mac-worker/`（請把 zip 解壓進 repo）。

---

## 0. 先把 Mac worker 推進 GitHub（給 Cursor 用）

在 **Mac Terminal** 執行：

```bash
cd ~/Downloads
unzip -o youtube-mac-worker.zip -d /tmp/youtube-mac-worker-unpack
# 找到實際資料夾（有 README.md 的那層）
WORKER="$(find /tmp/youtube-mac-worker-unpack -type f -name README.md | head -1 | xargs dirname)"

cd /path/to/ado_hermes_reddit_scrapper   # 或先 git clone
git fetch origin
git checkout -b cursor/mac-transcript-bridge-7a35 origin/cursor/mac-transcript-bridge-7a35 2>/dev/null \
  || git checkout -b cursor/mac-transcript-bridge-7a35 origin/main

mkdir -p youtube-mac-worker
rsync -a --delete "$WORKER"/ youtube-mac-worker/
# 不要把 cookie / secrets 提交進去
echo $'config.toml\n*.cookies\nqueue/\ndone/\nfailed/\nlogs/\n.venv/\n__pycache__/' >> youtube-mac-worker/.gitignore

git add youtube-mac-worker docs/mac-transcript-bridge.md
git commit -m "chore: add youtube-mac-worker package for residential caption fetch"
git push -u origin cursor/mac-transcript-bridge-7a35
```

Push 完告訴 Cursor「worker 已上傳」，即可延續讀 README / 補 launchd / Cloudflare。

---

## 1. VPS 端（已在本 PR）

| 元件 | 路徑 | 作用 |
|---|---|---|
| Webhook 接收 | `pipeline/scripts/mac_transcript_receiver.py` | `POST /transcript` → 寫入 transcript cache |
| Bridge lib | `pipeline.lib.mac_transcript` | ingest / enqueue / mode |
| Fetch 改寫 | `pipeline.lib.youtube_fetch.fetch_transcript` | `YOUTUBE_TRANSCRIPT_SOURCE=mac` 時只讀 cache |

### 環境變數（寫進 `/root/.hermes/.env`）

```bash
# 正式切到 Mac 抓稿後：
YOUTUBE_TRANSCRIPT_SOURCE=mac

# Mac worker Cloudflare Tunnel 公開 URL（可選；cache miss 時 VPS 會 POST /enqueue）
MAC_WORKER_URL=https://xxxx.trycloudflare.com

# Webhook 共用密鑰（Mac → VPS 與 VPS → Mac enqueue 共用）
MAC_WEBHOOK_TOKEN=請換成長隨機字串

MAC_RECEIVER_PORT=8791
```

### 啟動接收端

```bash
cd /root/projects/ado_hermes_reddit_scrapper
set -a && source /root/.hermes/.env && set +a
PYTHONPATH=. /root/.hermes/hermes-agent/venv/bin/python \
  pipeline/scripts/mac_transcript_receiver.py --port 8791
```

建議用 systemd / nohup 常駐。健康檢查：`curl -s http://127.0.0.1:8791/healthz`

Cache 落地位置：

```
immobilien-kb/vault/YouTube/<Channel>/_transcripts/<video_id>.md
immobilien-kb/vault/YouTube/_mac_inbox/_transcripts/<video_id>.md
```

既有 cron `ac28419b2498`（`youtube_daily.py --mode short`）**不用改 command**；設好 `YOUTUBE_TRANSCRIPT_SOURCE=mac` 即可。

---

## 2. Mac 端最短步驟

假設套件已在 `youtube-mac-worker/`：

```bash
cd youtube-mac-worker
bash scripts/install_mac.sh          # 建 venv、裝依賴
cp config.example.toml config.toml
```

編輯 `config.toml`（欄位名以套件 README 為準，對齊即可）：

```toml
# 抓稿
cookies_from_browser = "safari"   # 或 chrome；先在瀏覽器登入 YouTube
request_delay_sec = 8
daily_limit = 40
cooldown_on_429_sec = 1800

# 推到 VPS（公網 IP / 反代 / Tailscale 皆可）
[webhook]
url = "https://YOUR_VPS_OR_DOMAIN:8791/transcript"
# 或 http://100.x.y.z:8791/transcript （Tailscale）
token = "與 MAC_WEBHOOK_TOKEN 相同"
```

常駐：

```bash
# launchd（套件內 plist）
cp launchd/*.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.*.youtube-mac-worker.plist

# 或前景測試
.venv/bin/python -m youtube_mac_worker status
.venv/bin/python -m youtube_mac_worker fetch "https://www.youtube.com/watch?v=VIDEO_ID"
```

### （可選）Cloudflare Tunnel — 讓 VPS 能 enqueue 到 Mac

```bash
# Mac
brew install cloudflare/cloudflare/cloudflared
cloudflared tunnel --url http://127.0.0.1:8787
# 把印出的 https://xxxx.trycloudflare.com 寫進 VPS 的 MAC_WORKER_URL
```

Mac worker 需 `serve` 在 `8787`（套件預設）。免費 trycloudflare 網址重開會變；要固定可用 Named Tunnel（仍免費額度內）。

---

## 3. 驗證「Mac 抓得到、VPS 只分析」

### A. Mac 單片抓稿

```bash
cd youtube-mac-worker
.venv/bin/python -m youtube_mac_worker fetch "https://www.youtube.com/watch?v=有德文字幕的片"
# 預期：queue/done 有檔，log 無 429；webhook 回 200
```

### B. VPS 確認 cache

```bash
ls immobilien-kb/vault/YouTube/_mac_inbox/_transcripts/
curl -s -H "Authorization: Bearer $MAC_WEBHOOK_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"video_id":"AAAAAAAAAAA","text":"ping from manual","language":"de","meta":{"channel":"test"}}' \
  http://127.0.0.1:8791/transcript
```

### C. VPS dry-run 不碰 YouTube 字幕

```bash
export YOUTUBE_TRANSCRIPT_SOURCE=mac
# 不設 MAC_WORKER_URL 也可：缺 cache 就 skip
python3 pipeline/youtube_daily.py --channels finanztip --dry-run --skip-store
# 預期 log：mac mode / cached transcript；requests 不打 youtube.com caption
```

### D. 成功標準對照

| 標準 | 怎麼看 |
|---|---|
| Mac 能拿到現成字幕 | `fetch` 產出 text，`source` 為 youtube-transcript-api 或 yt-dlp |
| 結果進線上 Agent | VPS cache 有檔；下次 `youtube_daily` 產出 digest + Discord |
| VPS 不抓 YouTube 字幕 | `YOUTUBE_TRANSCRIPT_SOURCE=mac`；`fetch_transcript` 不呼叫 kome / yt caption |
| 防擋可觀察 | Mac：delay / daily_limit / cooldown；`queue/` `done/` `failed/` `logs/` |

---

## 4. 不要做的事

- 不要預設開 Whisper / 本地 ASR（沒字幕時先問你）
- 不要把 caption 抓取邏輯搬回 VPS
- 不要用 datacenter proxy 硬繞 YouTube
- 不要多執行緒狂抓
