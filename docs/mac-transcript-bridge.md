# 怎麼用（最短版）

## 你已經上傳了什麼

`youtube-mac-worker/` 已在 GitHub（commit `e144f5a`）。  
VPS 接收端也在 PR：`cursor/mac-transcript-bridge-7a35`。

## 誰選片？誰抓字幕？

| 角色 | 做什麼 |
|------|--------|
| **VPS cron** `youtube_daily` | 決定今天抓哪幾支（頻道 round-robin） |
| **Mac worker** | 只負責「這支 URL → 現成字幕」 |
| **VPS receiver** | 收字幕文字 → 存 cache → AI 整理 |

日常你**不用**在 Mac 上手動貼連結；測試時可以。

---

## Mac 上做一次（安裝）

```bash
# 1. 拿到程式（擇一）
#    - git clone 後 cd youtube-mac-worker
#    - 或把 GitHub 上的 youtube-mac-worker 資料夾拷到 ~/youtube-mac-worker

cd ~/youtube-mac-worker   # 改成你的實際路徑
bash scripts/install_mac.sh
cp config.example.toml config.toml
```

編輯 `config.toml`：

```toml
[fetch]
languages = ["de", "en"]          # 德國片優先

[yt_dlp]
cookies_from_browser = "safari"   # 或 chrome

[webhook]
url = "http://你的VPS公網IP:8791/hooks/transcript"
token = "自訂一串密碼"
```

瀏覽器（Safari/Chrome）先登入 YouTube。

**單支測試：**

```bash
.venv/bin/python -m worker fetch 'https://www.youtube.com/watch?v=有德文字幕的片'
```

成功會印出一大段 JSON（有 `text`）。

**掛背景（自動吃佇列 + 打 webhook）：**

```bash
# 安裝 plist（路徑請改成你的）
cp launchd/com.yt.mac.worker.plist ~/Library/LaunchAgents/
# 用文字編輯打開 plist，把 WorkingDirectory / ProgramArguments 改成你的路徑
launchctl load ~/Library/LaunchAgents/com.yt.mac.worker.plist
```

開本機 API，讓 VPS 經 **Tailscale** 丟片（你已有 Tailscale，不必 Cloudflare）：

```bash
# config.toml：
# [worker]
# http_host = "0.0.0.0"    # 重要：讓 Tailscale 網卡打得到
# http_port = 8787

.venv/bin/python -m worker serve
# 另開終端吃佇列：
.venv/bin/python -m worker run

# 查 Mac 的 Tailscale IP：
tailscale ip -4
# 例如 100.x.y.z
```

---

## VPS 上做一次（Tailscale）

```bash
# /root/.hermes/.env 加上：
YOUTUBE_TRANSCRIPT_SOURCE=mac
MAC_WEBHOOK_TOKEN=跟Mac config.toml 的 token 相同
MAC_WORKER_URL=http://100.x.y.z:8787     # ← Mac 的 Tailscale IP
MAC_RECEIVER_PORT=8791

cd /root/projects/ado_hermes_reddit_scrapper
git pull   # 含有 mac_transcript_receiver 的 branch / merge 後的 main

set -a && source /root/.hermes/.env && set +a
PYTHONPATH=. python pipeline/scripts/mac_transcript_receiver.py --port 8791
```

Mac `config.toml` 的 webhook 也建議打 VPS 的 Tailscale IP（不必公網）：

```toml
[webhook]
url = "http://100.VPS的IP:8791/hooks/transcript"
token = "同一組"
```

互通測試（在 VPS 上）：
```bash
curl -s http://100.Mac的IP:8787/health
curl -s -X POST http://100.Mac的IP:8787/enqueue \
  -H 'content-type: application/json' \
  -d '{"url":"https://www.youtube.com/watch?v=VIDEO_ID"}'
```

---

## 驗證成功

1. Mac：`python -m worker fetch '…'` 有 `text`  
2. Mac：`enqueue` 一支 → VPS `curl http://127.0.0.1:8791/healthz` 正常；cache 出現檔案  
3. VPS：`YOUTUBE_TRANSCRIPT_SOURCE=mac python pipeline/youtube_daily.py --dry-run --skip-store`  
   → 有 cache 的片會進分析；沒 cache 的會 enqueue 給 Mac（若設了 `MAC_WORKER_URL`）

---

## 指令速查（Mac）

```bash
python -m worker enqueue 'URL'   # 丟進佇列
python -m worker once            # 處理一筆
python -m worker run             # 常駐吃佇列
python -m worker serve           # HTTP :8787 給 VPS enqueue
python -m worker status          # 看 inbox/done/failed
python -m worker fetch 'URL'     # 立刻抓、印 JSON（不經佇列）
```
