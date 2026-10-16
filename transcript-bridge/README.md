# Transcript Bridge（GitHub 當信箱）

Agent 與 iPhone App **不直接連線**，改走 GitHub：

```
VPS / Hermes Agent                iPhone App                     VPS
      │                                │                           │
      │  寫入 inbox/*.json              │                           │
      │  git push ──────────────────►  │ 打開 App / 按同步          │
      │                                │ 撈 inbox                   │
      │                                │ 抓現成字幕（手機 IP）       │
      │                                │ 上傳 outbox/*.json         │
      │                                │ 刪除 inbox 該筆            │
      │                                │                           │
      │  cron 或手動 import ◄──────────┴── outbox ─────────────────┤
      │  → transcript_cache → youtube_daily AI 整理                │
```

## 目錄

| 路徑 | 誰寫 | 內容 |
|------|------|------|
| `inbox/<video_id>.json` | Agent / VPS | 待抓連結 |
| `outbox/<video_id>.json` | iPhone App | 字幕結果（同 webhook JSON） |
| `done/<video_id>.json` | import 腳本 | 已匯入 cache 的歸檔 |

## inbox 格式

```json
{
  "video_id": "dQw4w9WgXcQ",
  "url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
  "title": "可選",
  "channel": "Finanztip",
  "languages": ["de", "en"],
  "requested_at": "2026-10-16T05:00:00+00:00",
  "requested_by": "youtube_daily"
}
```

## outbox 格式

與 Mac worker webhook 相同（`video_id`, `url`, `title`, `language`, `text`, `source`, …）。

## VPS 指令

```bash
# 手動丟一支進信箱並 push
PYTHONPATH=. python3 pipeline/scripts/transcript_bridge_enqueue.py \
  --url 'https://www.youtube.com/watch?v=VIDEO_ID' \
  --channel Finanztip --push

# 把 App 上傳的 outbox 匯入 transcript cache
PYTHONPATH=. python3 pipeline/scripts/transcript_bridge_import.py --push
```

`YOUTUBE_TRANSCRIPT_SOURCE=mac` 時，`youtube_daily` 缺字幕會**自動 enqueue 到 inbox**（需 `--push` 環境或之後 cron push）。

## iPhone

見 `ios-yt-captions/README.md`：設定 GitHub repo + PAT，按「同步佇列」。
