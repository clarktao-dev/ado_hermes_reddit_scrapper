# iPhone 字幕小工具（取代 Mac 終端機）

在 **iPhone** 上：貼上／分享 YouTube 連結 → 用手機網路抓**現成字幕** → 推到 VPS webhook。  
VPS 仍然**只做 AI 整理**，不抓 YouTube。

> 我（雲端 Cursor）無法直接把 App 裝進你的 iPhone。  
> 你需要用 Mac 的 **Xcode** 裝一次（免費 Apple ID 即可，約 10 分鐘）。

## 和 Mac worker 比

| | Mac worker | 這個 iPhone App |
|--|--|--|
| 抓字幕 IP | 家用寬頻 | 手機 4G/5G／家用 Wi‑Fi |
| 操作 | 兩個終端 + Tailscale | 打開 App 貼網址按一下 |
| 背景自動 | launchd 常駐 | iOS 限制多，適合「手動／分享」 |
| 安裝 | Python / brew | Xcode 裝到手機一次 |

若你幾乎都是「看到片再整理」，iPhone 會輕鬆很多。  
若要 **完全無人值守每天自動**，仍建議之後再把 Mac worker 當後備。

## 一次安裝（Mac 上的 Xcode）

1. Mac 安裝 [Xcode](https://apps.apple.com/app/xcode/id497799835)（App Store，免費）
2. 打開 Xcode → **File → New → Project → iOS → App**
   - Product Name: `YTCaptions`
   - Interface: **SwiftUI**
   - Language: **Swift**
   - 不要勾 Core Data
3. 把本目錄 `YTCaptions/` 裡的 `.swift` 檔案拖進專案（勾 Copy items）
4. 刪掉 Xcode 自動產生、重複的 `ContentView.swift` / `*App.swift`（留我們這份）
5. 上方裝置選你的 **iPhone**（用線連或同一 Wi‑Fi + Developer Mode）
6. Signing：Team 選你的 Apple ID（Personal Team）
7. 按 ▶ Run

第一次可能要在 iPhone：**設定 → 一般 → VPN 與裝置管理** 信任開發者。

## App 裡要填的設定

| 欄位 | 例 |
|--|--|
| Webhook URL | `http://100.x.x.x:8791/hooks/transcript`（VPS 的 Tailscale IP） |
| Token | 與 VPS `MAC_WEBHOOK_TOKEN` 相同 |
| 語言偏好 | `de,en`（德國 Podcast） |

VPS 記得仍要開著：
```bash
cd /root/projects/ado_hermes_reddit_scrapper
git checkout cursor/mac-transcript-bridge-7a35
set -a && source /root/.hermes/.env && set +a
PYTHONPATH=. /root/.hermes/hermes-agent/venv/bin/python \
  pipeline/scripts/mac_transcript_receiver.py --port 8791
```

## 日常怎麼用

1. 複製 YouTube 連結  
2. 打開 **YTCaptions** → 貼上 → **抓取並傳送**  
3. 看到「已送到 VPS」即可；之後 `youtube_daily` 會吃 cache 做整理  

（可選）之後可再加「分享到 App」的 Share Extension；第一版先用貼上就好。

## 明確做不到的事

- 不能在雲端替你按「安裝到 iPhone」
- 不能做 App Store 上架（除非你要付費開發者帳號 + 審核）
- 影片**沒有字幕**時不會用 Whisper 硬轉（跟 Mac worker 相同原則）
