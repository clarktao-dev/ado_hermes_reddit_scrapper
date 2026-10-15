# youtube-mac-worker（佔位）

此目錄目前只有 **VPS 對接設定範例**。完整 Mac worker 套件在你本機：

`/Users/yanjtao/Downloads/youtube-mac-worker.zip`

請依 `docs/mac-transcript-bridge.md` §0 把套件解壓 push 進這個資料夾後，Cursor 才能延續讀原始 README / launchd / install script。

對接重點：

- Mac：抓現成字幕 → `POST` webhook JSON 到 VPS `:8791/transcript`
- VPS：`YOUTUBE_TRANSCRIPT_SOURCE=mac` → 只讀 cache，不抓 YouTube 字幕
