# 給本機（Mac）或本機 Cursor 的上傳 Prompt

複製以下整段執行／貼給本機 Agent：

---

請把 `/Users/yanjtao/Downloads/youtube-mac-worker.zip` 解壓並 push 到 GitHub repo `clarktao-dev/ado_hermes_reddit_scrapper` 的 branch `cursor/mac-transcript-bridge-7a35`，路徑固定為 repo 根目錄的 `youtube-mac-worker/`。

步驟：
1. `unzip` zip，找到含 `README.md` 的套件根目錄
2. `git fetch origin && git checkout cursor/mac-transcript-bridge-7a35`（若不存在則從 `origin/main` 建同名 branch）
3. `rsync` 套件內容到 `youtube-mac-worker/`（保留既有 `CONFIG_FOR_VPS.toml.example` 與佔位 README 可覆寫）
4. 加 `.gitignore`：排除 `config.toml`、cookies、`queue/`、`done/`、`failed/`、`logs/`、`.venv/`
5. **不要**提交任何 token / cookie
6. `git add youtube-mac-worker && git commit -m "chore: import youtube-mac-worker package" && git push -u origin cursor/mac-transcript-bridge-7a35`
7. 回報：commit SHA、README 裡的 CLI 指令名稱（enqueue/fetch/once/run/serve/status）、webhook 設定欄位名

完成後告訴雲端 Cursor：「mac worker 已上傳」。

---
