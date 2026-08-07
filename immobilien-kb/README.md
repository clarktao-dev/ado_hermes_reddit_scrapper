# Immobilien Knowledge Base — 2026-08-07

德國房地產/理財知識庫初始結構。包含今日測試的：
- 1 則新聞整理（Handelsblatt: Check24 退場 Baufinanzierung）
- 1 則 YouTube 影片整理（Finanzfluss: AVD 2027）
- 原始 transcript
- Discord 發送工具
- 測試 checklist

## 結構

```
vault/
├── Daily/                                    # 每日新聞歸檔
│   └── 2026-08-06/
│       └── handelsblatt-check24-rueckzug-baufinanzierung.md
├── YouTube/                                  # 影片整理
│   └── Finanzfluss/
│       ├── 2026-08-07-zkW9KjyCTEc-altersvorsorgedepot-2027.md
│       └── _transcripts/
│           └── finanzfluss-zkW9KjyCTEc.md
tools/
└── discord_sender.py                         # Discord 推送工具
CHECKLIST.md                                  # Reddit RSS 測試待辦
```

## 推送指令（任選一）

### 選項 A: 你本機有 `gh` CLI + 已登入
```bash
tar xzf immobilien-kb-backup-2026-08-07.tar.gz
cd immobilien-kb-backup
git init -b main
git add -A
git commit -m "Initial: Handelsblatt news + Finanzfluss video + Discord tool (2026-08-07)"
gh repo create immobilien-kb --public --source=. --remote=origin --push
```

### 選項 B: 純 HTTPS + PAT
```bash
tar xzf immobilien-kb-backup-2026-08-07.tar.gz
cd immobilien-kb-backup
git init -b main
git add -A
git commit -m "Initial: news + video + tool"
git remote add origin https://github.com/<你的帳號>/immobilien-kb.git
git push -u origin main
```

### 選項 C: SSH (你本機有 GitHub SSH key)
```bash
tar xzf immobilien-kb-backup-2026-08-07.tar.gz
cd immobilien-kb-backup
git init -b main
git add -A
git commit -m "Initial commit"
git remote add origin git"@"github.com:<你的帳號>/immobilien-kb.git
git push -u origin main
```

## 推完後
1. Obsidian App: File → Open vault → Open folder as vault → 選 clone 下來的 immobilien-kb
2. 安裝 Obsidian Git plugin (Settings → Community plugins → Git) 設 auto-pull
3. VPS 每天 pipeline 跑完 → push commit → 你 Obsidian 自動 sync
