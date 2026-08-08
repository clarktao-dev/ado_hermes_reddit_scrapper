# podcast-content-repurposer

從 podcast vault 自動生成 6 個社群平台內容（facebook / threads / twitter / linkedin / newsletter / podcast-outline）。

## 這是什麼

**Mirror** — 原始 skill 在 `~/.hermes/skills/productivity/podcast-content-repurposer/`（Hermes 內建）。
這個資料夾是為了 GitHub 備份追蹤修改。

## 結構

```
podcast-content-repurposer/
├── SKILL.md            # skill 使用說明
├── templates/
│   ├── formats.yaml          # 6 個格式的 LLM prompt + validator 設定
│   ├── validators.py         # post-processors + validators
│   └── repurpose_executor.py # 執行器
└── README.md
```

## 同步策略

修改時以本地 `~/.hermes/skills/...` 為準（執行會用這個版本）。
要備份時用 `cp -r ~/.hermes/skills/productivity/podcast-content-repurposer/* <這個 repo>/`。

## 責任分工（2026-08-08 修訂）

| 角色 | 負責 |
|:--|:--|
| **LLM prompt** | 內容方向、tone、長度彈性、是否分段、markdown 結構（`##` `###` `**`）|
| **Post-processor** | `##` → `▍ ` 前綴（threads 除外）、strip `**` / `*` / `<u>`、strip emoji、threads ≤280 切段 + `(N/M)` marker、LLM 沒寫完的行自動補 `。` |
| **Validator** | has_no_simplified、has_german_terms、is_nonempty、≤280 chars、≤8 則、marker 存在、section 存在（advisory）|

### 各格式 post-processor 對照

| 格式 | post-processor | 視覺 |
|:--|:--|:--|
| threads | split_threads_post | 純文字 + `(N/M)` marker + 短 hook + 內文換行 |
| facebook | _facebook_pipeline | `▍ ` 子標題 + 純文字 |
| twitter | _facebook_pipeline | 同上 |
| linkedin | _facebook_pipeline | 同上 |
| newsletter | _facebook_pipeline | 同上 |
| podcast-outline | _facebook_pipeline | 同上 |

## 開發

```bash
# 直接跑 executor
/root/reddit-safe/.venv/bin/python /root/.hermes/skills/productivity/podcast-content-repurposer/templates/repurpose_executor.py
```

## 修改記錄

- **2026-08-08**: LLM/post-processor 職責分離、emoji 移除、子標題 ▍ 前綴、auto-period 保守規則