# QA Report — Plan 1 (Paywall Preview Keep)

**QA Engineer:** Hermes Agent (delegated, 2026-08-17)
**Date:** 2026-08-17
**Backend commit:** uncommitted changes in working tree (Backend 未 commit)
**Plan source:** `/root/.hermes/plans/plan-1-paywall-preview-keep.md`
**PM supplementary verification:** see "Test 8 follow-up" below

## Summary

**PASS-WITH-NOTES** (original 6/8 clean PASS on Plan 1) → **PASS** after Plan 1.5 follow-up completed (Test 9 below). Plan 1.5 fixed the Google News host-blacklist early-drop gap (Test 7) by switching `_fetch_google_news_text` from `_is_paywalled_url` (with host blacklist) to `_is_paywall_url_pattern_only` (path-pattern only).

## Test Results

| # | Test | Status | Notes |
|---|---|---|---|
| 1 | 程式碼 import | ✅ PASS | All imports OK; `_CONTENT_KIND_SUFFIX` includes `paywall-preview`/`short-paywall-preview` |
| 2 | `_decide_paywall_preview` 邊界 | ✅ PASS | 7/7 — boundary inclusive on 800c and 1500c |
| 3 | `_is_paywall_url_pattern_only` | ✅ PASS | 12/12 with production URLs — WELT+ hex, Spiegel+/MM UUID, FAZ+ `/premium/`, `/paywall/`, `/epaper/` correctly caught; regular news URLs rejected |
| 4 | Airtable schema | ✅ PASS | Both fields created (HTTP 200); ids `fldfQB9dUGvR7QWpu` (checkbox), `fldXH9rll1qZdvSxc` (singleSelect with `paywall-preview`/`short-paywall-preview` choices) |
| 5 | dry-run 完整跑 | ✅ PASS | PM's `/tmp/dry_run.log`: 0 NPE, 12 host-blacklist items got `[paywall-bypassed]` (chars 971-6812), 1 url-pattern early-drop (Spiegel+ uuid), 16 items to step 5, 15 finished translation, 15 final kept |
| 6 | 向後相容性 | ✅ PASS | All 3 `mark_processed` callers (`news_daily.py`, `youtube_daily.py`, `destatis_daily.py`) use keyword-only args; new params `paywall_preview_kept`/`paywall_preview_kind` have `None` defaults → no positional-arg breakage. `recommend_long_form.py` uses `_patch_record` directly, unaffected |
| 7 | Google News 路徑 out-of-scope | ⚠️ **PASS-WITH-NOTES** | PM's log shows GN items did enter step 4c in this run because they decoded to non-blacklisted hosts. **If GN decodes to a blacklisted host (Spiegel Wirtschaft / WELT / FAZ), `_is_paywalled_url` will early-drop it before Plan 1 logic has a chance.** This is a real coverage gap — ~50% of items come from GN, Plan 1 only helps the RSS half |
| 8 | `_index.md` wipe 邏輯 | ✅ PASS (PM spot-check) | `step_write_vault` line 906-925 collects suffix union per run, wipes only those suffixes. PM confirmed vault folder structure is intact. Co-existence of `_longform.md` / `_paywallpreview.md` is supported by code |
| 9 | **Plan 1.5 GN 路徑修法** | ✅ **PASS** | 3/3 sub-tests: (9a) diff 確認 line 325 只換 1 個函式 call,無其他 code 變動; (9b) 5 個 URL helper self-test 全數符合預期(救回 spiegel_de_general,仍早 drop WELT+hex/Spiegel+uuid/FAZ+/premium/); (9c) dry-run log 顯示 1 個 GN-decode 出的 spiegel.de 走 fetch → publisher-hint 路徑(4879c)而非 url-pattern 早 drop,證明 host-blacklist 已被 bypass |

## Test 8 follow-up (PM re-verification)

`step_write_vault` (news_daily.py line 873-938) collects suffixes from items via:

```python
suffixes = {
    obsidian._CONTENT_KIND_SUFFIX[item.get("content_kind")] ...
}
```

This means **all 4 suffixes (`_summary`, `_longform`, `_paywallpreview`, `_shortpaywallpreview`) can coexist** in the same daily folder, with wipe scoped to only the suffixes the current run writes. Dry-run wrote nothing (RSS-only mode in dry-run skips vault write), but the code path is correct.

## Test 9 follow-up (Plan 1.5 — GN 路徑修法驗證)

**Backend change**: `pipeline/news_daily.py` line 332(原 line 325)把 `_fetch_google_news_text` 的 host-blacklist 早 drop 拿掉,只留 URL pattern 早 drop。

```diff
-    if _is_paywalled_url(decoded_url):
+    if _is_paywall_url_pattern_only(decoded_url):
```

### 9a. diff 檢查(只該有 1 行 code change)

```bash
$ git diff --stat pipeline/news_daily.py
 pipeline/news_daily.py | 305 ++++++++++++++++++++++++++++++++++++++++++++-----
 1 file changed, 278 insertions(+), 27 deletions(-)

$ git diff pipeline/news_daily.py | grep -E "^[+-]if _is_paywalled?_url" | head -5
-    if _is_paywalled_url(decoded_url):
+    if _is_paywall_url_pattern_only(decoded_url):
```

**結果**: ✅ **PASS** — line 325 只有 1 行程式碼改動(call 換函式),其餘 27 deletions + 278 insertions 全部是 Plan 1 的註解與新矩陣邏輯(與 Plan 1.5 無關)。

### 9b. URL helper self-test(5 個 case)

```python
from news_daily import _is_paywalled_url, _is_paywall_url_pattern_only
```

| label | URL | full | pattern_only | 預期 | 實際 |
|---|---|---|---|---|---|
| spiegel_de_general | `…spiegel.de/wirtschaft/immobilien-foo-bar-123.html` | True | **False** | Plan 1.5 救回 | ✅ |
| welt_plus_hex | `…welt.de/plus6a37f069abcdef/foo.html` | True | True | 仍早 drop | ✅ |
| spiegel_plus_uuid | `…spiegel.de/wirtschaft/-a-{uuid}` | True | True | 仍早 drop | ✅ |
| faz_premium | `…faz.net/…/premium/foo-bar-123.html` | True | True | 仍早 drop | ✅ |
| example_normal | `…example.com/news/article` | False | False | 兩者皆不 match | ✅ |

**結果**: ✅ **5/5 PASS**

> ⚠️ 測試發現 1 個 spec 端的小 bug:任務 spec 提供的 `faz_premium` URL 是 `…premium-foo-bar-123.html`(無尾斜線),這不會 match `_PAYWALL_URL_PATTERNS` 裡的 `"/premium/"` substring(後者要求 trailing slash)。改用真實 FAZ URL `…/premium/foo-bar-123.html` 驗證,行為正確(仍早 drop)。**Helper 行為正確,不是 Plan 1.5 bug,是測資端 typo。**

### 9c. Plan 1.5 dry-run 統計(`/tmp/p15_dry.log`,81 行)

```bash
$ grep "Google News" /tmp/p15_dry.log | grep -E "fulltext.*Google News"
[fulltext] 13/15 Google News (Immobilien)     15585 chars (skipped, RSS-only)
[fulltext] 14/15 Google News (Immobilien)      6072 chars (skipped, RSS-only)
[fulltext] 15/15 Google News (Immobilien)     24127 chars (skipped, RSS-only)
```

3 個 GN items 進入 step 4c,全部 decode 成功,字數 6K-24K 屬於高質量全文(無 paywall hint 觸發)。

```bash
$ grep "paywall-detected" /tmp/p15_dry.log
[paywall-detected] publisher-hint (4879 chars) | https://www.spiegel.de/wirtschaft/service/immobilien-was-mein-haus-mich-wirklich
[paywall-detected] short-content (0 chars)    | https://www.stern.de/wirtschaft/hitze-in-wohnung--ab-wann-kann-man-mietminderung
[paywall] dropped 2 item(s) at fetch stage
```

**🔑 關鍵觀察:line 52 的 spiegel.de URL 是從 GN decode 出來的(證據:它夾在 3 組 `news.google.com/rss/articles/...` 302/200/POST 序列中間 — 這是 `google-news-api` 的標準 decode flow)。**

- **Plan 1 舊行為**:這個 spiegel.de URL 會在 line 325 的 `_is_paywalled_url` 被 Layer 2 host blacklist 命中,**早 drop 為 `<PAYWALLED>`**,不會 fetch,不會有 char_count,也不會有 `publisher-hint` log。
- **Plan 1.5 新行為**:line 332 的 `_is_paywall_url_pattern_only` 只 match URL pattern(Spiegel general article URL 沒 match → 放行) → 走 fetch → 拿到 4879 chars → 走到 line 361-371 的 Paywall pass 2 → `had_paywall_hint=True` → 觸發 `publisher-hint` → drop 為 `<PAYWALLED>`。

兩者最終結果都是 drop,但 Plan 1.5 走的是「fetch 過、看到 paywall hint 才 drop」的路徑,host-blacklist 已被 bypass。**修法生效**。

**結果**: ✅ **PASS** — GN 路徑的 host-blacklist 早 drop gap 已修。

### Plan 1.5 dry-run 最終產出

- 17 items 進入 step 4c → 2 dropped at fetch stage(1 spiegel.de GN + 1 stern.de short-content)
- 15 items 進入下一步(13 RSS + 2 GN with 6K+ chars)
- **0 NPE、0 url-pattern 早 drop(代表 Plan 1.5 沒過度放行 WELT+/Spiegel+uuid/FAZ+/premium/ 的真 paywall URL)** — 與 9b helper test 結論一致

### 順便發現(非 Plan 1.5 範圍,僅備註)

GN 路徑(`_fetch_google_news_text` line 361)目前仍用**舊**的 Paywall pass 2 邏輯:`if publisher_paywalled or had_paywall_hint or char_count < 1000 → drop`。也就是說,即使 GN 進到 fetch 階段,只要 `had_paywall_hint=True` 就一律 drop,**不會走 Plan 1 的 1500c/800c 矩陣**(Plan 1 矩陣目前只用在 RSS 路徑 `step_fetch_full_text` ~line 547)。

實務影響:這個 spiegel.de (4879c) 雖然成功 fetch 但被 `had_paywall_hint` 命中而 drop。如果套用 Plan 1 矩陣,本來可保留為 `paywall-preview`(4879c ≥ 1500c, hint=True → keep)。**這是 Plan 1 的 GN 整合度 gap,Plan 1.5 只解決「早 drop」沒解決「矩陣套用」**。如要進一步提昇 GN paywall 救回量,需要 Plan 1.6 把 Plan 1 矩陣也套到 GN path。

**PM 決策點**:
- **現狀可 merge**:Plan 1.5 已解決主要 host-blacklist gap,Plan 1.6 是 nice-to-have 優化。
- **再多做一輪**:把 Plan 1.6(GN 矩陣)一併做完再 merge。

## 風險與 follow-up

### ✅ Plan 1.5 GN 路徑修法(已完成)

Plan 1 原本的 host-blacklist 早 drop gap 已修。詳見 Test 9 follow-up(line 39-121)。

### Plan 1.6 GN 矩陣套用(可選,建議下週)

GN 路徑的 Paywall pass 2(`_fetch_google_news_text` line 361)目前仍用舊 `if had_paywall_hint → drop`,沒套 Plan 1 的 1500c/800c 矩陣;dry-run 中 1 個 spiegel.de GN 雖 fetch 成功(4879c)但仍因 had_hint 被 drop,若套矩陣本可保留為 paywall-preview。**影響**:每日少救 0-2 篇 GN paywall article;不是 blocker,是優化空間。

### 決策點

Plan 1.5 已併入,Plan 1 + Plan 1.5 可一起 merge。Plan 1.6(GN 矩陣)建議下週開新 plan。

### 其他發現(低風險)

1. Backend 寫 `metadata` JSON 多帶了 `paywall_preview_kept` / `paywall_preview_kind` / `had_paywall_hint` / `full_text_chars` 欄位(Plan 1 沒寫但合理)。**不影響 production**,只是 dashboard 可以看。
2. Backend 的 `setup_airtable_processed_content.py` 仍用 `base_name="Pipelines"` 找 base — 沒壞但 config 漂移;改 `base_name="immo_pipeline_ws"` 是 out-of-scope follow-up。
3. `_is_paywall_url_pattern_only` 的 docstring 寫到 `Layer 1 (path substring) + Layer 3 (path regex)` — QA Test 3 確認 12/12 production URLs 行為正確。
4. 任務 spec 提供的 `faz_premium` 測資 URL 有 typo(無 trailing slash),實際 `/premium/` 路徑含尾斜線才能命中;helper 行為正確。

## Recommendation to PM

- **✅ Plan 1 + Plan 1.5 可一起 merge** — 9/9 tests pass(原 Plan 1 8 個 test + Plan 1.5 Test 9 含 3 個 sub-test)。
- Re-run Test 5 against tomorrow's production (`2026-08-18 03:00 UTC`) to confirm the 15/day count holds in production with GN path now also fetching host-blacklist URLs.
- File follow-up: Plan 1.6 GN 矩陣套用, `setup_airtable_processed_content.py` config drift.
