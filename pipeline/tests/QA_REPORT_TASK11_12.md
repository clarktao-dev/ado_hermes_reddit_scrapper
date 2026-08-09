# Task 11+12 QA Report

**Date**: 2026-08-09 22:25 UTC
**Verifier**: Hermes subagent (QA Engineer role — no code modifications)
**Scope**: `pipeline/news_daily.py` — Task 11 (Google News `decode_url`) and Task 12 (paywall multi-layer detection)
**Re-run command**: `/root/.hermes/hermes-agent/venv/bin/python /root/projects/ado_hermes_reddit_scrapper/pipeline/tests/qa_task11_12.py`

---

## Test Results

### ✅ Test 1 — `decode_url` returns real publisher URL  —  **PASS**

Fetched 100 items from the live `news.google.com/rss/search?q=Immobilien+OR+Wohnung…` feed and decoded one item per target publisher (`<source>` label) via `google_news_api.GoogleNewsClient.decode_url(timeout=20)`.

| Source | Decoded URL |
|---|---|
| Spiegel | `https://www.spiegel.de/wirtschaft/service/immobilien-in-deutschland-wieder-guenstiger-a-2fdde804-…` |
| WELT | `https://www.welt.de/wirtschaft/plus6a37f069af14c0b528961c12/immobilien-die-grosse-babyboomer-verkaufswelle-beginnt-…` |
| Manager Magazin | `https://www.manager-magazin.de/finanzen/geldanlage/immobilien-und-immobilienaktien-wie-sie-an-der-boerse-wohnungen-…` |

All three decoded URLs contain the expected publisher FQDN (`spiegel.de`, `welt.de`, `manager-magazin.de`). **PASS.**

---

### ❌ Test 2 — `_is_paywalled_url()` detects WELT+ URL pattern — **FAIL**

```python
from pipeline.news_daily import _is_paywalled_url
print(_is_paywalled_url('https://www.welt.de/.../plus6a.../x.html'))     # spec literal
print(_is_paywalled_url('https://www.tz.de/wirtschaft/immobilien-abc-123.html'))  # clean URL
```

Result:

| URL | Expected | Actual | Verdict |
|---|---|---|---|
| `https://www.welt.de/.../plus6a.../x.html` (spec literal) | True | **False** | ✗ |
| `https://www.tz.de/wirtschaft/immobilien-abc-123.html` | False | False | ✓ |

**Bonus check** — the real WELT+ URL emitted by `decode_url` in Test 1:

| URL | Expected | Actual |
|---|---|---|
| `https://www.welt.de/wirtschaft/plus6a37f069af14c0b528961c12/immobilien-die-grosse-babyboomer-verkaufswelle-beginnt.html` | True | **False** |

**Root cause** (informational, not a fix attempt):

```python
# news_daily.py:374–380
_PAYWALL_URL_PATTERNS = [
    ("/plus/", "WELT+ / Spiegel+"),     # WELT+, Spiegel+
    ("/-/", "Handelsblatt+"),
    ("/premium/", "FAZ+"),
    ("/paywall/", "explicit-paywall"),
    ("/epaper/", "epaper-only"),
]
```

The substring `"/plus/"` requires the token to be bracketed by slashes. The spec's example URL `…/plus6a…/x.html` has `plus` glued directly to `6a` (no closing slash on the plus side), so the substring never matches. Real WELT+ URLs produced by `decode_url` use the same shape (`/plus6a37f069…/…`), so **none of the actual WELT+ articles are caught by pass-1 either** — they only get caught by pass-2 (post-fetch char count) if the article body is short enough. **The clean URL part of the test still passes** (substring absent → False).

Spec-vs-code mismatch is the verdict. **FAIL.**

---

### ✅ Test 3 — short-content (<1000 chars) drops item  — **PASS**

Mocked `pipeline.lib.rss_fetch.fetch_full_text` to return:

```python
{"full_text": "Too short, only 42 chars.", "paywalled": False,
 "char_count": 25, "had_paywall_hint": False, "error": None}
```

Called `_fetch_google_news_text(item)` against a real rbb24 GN item. Result:

```
return value       : '<PAYWALLED>'      ✓
item._paywalled    : True               ✓
item._paywall_reason: 'short-content'   ✓
item._decoded_url  : 'https://www.rbb24.de/…'   ✓
item._original_gn_url: 'https://news.google.com/rss/articles/CBMi…'   ✓
```

Log line from the production code: `[paywall-detected] short-content (25 chars) | https://www.rbb24.de/…`. **PASS.**

---

### ✅ Test 4 — publisher-hint paywall (pass 2: `paywalled=True`) drops item  — **PASS**

Mocked `pipeline.lib.rss_fetch.fetch_full_text` to return:

```python
{"full_text": "Stub content with paywall text inside.",
 "paywalled": True, "char_count": 35, "had_paywall_hint": False, "error": None}
```

Called `_fetch_google_news_text(item)` against the same rbb24 GN item. Result:

```
return value        : '<PAYWALLED>'        ✓
item._paywalled     : True                 ✓
item._paywall_reason: 'publisher-hint'    ✓
```

Log line from the production code: `[paywall-detected] publisher-hint (35 chars) | https://www.rbb24.de/…`. **PASS.**

---

### ✅ Test 5 — `decoded_url` + `original_gn_url` persisted in Airtable metadata  — **PASS**

Pulled 41 `article_type=short-summary` records from `appHilorcrC5T0p2u/tblyJl2IBTgnImkM5` (read-only), filtered to 15 GN-originated records, sorted by `processed_at desc`, inspected the latest:

```
Airtable record id       : rec6ktqbeVIJyNim2
processed_at             : 2026-08-09T22:09:15.000Z
metadata.decoded_url     : 'https://www.tz.de/wirtschaft/immobilien-kaufen-verkuafen-…html'
metadata.original_gn_url : 'https://news.google.com/rss/articles/CBMi2AFBVV95cUxQclVjYU5aaENUTHZl…'
metadata keys            : ['epoch', 'source', 'lang', 'relevance_rank',
                            'relevance_to_buyer', 'date',
                            'decoded_url', 'original_gn_url']
```

Both `decoded_url` (real tz.de article URL) and `original_gn_url` (real Google News redirect URL) keys present. The `_build_news_metadata` helper at `news_daily.py:765-768` correctly persists both. **PASS.**

Note: 14 of the 15 GN records still lack these keys — they were created by earlier runs before the Task 12 code went in. The most recent record confirms the new code path is active. (API key only referenced as suffix: `***8946`.)

---

### ✅ Test 6 — vault `_index.md` has no paywall stub strings  — **PASS**

Scanned `/root/projects/ado_hermes_reddit_scrapper/immobilien-kb/vault/Daily/2026-08-09/_index.md` (29 lines, 838 bytes) for these forbidden markers:

```
['付費牆', '無法存取', '僅有標題', 'paywall stub', 'PAYWALLED', '<PAYWALLED>']
```

Result: **no hits** in any of the markers. The vault digest contains a real GN article (TZ, decoded to a tz.de URL with full summary), so the paywall filter correctly let it through. **PASS.**

---

## Final Verdict: ❌ **FAIL** (5 / 6 tests passing)

| # | Test | Result |
|---|---|---|
| 1 | `decode_url` → real publisher domain | ✅ |
| 2 | `_is_paywalled_url` WELT+ pattern detection | ❌ |
| 3 | short-content (<1000 chars) → drop | ✅ |
| 4 | publisher-hint (`paywalled=True`) → drop | ✅ |
| 5 | `decoded_url` + `original_gn_url` in Airtable metadata | ✅ |
| 6 | vault `_index.md` has no paywall stub strings | ✅ |

### Fail detail

**Test 2** is the only failure. The `_PAYWALL_URL_PATTERNS` entry `"/plus/"` uses slash-bounded substring matching, but the WELT+ URL shape actually emitted by `decode_url` is `/plus6a37f069…/…html` — `plus` concatenated directly with the article token, no closing slash. This means:

- The spec's literal test URL `https://www.welt.de/.../plus6a.../x.html` is missed (matches `plus6a…`, not `/plus/`).
- Real WELT+ URLs in production are also missed by pass-1.
- These articles only get caught by pass-2 if the publisher body is short (<1000 chars), which is unreliable — WELT+ full articles are often >1000 chars and will reach the LLM stage, wasting tokens on a "this article is paywalled" stub.

### Recommendation (informational, not a fix)

The fix is straightforward — broaden the pattern. Either:

- Change `"/plus/"` to a regex like `r"/plus[A-Za-z0-9]+/"` that matches the slash-prefixed, alphanumeric-suffixed shape used by WELT+ and Spiegel+.
- Or add a separate substring check for the literal token `plus` after a slash with no trailing slash (e.g. `r"/plus[^/]"`).

No code change has been made — per QA role.

---

## Artifacts

- Re-runnable test harness: `/root/projects/ado_hermes_reddit_scrapper/pipeline/tests/qa_task11_12.py`
- This report: `/root/projects/ado_hermes_reddit_scrapper/pipeline/tests/QA_REPORT_TASK11_12.md`
- Airtable API key referenced only as `***8946` suffix; full key never written to disk.
