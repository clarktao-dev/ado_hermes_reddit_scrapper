#!/usr/bin/env python3
"""QA harness for Task 11 (Google News decode_url) and Task 12 (paywall detection)
in pipeline/news_daily.py.

Six tests, all read-only. No Airtable writes, no pipeline side-effects.

Run:
    /root/.hermes/hermes-agent/venv/bin/python pipeline/tests/qa_task11_12.py

Exits non-zero if any test FAILS. Prints a final summary table.
"""
from __future__ import annotations

import json
import os
import re
import sys
import urllib.parse
import urllib.request
from pathlib import Path
from unittest.mock import patch

PROJECT_DIR = Path("/root/projects/ado_hermes_reddit_scrapper")
sys.path.insert(0, str(PROJECT_DIR))

# Ensure venv python is used (no-op if already running under venv).
PYTHON = "/root/.hermes/hermes-agent/venv/bin/python"

# Load .env so AIRTABLE_API_KEY is available (read-only).
for env_file in (Path.home() / ".hermes" / ".env"),:
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if "=" in line and not line.strip().startswith("#"):
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

# Ensure the API key only ever appears as a redacted suffix in any output.
def _redact_key(s: str) -> str:
    if not s:
        return ""
    if len(s) < 8:
        return "***"
    return "***" + s[-4:]

API_KEY_SUFFIX = _redact_key(os.environ.get("AIRTABLE_API_KEY", ""))

# Result accumulator
RESULTS: list[tuple[str, bool, str]] = []


def record(name: str, ok: bool, evidence: str) -> None:
    RESULTS.append((name, ok, evidence))
    icon = "✅ PASS" if ok else "❌ FAIL"
    print(f"\n=== {name}: {icon} ===")
    print(evidence)


# -------------------------------------------------------------------------- #
# Test 1: decode_url on real GN RSS items → real publisher domain
# -------------------------------------------------------------------------- #
def test1_decode_url_real_publisher() -> None:
    """Fetch GN RSS, decode 3 items, confirm decoded_url contains real publisher domain."""
    rss_url = (
        "https://news.google.com/rss/search"
        "?q=Immobilien+OR+Wohnung+OR+Mietrecht+when:7d"
        "&hl=de&gl=DE&ceid=DE:de"
    )
    try:
        req = urllib.request.Request(
            rss_url,
            headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"},
        )
        with urllib.request.urlopen(req, timeout=20) as r:
            raw = r.read().decode("utf-8", errors="replace")
    except Exception as e:
        record("Test 1 decode_url real publisher", False,
               f"FAIL: could not fetch GN RSS: {type(e).__name__}: {e}")
        return

    items = re.findall(r"<item>(.*?)</item>", raw, re.DOTALL)
    if not items:
        record("Test 1 decode_url real publisher", False,
               "FAIL: no <item> in RSS feed")
        return

    # Pick first match per target publisher domain.  <source> contains the human
    # label (e.g. "Spiegel", "WELT", "Manager Magazin"), NOT the FQDN.
    targets = [
        ("Spiegel", "spiegel.de"),
        ("WELT", "welt.de"),
        ("Manager Magazin", "manager-magazin.de"),
    ]
    decoded_pairs: dict[str, dict] = {}
    for src_label, domain in targets:
        for it in items:
            src_m = re.search(r"<source[^>]*>(.*?)</source>", it, re.DOTALL)
            if not src_m:
                continue
            if src_label.lower() not in src_m.group(1).lower():
                continue
            link_m = re.search(r"<link>(.*?)</link>", it, re.DOTALL)
            title_m = re.search(r"<title>(.*?)</title>", it, re.DOTALL)
            if not link_m:
                continue
            decoded_pairs[src_label] = {
                "title": (title_m.group(1) if title_m else "")[:80],
                "gn_url": link_m.group(1),
                "domain": domain,
            }
            break

    if len(decoded_pairs) < 3:
        record("Test 1 decode_url real publisher", False,
               f"FAIL: only found {len(decoded_pairs)}/3 target sources in RSS; "
               f"have: {list(decoded_pairs.keys())}")
        return

    # Now actually decode
    from google_news_api import GoogleNewsClient
    client = GoogleNewsClient(language="de", country="DE")
    log_lines = []
    all_ok = True
    for src_label, info in decoded_pairs.items():
        try:
            decoded = client.decode_url(info["gn_url"], timeout=20.0)
        except Exception as e:
            decoded = f"ERROR: {type(e).__name__}: {e}"
        decoded_pairs[src_label]["decoded_url"] = decoded
        domain_ok = info["domain"] in decoded.lower()
        all_ok = all_ok and domain_ok
        verdict = "✓" if domain_ok else "✗"
        log_lines.append(
            f"  {verdict} {src_label:25s} decoded_url={decoded[:140]}"
        )

    evidence = "\n".join(log_lines)
    if all_ok:
        evidence = (
            "All 3 GN items decoded to a real publisher URL containing the expected "
            "domain (spiegel.de / welt.de / manager-magazin.de):\n" + evidence
        )
    else:
        evidence = "Decoded URLs missing expected publisher domain:\n" + evidence

    record("Test 1 decode_url real publisher", all_ok, evidence)


# -------------------------------------------------------------------------- #
# Test 2: _is_paywalled_url() pattern detection
# -------------------------------------------------------------------------- #
def test2_is_paywalled_url_pattern() -> None:
    """Spec: `https://www.welt.de/.../plus6a.../x.html` → True; clean URL → False."""
    from pipeline.news_daily import _is_paywalled_url

    spec_paywalled = "https://www.welt.de/.../plus6a.../x.html"
    spec_clean = "https://www.tz.de/wirtschaft/immobilien-abc-123.html"

    spec_result = _is_paywalled_url(spec_paywalled)
    clean_result = _is_paywalled_url(spec_clean)

    # Bonus: real-world WELT+ URL (the kind actually produced by decode_url)
    real_welt_plus = (
        "https://www.welt.de/wirtschaft/plus6a37f069af14c0b528961c12/"
        "immobilien-die-grosse-babyboomer-verkaufswelle-beginnt.html"
    )
    real_welt_plus_result = _is_paywalled_url(real_welt_plus)

    spec_ok = spec_result is True
    clean_ok = clean_result is False
    overall = spec_ok and clean_ok

    evidence = (
        f"  spec_paywalled: {spec_paywalled}\n"
        f"    → _is_paywalled_url() = {spec_result}  "
        f"(expected True: {'✓' if spec_ok else '✗'})\n"
        f"  spec_clean    : {spec_clean}\n"
        f"    → _is_paywalled_url() = {clean_result}  "
        f"(expected False: {'✓' if clean_ok else '✗'})\n"
        f"  real_welt_plus (bonus, produced by decode_url above):\n"
        f"    {real_welt_plus}\n"
        f"    → _is_paywalled_url() = {real_welt_plus_result}\n"
        f"\n"
        f"NOTE on spec_paywalled: the task spec's example URL '.../plus6a.../x.html' "
        f"does NOT contain '/plus/' (with slashes), so the code's substring pattern "
        f"misses it. The pattern matches only '…path/plus/another-path…'.\n"
        f"The real WELT+ URL emitted by decode_url is also missed because "
        f"'plus' is concatenated with the article token (e.g. 'plus6a37...') "
        f"without a following slash."
    )
    record("Test 2 _is_paywalled_url pattern", overall, evidence)


# -------------------------------------------------------------------------- #
# Test 3: paywall short-content (<1000 chars) drop
# -------------------------------------------------------------------------- #
def test3_short_content_drop() -> None:
    """Mock fetch_full_text returning < 1000 chars; verify _fetch_google_news_text
    returns '<PAYWALLED>' AND sets item['_paywalled']=True."""
    from pipeline import news_daily
    from pipeline.news_daily import _fetch_google_news_text, _get_gn_decoder

    client = _get_gn_decoder()
    if client is None:
        record("Test 3 short-content drop", False,
               "SKIP: GoogleNewsClient unavailable — cannot build real decoded_url to mock-fetch")
        return

    # Build a mock item with a GN redirect URL — use a real one from RSS so decode succeeds.
    rss_url = (
        "https://news.google.com/rss/search"
        "?q=Immobilien+OR+Wohnung+when:7d&hl=de&gl=DE&ceid=DE:de"
    )
    try:
        req = urllib.request.Request(rss_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            raw = r.read().decode("utf-8", errors="replace")
    except Exception as e:
        record("Test 3 short-content drop", False, f"FAIL: cannot fetch RSS: {e}")
        return

    items = re.findall(r"<item>(.*?)</item>", raw, re.DOTALL)
    gn_url = None
    title = ""
    for it in items:
        link_m = re.search(r"<link>(.*?)</link>", it, re.DOTALL)
        title_m = re.search(r"<title>(.*?)</title>", it, re.DOTALL)
        if link_m:
            gn_url = link_m.group(1)
            title = (title_m.group(1) if title_m else "")[:80]
            break
    if not gn_url:
        record("Test 3 short-content drop", False, "FAIL: no GN URL in RSS")
        return

    # Sanity: this URL must NOT look like a paywall to test pass-2 (post-fetch short-content).
    decoded = client.decode_url(gn_url, timeout=20.0)
    from pipeline.news_daily import _is_paywalled_url
    if _is_paywalled_url(decoded):
        record("Test 3 short-content drop", False,
               f"FAIL: decoded URL triggers pass-1 paywall ({decoded}); "
               f"test URL is not paywalled — pick a different RSS item")
        return

    # Mock rss_fetch.fetch_full_text to return short content
    short_text = "Too short, only 42 chars."  # way under 1000
    mock_result = {
        "full_text": short_text,
        "paywalled": False,
        "char_count": len(short_text),
        "had_paywall_hint": False,
        "error": None,
    }
    item = {
        "url": gn_url,
        "title": title,
        "summary": "fallback summary",
        "no_full_text": True,
    }
    with patch("pipeline.lib.rss_fetch.fetch_full_text", return_value=mock_result):
        result = _fetch_google_news_text(item, delay_sec=0.0)

    is_paywalled_sentinel = result == "<PAYWALLED>"
    item_flagged = item.get("_paywalled") is True
    reason_correct = item.get("_paywall_reason") == "short-content"
    decoded_url_set = bool(item.get("_decoded_url"))
    original_gn_set = bool(item.get("_original_gn_url"))

    overall = is_paywalled_sentinel and item_flagged and reason_correct and decoded_url_set and original_gn_set

    evidence = (
        f"  decoded_url    : {item.get('_decoded_url', 'MISSING')[:120]}\n"
        f"  original_gn    : {item.get('_original_gn_url', 'MISSING')[:120]}\n"
        f"  mock char_count: {mock_result['char_count']}\n"
        f"  return value   : {result!r}  (expected '<PAYWALLED>': {'✓' if is_paywalled_sentinel else '✗'})\n"
        f"  item._paywalled: {item.get('_paywalled')}  (expected True: {'✓' if item_flagged else '✗'})\n"
        f"  item._paywall_reason: {item.get('_paywall_reason')}  "
        f"(expected 'short-content': {'OK' if reason_correct else 'BAD'})\n"
        f"  _decoded_url cached: {'✓' if decoded_url_set else '✗'}\n"
        f"  _original_gn_url cached: {'✓' if original_gn_set else '✗'}\n"
    )
    record("Test 3 short-content drop", overall, evidence)


# -------------------------------------------------------------------------- #
# Test 4: publisher-hint paywall (rss_fetch returns paywalled=True)
# -------------------------------------------------------------------------- #
def test4_publisher_hint_paywalled() -> None:
    """Mock fetch_full_text returning paywalled=True; verify _fetch_google_news_text
    returns '<PAYWALLED>' + sets item['_paywalled']=True."""
    from pipeline import news_daily
    from pipeline.news_daily import _fetch_google_news_text, _get_gn_decoder

    client = _get_gn_decoder()
    if client is None:
        record("Test 4 publisher-hint", False,
               "SKIP: GoogleNewsClient unavailable")
        return

    rss_url = (
        "https://news.google.com/rss/search"
        "?q=Immobilien+OR+Wohnung+when:7d&hl=de&gl=DE&ceid=DE:de"
    )
    try:
        req = urllib.request.Request(rss_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            raw = r.read().decode("utf-8", errors="replace")
    except Exception as e:
        record("Test 4 publisher-hint", False, f"FAIL: cannot fetch RSS: {e}")
        return

    items = re.findall(r"<item>(.*?)</item>", raw, re.DOTALL)
    gn_url = None
    title = ""
    for it in items:
        link_m = re.search(r"<link>(.*?)</link>", it, re.DOTALL)
        title_m = re.search(r"<title>(.*?)</title>", it, re.DOTALL)
        if link_m:
            gn_url = link_m.group(1)
            title = (title_m.group(1) if title_m else "")[:80]
            break
    if not gn_url:
        record("Test 4 publisher-hint", False, "FAIL: no GN URL in RSS")
        return

    decoded = client.decode_url(gn_url, timeout=20.0)
    from pipeline.news_daily import _is_paywalled_url
    if _is_paywalled_url(decoded):
        record("Test 4 publisher-hint", False,
               f"FAIL: decoded URL triggers pass-1 paywall ({decoded})")
        return

    # Mock fetch_full_text returning paywalled=True (publisher-hint path)
    mock_result = {
        "full_text": "Stub content with paywall text inside.",
        "paywalled": True,
        "char_count": 35,
        "had_paywall_hint": False,
        "error": None,
    }
    item = {
        "url": gn_url,
        "title": title,
        "summary": "fallback summary",
        "no_full_text": True,
    }
    with patch("pipeline.lib.rss_fetch.fetch_full_text", return_value=mock_result):
        result = _fetch_google_news_text(item, delay_sec=0.0)

    is_paywalled_sentinel = result == "<PAYWALLED>"
    item_flagged = item.get("_paywalled") is True
    reason_correct = item.get("_paywall_reason") == "publisher-hint"

    overall = is_paywalled_sentinel and item_flagged and reason_correct

    evidence = (
        f"  decoded_url    : {decoded[:120]}\n"
        f"  mock paywalled : True (publisher-hint path)\n"
        f"  return value   : {result!r}  (expected '<PAYWALLED>': {'✓' if is_paywalled_sentinel else '✗'})\n"
        f"  item._paywalled: {item.get('_paywalled')}  (expected True: {'✓' if item_flagged else '✗'})\n"
        f"  item._paywall_reason: {item.get('_paywall_reason')}  "
        f"(expected 'publisher-hint': {'✓' if reason_correct else '✗'})\n"
    )
    record("Test 4 publisher-hint", overall, evidence)


# -------------------------------------------------------------------------- #
# Test 5: decoded_url + original_gn_url persisted in Airtable metadata
# -------------------------------------------------------------------------- #
def test5_airtable_metadata_keys() -> None:
    """Read latest GN news record, confirm metadata JSON contains decoded_url +
    original_gn_url keys."""
    api_key = os.environ.get("AIRTABLE_API_KEY")
    if not api_key:
        record("Test 5 Airtable metadata", False,
               "SKIP: AIRTABLE_API_KEY not in env")
        return

    base = "appHilorcrC5T0p2u"
    table = "tblyJl2IBTgnImkM5"

    filter_formula = "{article_type}='short-summary'"
    url = (
        f"https://api.airtable.com/v0/{base}/{table}"
        f"?filterByFormula={urllib.parse.quote(filter_formula)}"
        "&pageSize=100"
    )
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {api_key}"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.load(r)
    except Exception as e:
        record("Test 5 Airtable metadata", False, f"FAIL: Airtable API error: {e}")
        return

    records = data.get("records", [])
    gn_records = [
        rec for rec in records
        if "news.google.com" in (rec["fields"].get("source_id", "") or "")
    ]

    if not gn_records:
        record("Test 5 Airtable metadata", False,
               "FAIL: no GN news records found in Airtable")
        return

    # Sort by processed_at desc, pick the most recent
    def sort_key(rec):
        return rec["fields"].get("processed_at", "")
    gn_records.sort(key=sort_key, reverse=True)
    latest = gn_records[0]
    md_field = latest["fields"].get("metadata", "")
    try:
        md = json.loads(md_field) if md_field else {}
    except Exception as e:
        md = {"_parse_error": str(e)}

    decoded_present = bool(md.get("decoded_url"))
    original_present = bool(md.get("original_gn_url"))
    overall = decoded_present and original_present

    evidence = (
        f"  Airtable record id       : {latest.get('id')}\n"
        f"  processed_at             : {latest['fields'].get('processed_at', '?')}\n"
        f"  metadata.decoded_url     : {repr(md.get('decoded_url', 'MISSING'))[:180]}\n"
        f"  metadata.original_gn_url : {repr(md.get('original_gn_url', 'MISSING'))[:180]}\n"
        f"  decoded_url present      : {'✓' if decoded_present else '✗'}\n"
        f"  original_gn_url present  : {'✓' if original_present else '✗'}\n"
        f"  API key suffix (audit)   : {API_KEY_SUFFIX}\n"
        f"  GN records scanned       : {len(gn_records)} (out of {len(records)} short-summary)\n"
        f"  Note                    : only the most recent GN record carries the "
        f"decoded keys — earlier records (pre-Task-12) lack them. We verify the "
        f"latest record carries both keys.\n"
        f"  metadata keys list      : {list(md.keys())}"
    )
    record("Test 5 Airtable metadata", overall, evidence)


# -------------------------------------------------------------------------- #
# Test 6: vault _index.md has no paywall stub strings
# -------------------------------------------------------------------------- #
def test6_vault_no_paywall_stub() -> None:
    """Scan immobilien-kb/vault/Daily/2026-08-09/_index.md for paywall stub markers."""
    vault = PROJECT_DIR / "immobilien-kb" / "vault" / "Daily" / "2026-08-09" / "_index.md"
    if not vault.exists():
        record("Test 6 vault no paywall stub", False,
               f"FAIL: vault file not found at {vault}")
        return

    text = vault.read_text(encoding="utf-8")
    # Forbidden markers (any of these = FAIL)
    forbidden = [
        "付費牆",      # "paywall" (zh-TW)
        "無法存取",    # "cannot access"
        "僅有標題",    # "only title"
        "paywall stub",
        "PAYWALLED",
        "<PAYWALLED>",
    ]
    hits = [(m, text.count(m)) for m in forbidden if m in text]
    overall = len(hits) == 0
    evidence_lines = [
        f"  vault file  : {vault}",
        f"  byte length : {len(text)}",
        f"  line count  : {text.count(chr(10)) + 1}",
        f"  forbidden markers scanned: {forbidden}",
        f"  hits        : {hits if hits else 'none'}",
    ]
    if not overall:
        evidence_lines.append("  FAIL: forbidden paywall-stub string(s) found in vault _index.md")
    else:
        evidence_lines.append("  PASS: no paywall-stub markers in vault _index.md")
    record("Test 6 vault no paywall stub", overall, "\n".join(evidence_lines))


# -------------------------------------------------------------------------- #
# Main
# -------------------------------------------------------------------------- #
def main() -> int:
    print(f"Using python: {sys.executable}")
    print(f"Project dir : {PROJECT_DIR}")
    print(f"API key suffix (audit): {API_KEY_SUFFIX}\n")

    test1_decode_url_real_publisher()
    test2_is_paywalled_url_pattern()
    test3_short_content_drop()
    test4_publisher_hint_paywalled()
    test5_airtable_metadata_keys()
    test6_vault_no_paywall_stub()

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    for name, ok, _ in RESULTS:
        print(f"  {'✅' if ok else '❌'} {name}")
    fails = [n for n, ok, _ in RESULTS if not ok]
    print()
    if fails:
        print(f"OVERALL: ❌ FAIL ({len(fails)} test(s) failed)")
        for n in fails:
            print(f"  - {n}")
        return 1
    print("OVERALL: ✅ PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
