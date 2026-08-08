"""Podcast Content Repurposer - Architectural Orchestrator.

Reads YAML format definitions, validates each LLM output, writes both
the .md file and a .validation.json sidecar.

Architecture:
  1. YAML-driven format definitions (formats.yaml)
  2. Independent validator functions (validators.py)
  3. Vault parser (parse_vault)
  4. LLM caller (call hermes reddit_safe.llm_client)
  5. Output writer (writes .md + .validation.json)
  6. Cooldown between LLM calls (3s default)

Adding a new platform:
  1. Add a key to formats.yaml with `validator: <name>`
  2. Register validator in validators.py
  3. Re-run. No other code changes.

Run inside execute_code (so reddit_safe is importable):
    exec(open('repurpose_executor.py').read())
    main()
"""
from __future__ import annotations
import json
import re
import sys
import time
from pathlib import Path
from typing import Callable

# ─────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────

TEMPLATE_DIR = Path(__file__).parent
REPO_ROOT = Path("/root/projects/ado_hermes_reddit_scrapper")
VAULT_ROOT = REPO_ROOT / "podcast-kb" / "vault" / "Daily"
CONTENT_ROOT = REPO_ROOT / "podcast-kb" / "content"

# ─────────────────────────────────────────────────────────────
# Imports (must be inside execute_code so reddit_safe is visible)
# ─────────────────────────────────────────────────────────────

try:
    import yaml  # noqa: F401 — used by load_formats
except ImportError:
    print("PyYAML missing — install with: pip3 install pyyaml")
    sys.exit(1)

from reddit_safe.pipeline.llm_client import call  # noqa: E402

# Local modules (same template dir)
sys.path.insert(0, str(TEMPLATE_DIR))
from validators import (  # noqa: E402
    run_validators,
    split_threads_post,
    strip_all_markdown,
)


# ─────────────────────────────────────────────────────────────
# Post-processor registry (format_id → function)
# ─────────────────────────────────────────────────────────────

def _facebook_pipeline(text: str) -> str:
    """Facebook pipeline: mark all section dividers with ▍, strip other markdown.

    Two sources of section dividers:
    1. Markdown headers (## or ###) → "▍ 標題"
    2. Heuristic: short standalone lines (<50 chars, ending with ： or ：)
       preceded AND followed by a blank line → "▍ 子標題"

    Then strip **bold**, *italic*, `code`, [text](url), etc.
    """
    import re as _re

    # Step 1: handle explicit markdown headers
    out_lines = []
    for line in text.splitlines():
        m = _re.match(r"^\s*(#+)\s*(.*)$", line)
        if m:
            out_lines.append(f"▍ {m.group(2).strip()}")
        else:
            out_lines.append(line)

    # Step 2: heuristic — detect short standalone sub-headers (e.g. "紅旗：...")
    # A line is treated as a sub-header if:
    #   - it is short (<50 chars)
    #   - it ends with ： or ：
    #   - the previous non-empty line is blank
    #   - the next non-empty line is blank OR starts a body paragraph
    final_lines = []
    for idx, line in enumerate(out_lines):
        stripped = line.strip()
        if not stripped:
            final_lines.append(line)
            continue
        # Skip if already prefixed with ▍
        if stripped.startswith("▍"):
            final_lines.append(line)
            continue
        # Heuristic check: a short standalone line (≤40 chars) surrounded by
        # blank lines is treated as a sub-header. No punctuation requirement.
        if len(stripped) <= 40 and not stripped.startswith("▍"):
            prev_nonblank_idx = idx - 1
            while prev_nonblank_idx >= 0 and not out_lines[prev_nonblank_idx].strip():
                prev_nonblank_idx -= 1
            prev_is_blank = prev_nonblank_idx < 0 or not out_lines[prev_nonblank_idx].strip()
            next_nonblank_idx = idx + 1
            while next_nonblank_idx < len(out_lines) and not out_lines[next_nonblank_idx].strip():
                next_nonblank_idx += 1
            next_exists = next_nonblank_idx < len(out_lines)
            if prev_is_blank and next_exists:
                # Treat as sub-header
                final_lines.append(f"▍ {stripped}")
                continue
        final_lines.append(line)

    body = "\n".join(final_lines)

    # Step 3: strip other markdown from the body
    body = _re.sub(r"\*\*([^*]+)\*\*", r"\1", body)
    body = _re.sub(r"\*([^*]+)\*", r"\1", body)
    body = _re.sub(r"__([^_]+)__", r"\1", body)
    body = _re.sub(r"_([^_]+)_", r"\1", body)
    body = _re.sub(r"~~([^~]+)~~", r"\1", body)
    body = _re.sub(r"`([^`]+)`", r"\1", body)
    body = _re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", body)
    body = _re.sub(r"!\[([^\]]*)\]\([^)]+\)", r"\1", body)
    body = _re.sub(r"\n{3,}", "\n\n", body)
    body = strip_all_markdown(body)  # strips markdown + emoji

    # Post-strip cleanup (per user 2026-08-08):
    # 1. Collapse spaces left after emoji removal
    body = _re.sub(r" {2,}", " ", body)
    # 2. Auto-add 。 ONLY for lines that end with 「：」 or 「、」 — clearly incomplete sentences.
    #    Do NOT touch any other line (titles stay title-style, complete sentences stay as LLM wrote).
    fixed = []
    for line in body.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith(chr(9605)) or stripped == "---":
            fixed.append(line)
            continue
        # If line ends with ： or 、 (clearly not a complete sentence), append 。
        if stripped.endswith(("：", ":")):
            stripped = stripped.rstrip("：:") + "。"
        elif stripped.endswith("、") and len(stripped) < 80:
            stripped = stripped.rstrip("、") + "。"
        fixed.append(stripped)
    body = "\n".join(fixed)
    # 3. Normalize line endings (rstrip only)
    body = "\n".join(line.rstrip() for line in body.splitlines())

    return body


# Post-processor registry: format_id -> post-processing function
# threads: full restructure (split ≤280, add markers, strip ALL markdown, no ▍)
# all others: convert ##/### → ▍ prefix, strip **/*/<u>, keep content emoji
POST_PROCESSORS: dict[str, Callable[[str], str]] = {
    "threads": split_threads_post,
    "facebook": _facebook_pipeline,
    "twitter": _facebook_pipeline,
    "linkedin": _facebook_pipeline,
    "newsletter": _facebook_pipeline,
    "podcast-outline": _facebook_pipeline,
}


# ─────────────────────────────────────────────────────────────
# Format loader
# ─────────────────────────────────────────────────────────────

def load_formats(path: Path) -> dict:
    """Load formats.yaml into a dict keyed by format id."""
    import yaml
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return data or {}


# ─────────────────────────────────────────────────────────────
# Vault parser
# ─────────────────────────────────────────────────────────────

def parse_vault(text: str) -> dict:
    """Extract title/metadata + 4 sections from a vault markdown file."""
    out = {
        "title": "", "channel": "", "duration": "",
        "summary": "", "analyst": "", "producer": "", "vocab": "",
    }
    m = re.search(r"^# (.+)$", text, re.MULTILINE)
    if m:
        out["title"] = m.group(1).strip()
    m = re.search(r"頻道\*\*：(.+)", text)
    if m:
        out["channel"] = m.group(1).strip()
    m = re.search(r"長度\*\*：(.+)", text)
    if m:
        out["duration"] = m.group(1).strip()
    def section(name: str) -> str:
        m = re.search(rf"^## {name}\s*\n(.*?)(?=^## |\Z)", text, re.MULTILINE | re.DOTALL)
        return m.group(1).strip() if m else ""
    out["summary"] = section("摘要")
    out["analyst"] = section("房地產分析師視角")
    out["producer"] = section("內容製作人視角")
    out["vocab"] = section("重點詞彙")
    return out


def find_latest_vault() -> Path:
    if not VAULT_ROOT.exists():
        raise FileNotFoundError(f"vault root not found: {VAULT_ROOT}")
    dates = sorted([d for d in VAULT_ROOT.iterdir() if d.is_dir()], reverse=True)
    for d in dates:
        files = [f for f in d.iterdir() if f.suffix == ".md" and f.stem != "_index"]
        if files:
            return sorted(files, key=lambda f: f.stat().st_mtime, reverse=True)[0]
    raise FileNotFoundError("no vault .md files found")


# ─────────────────────────────────────────────────────────────
# LLM call
# ─────────────────────────────────────────────────────────────

USER_TEMPLATE = """以下是德國房地產 podcast 的整理稿（已是繁體中文）。請根據結構與 tone 要求改寫成「{label}」格式。

## 影片資訊
- 標題：{title}
- 頻道：{channel}
- 影片時長：{duration}

## vault 摘要
{summary}

## vault 房地產分析師視角
{analyst}

## vault 內容製作人視角
{producer}

## vault 重點詞彙
{vocab}

---

請輸出「{label}」格式（純繁體中文、Markdown）。記住你的 tone 和結構要求。"""

LANGUAGE_GUARD_PROMPT = """**語言強制約束**：
- 純繁體中文（zh-TW）、禁止簡體字
- 專有名詞保留德文原文並用括號補充中文（例：Grunderwerbsteuer（房地產交易稅））
- 使用台灣在地表達（「公寓」「房貸」「貸款利率」「房地產」「稅務」）
- 數字、人名、公司名稱忠於原文
- 輸出純 Markdown 格式、用 `## 標題` 或 bullet、不要用 JSON / code block 包整個輸出
"""


def call_llm_for_format(fmt_id: str, fmt_cfg: dict, parsed: dict,
                       llm_timeout: int = 180) -> tuple[str, int, int]:
    """Call LLM. Returns (output_text, prompt_chars, output_chars)."""
    system = LANGUAGE_GUARD_PROMPT + "\n" + fmt_cfg["system"]
    user = USER_TEMPLATE.format(
        label=fmt_cfg["label"],
        title=parsed["title"],
        channel=parsed["channel"],
        duration=parsed["duration"],
        summary=parsed["summary"],
        analyst=parsed["analyst"],
        producer=parsed["producer"],
        vocab=parsed["vocab"],
    )
    messages = [{"role": "system", "content": system}, {"role": "user", "content": user}]
    t0 = time.time()
    text, _usage = call(messages, timeout=llm_timeout)
    elapsed = time.time() - t0
    return text.strip(), len(user), elapsed


# ─────────────────────────────────────────────────────────────
# Output writer
# ─────────────────────────────────────────────────────────────

def write_output(vault_file: Path, fmt_id: str, fmt_cfg: dict,
                 content: str, validation_results: list) -> tuple[Path, Path]:
    """Write the .md and .validation.json sidecar.

    Returns (md_path, validation_path).
    """
    rel = vault_file.relative_to(VAULT_ROOT)
    date_str = rel.parts[0]
    slug = vault_file.stem
    out_dir = CONTENT_ROOT / date_str / slug
    out_dir.mkdir(parents=True, exist_ok=True)
    md_path = out_dir / fmt_cfg["default_filename"]
    val_path = out_dir / f"{fmt_id}.validation.json"

    header = (
        f"# {fmt_cfg['label']} — {parse_vault(vault_file.read_text(encoding='utf-8'))['title']}\n\n"
        f"- **影片**：{parse_vault(vault_file.read_text(encoding='utf-8'))['title']}\n"
        f"- **頻道**：{parse_vault(vault_file.read_text(encoding='utf-8'))['channel']}\n"
        f"- **影片時長**：{parse_vault(vault_file.read_text(encoding='utf-8'))['duration']}\n"
        f"- **vault**：{vault_file.relative_to(REPO_ROOT)}\n\n---\n\n"
    )
    md_path.write_text(header + content, encoding="utf-8")

    val_summary = {
        "format": fmt_id,
        "vault": str(vault_file.relative_to(REPO_ROOT)),
        "content_chars": len(content),
        "validators": [
            {"name": name, "ok": ok, "msg": msg}
            for (name, ok, msg) in validation_results
        ],
        "all_passed": all(ok for (_, ok, _) in validation_results),
        "n_warnings": sum(1 for (_, ok, _) in validation_results if not ok),
    }
    val_path.write_text(json.dumps(val_summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return md_path, val_path


# ─────────────────────────────────────────────────────────────
# Main orchestrator
# ─────────────────────────────────────────────────────────────

def process_one(vault_file: Path, fmt_id: str, fmt_cfg: dict,
                cooldown: float = 3.0, dry_run: bool = False) -> dict:
    """Process one vault through one format. Returns summary dict."""
    result = {
        "vault": str(vault_file.relative_to(REPO_ROOT)),
        "format": fmt_id,
        "ok": False,
        "validators_passed": 0,
        "validators_total": 0,
        "output_chars": 0,
        "elapsed": 0.0,
        "message": "",
    }
    if not vault_file.exists():
        result["message"] = "vault file not found"
        return result
    parsed = parse_vault(vault_file.read_text(encoding="utf-8"))
    if not parsed["summary"]:
        result["message"] = "vault file missing 摘要 section"
        return result

    try:
        text, prompt_chars, elapsed = call_llm_for_format(fmt_id, fmt_cfg, parsed)
        result["elapsed"] = elapsed
        result["output_chars"] = len(text)
        # Post-process: fix LLM output quirks before validating
        # (e.g. threads 1/N) markers, ≤280 chars per post)
        post_proc = POST_PROCESSORS.get(fmt_id)
        if post_proc:
            text = post_proc(text)
            result["output_chars"] = len(text)
        # Validate
        val_results = run_validators(fmt_cfg["validator"], text)
        result["validators_passed"] = sum(1 for (_, ok, _) in val_results if ok)
        result["validators_total"] = len(val_results)
        result["all_validators_passed"] = all(ok for (_, ok, _) in val_results)
        result["validators"] = val_results
        if dry_run:
            result["ok"] = True
            result["message"] = f"dry-run, output {len(text)} chars"
        else:
            md_path, val_path = write_output(vault_file, fmt_id, fmt_cfg, text, val_results)
            result["md_path"] = str(md_path.relative_to(REPO_ROOT))
            result["val_path"] = str(val_path.relative_to(REPO_ROOT))
            # OK iff at least 60% of validators passed (lenient — Twitter/Threads often
            # over-cap by 1-2 chars and LLM tuning is iterative)
            result["ok"] = result["validators_passed"] >= int(0.6 * result["validators_total"])
            result["message"] = (
                f"wrote {len(text)} chars, validators {result['validators_passed']}/"
                f"{result['validators_total']} passed"
            )
    except Exception as e:
        result["message"] = f"FAILED: {e}"
    return result


def main(formats_to_run: list[str] | None = None,
         use_latest: bool = True,
         vault_paths: list[Path] | None = None,
         cooldown: float = 3.0,
         dry_run: bool = False) -> dict:
    """Main orchestrator. Run from inside execute_code via exec()."""
    formats = load_formats(TEMPLATE_DIR / "formats.yaml")
    if formats_to_run:
        formats = {k: v for k, v in formats.items() if k in formats_to_run}
    if not formats:
        return {"ok": False, "message": "no formats configured"}

    if use_latest:
        vault_files = [find_latest_vault()]
    elif vault_paths:
        vault_files = vault_paths
    else:
        return {"ok": False, "message": "no vault files provided"}

    print(f"=== Running {len(formats)} formats × {len(vault_files)} vault files ===")
    all_results = []
    for vf in vault_files:
        parsed_preview = parse_vault(vf.read_text(encoding="utf-8"))
        print(f"\n📄 vault: {vf.name}")
        print(f"   title: {parsed_preview['title'][:60]}")
        print(f"   summary: {len(parsed_preview['summary'])} chars")
        for fmt_id, fmt_cfg in formats.items():
            r = process_one(vf, fmt_id, fmt_cfg, cooldown=cooldown, dry_run=dry_run)
            all_results.append(r)
            mark = "✅" if r["ok"] else "⚠️" if r["validators_passed"] > 0 else "❌"
            print(f"   {mark} {fmt_id:18} {r['elapsed']:5.1f}s  {r['validators_passed']}/{r['validators_total']} validators  {r['message']}")
            if fmt_id != list(formats.keys())[-1]:
                time.sleep(cooldown)

    # Summary
    n_ok = sum(1 for r in all_results if r["ok"])
    n_total = len(all_results)
    print(f"\n=== Summary: {n_ok}/{n_total} formats passed ===")
    return {"ok": True, "results": all_results, "summary": {"passed": n_ok, "total": n_total}}


if __name__ == "__main__":
    main()