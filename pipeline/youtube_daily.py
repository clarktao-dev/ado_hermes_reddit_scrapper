#!/usr/bin/env python3
"""YouTube Podcast daily pipeline (kome.ai → Map-Reduce → Obsidian + Discord).

Steps (Task 7 short-first default, 2026-08-09):
  1. load_config (channels.json)
  2. youtube_fetch   — pick N channels (round-robin by day-of-year) → newest video each
  3. ProcessedStore  — Airtable ledger is the single source of truth for dedup.
state.json is **deprecated** but still read for backward-compat (so existing
processed_ids aren't re-processed). New marks go to Airtable, not state.json.
  4a. ``--mode short`` (default): Google translate chunks → ONE lightweight LLM
      call → 200-char summary + bullets + 1-2 sentence view. Saves ~25-30% of
      long-form tokens. Vault filename: ``<slug>_summary.md``. Writes
      ``article_type="short-summary"`` to the ledger.
  4b. ``--mode long``: legacy Map-Reduce + dual-lens analysis + OpenCC defense
      (the full output Task 3/6 produced). Vault filename:
      ``<slug>_longform.md``. Writes ``article_type="long-form"``.
  5. write_vault     — wipe + write podcast-kb/vault/Daily/<date>/
  6. send_discord    — push to channel `podcast` (alias)
  7. push_to_github  — git add + commit + push via existing paramiko script
  8. update_side_effects — backfill discord_message_id + github_commit_sha into
     the ProcessedContent ledger (best-effort; failure doesn't lose the mark).
     Skipped under ``--dry-run``.

Long-form is on-demand via ``pipeline/scripts/recommend_long_form.py confirm``.

Run:
  python3 youtube_daily.py                # default short mode
  python3 youtube_daily.py --mode long    # full long-form Map-Reduce
  python3 youtube_daily.py --dry-run      # no Discord, no GitHub, no mark_processed
  python3 youtube_daily.py --channels '1alage,marktcheck'
"""
from __future__ import annotations
import argparse
import json
import logging
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE.parent))

from pipeline.lib import (  # noqa: E402
    youtube_fetch,
    youtube_state,
    youtube_translate,
    youtube_obsidian,
    youtube_discord,
)
from pipeline.lib.processed_store import (  # noqa: E402
    ProcessedStore,
    make_hash,
    DEFAULT_TABLE,
)


from pipeline.lib.paths import (  # noqa: E402
    CHANNELS_CONFIG,
    PODCAST_STATE_PATH,
    PODCAST_VAULT_GIT_PATH,
    PUSH_TO_GITHUB_SCRIPT,
    VAULT_ROOT,
)
PROCESSED_BASE_ID = os.environ.get(
    "AIRTABLE_PROCESSED_CONTENT_BASE_ID", "appHilorcrC5T0p2u"
)
PROCESSED_TABLE = os.environ.get(
    "AIRTABLE_PROCESSED_CONTENT_TABLE", DEFAULT_TABLE
)

logger = logging.getLogger("youtube_daily")
if not logger.handlers:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )


def load_channels() -> list:
    data = json.loads(Path(CHANNELS_CONFIG).read_text(encoding="utf-8"))
    return [c for c in data.get("channels", []) if c.get("enabled", True)]


# Persistent run counter for round-robin channel rotation (Task 9).
# Stored as a plain text file so the counter survives across runs in the
# same week, regardless of whether the run is cron-triggered or manual.
RUN_COUNTER_PATH = "/tmp/youtube_daily_run_count"


def _read_run_counter() -> int:
    """Return the previous run count, or 0 if the file is missing/corrupt."""
    try:
        return int(Path(RUN_COUNTER_PATH).read_text().strip() or "0")
    except (FileNotFoundError, ValueError):
        return 0


def _write_run_counter(n: int) -> None:
    Path(RUN_COUNTER_PATH).write_text(str(n))


def pick_channels(channels: list, n: int = 4) -> list:
    """Round-robin by run count: pick n consecutive channels starting at
    (run_count % len(channels)). This rotates every time the pipeline runs
    (not just every day), so running twice in the same day hits different
    channels than the first run.

    n default was 3 (2026-08-08); bumped to 4 on 2026-08-24 because ex_makler
    has 22 zombie/unprocessed videos (state.json false positives — see
    commit 4da1c72) and 3-channel cadence would take 6+ days to clear one
    channel. 4-channel cadence brings total wall-clock to ~7 min vs ~5 min
    at n=3, well within ollama-cloud's hang threshold (<8 sequential LLM
    calls in tight loop).

    The run counter is incremented and persisted after each call so the
    next run starts from a different offset.
    """
    if n >= len(channels):
        return list(channels)
    counter = _read_run_counter()
    start = counter % len(channels)
    _write_run_counter(counter + 1)
    return [channels[(start + i) % len(channels)] for i in range(n)]


def _state_json_has(state: youtube_state.StateStore, channel_id: str,
                    video_id: str) -> bool:
    """Read legacy state.json (deprecated). Kept for backward compat only.

    state.json is no longer *written* by this pipeline — ProcessedContent
    (Airtable) is the single source of truth. But existing state.json
    records are still respected so we don't reprocess them before they
    migrate into Airtable.

    **Trust guard (2026-08-24)**: a state.json record is ONLY trusted when the
    vault has physical evidence the digest was actually written. Earlier runs
    (Task 3) would sometimes record ``processed_ids`` for videos whose digest
    failed silently (e.g. empty transcript + no fallback). Result: those
    videos were stuck in a "processed but never summarized" zombie state —
    pipeline always skipped them even though they were new material. The vault
    cross-check restores them as eligible candidates.
    """
    try:
        if not state.is_processed(channel_id, video_id):
            return False
        # state.json says processed → verify vault has a matching file.
        return _vault_has_digest(channel_id, video_id)
    except Exception as e:  # noqa: BLE001
        logger.debug("state.json read failed (%s) — ignoring", e)
        return False


def _vault_has_digest(channel_id: str, video_id: str) -> bool:
    """Best-effort check: does the vault have a digest file mentioning this
    video_id? Search the Daily/<date>/ folders for a filename containing the
    video id. Cheap (one glob per check), only runs when state.json has the
    id marked.

    Note: this only checks historical Daily/ folders. Long-form vault files
    under immobilien-kb/vault/YouTube/<Channel>/ also count (written by
    recommend_long_form confirm).
    """
    repo = Path(__file__).parent.parent  # /root/projects/ado_hermes_reddit_scrapper
    # 1. podcast-kb/vault/Daily/*/<slug>*<video_id>*.md
    daily_glob = list((repo / "podcast-kb" / "vault" / "Daily").glob(
        f"*/*/*{video_id}*.md"))
    if daily_glob:
        return True
    # 2. immobilien-kb/vault/YouTube/<Channel>/ long-form (rare path)
    yt_dir = repo / "immobilien-kb" / "vault" / "YouTube"
    if yt_dir.exists():
        for ch_dir in yt_dir.iterdir():
            if not ch_dir.is_dir():
                continue
            if list(ch_dir.glob(f"**/*{video_id}*.md")):
                return True
    return False


# --------------------------------------------------------------------------- #
# Short-summary mode (Task 7, 2026-08-09)
# --------------------------------------------------------------------------- #

SHORT_STRUCTURE_SYSTEM_PROMPT = """你是專精於德國房地產的資深編輯助理,幫台灣投資人做「一句話 + bullets + 觀點 + 重點詞彙」的快速摘要。

**輸入**:YouTube 影片的繁體中文逐字稿(已是德文→繁中的機器翻譯)。

**輸出結構**(純繁體中文、Markdown):

## 一句話摘要
(一段話,200 字以內,完整覆蓋影片核心訊息)

## 重點 bullets
(3-5 個 bullet,每個 bullet 用 `- ` 開頭,點出影片的關鍵事實、數據、論點)

## 觀點
(1-2 句,給台灣房地產投資人的實質觀察 — 為何這部影片值得看、後續要追蹤什麼)

## 重點詞彙
(3-5 個德文術語,每個用「- **德文(中文)**：用法說明 1-2 句,含使用情境或計算範例」的格式。**若影片確實沒有專業術語,這個 section 留空、輸出 `## 重點詞彙` 後直接空一行** — LLM 失敗也允許,parser 會顯示「(無)」)

**規則**:
- 只能根據輸入內容整理,禁止補充原文沒有的資料
- 專有名詞保留德文原文並用括號補充中文(例:Grunderwerbsteuer(房地產交易稅))
- 數字、人名、公司名稱忠於原文
- 使用台灣在地表達
- **嚴格控制長度**:一句話 ≤200 字、bullets 3-5 個、觀點 1-2 句、詞彙 3-5 個。**不要展開分析、不要寫長段落** — 這是 daily 預設的輕量版,完整分析請走 ``--mode long``。

## ⛔ 反 hallucination 強制規則(2026-08-27 修補)

你**絕對禁止**寫出以下任何 placeholder/錯誤訊息字串,即使聽起來「自然」、「合理」也不行:

- `影片因伺服器錯誤無法載入` / `影片內容無法取得` / `無法取得影片內容`
- `影片無法觀看` / `影片無法載入` / `影片已下架`
- `伺服器錯誤` / `Server Error` / `Error 500`
- `placeholder` / `字幕檔無法讀取`
- 任何以「影片...無法...」開頭的句子

**判斷邏輯**:
- 若輸入的「逐字稿」有任何實質內容(德文對話/敘述/數字/人物/案例),你**必須根據內容寫摘要**,從影片主題直接切入,**不要**寫「無法取得」「伺服器錯誤」這類 placeholder。
- **只有**當輸入逐字稿是**空字串、只有空白、或內容僅由 HTML/錯誤訊息組成**時,你才能在「一句話摘要」寫:`影片內容無法取得,請手動確認。`(這是唯一的合法 fallback 寫法,且**僅限**空逐字稿情境)
- 「一句話摘要」的**開頭**必須直接是影片主題或結論,絕對不能以「影片...」開頭。

**Pipeline 已加守門員**:即便你違反規則輸出 placeholder,後處理也會自動 re-prompt 或降級為 `(內容待補)`。請直接遵守,不要測試 pipeline 的容錯。
"""

SHORT_STRUCTURE_USER_TEMPLATE = """以下是 YouTube 影片逐字稿的繁體中文機器翻譯結果。請做輕量版摘要。

## 影片資訊
- 標題:{title}
- 頻道:{channel}
- 影片時長:{duration} 秒

## 繁中逐字稿全文
{translated_text}

---

請按結構輸出(純繁體中文、Markdown):"""


def step_structure_short(video, translated_text: str,
                         llm_timeout: int = 120) -> dict:
    """Run ONE lightweight LLM call for short-summary mode (Task 7).

    Reuses the Google-translated zh text produced by ``digest_video``'s
    pipeline but skips the full Map-Reduce dual-lens structuring.
    Returns ``{"summary_zh", "analyst_zh", "producer_zh"}`` — these map
    to the existing ``VideoDigest`` fields (``summary_zh`` = 一句話,
    ``analyst_zh`` = bullets, ``producer_zh`` = 觀點; ``vocab_zh`` stays
    empty because vocabulary is a long-form affordance).

    Anti-stub guard (2026-08-27):
        If the LLM returns a stub summary ("影片因伺服器錯誤無法載入",
        etc.) but the translated text actually contains content, retry ONCE
        with a stricter user message that explicitly cites the input length
        and forbids placeholder output. If the second attempt still looks
        like a stub, downgrade to ``(內容待補)` placeholders so the vault
        file is still produced (per the pipeline contract) but the user
        can tell from Discord / vault that the digest needs manual review.

    Failure path: any LLM error → return empty strings so the caller can
    still write a vault file with `(無)` placeholders. We do not raise —
    the video is already transcribed and translated at this point; losing
    the structuring step should not block the daily pipeline.
    """
    duration_min = video.duration_sec // 60
    duration_str = f"{duration_min} 分 {video.duration_sec % 60} 秒"
    user = SHORT_STRUCTURE_USER_TEMPLATE.format(
        title=video.title,
        channel=video.channel_name,
        duration=duration_str,
        translated_text=translated_text,
    )
    text = _call_llm_short(user, llm_timeout=llm_timeout)
    if text is None:
        return {"summary_zh": "", "analyst_zh": "", "producer_zh": "", "vocab_zh": ""}
    text = text.strip()
    # OpenCC belt-and-braces
    from pipeline.lib.translate import (  # noqa: PLC0415
        force_traditional,
        has_simplified,
    )
    if has_simplified(text):
        fixed = force_traditional(text)
        text = fixed[0] if isinstance(fixed, tuple) else fixed
    sections = _split_short_digest(text)

    # ---- Output gate (anti-stub) --------------------------------------- #
    # If the LLM hallucinated a placeholder even though we have real
    # content, retry once with a hard refusal prompt. If the second attempt
    # still stubs, downgrade cleanly so the user can tell from Discord /
    # vault that the digest needs manual review.
    from pipeline.lib import stub_detection  # noqa: PLC0415
    summary = sections.get("summary_zh", "")
    has_real_input = bool(translated_text and translated_text.strip())
    if has_real_input and stub_detection.is_stub_summary(summary):
        reason_first = stub_detection.stub_reason(summary)
        logger.warning(
            "step_structure_short: stub detected (reason=%r) on first call "
            "for video %s; retrying with hard refusal prompt",
            reason_first, video.id,
        )
        retry_user = (
            f"{user}\n\n---\n\n"
            f"⚠️ 你上一個回應({reason_first!r})是 placeholder,違反規則。"
            f"輸入逐字稿實際有 {len(translated_text)} 個字、實際有影片內容。"
            "請**只用**上面那段逐字稿寫「一句話摘要」(200 字以內、直接從影片主題切入),"
            "**禁止**寫「影片...無法...」「伺服器錯誤」「placeholder」這類字串。"
            "若你仍堅持要寫 placeholder,請回應 `__REFUSE__` 三個字就好。"
        )
        text2 = _call_llm_short(retry_user, llm_timeout=llm_timeout)
        if text2 is not None:
            text2 = text2.strip()
            if has_simplified(text2):
                fixed = force_traditional(text2)
                text2 = fixed[0] if isinstance(fixed, tuple) else fixed
            if text2.strip() == "__REFUSE__":
                logger.warning(
                    "step_structure_short: LLM refused after stub-detection "
                    "retry for video %s; downgrading to (內容待補)",
                    video.id,
                )
            else:
                sections2 = _split_short_digest(text2)
                if not stub_detection.is_stub_summary(sections2.get("summary_zh", "")):
                    logger.info(
                        "step_structure_short: stub fixed on retry for video %s",
                        video.id,
                    )
                    return sections2
                logger.warning(
                    "step_structure_short: stub STILL detected after retry "
                    "(reason=%r) for video %s; downgrading to (內容待補)",
                    stub_detection.stub_reason(sections2.get("summary_zh", "")),
                    video.id,
                )
        # Fallthrough: downgrade to placeholder so the vault file is still
        # produced but the user can see the digest needs manual review.
        return {
            "summary_zh": "(內容待補 — LLM 兩次都輸出 placeholder,需手動確認)",
            "analyst_zh": "(無)",
            "producer_zh": "(無 — 自動跳過)",
            "vocab_zh": "",
        }
    return sections


def _call_llm_short(user: str, llm_timeout: int) -> str | None:
    """Single LLM call helper for ``step_structure_short``.

    Returns the assistant text on success, ``None`` on any error (timeout,
    connection, JSON decode). Kept separate so the retry path can reuse it.
    """
    try:
        from reddit_safe.pipeline.llm_client import call  # type: ignore
        messages = [
            {"role": "system", "content": SHORT_STRUCTURE_SYSTEM_PROMPT},
            {"role": "user", "content": user},
        ]
        text, _usage = call(messages, timeout=llm_timeout)
        return text
    except Exception as e:  # noqa: BLE001
        logger.warning("step_structure_short: LLM call failed: %s", e)
        return None


def _split_short_digest(text: str) -> dict:
    """Parse the short-mode structured Markdown into the four sections.

    ``vocab_zh`` is OPTIONAL — empty string when the LLM left the
    vocabulary section blank or the parser couldn't find it (Task 9).
    """
    sections = {"summary_zh": "", "analyst_zh": "", "producer_zh": "", "vocab_zh": ""}
    if not text or text.startswith("[LLM_ERROR]"):
        sections["summary_zh"] = text or ""
        return sections
    parts = re.split(r"^## (.+)$", text, flags=re.MULTILINE)
    if len(parts) < 3:
        # Fallback: dump everything into summary
        sections["summary_zh"] = text.strip()
        return sections
    headers_bodies = []
    for i in range(1, len(parts), 2):
        h = parts[i].strip()
        b = parts[i + 1].strip() if i + 1 < len(parts) else ""
        headers_bodies.append((h, b))
    for h, b in headers_bodies:
        if "一句話" in h or "一句" in h:
            sections["summary_zh"] = b
        elif "bullets" in h.lower() or "重點" in h and "詞彙" not in h:
            sections["analyst_zh"] = b
        elif "觀點" in h:
            sections["producer_zh"] = b
        elif "詞彙" in h or "vocab" in h.lower():
            sections["vocab_zh"] = b
    return sections


def pick_video_for_channel(
    channel: dict,
    store: ProcessedStore,
    state: youtube_state.StateStore | None = None,
    look_back: int = 10,
) -> list[youtube_fetch.VideoMeta]:
    """List videos and return ALL unprocessed candidates (newest first).

    Dedup is checked against the ProcessedStore (Airtable). When state.json
    still has records not yet migrated to Airtable, those are also respected
    (backward compat).

    Returns a LIST (not a single VideoMeta) so the caller can retry with the
    next candidate if the current one fails (e.g. empty transcript from a
    paywalled member video). Task 9: a single empty-transcript video should
    not abort the whole run — caller walks through the list.

    The returned list is capped at ``look_back`` items.
    """
    metas = youtube_fetch.list_channel_videos(
        channel_id=channel["id"],
        channel_name=channel["name"],
        channel_url=channel["url"],
        youtube_channel_id=channel.get("channel_id"),
        limit=look_back,
    )
    candidates: list[youtube_fetch.VideoMeta] = []
    for m in metas:
        # Primary check: Airtable ledger.
        if store.is_processed("youtube", m.id):
            logger.info(
                "skip already-processed (airtable): %s | %s",
                channel["id"], m.id,
            )
            continue
        # Backward-compat: legacy state.json. Remove once state.json is
        # fully migrated (see integration task notes).
        if state is not None and _state_json_has(state, channel["id"], m.id):
            logger.info(
                "skip already-processed (legacy state.json): %s | %s",
                channel["id"], m.id,
            )
            continue
        candidates.append(m)
    return candidates


def push_to_github(vault_root: str, dry_run: bool) -> dict:
    """Stage + commit + push podcast-kb/vault/ to the vault repo.

    Returns dict with `pushed`, `commit_sha` (str|None), and command stdout/stderr.
    """
    out: dict = {"pushed": False, "commit_sha": None, "commands": [],
                 "dry_run": dry_run}
    if dry_run:
        return out
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    msg = f"podcast-kb: {today} daily digest"

    cmds = [
        ["git", "-C", vault_root, "add", f"{PODCAST_VAULT_GIT_PATH}/"],
        ["git", "-C", vault_root, "-c", "user.email=ado@hermes.local", "-c",
         "user.name=Ado", "commit", "-m", msg],
    ]
    for cmd in cmds:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        out["commands"].append({
            "cmd": " ".join(cmd),
            "rc": proc.returncode,
            "stdout_tail": proc.stdout[-200:],
            "stderr_tail": proc.stderr[-200:],
        })
        if proc.returncode != 0 and "nothing to commit" not in proc.stderr:
            out["error"] = proc.stderr[-300:]
            return out
    # Capture the commit SHA so we can backfill ProcessedContent.github_commit_sha
    sha_proc = subprocess.run(
        ["git", "-C", vault_root, "rev-parse", "HEAD"],
        capture_output=True, text=True, timeout=10,
    )
    if sha_proc.returncode == 0:
        out["commit_sha"] = sha_proc.stdout.strip()

    # push via existing paramiko script
    push_proc = subprocess.run(
        ["python3", str(PUSH_TO_GITHUB_SCRIPT), "--scope", "podcast"],
        capture_output=True, text=True, timeout=60,
    )
    out["push"] = {
        "rc": push_proc.returncode,
        "stdout_tail": push_proc.stdout[-200:],
        "stderr_tail": push_proc.stderr[-200:],
    }
    out["pushed"] = push_proc.returncode == 0
    return out


def _video_output_md_path(repo_root: str, date_str: str, digest, content_kind: str = "longform") -> str | None:
    """Reconstruct the per-video Markdown file path written by youtube_obsidian.

    The slug is ``<channel>_<title>_<video_id>_<content_kind>`` (truncated to 80 chars) plus
    ``.md`` under ``podcast-kb/vault/Daily/<date>/``. We don't trust the slug
    char-for-char (OpenCC might mangle), so we look up the actual written file
    by video_id substring.
    """
    out_dir = Path(repo_root) / "podcast-kb" / "vault" / "Daily" / date_str
    if not out_dir.exists():
        return None
    needle = digest.video_id
    # Match either `_summary.md` (short mode) or `_longform.md` (long mode).
    suffix = "_summary.md" if content_kind == "short-summary" else "_longform.md"
    for p in sorted(out_dir.glob(f"*{suffix}")):
        if needle in p.name:
            return str(p.relative_to(repo_root))
    return None


def _build_metadata(digest, video: youtube_fetch.VideoMeta,
                    channel: dict) -> dict:
    return {
        "epoch": int(video.epoch) if video.epoch else None,
        "published_epoch": int(video.epoch) if video.epoch else None,
        "duration_sec": int(video.duration_sec),
        "lang": digest.source_language or "de",
        "n_chars": int(digest.n_chars),
        "channel_id": channel["id"],
        "channel_name": channel["name"],
        "youtube_url": video.url,
        "map_calls": int(digest.map_calls),
        "reduce_calls": int(digest.reduce_calls),
        "elapsed_sec": float(digest.elapsed_sec),
    }


def _translate_only(transcript_text: str, source: str = "de",
                    target: str = "zh-TW", cooldown_sec: float = 3.0) -> str:
    """Run Google Translate on chunks (no LLM). Used by short-summary mode.

    Mirrors the first half of ``youtube_translate.digest_video`` — split
    into chunks, Google-Translate each one with a small cooldown, then
    OpenCC belt-and-braces. Stops there: no LLM structuring call. The
    caller invokes :func:`step_structure_short` next.

    Kept private (underscore-prefixed) because short-mode uses a
    different structuring prompt and we don't want callers reaching for
    ``_translate_only`` independently of :func:`step_structure_short`.
    """
    from pipeline.lib.translate import force_traditional, has_simplified  # noqa: PLC0415
    chunks = youtube_translate._split_chunks(transcript_text)
    print(f"    [translate] {len(chunks)} chunks via Google Translate "
          f"({source}→{target})")
    out: list = []
    for i, c in enumerate(chunks):
        zh = youtube_translate._translate_chunk(c, source=source, target=target)
        out.append(zh)
        if i < len(chunks) - 1:
            time.sleep(cooldown_sec)
    text = "\n\n".join(out)
    if has_simplified(text):
        text = youtube_translate._normalize(force_traditional(text))
        print("    [OpenCC] cleaned simplified chars in translation")
    return text


def _build_short_digest(video, translated_text: str, t0: float) -> youtube_translate.VideoDigest:
    """Build a VideoDigest with summary_zh/analyst_zh/producer_zh/vocab_zh
    populated from :func:`step_structure_short}. ``vocab_zh`` is
    OPTIONAL — the LLM may leave it empty (e.g. videos without real-estate
    jargon), in which case the vault renders ``(無)``.
    """
    sections = step_structure_short(video, translated_text)
    return youtube_translate.VideoDigest(
        video_id=video.id,
        title=video.title,
        channel_name=video.channel_name,
        url=video.url,
        published_epoch=video.epoch,
        duration_sec=video.duration_sec,
        source_language="de",
        n_chars=0,  # not used in short render; set by caller if needed
        summary_zh=sections["summary_zh"],
        analyst_zh=sections["analyst_zh"],
        producer_zh=sections["producer_zh"],
        vocab_zh=sections.get("vocab_zh", ""),
        map_calls=0,
        reduce_calls=1,
        elapsed_sec=time.time() - t0,
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="Fetch + translate + write vault, but skip Discord and GitHub push")
    ap.add_argument("--mode", choices=("short", "long"), default="short",
                    help="Pipeline output mode. ``short`` (default) emits a "
                         "compact 200-char + 5 bullets + 1-2 sentence view "
                         "for every video — saves ~25-30%% LLM tokens. "
                         "``long`` runs the full Map-Reduce dual-lens digest "
                         "and is on-demand via "
                         "pipeline/scripts/recommend_long_form.py confirm. "
                         "Both modes are written to the vault under "
                         "podcast-kb/vault/Daily/<date>/ as "
                         "``<slug>_summary.md`` / ``<slug>_longform.md``.")
    ap.add_argument("--channels", default="",
                    help="Comma-separated channel IDs to override round-robin (e.g. '1alage,marktcheck')")
    ap.add_argument("--n-channels", type=int, default=4,
                    help="How many channels to process per run (default 4 — covers 9 channels in ~10-12 days; bumped from 3 on 2026-08-24 to clear ex_makler zombie backlog from state.json false positives)")
    ap.add_argument("--skip-store", action="store_true",
                    help="Bypass ProcessedStore (for local debugging)")
    ap.add_argument("--pipeline-run-id", default="",
                    help="Pipeline run id to record (default: auto-generated)")
    ap.add_argument("--video-id", default="",
                    help="Force-process a single YouTube video id "
                         "(testing / re-processing override; bypasses "
                         "channel selection)")
    ap.add_argument("--force", action="store_true",
                    help="With --video-id, re-process even if the ledger "
                         "already has this video")
    args = ap.parse_args()

    t0 = time.time()
    run_id = args.pipeline_run_id or datetime.now(timezone.utc).strftime(
        "yt-%Y%m%d-%H%M%S"
    )

    channels = load_channels()
    forced_video: youtube_fetch.VideoMeta | None = None
    if args.video_id:
        forced_video = _force_fetch_video_meta(args.video_id)
        if forced_video is None:
            logger.error("could not resolve --video-id %s", args.video_id)
            return 2
        # Pick the channel that owns this video (if we have a matching channel
        # in channels.json); otherwise synthesise a channel dict from the
        # metadata.
        ch_match = next(
            (c for c in channels if c.get("channel_id") == forced_video.channel_id
             or c["id"] == forced_video.channel_id),
            None,
        )
        if ch_match is not None:
            channels = [ch_match]
        else:
            channels = [{
                "id": forced_video.channel_id or "forced",
                "name": forced_video.channel_name or "(unknown)",
                "url": f"https://www.youtube.com/watch?v={forced_video.id}",
                "channel_id": forced_video.channel_id,
                "_forced": True,
            }]
        logger.info(
            "forced video: id=%s channel=%s title=%s",
            forced_video.id, channels[0]["id"], forced_video.title,
        )
    elif args.channels:
        wanted = set(args.channels.split(","))
        channels = [c for c in channels if c["id"] in wanted]
    else:
        channels = pick_channels(channels, n=args.n_channels)
    print(f"[channels] picked {len(channels)}: {[c['id'] for c in channels]}")

    store = None
    if not args.skip_store:
        store = ProcessedStore(
            PROCESSED_BASE_ID, table_name=PROCESSED_TABLE,
        )
        logger.info(
            "ProcessedStore ready: base=%s table=%s run_id=%s",
            PROCESSED_BASE_ID, PROCESSED_TABLE, run_id,
        )

    # state.json is deprecated — we still load it for backward-compat reads
    # but never write to it from this pipeline.
    state = youtube_state.StateStore(str(PODCAST_STATE_PATH))

    digests = []
    selected: list[tuple[dict, youtube_fetch.VideoMeta, youtube_translate.VideoDigest]] = []
    for i, ch in enumerate(channels):
        # Cooldown between channels so kome.ai (third-party transcript API) doesn't
        # throttle us. Per user 2026-08-09: enforce ≥45s between two videos.
        if i > 0:
            cooldown = 45
            print(f"\n  [cooldown] sleeping {cooldown}s before next channel...")
            time.sleep(cooldown)
        print(f"\n=== {ch['name']} ({ch['id']}) ===")
        # Walk through unprocessed candidates until one yields a usable
        # transcript (Task 9). Previously a single empty-transcript video
        # (e.g. paywalled member content) would abort the channel; now we
        # try the next candidate.
        v: youtube_fetch.VideoMeta | None = None
        candidates: list[youtube_fetch.VideoMeta] = []
        if forced_video is not None and i == 0:
            # --video-id still respects the ledger unless --force is given.
            if store is not None and store.is_processed("youtube",
                                                        forced_video.id):
                if args.force:
                    logger.warning(
                        "FORCE: re-processing already-processed video %s",
                        forced_video.id,
                    )
                else:
                    logger.info(
                        "skip already-processed: %s", forced_video.id,
                    )
                    print(f"  [skip] already-processed: {forced_video.id}")
                    v = None
            else:
                v = forced_video
                logger.info("using forced video: %s", v.id)
        elif store is not None:
            candidates = pick_video_for_channel(
                ch, store=store, state=state, look_back=30,
            )
            if candidates:
                v = candidates[0]
        else:
            legacy_v = _legacy_pick(ch, state)
            if legacy_v is not None:
                candidates = [legacy_v]
                v = legacy_v
        if v is None:
            print(f"  [skip] no unprocessed video found in latest 30")
            continue
        # Try the chosen video first; if transcript is empty, walk to next.
        idx = 0
        tr = None
        while v is not None:
            print(f"  [fetch] {v.id} | {v.title[:60]} ({v.duration_sec}s)")
            tr = youtube_fetch.fetch_transcript(v)
            if tr.text:
                break  # got a usable transcript
            print(f"  [warn] empty transcript (lang={tr.language}); "
                  f"trying next candidate")
            idx += 1
            if idx >= len(candidates):
                print(f"  [skip] exhausted {len(candidates)} candidate(s) "
                      f"in {ch['id']}; none had transcripts")
                v = None
                break
            v = candidates[idx]
        if v is None or tr is None or not tr.text:
            continue
        print(f"  [transcript] {tr.n_chars} chars (premium={tr.is_premium})")
        if args.mode == "short":
            print(f"  [translate+short] Google → ONE lightweight LLM call")
            t0 = time.time()
            translated = _translate_only(tr.text)
            digest = _build_short_digest(v, translated, t0=t0)
            digest.n_chars = tr.n_chars  # back-fill for vault frontmatter
        else:
            print(f"  [translate] starting Map-Reduce...")
            digest = youtube_translate.digest_video(v, tr.text)
        digests.append(digest)
        selected.append((ch, v, digest))
        # Note: do NOT call state.mark_processed() anymore — state.json is
        # deprecated. ProcessedStore.mark_processed() (below) is the new home.

    if not digests:
        print("\n[nothing to process] no digests produced")
        return 0

    print(f"\n=== Step 5: write_vault ===")
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    content_kind = "short-summary" if args.mode == "short" else "longform"
    vault_root = str(VAULT_ROOT)
    vault_summary = youtube_obsidian.step_write_vault(
        digests, repo_root=vault_root,
        date_str=today_str,
        wipe=True,
        content_kind=content_kind,
    )
    print(f"  wrote {vault_summary.get('n_files', 0)} files, "
          f"errors={vault_summary.get('n_errors', 0)} "
          f"(kind={vault_summary.get('content_kind')})")

    print(f"\n=== Step 6: send_discord ===")
    discord_summary = youtube_discord.step_send_discord(
        digests, channel="podcast", dry_run=args.dry_run,
    )
    print(f"  embeds sent: {discord_summary['n_embeds']}, errors={len(discord_summary['errors'])}")
    if discord_summary.get("errors"):
        for e in discord_summary["errors"]:
            print(f"    - {e}")

    print(f"\n=== Step 7: push_to_github ===")
    push_summary = push_to_github(vault_root, dry_run=args.dry_run)
    print(f"  pushed={push_summary.get('pushed')}, "
          f"commit_sha={push_summary.get('commit_sha')}, "
          f"dry_run={push_summary.get('dry_run')}")

    # ------------------------------------------------------------------
    # Step 8: ledger writes
    # ------------------------------------------------------------------
    # Order matters:
    #   1. mark_processed() (no side effects yet) → Airtable ledger is the
    #      authoritative record of "this video was processed". Failure here
    #      is fatal: we lose dedup safety.
    #   2. update_side_effects() — best-effort. Failure here means the
    #      ledger still says "processed" but lacks discord_message_id /
    #      github_commit_sha. That's acceptable; side effects are debug info.
    print(f"\n=== Step 8: mark_processed + update_side_effects ===")
    if store is not None and not args.dry_run:
        per_video_msg_ids = {
            pv["video_id"]: pv.get("message_ids", [])
            for pv in discord_summary.get("per_video", [])
        }
        commit_sha = push_summary.get("commit_sha")

        for ch, video, digest in selected:
            output_path = _video_output_md_path(
                vault_root, today_str, digest,
                content_kind=content_kind,
            )
            tags = ["long-form"] if args.mode == "long" else ["short"]
            if digest.duration_sec and digest.duration_sec < 60 * 5:
                tags.append("short")
            metadata = _build_metadata(digest, video, ch)
            try:
                record_id = store.mark_processed(
                    source_type="youtube",
                    source_id=video.id,
                    title=video.title,
                    channels=[f"youtube.{ch['id']}"],
                    pipeline_run_id=run_id,
                    output_path=output_path,
                    metadata=metadata,
                    tags=tags,
                    article_type=content_kind,
                )
                logger.info(
                    "marked processed: %s | %s -> %s (article_type=%s)",
                    ch["id"], video.id, record_id, content_kind,
                )
            except Exception as e:  # noqa: BLE001
                logger.error(
                    "mark_processed failed for %s | %s: %s",
                    ch["id"], video.id, e,
                )
                continue

            # update_side_effects — best effort
            msg_ids = per_video_msg_ids.get(video.id, [])
            if msg_ids or commit_sha:
                try:
                    store.update_side_effects(
                        source_hash=make_hash("youtube", video.id),
                        discord_message_id=(",".join(msg_ids) or None),
                        github_commit_sha=commit_sha,
                    )
                except Exception as e:  # noqa: BLE001
                    logger.warning(
                        "update_side_effects failed for %s: %s",
                        video.id, e,
                    )
    elif args.dry_run:
        print("  [dry-run] skipping mark_processed + update_side_effects")

    print(f"\n[done] total {time.time()-t0:.1f}s, {len(digests)} videos")
    return 0


def _legacy_pick(channel: dict, state: youtube_state.StateStore,
                 look_back: int = 10) -> youtube_fetch.VideoMeta | None:
    """Fallback when ProcessedStore is disabled (--skip-store)."""
    metas = youtube_fetch.list_channel_videos(
        channel_id=channel["id"],
        channel_name=channel["name"],
        channel_url=channel["url"],
        youtube_channel_id=channel.get("channel_id"),
        limit=look_back,
    )
    for m in metas:
        if not _state_json_has(state, channel["id"], m.id):
            return m
    return None


def _force_fetch_video_meta(video_id: str) -> youtube_fetch.VideoMeta | None:
    """Look up a single video's metadata via yt-dlp --dump-single-json.

    Used for --video-id override (testing / re-processing). Returns a VideoMeta
    stub if the id is a known YouTube 11-char id and yt-dlp can resolve it.
    """
    import re as _re
    if not _re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id):
        logger.error("invalid video id: %s", video_id)
        return None
    url = f"https://www.youtube.com/watch?v={video_id}"
    proc = subprocess.run(
        ["yt-dlp", "--dump-single-json", "--skip-download", url],
        capture_output=True, text=True, timeout=30,
    )
    if proc.returncode != 0:
        logger.error("yt-dlp --dump-single-json failed: %s", proc.stderr[:300])
        return None
    try:
        d = json.loads(proc.stdout)
    except json.JSONDecodeError as e:
        logger.error("yt-dlp output not JSON: %s", e)
        return None
    epoch = (
        d.get("release_timestamp")
        or d.get("timestamp")
        or d.get("upload_date")
    )
    if isinstance(epoch, str):
        # upload_date is YYYYMMDD
        try:
            from datetime import datetime as _dt
            epoch = int(_dt.strptime(epoch, "%Y%m%d").replace(
                tzinfo=timezone.utc).timestamp())
        except ValueError:
            epoch = None
    elif isinstance(epoch, (int, float)):
        epoch = int(epoch)
    return youtube_fetch.VideoMeta(
        id=video_id,
        title=d.get("title", ""),
        duration_sec=int(d.get("duration") or 0),
        epoch=epoch,
        url=d.get("webpage_url") or url,
        channel_id=d.get("channel_id") or d.get("uploader_id") or "",
        channel_name=d.get("channel") or d.get("uploader") or "",
    )


if __name__ == "__main__":
    sys.exit(main())
