"""Independent validators — each format has a validator func.

A validator raises ValueError on failure. The orchestrator catches, logs the
validation error, and writes a `.validation.json` sidecar next to the output
so downstream tools can inspect. Lenient: most validators produce warnings,
only blockers raise.

Also exposes split_threads_post — a post-processor that enforces the
≤280-chars-per-post rule by splitting at the last sentence-ending punctuation
and re-numbering the (1/N) markers. Runs in executor before write_output.

Why YAML-driven `validator` keys map to these functions:
- Adding a new format = add YAML entry AND register a validator here.
- Validators are reusable across formats (e.g. `has_no_simplified` runs on all).
- Tests can be run independently of the LLM cost.
"""
from __future__ import annotations
import re
from typing import Callable, List, Tuple


# ─────────────────────────────────────────────────────────────
# Post-processors (run before validation, fix LLM output quirks)
# ─────────────────────────────────────────────────────────────

def strip_all_markdown(text: str) -> str:
    """Strip ALL markdown formatting — Threads / Facebook don't render it.

    Removes:
    - ## / ### headers (any line starting with #)
    - **bold** / *italic* markers (but keeps the inner text)
    - `_underline_` markers
    - `inline code` backticks
    - ![alt](url) image / [text](url) link syntax — keeps alt/text
    - > blockquote markers
    - Bullet markers at line start (-, *, +, 1.)
    Body content (the actual words) is preserved.
    """
    import re
    lines = []
    for line in text.splitlines():
        stripped = line.lstrip()
        # Skip pure-header lines
        if stripped.startswith("#"):
            continue
        # Strip blockquote markers
        if stripped.startswith(">"):
            line = line.replace(">", "", 1).lstrip()
        # Strip leading bullet markers (-, *, +, 1. , 2. , etc.)
        import re as _re
        line = _re.sub(r"^\s*([-*+]|\d+\.)\s+", "", line)
        # Strip leading list indentation
        line = line.lstrip()
        lines.append(line)

    text = "\n".join(lines)

    # Remove inline markdown formatting (keep inner text)
    text = re.sub(r"\*\*([^*]+)\*\*", r"\1", text)   # **bold**
    text = re.sub(r"\*([^*]+)\*", r"\1", text)         # *italic*
    text = re.sub(r"__([^_]+)__", r"\1", text)           # __bold__
    text = re.sub(r"_([^_]+)_", r"\1", text)             # _italic_
    text = re.sub(r"~~([^~]+)~~", r"\1", text)           # ~~strike~~
    text = re.sub(r"`([^`]+)`", r"\1", text)             # `code`

    # Convert [text](url) → text
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
    # Convert ![alt](url) → alt
    text = re.sub(r"!\[([^\]]*)\]\([^)]+\)", r"\1", text)

    # Collapse blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)

    # Remove all emoji characters (Unicode ranges for emoji + pictographs + symbols).
    # Per user request (2026-08-08): emoji should not appear in any output format.
    emoji_pattern = re.compile(
        "["
        "\U0001F300-\U0001F5FF"   # symbols & pictographs
        "\U0001F600-\U0001F64F"   # emoticons
        "\U0001F680-\U0001F6FF"   # transport & map symbols
        "\U0001F700-\U0001F77F"   # alchemical symbols
        "\U0001F780-\U0001F7FF"   # Geometric Shapes Extended
        "\U0001F800-\U0001F8FF"   # Supplemental Arrows-C
        "\U0001F900-\U0001F9FF"   # Supplemental Symbols and Pictographs
        "\U0001FA00-\U0001FA6F"   # Chess Symbols
        "\U0001FA70-\U0001FAFF"   # Symbols and Pictographs Extended-A
        "\U00002600-\U000026FF"   # Misc Symbols (☀-⛿)
        "\U00002700-\U000027BF"   # Dingbats (✀-➿)
        "]+",
        flags=re.UNICODE,
    )
    text = emoji_pattern.sub("", text)

    return text.strip()


def strip_markdown_headers(text: str) -> str:
    """Strip leading ## / ### markdown headers — Facebook/Threads don't render them.

    Removes any line whose first non-whitespace char is '#'.
    Body content is preserved.
    """
    return "\n".join(
        line for line in text.splitlines()
        if not line.lstrip().startswith("#")
    ).strip()


def split_threads_post(text: str, max_chars: int = 280, min_chars: int = 50) -> str:
    """Split any post >max_chars at the last sentence-ending punctuation.

    Iteratively merges too-short tails (<min_chars) into the next post.
    Re-numbers markers (1/N) so the post is always publishable as-is.
    Does NOT touch posts that already fit within max_chars.
    """
    SENT_END = ("。", "！", "!", "？", "?", "；", ";", ".")
    marker_re = re.compile(r"\(\d+/\d+\)\s*$")

    # 1. Split by === first
    posts = [p.strip() for p in text.split("===") if p.strip()]
    posts = [marker_re.sub("", p).rstrip() for p in posts]
    # 1a. Strip ALL markdown formatting (Threads doesn't render it)
    posts = [strip_all_markdown(p) for p in posts]

    # 2. Extract intro_note (first line of first post)
    intro_note = ""
    if posts and posts[0].startswith("編號說明"):
        first = posts[0]
        lines = first.split("\n")
        intro_lines = []
        for ln in lines:
            if ln.strip() == "":
                break
            intro_lines.append(ln)
        intro_note = "\n".join(intro_lines).strip()
        remainder = "\n".join(lines[len(intro_lines):]).strip()
        if remainder:
            posts = [remainder] + posts[1:]
        else:
            posts = posts[1:]

    # 3. Iteratively split each post while too long
    fixed = []
    for p in posts:
        while len(p) > max_chars:
            cut = -1
            best_sep_len = 0
            for sep in SENT_END:
                idx = p.rfind(sep, 0, max_chars)
                if idx > cut:
                    cut = idx
                    best_sep_len = len(sep)
            if cut <= 0:
                for sep in ("，", ","):
                    idx = p.rfind(sep, 0, max_chars)
                    if idx > cut:
                        cut = idx
                        best_sep_len = len(sep)
            if cut <= 0:
                cut = max_chars - 1
                best_sep_len = 0
            fixed.append(p[: cut + best_sep_len].strip())
            p = p[cut + best_sep_len :].strip()
        if p:
            fixed.append(p)

    # 4. Merge too-short tails
    merged = []
    i = 0
    while i < len(fixed):
        cur = fixed[i]
        while len(cur) < min_chars and i + 1 < len(fixed):
            i += 1
            cur = cur + "\n\n" + fixed[i]
        merged.append(cur)
        i += 1

    # 5. Re-number markers + prepend bullet to first non-empty line (visual header cue)
    total = len(merged)
    numbered = []
    for i, p in enumerate(merged, 1):
        # Find first non-empty line
        lines = p.splitlines()
        first_idx = 0
        while first_idx < len(lines) and lines[first_idx].strip() == "":
            first_idx += 1
        # Prepend block marker to that line (only)
        if first_idx < len(lines):
            full_first = lines[first_idx].strip()
            # Skip pure N/M marker lines like "1/5" — they are not titles
            import re as _re
            if _re.fullmatch(r"\d+\s*/\s*\d+", full_first):
                # Remove this line and recurse
                lines = lines[:first_idx] + lines[first_idx + 1:]
                # Re-find the first non-empty line
                first_idx = 0
                while first_idx < len(lines) and lines[first_idx].strip() == "":
                    first_idx += 1
                if first_idx < len(lines):
                    full_first = lines[first_idx].strip()
                else:
                    rebuilt = ""
                    numbered.append(f"{rebuilt}\n({i}/{total})")
                    continue
            body_lines = lines[first_idx + 1:]
            # Strip leading blanks from body
            while body_lines and body_lines[0].strip() == "":
                body_lines.pop(0)
            body_text = "\n".join(body_lines).strip()
            # If the "first line" is actually a long paragraph (no real break),
            # extract a short title from its start.
            if len(full_first) > 30:
                # Priority 1: sentence-ending punctuation (。！？)
                cut = -1
                for sep in ("。", "！", "!", "？", "?"):
                    idx_sep = full_first.find(sep, 0, 50)
                    if 0 < idx_sep and (cut == -1 or idx_sep < cut):
                        cut = idx_sep + 1
                # If cut lands right before a closing quote (」, 』, ", '), extend cut to include it
                # so we don\'t get 「。\n」 (broken quote on next line)
                if cut > 0 and cut < len(full_first):
                    nxt = full_first[cut]
                    if nxt in ("」", "』", "", "'", "』"):
                        cut += 1
                # Priority 2: ) in first 50 chars (close paren like 「(Xxx)」)
                if cut == -1 or cut > 45:
                    idx_sep = full_first.find(")", 0, 50)
                    if 0 < idx_sep and idx_sep < 45:
                        cut = idx_sep + 1
                # Priority 3: comma/、 in first 45 chars
                if cut == -1 or cut > 45:
                    for sep in ("，", ",", "、", "；", ";", "：", ":"):
                        idx_sep = full_first.find(sep, 0, 45)
                        if 0 < idx_sep and (cut == -1 or idx_sep < cut):
                            cut = idx_sep + 1
                # Priority 4: hard cut at 40 chars
                if cut == -1 or cut > 40:
                    cut = 40
                title = full_first[:cut].rstrip()
                rest = full_first[cut:].strip()
                rebuilt = f"{title}\n\n{rest}" if rest else title
            else:
                # First line is short — it IS the title
                rebuilt = f"{full_first}\n\n{body_text}" if body_text else full_first
        else:
            rebuilt = ""
        numbered.append(f"{rebuilt}\n({i}/{total})")

    # 6. Rebuild intro_note with correct total
    if intro_note:
        others = "、".join(f"({i}/{total})" for i in range(2, total + 1))
        intro_note = f"編號說明：本文共 {total} 則，請發主文後於留言依序張貼 {others}。"

    parts = ([intro_note] if intro_note else []) + numbered
    return "\n\n===\n\n".join(parts)


# ─────────────────────────────────────────────────────────────
# Generic validators (apply to all formats)
# ─────────────────────────────────────────────────────────────

def has_no_simplified(text: str) -> Tuple[bool, str]:
    """All output must be Traditional Chinese — no simplified chars slipping in."""
    # Pick the most common simplified-only chars used in mixed CN output
    simplified_markers = ["国", "经", "会", "产", "动", "对", "现", "们", "来", "说", "这", "为", "开", "发"]
    found = [c for c in simplified_markers if c in text]
    if found:
        return False, f"found simplified markers: {found[:5]}"
    return True, ""

def has_german_terms(text: str, min_count: int = 2) -> Tuple[bool, str]:
    """At least N German terms in parentheses (matching the vault's voice)."""
    parens = re.findall(r"[A-Za-zÄÖÜäöüß][A-Za-zÄÖÜäöüß\s-]{2,30}（[^）]+）", text)
    if len(parens) < min_count:
        return False, f"only {len(parens)} German-in-parens terms found (need {min_count})"
    return True, ""

def is_nonempty(text: str) -> Tuple[bool, str]:
    if len(text.strip()) < 50:
        return False, f"too short ({len(text.strip())} chars)"
    return True, ""

def starts_with_markdown_title(text: str) -> Tuple[bool, str]:
    """First non-empty line should be a markdown title."""
    first = next((l for l in text.splitlines() if l.strip()), "")
    if not first.startswith("#"):
        return False, f"first line not a markdown title: {first[:50]}"
    return True, ""


# ─────────────────────────────────────────────────────────────
# Platform-specific validators
# ─────────────────────────────────────────────────────────────

def facebook_post(text: str) -> List[Tuple[str, bool, str]]:
    """Facebook post: 800-1500 chars, no markdown headers (FB doesn't render them)."""
    warnings = []
    warnings.append(("has_no_simplified", *has_no_simplified(text)))
    warnings.append(("has_german_terms", *has_german_terms(text)))
    warnings.append(("is_nonempty", *is_nonempty(text)))
    # FB doesn't render ## headers — flag if present
    if re.search(r"^##\s", text, re.MULTILINE):
        warnings.append(("no_markdown_headers", False, "Facebook doesn't render `##` headers — consider plain text"))
    if "今天來談" in text or "今天来谈" in text:
        warnings.append(("no_weak_hook", False, "starts with the weak '今天來談…' pattern"))
    return warnings

def threads_post(text: str) -> List[Tuple[str, bool, str]]:
    """Threads: each post ≤280 chars, ≥1 post, each ends with (N/M) marker.

    The "編號說明" header line is treated as intro_note (not a post) — same
    convention as split_threads_post, so validator and post-processor agree.
    """
    warnings = []
    warnings.append(("has_no_simplified", *has_no_simplified(text)))
    warnings.append(("is_nonempty", *is_nonempty(text)))
    # Split by === (Threads separator)
    posts = [p.strip() for p in text.split("===") if p.strip()]
    if not posts:
        warnings.append(("has_posts", False, "no === separators found"))
        return warnings
    # Skip the "編號說明" header — it's intro_note, not a post
    body_posts = [p for p in posts if not p.startswith("編號說明")]
    if not body_posts:
        warnings.append(("has_posts", False, "only 編號說明 header found, no real posts"))
        return warnings
    # Total post count cap — too many = bad reading experience
    if len(body_posts) > 8:
        warnings.append(("max_posts", False, f"FAIL: {len(body_posts)} posts (max 8) — too many, condense content"))
    # Strict char count check on real posts only
    for i, p in enumerate(body_posts, 1):
        if len(p) > 280:
            warnings.append((f"post_{i}_char_count", False, f"FAIL: {len(p)} chars (max 280) — must split into more posts"))
    # Verify (N/M) marker on last line of each real post
    marker_pattern = re.compile(r"\(\d+/\d+\)\s*$")
    for i, p in enumerate(body_posts, 1):
        last_line = p.rstrip().splitlines()[-1] if p.strip() else ""
        if not marker_pattern.search(last_line):
            warnings.append((f"post_{i}_marker", False, f"last line missing (N/M) marker: '{last_line[:30]}'"))
    # Verify N (total) is consistent across all real posts
    total_markers = []
    for p in body_posts:
        m = re.search(r"\((\d+)/(\d+)\)", p.rstrip().splitlines()[-1] if p.strip() else "")
        if m:
            total_markers.append((int(m.group(1)), int(m.group(2))))
    if total_markers:
        ns = [m[0] for m in total_markers]
        ms = [m[1] for m in total_markers]
        if len(set(ms)) > 1:
            warnings.append(("marker_total_consistent", False, f"inconsistent totals: {ms}"))
        if ms and max(ns) != ms[0]:
            warnings.append(("marker_max_position", False, f"max position {max(ns)} != total {ms[0]}"))
    return warnings

def twitter_thread(text: str) -> List[Tuple[str, bool, str]]:
    """Twitter thread: 5-8 tweets, each ≤280 chars, separated by `---`."""
    warnings = []
    warnings.append(("has_no_simplified", *has_no_simplified(text)))
    warnings.append(("is_nonempty", *is_nonempty(text)))
    tweets = [t.strip() for t in text.split("---") if t.strip()]
    if len(tweets) < 5:
        warnings.append(("min_tweets", False, f"only {len(tweets)} tweets (need 5-8)"))
    if len(tweets) > 8:
        warnings.append(("max_tweets", False, f"{len(tweets)} tweets (max 8)"))
    for i, t in enumerate(tweets, 1):
        if len(t) > 280:
            warnings.append((f"tweet_{i}_char_count", False, f"{len(t)} chars (max 280)"))
    if tweets and not "🧵" in tweets[0]:
        warnings.append(("thread_marker", False, "first tweet missing 🧵 marker"))
    return warnings

def linkedin_post(text: str) -> List[Tuple[str, bool, str]]:
    """LinkedIn: 1500-2000 chars, no emojis (per platform norms)."""
    warnings = []
    warnings.append(("has_no_simplified", *has_no_simplified(text)))
    warnings.append(("has_german_terms", *has_german_terms(text)))
    warnings.append(("is_nonempty", *is_nonempty(text)))
    if len(text) < 1500:
        warnings.append(("min_chars", False, f"only {len(text)} chars (target 1500+)"))
    if len(text) > 2000:
        warnings.append(("max_chars", False, f"{len(text)} chars (target ~2000)"))
    # Check for emojis (basic Unicode range)
    if re.search(r"[\U0001F300-\U0001FAFF\U0001F000-\U0001F02F]", text):
        warnings.append(("no_emojis", False, "LinkedIn professional tone — zero emojis required"))
    return warnings

def newsletter(text: str) -> List[Tuple[str, bool, str]]:
    """Newsletter: must have 主旨 + 引言 + 主體 + CTA + 關鍵字 box."""
    warnings = []
    warnings.append(("has_no_simplified", *has_no_simplified(text)))
    warnings.append(("has_german_terms", *has_german_terms(text)))
    warnings.append(("is_nonempty", *is_nonempty(text)))
    # Each section is advisory (warn but don't fail)
    required = [("主旨", "subject"), ("引言", "intro"), ("主體", "body"), ("CTA", "cta"), ("關鍵字", "keyword_box")]
    for label, key in required:
        if re.search(rf"^\s*[#\*\s]*{label}", text, re.MULTILINE):
            warnings.append((f"section_{key}", True, f"present: {label}"))
        else:
            warnings.append((f"section_{key}", True, f"missing (advisory): {label}"))
    return warnings

def podcast_outline(text: str) -> List[Tuple[str, bool, str]]:
    """Podcast outline: Intro + 5-8 段落 + Outro."""
    warnings = []
    warnings.append(("has_no_simplified", *has_no_simplified(text)))
    warnings.append(("has_german_terms", *has_german_terms(text)))
    warnings.append(("is_nonempty", *is_nonempty(text)))
    for label, key in [("Intro", "intro"), ("Outro", "outro")]:
        if re.search(rf"^\s*[#\*\s]*{label}", text, re.MULTILINE):
            warnings.append((f"section_{key}", True, f"present: {label}"))
        else:
            warnings.append((f"section_{key}", True, f"missing (advisory): {label}"))
    # Count 段落 markers — also advisory
    sections = re.findall(r"^\s*[#\*\s]*段落[一二三四五六七八九十0-9]+", text, re.MULTILINE)
    warnings.append(("section_count", True, f"{len(sections)} 段落 markers (advisory)"))
    return warnings


# ─────────────────────────────────────────────────────────────
# Registry
# ─────────────────────────────────────────────────────────────

VALIDATORS: dict[str, Callable[[str], List[Tuple[str, bool, str]]]] = {
    "facebook_post": facebook_post,
    "threads_post": threads_post,
    "twitter_thread": twitter_thread,
    "linkedin_post": linkedin_post,
    "newsletter": newsletter,
    "podcast_outline": podcast_outline,
}


def run_validators(validator_id: str, text: str) -> List[Tuple[str, bool, str]]:
    """Run a validator by ID. Returns list of (name, ok, msg)."""
    fn = VALIDATORS.get(validator_id)
    if fn is None:
        return [(f"validator:{validator_id}", False, "no such validator")]
    return fn(text)