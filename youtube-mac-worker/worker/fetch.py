from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


def _find_ytdlp() -> str | None:
    found = shutil.which("yt-dlp")
    if found:
        return found
    # Common when launched via venv python without PATH including .venv/bin
    sibling = Path(sys.executable).resolve().parent / "yt-dlp"
    if sibling.exists():
        return str(sibling)
    return None


@dataclass
class TranscriptResult:
    video_id: str
    title: str
    language: str
    is_generated: bool
    source: str  # youtube-transcript-api | yt-dlp
    text: str
    segments: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class FetchError(Exception):
    """Raised when all fetch strategies fail."""

    def __init__(self, message: str, *, blocked: bool = False):
        super().__init__(message)
        self.blocked = blocked


def _segments_to_text(segments: list[dict[str, Any]]) -> str:
    parts: list[str] = []
    for seg in segments:
        t = (seg.get("text") or "").replace("\n", " ").strip()
        if t:
            parts.append(t)
    # Collapse duplicated rolling auto-caption fragments a bit
    out: list[str] = []
    prev = ""
    for p in parts:
        if p == prev:
            continue
        if prev and p.startswith(prev):
            out[-1] = p
            prev = p
            continue
        out.append(p)
        prev = p
    return "\n".join(out).strip()


def _is_block_message(msg: str) -> bool:
    m = msg.lower()
    return any(
        k in m
        for k in (
            "blocking requests from your ip",
            "ip belonging to a cloud provider",
            "requestblocked",
            "ipblocked",
            "too many requests",
            "http error 429",
            "confirm you're not a bot",
            "confirm you’re not a bot",
            "sign in to confirm",
            "captcha",
        )
    )


def fetch_via_transcript_api(video_id: str, languages: list[str]) -> TranscriptResult:
    from youtube_transcript_api import YouTubeTranscriptApi

    api = YouTubeTranscriptApi()
    try:
        # Prefer requested languages (manual first, then generated — library handles order).
        fetched = api.fetch(video_id, languages=languages)
        segments = [
            {"start": float(i.start), "duration": float(i.duration), "text": i.text}
            for i in fetched
        ]
        return TranscriptResult(
            video_id=video_id,
            title="",
            language=getattr(fetched, "language_code", languages[0] if languages else "unknown"),
            is_generated=bool(getattr(fetched, "is_generated", False)),
            source="youtube-transcript-api",
            text=_segments_to_text(segments),
            segments=segments,
        )
    except Exception as e:  # noqa: BLE001
        # Fall through to cookie-backed yt-dlp on the Mac.
        raise FetchError(str(e), blocked=_is_block_message(str(e))) from e


_VTT_TS = re.compile(
    r"^\d{2}:\d{2}:\d{2}\.\d{3}\s+-->\s+\d{2}:\d{2}:\d{2}\.\d{3}"
)
_TAG = re.compile(r"<[^>]+>")


def _parse_vtt(path: Path) -> list[dict[str, Any]]:
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    segments: list[dict[str, Any]] = []
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if _VTT_TS.match(line):
            start_s = line.split("-->")[0].strip()
            text_lines: list[str] = []
            i += 1
            while i < len(lines) and lines[i].strip():
                text_lines.append(_TAG.sub("", lines[i]).strip())
                i += 1
            text = " ".join(t for t in text_lines if t)
            if text:
                segments.append({"start": _ts_to_sec(start_s), "duration": 0.0, "text": text})
            continue
        i += 1
    return segments


def _ts_to_sec(ts: str) -> float:
    hh, mm, rest = ts.split(":")
    ss, ms = rest.split(".")
    return int(hh) * 3600 + int(mm) * 60 + int(ss) + int(ms) / 1000.0


def fetch_via_ytdlp(
    video_id: str,
    languages: list[str],
    *,
    cookies_from_browser: str = "safari",
    extra_args: list[str] | None = None,
) -> TranscriptResult:
    ytdlp = _find_ytdlp()
    if not ytdlp:
        raise FetchError("yt-dlp not installed. On Mac: brew install yt-dlp")

    url = f"https://www.youtube.com/watch?v={video_id}"
    lang_pref = ",".join(languages)

    with tempfile.TemporaryDirectory(prefix="yt-subs-") as tmp:
        outtmpl = str(Path(tmp) / "%(id)s.%(ext)s")
        cmd = [
            ytdlp,
            "--skip-download",
            "--write-subs",
            "--write-auto-subs",
            "--sub-langs",
            lang_pref,
            "--sub-format",
            "vtt",
            "--print",
            "%(title)s",
            "-o",
            outtmpl,
        ]
        if cookies_from_browser:
            # Residential browser session — main anti-block lever.
            cmd.extend(["--cookies-from-browser", cookies_from_browser])
        if extra_args:
            cmd.extend(extra_args)
        cmd.append(url)

        try:
            proc = subprocess.run(
                cmd,
                check=False,
                capture_output=True,
                text=True,
                timeout=180,
            )
        except subprocess.TimeoutExpired as e:
            raise FetchError("yt-dlp timed out", blocked=False) from e

        if proc.returncode != 0:
            err = (proc.stderr or "").strip()
            raise FetchError(
                f"yt-dlp failed ({proc.returncode}): {err[:500]}",
                blocked=_is_block_message(err),
            )

        title = (proc.stdout or "").strip().splitlines()
        title_s = title[0] if title else ""

        vtts = sorted(Path(tmp).glob("*.vtt"))
        if not vtts:
            raise FetchError("yt-dlp ran but no subtitle file was written")

        # Prefer non-auto file if both exist
        chosen = vtts[0]
        for p in vtts:
            name = p.name.lower()
            if ".zh" in name or "hant" in name or "zh-tw" in name:
                chosen = p
                break
        is_generated = "auto" in chosen.name.lower() or ".asr." in chosen.name.lower()
        # language hint from filename: VIDEOID.zh-Hant.vtt
        lang = "unknown"
        parts = chosen.name.split(".")
        if len(parts) >= 3:
            lang = parts[-2]

        segments = _parse_vtt(chosen)
        text = _segments_to_text(segments)
        if not text:
            raise FetchError("parsed subtitle file was empty")

        return TranscriptResult(
            video_id=video_id,
            title=title_s,
            language=lang,
            is_generated=is_generated,
            source="yt-dlp",
            text=text,
            segments=segments,
        )


def fetch_transcript(
    video_id: str,
    languages: list[str],
    *,
    cookies_from_browser: str = "safari",
    extra_args: list[str] | None = None,
) -> TranscriptResult:
    """Try lightweight API first, then cookie-backed yt-dlp."""
    errors: list[str] = []

    try:
        result = fetch_via_transcript_api(video_id, languages)
        if result.text:
            return result
        errors.append("transcript-api returned empty text")
    except FetchError as e:
        errors.append(f"api: {e}")
        # Cloud/VPS IPs often die here; cookie path on home Mac usually works.
    except Exception as e:  # noqa: BLE001
        errors.append(f"api: {e}")

    try:
        return fetch_via_ytdlp(
            video_id,
            languages,
            cookies_from_browser=cookies_from_browser,
            extra_args=extra_args,
        )
    except FetchError as e:
        errors.append(f"yt-dlp: {e}")
        raise FetchError(" | ".join(errors), blocked=e.blocked or _is_block_message(" ".join(errors))) from e


def result_to_json(result: TranscriptResult) -> str:
    return json.dumps(result.to_dict(), ensure_ascii=False, indent=2)
