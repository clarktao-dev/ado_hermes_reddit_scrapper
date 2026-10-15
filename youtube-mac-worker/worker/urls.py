from __future__ import annotations

import re
from urllib.parse import parse_qs, urlparse

VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")


def extract_video_id(url_or_id: str) -> str:
    s = url_or_id.strip()
    if VIDEO_ID_RE.match(s):
        return s

    parsed = urlparse(s)
    host = (parsed.netloc or "").lower()

    if "youtu.be" in host:
        vid = parsed.path.lstrip("/").split("/")[0]
        if VIDEO_ID_RE.match(vid):
            return vid

    if "youtube.com" in host or "youtube-nocookie.com" in host:
        qs = parse_qs(parsed.query)
        if "v" in qs and VIDEO_ID_RE.match(qs["v"][0]):
            return qs["v"][0]
        parts = [p for p in parsed.path.split("/") if p]
        # /shorts/ID, /embed/ID, /live/ID
        if len(parts) >= 2 and parts[0] in {"shorts", "embed", "live", "v"}:
            if VIDEO_ID_RE.match(parts[1]):
                return parts[1]

    raise ValueError(f"Cannot parse YouTube video id from: {url_or_id!r}")
