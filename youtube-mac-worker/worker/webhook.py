from __future__ import annotations

from typing import Any

import requests


def post_transcript(
    url: str,
    payload: dict[str, Any],
    *,
    token: str = "",
    timeout_sec: int = 60,
) -> None:
    if not url:
        return
    headers = {"Content-Type": "application/json", "User-Agent": "youtube-mac-worker/1.0"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    resp = requests.post(url, json=payload, headers=headers, timeout=timeout_sec)
    if resp.status_code >= 400:
        raise RuntimeError(f"webhook HTTP {resp.status_code}: {resp.text[:400]}")
