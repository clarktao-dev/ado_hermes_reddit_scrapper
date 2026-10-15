from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any

from .config import QUEUE_DIR
from .urls import extract_video_id


def enqueue(url_or_id: str, *, meta: dict[str, Any] | None = None) -> Path:
    video_id = extract_video_id(url_or_id)
    job_id = f"{int(time.time())}-{video_id}-{uuid.uuid4().hex[:8]}"
    payload = {
        "id": job_id,
        "video_id": video_id,
        "url": f"https://www.youtube.com/watch?v={video_id}",
        "source_input": url_or_id.strip(),
        "meta": meta or {},
        "created_at": time.time(),
    }
    path = QUEUE_DIR / "inbox" / f"{job_id}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def list_inbox() -> list[Path]:
    return sorted((QUEUE_DIR / "inbox").glob("*.json"))


def claim_next() -> tuple[Path, dict[str, Any]] | None:
    inbox = list_inbox()
    if not inbox:
        return None
    src = inbox[0]
    dst = QUEUE_DIR / "processing" / src.name
    src.replace(dst)
    data = json.loads(dst.read_text(encoding="utf-8"))
    return dst, data


def finish_ok(processing_path: Path, result: dict[str, Any]) -> Path:
    out = QUEUE_DIR / "done" / processing_path.name
    payload = json.loads(processing_path.read_text(encoding="utf-8"))
    payload["result"] = result
    payload["finished_at"] = time.time()
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    processing_path.unlink(missing_ok=True)
    return out


def finish_fail(processing_path: Path, error: str, *, blocked: bool = False) -> Path:
    out = QUEUE_DIR / "failed" / processing_path.name
    payload = json.loads(processing_path.read_text(encoding="utf-8"))
    payload["error"] = error
    payload["blocked"] = blocked
    payload["failed_at"] = time.time()
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    processing_path.unlink(missing_ok=True)
    return out
