"""GitHub-as-mailbox transcript bridge.

VPS/Agent writes ``transcript-bridge/inbox/<video_id>.json``.
iPhone App fills ``transcript-bridge/outbox/<video_id>.json``.
VPS imports outbox into ``transcript_cache`` then archives to ``done/``.
"""
from __future__ import annotations

import json
import logging
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from pipeline.lib.paths import get_pipeline_root

logger = logging.getLogger(__name__)

BRIDGE_DIRNAME = "transcript-bridge"


def bridge_root() -> Path:
    return get_pipeline_root() / BRIDGE_DIRNAME


def inbox_dir() -> Path:
    return bridge_root() / "inbox"


def outbox_dir() -> Path:
    return bridge_root() / "outbox"


def done_dir() -> Path:
    return bridge_root() / "done"


def _ensure_dirs() -> None:
    for d in (inbox_dir(), outbox_dir(), done_dir()):
        d.mkdir(parents=True, exist_ok=True)


def _safe_video_id(video_id: str) -> str:
    vid = (video_id or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]{6,64}", vid):
        raise ValueError(f"invalid video_id: {video_id!r}")
    return vid


def enqueue_job(
    video_id: str,
    url: str = "",
    *,
    title: str = "",
    channel: str = "",
    languages: Optional[list[str]] = None,
    requested_by: str = "manual",
    meta: Optional[dict[str, Any]] = None,
) -> Path:
    """Write (or overwrite) an inbox job file. Does not git-push."""
    _ensure_dirs()
    vid = _safe_video_id(video_id)
    if not url:
        url = f"https://www.youtube.com/watch?v={vid}"
    job = {
        "video_id": vid,
        "url": url,
        "title": title or "",
        "channel": channel or "",
        "languages": languages or ["de", "en"],
        "requested_at": datetime.now(timezone.utc).isoformat(),
        "requested_by": requested_by,
        "meta": meta or {},
    }
    path = inbox_dir() / f"{vid}.json"
    path.write_text(
        json.dumps(job, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    logger.info("transcript-bridge enqueued: %s → %s", vid, path)
    return path


def list_inbox() -> list[dict[str, Any]]:
    _ensure_dirs()
    jobs = []
    for p in sorted(inbox_dir().glob("*.json")):
        try:
            jobs.append(json.loads(p.read_text(encoding="utf-8")))
        except Exception as e:  # noqa: BLE001
            logger.warning("skip bad inbox %s: %s", p.name, e)
    return jobs


def list_outbox_files() -> list[Path]:
    _ensure_dirs()
    return sorted(outbox_dir().glob("*.json"))


def import_outbox(*, move_to_done: bool = True) -> list[dict[str, Any]]:
    """Ingest each outbox JSON into transcript_cache. Returns summaries."""
    from pipeline.lib import mac_transcript

    summaries = []
    for path in list_outbox_files():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            # Prefer channel from job meta if present
            if isinstance(payload.get("meta"), dict):
                ch = payload["meta"].get("channel") or payload["meta"].get("channel_name")
                if ch and "meta" in payload:
                    payload.setdefault("meta", {})["channel"] = ch
            summary = mac_transcript.ingest_webhook_payload(payload)
            summary["outbox_file"] = path.name
            summaries.append(summary)
            if move_to_done:
                dest = done_dir() / path.name
                dest.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
                path.unlink(missing_ok=True)
                # Clear matching inbox if still present
                inbox = inbox_dir() / path.name
                if inbox.exists():
                    inbox.unlink()
        except Exception as e:  # noqa: BLE001
            logger.exception("import failed for %s", path.name)
            summaries.append({"ok": False, "outbox_file": path.name, "error": str(e)})
    return summaries


def git_push_bridge(paths: list[Path], message: str) -> dict[str, Any]:
    """Stage given paths under transcript-bridge/, commit, push. Best-effort."""
    root = get_pipeline_root()
    rels = []
    for p in paths:
        try:
            rels.append(str(p.resolve().relative_to(root.resolve())))
        except ValueError:
            rels.append(str(p))
    if not rels:
        return {"ok": False, "error": "no paths"}
    out: dict[str, Any] = {"ok": False, "files": rels, "commands": []}

    def run(cmd: list[str]) -> subprocess.CompletedProcess:
        proc = subprocess.run(
            cmd, cwd=str(root), capture_output=True, text=True, timeout=120,
        )
        out["commands"].append({
            "cmd": " ".join(cmd),
            "rc": proc.returncode,
            "stdout": (proc.stdout or "")[-300:],
            "stderr": (proc.stderr or "")[-300:],
        })
        return proc

    run(["git", "add", "--"] + rels)
    commit = run([
        "git", "-c", "user.email=ado@hermes.local", "-c", "user.name=Ado",
        "commit", "-m", message,
    ])
    if commit.returncode != 0 and "nothing to commit" not in (commit.stderr + commit.stdout):
        out["error"] = commit.stderr[-300:] or commit.stdout[-300:]
        return out
    push = run(["git", "push"])
    out["ok"] = push.returncode == 0
    if not out["ok"]:
        out["error"] = push.stderr[-300:]
    return out


def maybe_enqueue_github_on_cache_miss(
    video_id: str,
    video_url: str,
    *,
    channel: str = "",
    title: str = "",
) -> Optional[dict]:
    """When ``TRANSCRIPT_BRIDGE_ENQUEUE=1`` (default on in mac mode), write inbox job.

    Set ``TRANSCRIPT_BRIDGE_PUSH=1`` to also ``git commit && git push`` so the
    iPhone App can see the job on GitHub without waiting for a manual push.
    """
    flag = (os.environ.get("TRANSCRIPT_BRIDGE_ENQUEUE") or "1").strip().lower()
    if flag in ("0", "false", "no", "off"):
        return None
    try:
        path = enqueue_job(
            video_id,
            video_url,
            title=title,
            channel=channel,
            requested_by="youtube_daily",
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("github inbox enqueue failed for %s: %s", video_id, e)
        return {"ok": False, "error": str(e)}

    result: dict[str, Any] = {"ok": True, "path": str(path), "video_id": video_id}
    push_flag = (os.environ.get("TRANSCRIPT_BRIDGE_PUSH") or "0").strip().lower()
    if push_flag in ("1", "true", "yes", "on"):
        result["push"] = git_push_bridge(
            [path],
            f"transcript-bridge: enqueue {video_id}",
        )
    return result
