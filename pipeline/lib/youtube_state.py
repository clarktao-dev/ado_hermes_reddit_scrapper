"""Per-channel state for already-processed YouTube videos.

State file: <repo_root>/podcast-kb/state.json

Schema (version 1):
{
    "version": 1,
    "channels": {
        "<channel_id>": {
            "last_processed_epoch": int,
            "processed_ids": ["video_id_1", "video_id_2", ...]
        }
    }
}

We keep processed_ids as a set to detect repeats and walk backwards through a
channel's video list to find the next unprocessed one. The list is bounded to
the last `max_history` entries (default 200) so the file stays small.
"""
from __future__ import annotations
import json
import os
from pathlib import Path
from typing import Dict, List, Optional


MAX_HISTORY = 200


class StateStore:
    def __init__(self, state_path: str | Path):
        self.state_path = Path(state_path)
        self.data: dict = {"version": 1, "channels": {}}
        if self.state_path.exists():
            try:
                loaded = json.loads(self.state_path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict) and loaded.get("version") == 1:
                    self.data = loaded
            except (json.JSONDecodeError, OSError):
                # Corrupted state — start fresh
                pass

    def is_processed(self, channel_id: str, video_id: str) -> bool:
        ch = self.data["channels"].get(channel_id, {})
        return video_id in ch.get("processed_ids", [])

    def mark_processed(self, channel_id: str, video_id: str, epoch: Optional[int]) -> None:
        ch = self.data["channels"].setdefault(channel_id, {
            "last_processed_epoch": None,
            "processed_ids": [],
        })
        if video_id not in ch["processed_ids"]:
            ch["processed_ids"].append(video_id)
        # Bound history
        if len(ch["processed_ids"]) > MAX_HISTORY:
            ch["processed_ids"] = ch["processed_ids"][-MAX_HISTORY:]
        if epoch is not None:
            ch["last_processed_epoch"] = max(
                ch.get("last_processed_epoch") or 0, epoch
            )

    def save(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(
            json.dumps(self.data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def channels_seen(self) -> List[str]:
        return list(self.data["channels"].keys())
