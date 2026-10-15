from __future__ import annotations

import json
import time
from datetime import date
from pathlib import Path

from .config import STATE_DIR


def _daily_path() -> Path:
    return STATE_DIR / f"daily-{date.today().isoformat()}.json"


def get_daily_count() -> int:
    path = _daily_path()
    if not path.exists():
        return 0
    try:
        return int(json.loads(path.read_text()).get("count", 0))
    except Exception:  # noqa: BLE001
        return 0


def bump_daily_count() -> int:
    path = _daily_path()
    count = get_daily_count() + 1
    path.write_text(json.dumps({"count": count, "updated_at": time.time()}))
    return count


def get_cooldown_until() -> float:
    path = STATE_DIR / "cooldown.json"
    if not path.exists():
        return 0.0
    try:
        return float(json.loads(path.read_text()).get("until", 0))
    except Exception:  # noqa: BLE001
        return 0.0


def set_cooldown(seconds: float) -> None:
    until = time.time() + max(0.0, seconds)
    (STATE_DIR / "cooldown.json").write_text(
        json.dumps({"until": until, "set_at": time.time()})
    )


def clear_cooldown() -> None:
    path = STATE_DIR / "cooldown.json"
    if path.exists():
        path.unlink()
