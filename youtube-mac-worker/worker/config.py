from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import toml

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "config.toml"
EXAMPLE_CONFIG = ROOT / "config.example.toml"
QUEUE_DIR = ROOT / "queue"
STATE_DIR = ROOT / ".state"


def load_config(path: Path | None = None) -> dict[str, Any]:
    cfg_path = path or Path(os.environ.get("YT_WORKER_CONFIG", DEFAULT_CONFIG))
    if not cfg_path.exists():
        if EXAMPLE_CONFIG.exists():
            raise SystemExit(
                f"Missing {cfg_path}. Run:\n  cp config.example.toml config.toml\nthen edit webhook.url"
            )
        raise SystemExit(f"Missing config: {cfg_path}")
    data = toml.load(cfg_path)
    # Defaults
    data.setdefault("fetch", {})
    data.setdefault("yt_dlp", {})
    data.setdefault("webhook", {})
    data.setdefault("worker", {})
    data["fetch"].setdefault("languages", ["zh-Hant", "zh-TW", "zh", "en"])
    data["fetch"].setdefault("delay_min_sec", 4)
    data["fetch"].setdefault("delay_max_sec", 10)
    data["fetch"].setdefault("daily_limit", 80)
    data["fetch"].setdefault("backoff_base_sec", 30)
    data["fetch"].setdefault("backoff_max_sec", 600)
    data["fetch"].setdefault("max_retries", 3)
    data["yt_dlp"].setdefault("cookies_from_browser", "safari")
    data["yt_dlp"].setdefault("extra_args", [])
    data["webhook"].setdefault("url", "")
    data["webhook"].setdefault("token", "")
    data["webhook"].setdefault("timeout_sec", 60)
    data["worker"].setdefault("poll_interval_sec", 20)
    data["worker"].setdefault("http_port", 8787)
    data["worker"].setdefault("http_host", "127.0.0.1")
    return data


def ensure_dirs() -> None:
    for name in ("inbox", "processing", "done", "failed"):
        (QUEUE_DIR / name).mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)
