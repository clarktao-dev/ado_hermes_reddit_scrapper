from __future__ import annotations

import random
import time
from typing import Any

from .config import ensure_dirs, load_config
from .fetch import FetchError, fetch_transcript
from .queueing import claim_next, finish_fail, finish_ok
from .state import (
    bump_daily_count,
    clear_cooldown,
    get_cooldown_until,
    get_daily_count,
    set_cooldown,
)
from .webhook import post_transcript


def _human_delay(cfg: dict[str, Any]) -> None:
    lo = float(cfg["fetch"]["delay_min_sec"])
    hi = float(cfg["fetch"]["delay_max_sec"])
    if hi < lo:
        hi = lo
    time.sleep(random.uniform(lo, hi))


def process_one(cfg: dict[str, Any] | None = None) -> bool:
    """Process at most one inbox job. Returns True if a job was attempted."""
    cfg = cfg or load_config()
    ensure_dirs()

    until = get_cooldown_until()
    now = time.time()
    if until > now:
        print(f"[cooldown] waiting until {time.ctime(until)}")
        return False

    daily_limit = int(cfg["fetch"]["daily_limit"] or 0)
    if daily_limit and get_daily_count() >= daily_limit:
        print(f"[limit] daily_limit={daily_limit} reached; stop for today")
        return False

    claimed = claim_next()
    if not claimed:
        return False
    path, job = claimed
    video_id = job["video_id"]
    print(f"[job] fetching {video_id}")

    retries = int(cfg["fetch"]["max_retries"])
    backoff = float(cfg["fetch"]["backoff_base_sec"])
    backoff_max = float(cfg["fetch"]["backoff_max_sec"])

    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            result = fetch_transcript(
                video_id,
                list(cfg["fetch"]["languages"]),
                cookies_from_browser=str(cfg["yt_dlp"].get("cookies_from_browser") or ""),
                extra_args=list(cfg["yt_dlp"].get("extra_args") or []),
            )
            payload = {
                "video_id": result.video_id,
                "url": job["url"],
                "title": result.title,
                "language": result.language,
                "is_generated": result.is_generated,
                "source": result.source,
                "text": result.text,
                "segments": result.segments,
                "meta": job.get("meta") or {},
                "fetched_at": time.time(),
            }
            post_transcript(
                str(cfg["webhook"].get("url") or ""),
                payload,
                token=str(cfg["webhook"].get("token") or ""),
                timeout_sec=int(cfg["webhook"].get("timeout_sec") or 60),
            )
            finish_ok(path, payload)
            bump_daily_count()
            clear_cooldown()
            print(
                f"[ok] {video_id} via {result.source} lang={result.language} "
                f"chars={len(result.text)}"
            )
            _human_delay(cfg)
            return True
        except FetchError as e:
            last_err = e
            print(f"[retry {attempt}/{retries}] {e}")
            if e.blocked:
                sleep_for = min(backoff * (2 ** (attempt - 1)), backoff_max)
                # jitter
                sleep_for *= random.uniform(0.8, 1.3)
                set_cooldown(sleep_for)
                print(f"[blocked] cooldown {sleep_for:.0f}s")
                # put job back? currently leave in processing until fail after retries
            else:
                time.sleep(min(backoff, 60))
        except Exception as e:  # noqa: BLE001
            last_err = e
            print(f"[retry {attempt}/{retries}] unexpected: {e}")
            time.sleep(min(backoff, 60))

    finish_fail(path, str(last_err or "unknown"), blocked=isinstance(last_err, FetchError) and last_err.blocked)
    print(f"[fail] {video_id}: {last_err}")
    _human_delay(cfg)
    return True


def run_forever(cfg: dict[str, Any] | None = None) -> None:
    cfg = cfg or load_config()
    ensure_dirs()
    interval = float(cfg["worker"]["poll_interval_sec"])
    print(f"[worker] polling inbox every {interval}s")
    while True:
        tried = process_one(cfg)
        if not tried:
            time.sleep(interval)
