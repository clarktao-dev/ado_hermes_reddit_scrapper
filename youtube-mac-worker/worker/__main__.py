from __future__ import annotations

import argparse
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from .config import ensure_dirs, load_config
from .fetch import fetch_transcript, result_to_json
from .queueing import enqueue, list_inbox
from .run import process_one, run_forever
from .urls import extract_video_id


def cmd_enqueue(args: argparse.Namespace) -> int:
    ensure_dirs()
    meta = {}
    if args.meta:
        meta = json.loads(args.meta)
    path = enqueue(args.url, meta=meta)
    print(path)
    return 0


def cmd_fetch(args: argparse.Namespace) -> int:
    cfg = load_config()
    vid = extract_video_id(args.url)
    result = fetch_transcript(
        vid,
        list(cfg["fetch"]["languages"]),
        cookies_from_browser=str(cfg["yt_dlp"].get("cookies_from_browser") or ""),
        extra_args=list(cfg["yt_dlp"].get("extra_args") or []),
    )
    print(result_to_json(result))
    return 0


def cmd_once(_: argparse.Namespace) -> int:
    process_one()
    return 0


def cmd_run(_: argparse.Namespace) -> int:
    run_forever()
    return 0


def cmd_status(_: argparse.Namespace) -> int:
    ensure_dirs()
    from .config import QUEUE_DIR
    from .state import get_cooldown_until, get_daily_count
    import time

    print(
        json.dumps(
            {
                "inbox": len(list_inbox()),
                "processing": len(list((QUEUE_DIR / "processing").glob("*.json"))),
                "done": len(list((QUEUE_DIR / "done").glob("*.json"))),
                "failed": len(list((QUEUE_DIR / "failed").glob("*.json"))),
                "daily_count": get_daily_count(),
                "cooldown_until": get_cooldown_until(),
                "cooldown_active": get_cooldown_until() > time.time(),
            },
            indent=2,
        )
    )
    return 0


def _make_handler(cfg: dict[str, Any]):
    class Handler(BaseHTTPRequestHandler):
        def _json(self, code: int, obj: Any) -> None:
            body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt: str, *args: Any) -> None:
            sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

        def do_GET(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if path == "/health":
                self._json(200, {"ok": True})
                return
            if path == "/status":
                cmd_status(argparse.Namespace())
                # cmd_status prints; rebuild briefly
                from .config import QUEUE_DIR
                from .state import get_cooldown_until, get_daily_count
                import time

                self._json(
                    200,
                    {
                        "inbox": len(list_inbox()),
                        "processing": len(list((QUEUE_DIR / "processing").glob("*.json"))),
                        "done": len(list((QUEUE_DIR / "done").glob("*.json"))),
                        "failed": len(list((QUEUE_DIR / "failed").glob("*.json"))),
                        "daily_count": get_daily_count(),
                        "cooldown_until": get_cooldown_until(),
                        "cooldown_active": get_cooldown_until() > time.time(),
                    },
                )
                return
            self._json(404, {"error": "not found"})

        def do_POST(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length else b"{}"
            try:
                data = json.loads(raw.decode("utf-8") or "{}")
            except json.JSONDecodeError:
                # also allow url= form
                qs = parse_qs(raw.decode("utf-8", errors="replace"))
                data = {k: v[0] for k, v in qs.items()}

            if path == "/enqueue":
                url = data.get("url") or data.get("video_id") or data.get("id")
                if not url:
                    self._json(400, {"error": "missing url"})
                    return
                try:
                    job_path = enqueue(str(url), meta=data.get("meta") or {})
                except ValueError as e:
                    self._json(400, {"error": str(e)})
                    return
                self._json(200, {"ok": True, "job": job_path.name})
                return

            if path == "/fetch":
                # Synchronous fetch — use sparingly.
                url = data.get("url") or data.get("video_id")
                if not url:
                    self._json(400, {"error": "missing url"})
                    return
                try:
                    vid = extract_video_id(str(url))
                    result = fetch_transcript(
                        vid,
                        list(cfg["fetch"]["languages"]),
                        cookies_from_browser=str(
                            cfg["yt_dlp"].get("cookies_from_browser") or ""
                        ),
                        extra_args=list(cfg["yt_dlp"].get("extra_args") or []),
                    )
                except Exception as e:  # noqa: BLE001
                    self._json(502, {"error": str(e)})
                    return
                self._json(200, result.to_dict())
                return

            self._json(404, {"error": "not found"})

    return Handler


def cmd_serve(_: argparse.Namespace) -> int:
    cfg = load_config()
    ensure_dirs()
    host = str(cfg["worker"]["http_host"])
    port = int(cfg["worker"]["http_port"])
    server = ThreadingHTTPServer((host, port), _make_handler(cfg))
    print(f"[serve] http://{host}:{port}  POST /enqueue  GET /status")
    # Also process queue in-process lightly: user can run `run` separately.
    # For simplicity, serve only accepts jobs; pair with `python -m worker run`.
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[serve] stop")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m worker",
        description="Fetch existing YouTube captions on your Mac and push to an online agent.",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    e = sub.add_parser("enqueue", help="Add a YouTube URL/ID to the inbox")
    e.add_argument("url")
    e.add_argument("--meta", default="", help="Optional JSON metadata")
    e.set_defaults(func=cmd_enqueue)

    f = sub.add_parser("fetch", help="Fetch one transcript now (prints JSON)")
    f.add_argument("url")
    f.set_defaults(func=cmd_fetch)

    o = sub.add_parser("once", help="Process one inbox job")
    o.set_defaults(func=cmd_once)

    r = sub.add_parser("run", help="Daemon: poll inbox forever")
    r.set_defaults(func=cmd_run)

    s = sub.add_parser("serve", help="Local HTTP API to enqueue jobs (localhost)")
    s.set_defaults(func=cmd_serve)

    st = sub.add_parser("status", help="Show queue counts")
    st.set_defaults(func=cmd_status)

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
