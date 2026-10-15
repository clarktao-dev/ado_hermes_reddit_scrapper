#!/usr/bin/env python3
"""VPS webhook receiver for Mac youtube-mac-worker captions.

Listens on ``0.0.0.0:8791`` (override with ``--host`` / ``--port``) and accepts::

    POST /transcript   JSON body from Mac worker
    GET  /healthz      liveness

Auth (optional but recommended)::

    Authorization: Bearer $MAC_WEBHOOK_TOKEN
    or header X-Webhook-Token: $MAC_WEBHOOK_TOKEN

Run on the VPS (systemd / nohup / Hermes)::

    set -a && source /root/.hermes/.env && set +a
    PYTHONPATH=/root/projects/ado_hermes_reddit_scrapper \\
      /root/.hermes/hermes-agent/venv/bin/python \\
      pipeline/scripts/mac_transcript_receiver.py --port 8791

Mac worker ``config.toml`` webhook URL should point here (direct or via
reverse proxy). See ``docs/mac-transcript-bridge.md``.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

PIPELINE_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PIPELINE_ROOT))

from pipeline.lib import mac_transcript  # noqa: E402

logger = logging.getLogger("mac_transcript_receiver")
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)


def _check_auth(handler: BaseHTTPRequestHandler) -> bool:
    expected = mac_transcript.webhook_token()
    if not expected:
        return True  # open mode (local / trusted network)
    auth = handler.headers.get("Authorization") or ""
    if auth.lower().startswith("bearer ") and auth[7:].strip() == expected:
        return True
    if (handler.headers.get("X-Webhook-Token") or "").strip() == expected:
        return True
    return False


class Handler(BaseHTTPRequestHandler):
    server_version = "MacTranscriptReceiver/1.0"

    def log_message(self, fmt: str, *args) -> None:
        logger.info("%s - " + fmt, self.address_string(), *args)

    def _send(self, code: int, body: dict) -> None:
        raw = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path in ("/healthz", "/health", "/"):
            self._send(200, {
                "ok": True,
                "service": "mac_transcript_receiver",
                "transcript_source": mac_transcript.transcript_source_mode(),
            })
            return
        self._send(404, {"ok": False, "error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        # /hooks/transcript matches youtube-mac-worker config.example.toml
        if path not in (
            "/transcript", "/webhook", "/v1/transcript", "/hooks/transcript",
        ):
            self._send(404, {"ok": False, "error": "not found"})
            return
        if not _check_auth(self):
            self._send(401, {"ok": False, "error": "unauthorized"})
            return
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0 or length > 5_000_000:
            self._send(400, {"ok": False, "error": "invalid Content-Length"})
            return
        raw = self.rfile.read(length)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            self._send(400, {"ok": False, "error": f"invalid JSON: {e}"})
            return
        try:
            summary = mac_transcript.ingest_webhook_payload(payload)
        except ValueError as e:
            self._send(400, {"ok": False, "error": str(e)})
            return
        except Exception as e:  # noqa: BLE001
            logger.exception("ingest failed")
            self._send(500, {"ok": False, "error": str(e)})
            return
        self._send(200, summary)


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", default=os.environ.get("MAC_RECEIVER_HOST", "0.0.0.0"))
    ap.add_argument("--port", type=int,
                    default=int(os.environ.get("MAC_RECEIVER_PORT", "8791")))
    args = ap.parse_args(argv)
    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    logger.info(
        "listening on http://%s:%s  (mode=%s token=%s)",
        args.host, args.port,
        mac_transcript.transcript_source_mode(),
        "set" if mac_transcript.webhook_token() else "open",
    )
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("shutting down")
        httpd.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
