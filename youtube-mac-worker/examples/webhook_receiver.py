#!/usr/bin/env python3
"""Minimal example endpoint for your VPS / online agent.

Run on the server (NOT for fetching YouTube):

    pip install flask
    python examples/webhook_receiver.py

Your Mac worker posts JSON like:
{
  "video_id": "...",
  "url": "...",
  "title": "...",
  "language": "zh-Hant",
  "text": "...full transcript...",
  "source": "yt-dlp",
  "meta": {}
}

Replace the analyze() stub with your real AI summarization call.
"""

from __future__ import annotations

import os
from flask import Flask, jsonify, request

app = Flask(__name__)
TOKEN = os.environ.get("WEBHOOK_TOKEN", "")


def analyze(text: str, title: str, url: str) -> str:
    # TODO: call your existing agent / LLM here
    preview = text[:400].replace("\n", " ")
    return f"[stub summary] {title or url}: {preview}..."


@app.post("/hooks/transcript")
def transcript_hook():
    if TOKEN:
        auth = request.headers.get("Authorization", "")
        if auth != f"Bearer {TOKEN}":
            return jsonify({"error": "unauthorized"}), 401

    data = request.get_json(force=True, silent=True) or {}
    text = (data.get("text") or "").strip()
    if not text:
        return jsonify({"error": "empty transcript"}), 400

    summary = analyze(text, data.get("title") or "", data.get("url") or "")
    # Persist / forward summary however you like
    print("=== SUMMARY ===")
    print(summary)
    print("===============")
    return jsonify({"ok": True, "summary": summary})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
