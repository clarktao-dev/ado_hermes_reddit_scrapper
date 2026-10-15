#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "==> Project: $ROOT"

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 not found. Install Xcode CLT or python.org build first."
  exit 1
fi

if ! command -v brew >/dev/null 2>&1; then
  echo "Homebrew not found. Install from https://brew.sh then re-run."
  exit 1
fi

echo "==> Installing yt-dlp (caption fallback + cookie support)"
brew list yt-dlp >/dev/null 2>&1 || brew install yt-dlp
brew upgrade yt-dlp >/dev/null 2>&1 || true

echo "==> Python venv"
# youtube-transcript-api does not support Python 3.15+ yet. Prefer 3.12/3.13.
PY=""
for cand in python3.13 python3.12 python3.11 python3; do
  if command -v "$cand" >/dev/null 2>&1; then
    ver="$("$cand" -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")')"
    major="${ver%%.*}"
    minor="${ver#*.}"
    if [[ "$major" -eq 3 && "$minor" -lt 15 ]]; then
      PY="$cand"
      break
    fi
  fi
done
if [[ -z "$PY" ]]; then
  echo "Need Python 3.11–3.13 (3.15 is too new for youtube-transcript-api)."
  echo "Install with:  brew install python@3.12"
  exit 1
fi
echo "    using: $PY ($("$PY" -c 'import sys; print(sys.version.split()[0])'))"
rm -rf .venv
"$PY" -m venv .venv
# shellcheck disable=SC1091
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt

if [[ ! -f config.toml ]]; then
  cp config.example.toml config.toml
  echo "==> Created config.toml — edit webhook.url"
fi

mkdir -p queue/inbox queue/processing queue/done queue/failed .state

USER_HOME="$HOME"
PLIST_SRC="$ROOT/launchd/com.yt.mac.worker.plist"
PLIST_DST="$HOME/Library/LaunchAgents/com.yt.mac.worker.plist"
mkdir -p "$HOME/Library/LaunchAgents"

sed "s|/Users/REPLACE_ME/youtube-mac-worker|$ROOT|g" "$PLIST_SRC" > "$PLIST_DST"

echo
echo "Install done."
echo
echo "Next:"
echo "  1) Edit $ROOT/config.toml  (set webhook.url)"
echo "  2) Open Safari/Chrome and log into YouTube once"
echo "  3) Test:  $ROOT/.venv/bin/python -m worker fetch 'YOUTUBE_URL'"
echo "  4) Enqueue: $ROOT/.venv/bin/python -m worker enqueue 'YOUTUBE_URL'"
echo "  5) Start daemon:"
echo "       launchctl unload '$PLIST_DST' 2>/dev/null || true"
echo "       launchctl load '$PLIST_DST'"
echo
echo "Keep Mac awake (System Settings → Battery → Prevent automatic sleeping when on power)."
echo "User home: $USER_HOME"
