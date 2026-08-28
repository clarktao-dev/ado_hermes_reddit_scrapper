#!/usr/bin/env bash
# Install discord_picks as a systemd service (one-time on the VPS).
#
# Usage (as root):
#   sudo bash pipeline/scripts/install_discord_picks_service.sh
#
# Paths below match the default Hermes VPS layout; edit discord-picks.service
# first if your repo or venv lives elsewhere.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
UNIT_SRC="${REPO_ROOT}/pipeline/scripts/discord-picks.service"
UNIT_DST="/etc/systemd/system/discord-picks.service"

if [[ ! -f "${UNIT_SRC}" ]]; then
  echo "missing ${UNIT_SRC}" >&2
  exit 1
fi

# Stop Hermes/nohup instances if any
pkill -f 'pipeline/lib/discord_picks.py' 2>/dev/null || true
sleep 1

install -m 644 "${UNIT_SRC}" "${UNIT_DST}"
systemctl daemon-reload
systemctl enable discord-picks
systemctl restart discord-picks
systemctl status discord-picks --no-pager
echo
echo "Logs: tail -f /var/log/discord_picks.log"
