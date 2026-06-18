#!/usr/bin/env bash
#
# status.sh — Quick health snapshot of Watchdog PickD, for when you need to know
# "where are we / is it running". Safe to run anytime.
#
#   ./scripts/status.sh
#
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DOMAIN="gui/$(id -u)"
APP_PORT="${PICKD_PORT:-5757}"
APP_URL="http://127.0.0.1:${APP_PORT}"

echo "── PickD status ───────────────────────────────"

agent() {
  local label="$1"
  if launchctl print "$DOMAIN/$label" >/dev/null 2>&1; then
    echo "✓ agent  $label: loaded"
  elif [ -f "$HOME/Library/LaunchAgents/$label.plist" ]; then
    echo "✗ agent  $label: installed but NOT loaded"
  else
    echo "• agent  $label: not installed"
  fi
}
agent "com.antigravity.watchdog-pickd"
agent "com.antigravity.pickd-app"

# Capture app: is something serving the UI?
pids="$(lsof -ti tcp:"$APP_PORT" 2>/dev/null | tr '\n' ' ')"
if curl -s -o /dev/null --max-time 2 "$APP_URL"; then
  echo "✓ app    answering on $APP_URL (pid: ${pids:-?})"
else
  if [ -n "$pids" ]; then
    echo "✗ app    NOT answering, but port $APP_PORT is held (stale pid: $pids) — run ./scripts/update.sh to recover"
  else
    echo "✗ app    not running (nothing on port $APP_PORT)"
  fi
fi

# Auto-scan cache: how many orders scanned, and the next number to try.
if [ -f "$REPO/venv/bin/python3" ] && [ -f "$REPO/scanned_store.py" ]; then
  ( cd "$REPO" && ./venv/bin/python3 - <<'PY' 2>/dev/null
import scanned_store
print(f"• scan   cached {scanned_store.count()} orders; next to scan: #{scanned_store.next_scan_number()}")
PY
  ) || echo "• scan   (could not read scanned cache)"
fi

echo "── recent update log ──────────────────────────"
if [ -f "$REPO/logs/update.log" ]; then
  tail -n 6 "$REPO/logs/update.log"
else
  echo "(no update log yet)"
fi
echo "───────────────────────────────────────────────"
