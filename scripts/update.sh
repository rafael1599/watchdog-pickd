#!/usr/bin/env bash
#
# update.sh — Update Watchdog PickD to the latest code with a single command.
#
# Pulls the newest commits, refreshes the virtualenv dependencies, and restarts
# both LaunchAgents (the PDF watcher and the capture app) so the running daemons
# pick up the new code.
#
#   ./scripts/update.sh            # update the currently checked-out branch
#   ./scripts/update.sh main       # update a specific branch
#
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

BRANCH="${1:-$(git rev-parse --abbrev-ref HEAD)}"
echo "▶ Updating Watchdog PickD on branch '$BRANCH'…"

# 1. Pull the latest code (fast-forward only; aborts if local work would conflict).
if ! git pull --ff-only origin "$BRANCH"; then
  echo "✗ git pull failed (local changes or diverged history). Resolve, then retry." >&2
  exit 1
fi

# 2. Refresh dependencies inside the venv (creating it if missing).
if [ ! -d venv ]; then
  echo "▶ Creating virtualenv…"
  python3 -m venv venv
fi
./venv/bin/python3 -m pip install --quiet --upgrade pip
./venv/bin/python3 -m pip install --quiet -r requirements.txt
echo "✓ Dependencies up to date."

# 3. Restart the LaunchAgents so the daemons reload the new code.
restart_agent() {
  local label="$1"
  local plist="$HOME/Library/LaunchAgents/$label.plist"
  if [ -f "$plist" ]; then
    launchctl unload "$plist" 2>/dev/null || true
    launchctl load "$plist" 2>/dev/null || true
    echo "✓ Restarted $label"
  else
    echo "• $label not installed (skipped)"
  fi
}

restart_agent "com.antigravity.watchdog-pickd"
restart_agent "com.antigravity.pickd-app"

echo "✅ Update complete — now on $(git rev-parse --short HEAD) ($BRANCH)."
