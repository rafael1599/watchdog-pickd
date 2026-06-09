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
#
# Prefer the modern launchctl API (bootout/bootstrap). The legacy unload+load pair
# is unreliable on recent macOS: a load issued right after an unload is often
# rejected as "already loaded", so RunAtLoad never fires again — and the capture
# app's launcher (start_pickd.py, which reopens the UI in Safari) is never re-run.
# We bootout, let it settle, then bootstrap; fall back to unload/load on old macOS.
restart_agent() {
  local label="$1"
  local plist="$HOME/Library/LaunchAgents/$label.plist"
  local domain="gui/$(id -u)"

  if [ ! -f "$plist" ]; then
    echo "• $label not installed (skipped)"
    return
  fi

  if launchctl bootout "$domain/$label" 2>/dev/null; then
    sleep 1  # let the old process fully exit before re-bootstrapping
  else
    launchctl unload "$plist" 2>/dev/null || true
    sleep 1
  fi

  if ! launchctl bootstrap "$domain" "$plist" 2>/dev/null; then
    launchctl load "$plist" 2>/dev/null || true
  fi
  echo "✓ Restarted $label"
}

restart_agent "com.antigravity.watchdog-pickd"
restart_agent "com.antigravity.pickd-app"

# 4. Safety net for the UI: once the capture app answers, open it in Safari. The
# agent's launcher (start_pickd.py) normally does this, but if it didn't (timing /
# launchctl quirks) this guarantees the window comes back. `open` focuses the
# existing tab for the same URL rather than spawning a duplicate, so running it in
# addition to the launcher is harmless.
APP_URL="http://127.0.0.1:5000"
if [ -f "$HOME/Library/LaunchAgents/com.antigravity.pickd-app.plist" ]; then
  echo "▶ Waiting for the capture app to come back up…"
  for _ in $(seq 1 30); do
    if curl -s -o /dev/null --max-time 1 "$APP_URL"; then
      open -a Safari "$APP_URL" 2>/dev/null || open "$APP_URL" 2>/dev/null || true
      echo "✓ Opened $APP_URL"
      break
    fi
    sleep 1
  done
fi

echo "✅ Update complete — now on $(git rev-parse --short HEAD) ($BRANCH)."
