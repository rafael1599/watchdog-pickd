#!/usr/bin/env bash
#
# update.sh — Update Watchdog PickD to the latest code with a single command.
#
# Pulls the newest commits, refreshes the virtualenv dependencies, and restarts
# both LaunchAgents (the PDF watcher and the capture app) so the running daemons
# pick up the new code. Prints staged progress so you can see where it is.
#
#   ./scripts/update.sh            # update the currently checked-out branch
#   ./scripts/update.sh main       # update a specific branch
#
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

mkdir -p logs
LOG="$REPO/logs/update.log"

# say: timestamped progress line to both the terminal and logs/update.log, so the
# state is visible whether update runs in a terminal or detached (the ⟳ button).
say() {
  local line
  line="$(date '+%H:%M:%S')  $*"
  echo "$line"
  echo "$line" >>"$LOG"
}

APP_URL="http://127.0.0.1:5000"
APP_PORT=5000
DOMAIN="gui/$(id -u)"

BRANCH="${1:-$(git rev-parse --abbrev-ref HEAD)}"
say "▶ [1/6] Updating Watchdog PickD on branch '$BRANCH' (repo: $REPO)…"

# 1. Pull the latest code (fast-forward only; aborts if local work would conflict).
if ! git pull --ff-only origin "$BRANCH"; then
  say "✗ git pull failed (local changes or diverged history). Resolve, then retry."
  exit 1
fi

# 2. Refresh dependencies inside the venv — but ONLY when requirements.txt actually
# changed. A full reinstall every update took minutes (pip re-hits PyPI for every
# pinned range, and `--upgrade pip` hung with no output on a slow network). We stamp
# the file's hash after a successful install and skip while it matches.
# Force a reinstall any time with:  FORCE_DEPS=1 ./scripts/update.sh
say "▶ [2/6] Checking dependencies…"
REQ_STAMP="venv/.requirements.sha256"
req_hash="$(shasum -a 256 requirements.txt | awk '{print $1}')"
if [ ! -d venv ]; then
  say "  creating virtualenv…"
  python3 -m venv venv
  ./venv/bin/python3 -m pip install --quiet --upgrade pip  # only on first creation
fi
if [ "${FORCE_DEPS:-0}" != "1" ] && [ -f "$REQ_STAMP" ] && [ "$(cat "$REQ_STAMP" 2>/dev/null)" = "$req_hash" ]; then
  say "✓ Dependencies unchanged — skipped (run FORCE_DEPS=1 ./scripts/update.sh to reinstall)."
else
  say "  requirements.txt changed — installing (no --quiet, so you see progress)…"
  ./venv/bin/python3 -m pip install -r requirements.txt
  echo "$req_hash" >"$REQ_STAMP"
  say "✓ Dependencies up to date."
fi

# 3. Apply the DB schema the watcher depends on (e.g. picking_lists.source_order_date).
# Idempotent (ADD COLUMN IF NOT EXISTS) and non-fatal: skips cleanly when
# SUPABASE_DB_URL is unset, and a DB hiccup must never block the app update.
say "▶ [3/6] Applying DB schema migrations…"
if mig_out="$(./venv/bin/python3 migrations.py 2>&1)"; then
  say "  ${mig_out}"
else
  say "⚠ Migration step failed (non-fatal): ${mig_out}"
fi

# Kill any process still listening on the app port. A stale/hung app.py left on
# :5000 is exactly what makes the new window load forever and show blank — the new
# server can't bind, so Safari talks to a dead one. Clearing it lets the fresh app
# take the port.
free_port() {
  local pids
  pids="$(lsof -ti tcp:"$APP_PORT" 2>/dev/null || true)"
  if [ -n "$pids" ]; then
    say "  freeing port $APP_PORT (killing stale PIDs: $(echo "$pids" | tr '\n' ' '))"
    # shellcheck disable=SC2086
    kill -9 $pids 2>/dev/null || true
    sleep 1
  fi
}

# Restart a LaunchAgent. Prefer the modern launchctl API (bootout/bootstrap): the
# legacy unload+load pair is unreliable on recent macOS — a load right after an
# unload is often rejected as "already loaded", so RunAtLoad never fires again and
# the app's launcher (start_pickd.py, which reopens the UI) is never re-run. We
# bootout, free the port, then bootstrap; fall back to unload/load on old macOS.
restart_agent() {
  local label="$1"
  local free="${2:-}"
  local plist="$HOME/Library/LaunchAgents/$label.plist"

  if [ ! -f "$plist" ]; then
    say "• $label not installed (skipped)"
    return
  fi

  if launchctl bootout "$DOMAIN/$label" 2>/dev/null; then
    sleep 1  # let the old process fully exit before re-bootstrapping
  else
    launchctl unload "$plist" 2>/dev/null || true
    sleep 1
  fi

  [ -n "$free" ] && free_port

  if ! launchctl bootstrap "$DOMAIN" "$plist" 2>/dev/null; then
    launchctl load "$plist" 2>/dev/null || true
  fi
  say "✓ Restarted $label"
}

# 4. Restart the PDF watcher.
say "▶ [4/6] Restarting the PDF watcher…"
restart_agent "com.antigravity.watchdog-pickd"

# 5. Restart the capture app — freeing the port between stop and start so the new
# server can actually bind.
say "▶ [5/6] Restarting the capture app…"
restart_agent "com.antigravity.pickd-app" free_port

# 6. Verify the app is really serving, then open it. We open ONLY once the server
# answers — opening a not-yet-ready server is what produces the endless blank page.
say "▶ [6/6] Waiting for the capture app to answer on ${APP_URL} ..."
if [ -f "$HOME/Library/LaunchAgents/com.antigravity.pickd-app.plist" ]; then
  up=""
  for i in $(seq 1 60); do
    if curl -s -o /dev/null --max-time 1 "$APP_URL"; then
      up="yes"
      say "✓ App is up after ${i}s."
      # An answering server is NOT proof of the new build: a stale process or a
      # LaunchAgent pointing at ANOTHER clone serves the old UI with a 200.
      # /api/version exposes the running build's git SHA — compare with ours.
      want="$(git rev-parse --short HEAD)"
      served="$(curl -s --max-time 2 "$APP_URL/api/version" | grep -oE '[0-9a-f]{7,12}' || true)"
      if [ "$served" = "$want" ]; then
        say "✓ Serving the new build ($served)."
      elif [ -z "$served" ]; then
        say "⚠ App answers but has no /api/version (build older than this check)."
      else
        say "✗ App answers but serves build '$served' — this repo is at '$want'."
        say "  Causes: a stale app.py survived the restart, or the LaunchAgent runs"
        say "  ANOTHER clone. Check: grep -A3 ProgramArguments \"$HOME/Library/LaunchAgents/com.antigravity.pickd-app.plist\""
      fi
      open -a Safari "$APP_URL" 2>/dev/null || open "$APP_URL" 2>/dev/null || true
      say "✓ Opened $APP_URL in Safari."
      break
    fi
    sleep 1
  done
  if [ -z "$up" ]; then
    say "✗ The app didn't answer within 60s. NOT opening a blank window."
    say "  Check logs:  tail -n 40 '$REPO/logs/app-stderr.log'"
    say "  Then retry:  ./scripts/update.sh $BRANCH"
  fi
fi

say "✅ Update complete — now on $(git rev-parse --short HEAD) ($BRANCH)."
