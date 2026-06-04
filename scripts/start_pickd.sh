#!/bin/bash
# start_pickd.sh — Launch the AS400 emulator + PickD capture app and open it in
# Safari, with no visible Terminal. Meant to be run by a macOS LaunchAgent at login
# (see scripts/install_autostart.py).
#
# Steps: open Mocha TN5250 → start the Flask app in the background → wait until it
# answers → open Safari at the UI. The app process keeps this script alive so the
# LaunchAgent tracks it.

# Move to the repo root (this script lives in scripts/).
cd "$(dirname "$0")/.." || exit 1

# Resolve the emulator bundle id from .env (safe grep; no shell-sourcing of .env,
# which can contain values with spaces). Fall back to MochaSoft's bundle id.
LAUNCH_TARGET="$(grep -E '^AS400_LAUNCH_TARGET=' .env 2>/dev/null | tail -1 | cut -d= -f2-)"
LAUNCH_TARGET="${LAUNCH_TARGET:-dk.mochasoft.tn5250}"

# 1. Launch the AS400 emulator (by bundle id; ignore errors if already open).
open -b "$LAUNCH_TARGET" 2>/dev/null || true

# 2. Start the web app in the background using the project's venv.
source venv/bin/activate
python3 app.py &
APP_PID=$!

# 3. Wait until the server answers (up to ~20s), then open it in Safari.
for _ in $(seq 1 40); do
  if curl -s http://127.0.0.1:5000 >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done
open -a Safari http://127.0.0.1:5000

# 4. Keep this script tied to the app process.
wait "$APP_PID"
