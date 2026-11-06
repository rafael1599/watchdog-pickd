#!/usr/bin/env bash
#
# bay2_host.sh — Bay 2 is a machine for the watcher and the AS400, nothing else.
#
# Rafael, 6 nov 2026: «ya no necesitamos que se abran más procesos de los
# necesarios … porque ahora se piden órdenes directamente desde PickD … que le
# permita a la mac seguir enviando órdenes con la pantalla cerrada y asegurarse
# de que sólo los procesos necesarios para el watcher y as400 estén corriendo».
# Nobody uses that Mac for anything else, so this runs on every update.
#
#   ./scripts/bay2_host.sh setup   # ONCE, by hand: asks for the password (sudoers rule)
#   ./scripts/bay2_host.sh power   # lid closed keeps working on AC; on battery it may sleep
#   ./scripts/bay2_host.sh apps    # quit every app with a window that the watcher doesn't need
#   ./scripts/bay2_host.sh status  # what the three look like right now
#
# Why `pmset disablesleep`: closing the lid without an external display puts a
# MacBook to sleep whatever `caffeinate` asserts, and a sleeping Mac answers no
# order asked from PickD. It needs root, and update.sh runs as the user with no
# terminal (auto_update launches it), so `setup` installs a sudoers rule that
# allows exactly `pmset -a disablesleep 0|1` and nothing else.
#
# Why only on AC: with the lid shut and sleep disabled, a Mac on battery runs
# until it is flat. On battery we give sleep back.
#
# Why the screen lock matters: the capture types into Mocha with System Events.
# A LOCKED session sends those keys to the login window, so the screen must not
# ask for a password when it turns off. `status` and `power` say so in the log;
# changing it is a System Settings switch, not something a script should flip.
set -uo pipefail

PMSET=/usr/bin/pmset
SUDOERS=/etc/sudoers.d/watchdog-pickd

# Apps with a window that stay open. Everything else with a window is quit.
# Background processes (the watcher, app.py, the operator sentinel, system
# daemons) are never touched: only `background only is false` processes count.
# A name here is the process name System Events reports.
KEEP_APPS=(
  "${MOCHA_APP_NAME:-Mocha TN5250}"
  "Finder"
  "Terminal"
  "iTerm2"
  "AnyDesk"
  "Tailscale"
  "Screen Sharing"
  "Python"
  "osascript"
)

on_ac() { "$PMSET" -g batt 2>/dev/null | grep -q "AC Power"; }

lock_note() {
  # macOS 13+: `sysadminctl -screenLock status` says when the screen locks.
  local s
  s="$(sysadminctl -screenLock status 2>&1 | tail -n 1)"
  case "$s" in
    *"is off"*|*disabled*) echo "screen lock: off" ;;
    "") echo "screen lock: unknown" ;;
    *) echo "⚠ screen lock: $s — a locked screen stops the capture (System Settings → Lock Screen → Require password: Never)" ;;
  esac
}

cmd_setup() {
  local user rule tmp
  user="$(id -un)"
  rule="$user ALL=(root) NOPASSWD: $PMSET -a disablesleep 0, $PMSET -a disablesleep 1"
  tmp="$(mktemp)"
  printf '%s\n' "# watchdog-pickd: lid closed keeps sending orders (scripts/bay2_host.sh)" "$rule" >"$tmp"
  if ! /usr/sbin/visudo -cf "$tmp" >/dev/null; then
    echo "✗ the sudoers rule did not validate; nothing installed"
    rm -f "$tmp"
    return 1
  fi
  sudo install -m 0440 -o root -g wheel "$tmp" "$SUDOERS" && echo "✓ installed $SUDOERS"
  rm -f "$tmp"
  cmd_power
}

cmd_power() {
  local want=0
  on_ac && want=1
  if sudo -n "$PMSET" -a disablesleep "$want" 2>/dev/null; then
    if [ "$want" = 1 ]; then
      echo "✓ on AC: the lid can close, the Mac stays awake"
    else
      echo "• on battery: sleep allowed (so it doesn't run flat)"
    fi
  else
    echo "⚠ cannot set disablesleep without a password — run once: ./scripts/bay2_host.sh setup"
  fi
  lock_note
}

cmd_apps() {
  # Names and pids from System Events, which the watcher is already allowed to
  # drive. `tell application "X" to quit` would make macOS ask a NEW Automation
  # permission for every app — a dialog on an unattended Mac that hangs this
  # script. SIGTERM asks the app to quit the normal way; nothing is -9.
  local rows quit=()
  rows="$(osascript -e 'tell application "System Events"
    set out to ""
    repeat with p in (every application process whose background only is false)
      set out to out & (unix id of p) & tab & (name of p) & linefeed
    end repeat
    return out
  end tell' 2>/dev/null)"
  while IFS=$'\t' read -r pid app; do
    [ -z "$pid" ] || [ -z "$app" ] && continue
    local skip=""
    for k in "${KEEP_APPS[@]}"; do [ "$app" = "$k" ] && skip=1; done
    [ -n "$skip" ] && continue
    if kill -TERM "$pid" 2>/dev/null; then
      quit+=("$app")
    else
      echo "⚠ could not quit $app ($pid)"
    fi
  done <<<"$rows"
  if [ ${#quit[@]} -gt 0 ]; then
    echo "✓ quit: ${quit[*]}"
  else
    echo "✓ nothing to quit"
  fi
}

cmd_status() {
  echo "power: $("$PMSET" -g batt 2>/dev/null | head -n 1 | sed "s/.*'\(.*\)'.*/\1/")"
  echo "disablesleep: $("$PMSET" -g 2>/dev/null | awk '/SleepDisabled/ {print $2}')"
  [ -f "$SUDOERS" ] && echo "sudoers rule: installed" || echo "sudoers rule: missing (./scripts/bay2_host.sh setup)"
  lock_note
  echo "apps with a window: $(osascript -e 'tell application "System Events" to get name of every application process whose background only is false' 2>/dev/null)"
}

case "${1:-status}" in
  setup) cmd_setup ;;
  power) cmd_power ;;
  apps) cmd_apps ;;
  status) cmd_status ;;
  *) echo "usage: $0 setup|power|apps|status"; exit 2 ;;
esac
