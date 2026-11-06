"""
obstructions.py — Whatever window covers Mocha is taken away, not just one kind.

Rafael, 6 nov 2026, after a reboot left an App Store «Enter the Apple ID
password» dialog on top of the Sign On screen: «quiero una solución general, no
solamente para eso específico». The capture put Mocha in front, sent Cmd+A and
Cmd+C, and the dialog took the keys: the clipboard never changed, the watcher
never saw the Sign On it knows how to log into, and three requests from PickD
failed with «the clipboard didn't change».

So when a read copies nothing, the driver asks this module what windows OTHER
processes have up, and takes them away the gentlest way that works:

  1. a button that says no — Cancel, Not Now, Later, Close, Don't Allow, Skip;
  2. Escape, with that process in front;
  3. SIGTERM to the process that owns it.

Then the read is tried once more. Nothing is done before a read that worked:
on the Bay 2 Mac an Apple event costs ~0.3 s, and the scan only matters when
something already went wrong.

What is never touched:

  - the macOS shell (Dock, menu bar, Control Center, …) — killing it is worse
    than any dialog;
  - **protected** windows macOS keeps out of a script's reach on purpose — the
    administrator password (SecurityAgent), the login window. They are reported
    by name, never pressed;
  - Mocha itself (its own dialogs are the capture's business) and the processes
    the watcher runs in.

A window that stays is reported in the error, so PickD's request says what is on
the screen instead of «the clipboard didn't change». `AS400_OBSTRUCTION_GUARD=0`
switches the whole thing off.
"""

from __future__ import annotations

import logging
import os
import subprocess
from dataclasses import dataclass

log = logging.getLogger("pickd-as400")

# The desktop itself. Their windows are the menu bar, the Dock, the wallpaper.
SHELL = frozenset(
    {
        "Dock",
        "SystemUIServer",
        "ControlCenter",
        "Control Center",
        "NotificationCenter",
        "Notification Center",
        "Spotlight",
        "WindowManager",
        "Window Server",
        "WindowServer",
        "TextInputMenuAgent",
        "TextInputSwitcher",
        "Finder",
    }
)

# What the watcher itself runs in, and where a person types commands.
OURS = frozenset({"Python", "python3", "osascript", "Terminal", "iTerm2"})

# macOS keeps these out of a script's reach; pressing at them does nothing and
# killing them can log the user out. Reported, never touched.
PROTECTED = frozenset({"SecurityAgent", "loginwindow", "coreautha", "ScreenSaverEngine"})

# Buttons that decline: pressing one never agrees to anything.
DECLINE_BUTTONS = (
    "Cancel",
    "Not Now",
    "Later",
    "Close",
    "Don’t Allow",
    "Don't Allow",
    "Skip",
    "Ignore",
    "Remind Me Later",
    "Cancelar",
    "Ahora no",
    "Más tarde",
    "Cerrar",
    "No permitir",
)


@dataclass(frozen=True)
class Window:
    pid: int
    process: str
    title: str
    text: str

    def describe(self) -> str:
        what = self.title or self.text
        what = " ".join(what.split())[:80]
        return f"{self.process} «{what}»" if what else self.process


def enabled() -> bool:
    return os.getenv("AS400_OBSTRUCTION_GUARD", "1") not in ("0", "false", "no")


def _literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def build_scan_script(skip: frozenset[str]) -> str:
    """Every window of every process not in `skip`: pid, process, title, first text.

    One line per window, tab-separated. `every window` raises for some system
    processes, so each one is inside a `try`.
    """
    names = ", ".join(f'"{_literal(n)}"' for n in sorted(skip))
    return (
        'set report to ""\n'
        'tell application "System Events"\n'
        "  repeat with p in (every process)\n"
        "    try\n"
        "      set pn to name of p\n"
        f"      if pn is not in {{{names}}} then\n"
        "        repeat with w in (every window of p)\n"
        '          set t to ""\n'
        "          try\n"
        "            set t to name of w\n"
        "          end try\n"
        '          if t is missing value then set t to ""\n'
        '          set x to ""\n'
        "          try\n"
        "            set x to value of first static text of w\n"
        "          end try\n"
        '          if x is missing value then set x to ""\n'
        "          set report to report & (unix id of p) & tab & pn & tab & t & tab & x & linefeed\n"
        "        end repeat\n"
        "      end if\n"
        "    end try\n"
        "  end repeat\n"
        "end tell\n"
        "return report"
    )


def parse_scan(out: str) -> list[Window]:
    windows = []
    for line in (out or "").splitlines():
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        try:
            pid = int(parts[0].strip())
        except ValueError:
            continue
        parts += ["", ""]
        windows.append(Window(pid, parts[1].strip(), parts[2].strip(), parts[3].strip()))
    return windows


def build_decline_script(pid: int) -> str:
    """Press the first declining button any window of `pid` has; else Escape.

    Returns "button:<name>" or "escape".
    """
    buttons = ", ".join(f'"{_literal(b)}"' for b in DECLINE_BUTTONS)
    return (
        'tell application "System Events"\n'
        f"  set p to first process whose unix id is {int(pid)}\n"
        "  repeat with w in (every window of p)\n"
        f"    repeat with b in {{{buttons}}}\n"
        "      try\n"
        "        click button (contents of b) of w\n"
        '        return "button:" & (contents of b)\n'
        "      end try\n"
        "    end repeat\n"
        "  end repeat\n"
        "  set frontmost of p to true\n"
        "  delay 0.2\n"
        "  key code 53\n"
        '  return "escape"\n'
        "end tell"
    )


def clear(run_script, mocha_name: str, terminate=None) -> tuple[list[str], list[str]]:
    """Take away what covers Mocha. Returns (what was removed, what is still there).

    `run_script(script) -> str` runs one AppleScript and returns its output (the
    driver's own `_osascript`, so the operator brake and the timeout apply).
    `terminate(pid)` sends SIGTERM; injectable for tests.
    """
    terminate = terminate or (lambda pid: subprocess.run(["kill", "-TERM", str(pid)], check=False))
    skip = SHELL | OURS | {mocha_name}

    def scan() -> list[Window]:
        return parse_scan(run_script(build_scan_script(skip)))

    removed: list[str] = []
    for w in scan():
        if w.process in PROTECTED:
            continue
        if any(r.startswith(f"{w.process} ") or r == w.process for r in removed):
            continue
        how = None
        try:
            how = (run_script(build_decline_script(w.pid)) or "").strip()
        except Exception as e:  # noqa: BLE001 — try the next way
            # A person at the Mac stops everything here too: never press, and
            # never kill, under somebody's hands.
            if type(e).__name__ == "OperatorTookOver":
                raise
            log.info("obstruction: could not press at %s (%s)", w.describe(), e)
        still = [x for x in scan() if x.pid == w.pid]
        if still:
            terminate(w.pid)
            how = "quit"
        removed.append(f"{w.describe()} ({how or 'gone'})")
        log.warning("obstruction: took away %s from in front of Mocha", removed[-1])

    left = [w.describe() for w in scan() if w.process not in OURS]
    return removed, left
