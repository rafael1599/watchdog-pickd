"""
start_pickd.py — Launcher for the autostart LaunchAgent.

Opens the AS400 emulator, starts the PickD capture app (no Terminal window), waits
for it to answer, then opens Safari at the UI.

Run BY THE VENV PYTHON (the LaunchAgent invokes it the same way the PDF watcher is
invoked). Using the venv python — instead of /bin/bash on a .sh — avoids macOS TCC
"Operation not permitted" when the repo lives inside a protected folder like
~/Documents (bash gets blocked there; the venv python already has access).
"""

import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
PORT = int(os.environ.get("PICKD_PORT", "5757"))  # keep in sync with app.py's default
URL = f"http://127.0.0.1:{PORT}"


def _venv_python() -> str:
    """The repo's venv python (has flask/dotenv/supabase); fall back to current."""
    venv_python = REPO / "venv" / "bin" / "python3"
    return str(venv_python) if venv_python.exists() else sys.executable


def _launch_target() -> str:
    """Read AS400_LAUNCH_TARGET from .env (no shell-sourcing); fall back to Mocha's id."""
    env_path = REPO / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.strip().startswith("AS400_LAUNCH_TARGET="):
                return line.split("=", 1)[1].strip()
    return "dk.mochasoft.tn5250"


def _server_is_up() -> bool:
    try:
        urllib.request.urlopen(URL, timeout=1)
        return True
    except Exception:
        return False


def _free_port(port: int = PORT) -> None:
    """Kill any process still listening on the app port before we start a new one.

    A stale/hung app.py left on the port blocks the new server from binding — the
    browser then loads a dead server forever (the blank-window symptom). Clearing it
    first guarantees the fresh app owns the port.
    """
    try:
        out = subprocess.run(
            ["lsof", "-ti", f"tcp:{port}"], capture_output=True, text=True, check=False
        ).stdout
        pids = [p for p in out.split() if p.strip()]
        for pid in pids:
            subprocess.run(["kill", "-9", pid], check=False)
        if pids:
            print(f"start_pickd: freed port {port} (killed {pids})", flush=True)
            time.sleep(1)
    except Exception as e:
        print(f"start_pickd: could not free port {port}: {e}", flush=True)


def main():
    # 1. Launch the AS400 emulator (by bundle id; ignore if already open).
    subprocess.run(["open", "-b", _launch_target()], check=False)

    # 2. Free the port from any previous instance, then start the web app.
    _free_port()
    app_proc = subprocess.Popen([_venv_python(), str(REPO / "app.py")], cwd=str(REPO))

    # 3. Wait until the server actually answers (up to ~60s — the app imports
    # supabase/flask, which can be slow on a cold start), THEN open Safari. Opening
    # before it's ready is what leaves a blank, never-loading window.
    up = False
    for _ in range(120):
        if _server_is_up():
            up = True
            break
        time.sleep(0.5)
    if up:
        subprocess.run(["open", "-a", "Safari", URL], check=False)
        print(f"start_pickd: app is up, opened {URL}", flush=True)
    else:
        print(
            f"start_pickd: app did NOT answer on {URL} within 60s — not opening a "
            "blank window. Check logs/app-stderr.log.",
            flush=True,
        )

    # 4. Keep this launcher tied to the app process (so the LaunchAgent tracks it).
    app_proc.wait()


if __name__ == "__main__":
    main()
