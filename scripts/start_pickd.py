"""
start_pickd.py — Launcher for the autostart LaunchAgent.

Opens the AS400 emulator, starts the PickD capture app (no Terminal window), waits
for it to answer, then opens Safari at the UI.

Run BY THE VENV PYTHON (the LaunchAgent invokes it the same way the PDF watcher is
invoked). Using the venv python — instead of /bin/bash on a .sh — avoids macOS TCC
"Operation not permitted" when the repo lives inside a protected folder like
~/Documents (bash gets blocked there; the venv python already has access).
"""

import subprocess
import sys
import time
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
URL = "http://127.0.0.1:5000"


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


def main():
    # 1. Launch the AS400 emulator (by bundle id; ignore if already open).
    subprocess.run(["open", "-b", _launch_target()], check=False)

    # 2. Start the web app in the background using the venv python.
    app_proc = subprocess.Popen([_venv_python(), str(REPO / "app.py")], cwd=str(REPO))

    # 3. Wait until the server answers (up to ~20s), then open it in Safari.
    for _ in range(40):
        if _server_is_up():
            break
        time.sleep(0.5)
    subprocess.run(["open", "-a", "Safari", URL], check=False)

    # 4. Keep this launcher tied to the app process (so the LaunchAgent tracks it).
    app_proc.wait()


if __name__ == "__main__":
    main()
