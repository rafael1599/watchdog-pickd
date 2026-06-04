"""
install_autostart.py — Install a macOS LaunchAgent so the PickD capture app starts
automatically at login: it opens the AS400 emulator, runs the app (no Terminal
window), and opens the UI in Safari.

Run once on the AS400 Mac:

    python3 scripts/install_autostart.py

To remove it later:

    launchctl unload ~/Library/LaunchAgents/com.antigravity.pickd-app.plist
    rm ~/Library/LaunchAgents/com.antigravity.pickd-app.plist
"""

import plistlib
import subprocess
import sys
from pathlib import Path

LABEL = "com.antigravity.pickd-app"


def main():
    repo = Path(__file__).resolve().parent.parent
    start_script = repo / "scripts" / "start_pickd.py"
    logs = repo / "logs"
    logs.mkdir(exist_ok=True)

    # Always use the repo's venv python (it has flask/dotenv/supabase), regardless of
    # how this installer was invoked. Falls back to the current interpreter.
    venv_python = repo / "venv" / "bin" / "python3"
    python = str(venv_python) if venv_python.exists() else sys.executable

    plist_path = Path.home() / "Library" / "LaunchAgents" / f"{LABEL}.plist"
    plist = {
        "Label": LABEL,
        # Run the launcher with the venv python. Same pattern as the PDF watcher —
        # avoids macOS TCC blocking bash inside ~/Documents, and ensures deps exist.
        "ProgramArguments": [python, str(start_script)],
        "RunAtLoad": True,
        # Run once at login; don't relaunch (avoids reopening Safari/Mocha in a loop).
        "KeepAlive": False,
        "StandardOutPath": str(logs / "app-stdout.log"),
        "StandardErrorPath": str(logs / "app-stderr.log"),
    }

    plist_path.parent.mkdir(parents=True, exist_ok=True)
    # Reload cleanly if it was already installed.
    subprocess.run(["launchctl", "unload", str(plist_path)], check=False)
    with open(plist_path, "wb") as f:
        plistlib.dump(plist, f)
    subprocess.run(["launchctl", "load", str(plist_path)], check=False)

    print(f"✅ Autostart installed: {plist_path}")
    print("   It will run at every login: open AS400, start the app, open Safari.")
    print(f"   Python:   {python}")
    print(f"   Launcher: {start_script}")


if __name__ == "__main__":
    main()
