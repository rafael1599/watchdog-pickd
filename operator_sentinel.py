"""
operator_sentinel.py — So a person can always stop the watcher.

Rafael, 29 sep 2026: «nadie puede parar al watcher, no escucha cuando
presionamos una tecla o movemos el mouse. Quizá deberíamos usar la tecla delete
dos veces para indicarle que pare como medida de emergencia».

Why it did not listen: `operator_idle_seconds` recognizes a person by comparing
macOS's HID idle clock with the time since the watcher's OWN last keystroke. A
busy watcher types or copies the screen several times a second, so its own
clock never leaves zero and the operator's events hide among its own. And the
customer step and its expedition only looked for a person BETWEEN accounts.

This watches two signals the watcher itself never produces, in a small
JavaScript-for-Automation process (`scripts/operator_sentinel.js`, ~3 % CPU):

  - **the mouse** — any move, click or scroll is a person. The automated work
    stops at its next keystroke and stays off until the operator has been still
    for `SCAN_IDLE_THRESHOLD_SEC` (60 s), the same gate as before;
  - **Delete twice within two seconds** — the emergency stop: hands off the
    terminal for `OPERATOR_EMERGENCY_STOP_SEC` (30 min).

Only the AUTOMATED thread is stopped (`as400_capture.mark_automated_thread`): a
button the operator presses in the Bay 2 UI is the operator, and still works.
`OPERATOR_SENTINEL=0` switches the watching off.
"""

from __future__ import annotations

import logging
import os
import subprocess
import threading
import time
from pathlib import Path

log = logging.getLogger("pickd-operator-sentinel")

SCRIPT = Path(__file__).resolve().parent / "scripts" / "operator_sentinel.js"
DOUBLE_DELETE_WINDOW_SEC = 2.0

_lock = threading.Lock()
_state = {"operator_at": None, "stop_until": 0.0, "deletes": [], "running": False}
_thread = None


def _now() -> float:
    return time.monotonic()


def emergency_stop_sec() -> float:
    return max(0.0, float(os.getenv("OPERATOR_EMERGENCY_STOP_SEC", "1800")))


def idle_threshold_sec() -> float:
    return max(0.0, float(os.getenv("SCAN_IDLE_THRESHOLD_SEC", "60")))


def on_signal(line: str, now: float | None = None) -> str | None:
    """Feed one line from the sentinel. Returns what it meant, for the log/tests."""
    now = _now() if now is None else now
    word = (line or "").strip().upper()
    with _lock:
        if word == "MOUSE":
            _state["operator_at"] = now
            return "operator"
        if word == "DELETE":
            _state["operator_at"] = now
            recent = [t for t in _state["deletes"] if now - t <= DOUBLE_DELETE_WINDOW_SEC]
            recent.append(now)
            _state["deletes"] = recent[-2:]
            if len(recent) >= 2:
                _state["stop_until"] = now + emergency_stop_sec()
                _state["deletes"] = []
                return "emergency_stop"
            return "operator"
    return None


def last_operator_at() -> float | None:
    """Monotonic time of the last signal a person gave, or None."""
    return _state["operator_at"]


def emergency_remaining(now: float | None = None) -> float:
    now = _now() if now is None else now
    return max(0.0, _state["stop_until"] - now)


def hands_off(now: float | None = None) -> bool:
    """Should the automated work keep its hands off the terminal right now?"""
    now = _now() if now is None else now
    if emergency_remaining(now) > 0:
        return True
    at = _state["operator_at"]
    return at is not None and now - at < idle_threshold_sec()


def resume() -> None:
    """Lift an emergency stop (the Bay 2 UI, or a person at the Mac)."""
    with _lock:
        _state["stop_until"] = 0.0


def _reset_for_tests() -> None:
    with _lock:
        _state.update(operator_at=None, stop_until=0.0, deletes=[], running=False)


def _run() -> None:
    backoff = 5.0
    while True:
        try:
            proc = subprocess.Popen(
                ["osascript", "-l", "JavaScript", str(SCRIPT)],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                bufsize=1,
            )
            _state["running"] = True
            for line in proc.stdout:
                meaning = on_signal(line)
                if meaning == "emergency_stop":
                    log.warning(
                        "OPERATOR: Delete ×2 — emergency stop, hands off the AS400 for %.0f min",
                        emergency_stop_sec() / 60,
                    )
                elif line.strip() == "READY":
                    log.info("operator sentinel: watching the mouse and the Delete key")
                    backoff = 5.0
            proc.wait()
        except Exception as e:  # noqa: BLE001 — watching may not take the watcher down
            log.warning("operator sentinel: %s", e)
        _state["running"] = False
        log.warning("operator sentinel stopped — restarting in %.0fs", backoff)
        time.sleep(backoff)
        backoff = min(backoff * 2, 300.0)


def start() -> bool:
    """Start watching (idempotent). False when switched off or not on a Mac."""
    global _thread
    if os.getenv("OPERATOR_SENTINEL", "1") in ("0", "false", "False", "no"):
        log.info("operator sentinel: switched off (OPERATOR_SENTINEL=0)")
        return False
    if not SCRIPT.exists() or os.uname().sysname != "Darwin":
        return False
    if _thread and _thread.is_alive():
        return True
    _thread = threading.Thread(target=_run, name="operator-sentinel", daemon=True)
    _thread.start()
    return True
