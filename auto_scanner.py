"""
auto_scanner.py — Continuously capture new AS400 orders into the local scanned cache.

Design (confirmed with the operator, 2026-06-10):
  - Runs inside the capture-UI process (app.py), the single owner of Mocha, on a
    background thread. ONE order per step (not a catch-up burst): each captured order
    is stored immediately so it shows in the UI right away.
  - Pace per step result:
      * captured      → wait FOUND_NEXT_DELAY_SEC (5s), then try the next number.
      * not_found     → the number isn't an order yet → wait NOT_FOUND_WAIT_SEC (20m).
      * incomplete    → capture stalled (no END OF ORDER, e.g. the operator grabbed
                        the keyboard) → wait INCOMPLETE_RETRY_SEC (5m) and retry the
                        SAME number (the cursor only advances on success).
      * unavailable   → AS400 not connected / not logged in → (re)bootstrap, retry.
  - Pause entirely while the operator is using the computer: if the system has had
    mouse/keyboard input recently (idle < IDLE_THRESHOLD_SEC) we don't scan, so we
    never fight the human for the keyboard. A manual capture (which holds capture_lock)
    also pauses us.
  - Cache only: captures go to scanned_store; sending happens from the UI.

Only the threaded runner touches macOS/Mocha; `run_scan_step` is pure (injectable
capture/preview fns) and unit-tested with a fake driver.
"""

# PEP 563: defer annotations so "X | None" hints work on Python 3.9 (Bay 2 Mac).
from __future__ import annotations

import logging
import os
import re
import subprocess
import threading

import scanned_store
from as400_capture import (
    AS400Disconnected,
    AS400ManualLoginRequired,
    CaptureError,
    MochaDriver,
    OrderNotFound,
    bootstrap_session,
    capture_order,
)

log = logging.getLogger("pickd-auto-scanner")

# Wait after startup before the first capture, so the UI loads first.
SCAN_INITIAL_DELAY_SEC = float(os.getenv("SCAN_INITIAL_DELAY_SEC", "20"))
# Between two consecutive found orders.
FOUND_NEXT_DELAY_SEC = float(os.getenv("SCAN_FOUND_DELAY_SEC", "5"))
# After the next number doesn't exist yet (not registered in AS400).
NOT_FOUND_WAIT_SEC = float(os.getenv("SCAN_NOT_FOUND_WAIT_SEC", "1200"))  # 20 min
# After a partial/stalled capture — retry the same number.
INCOMPLETE_RETRY_SEC = float(os.getenv("SCAN_INCOMPLETE_RETRY_SEC", "300"))  # 5 min
# After AS400 is unavailable (disconnected / needs login).
UNAVAILABLE_WAIT_SEC = float(os.getenv("SCAN_UNAVAILABLE_WAIT_SEC", "300"))  # 5 min
# Operator is "using the computer" if there was input within this many seconds.
IDLE_THRESHOLD_SEC = float(os.getenv("SCAN_IDLE_THRESHOLD_SEC", "60"))
# How often to re-check while paused (operator active / manual capture running).
IDLE_POLL_SEC = float(os.getenv("SCAN_IDLE_POLL_SEC", "15"))

# Serializes all AS400/Mocha access between the auto-scanner and manual captures.
capture_lock = threading.Lock()
# A manual capture sets this; the loop also pauses on general user activity.
manual_waiting = threading.Event()

_stop = threading.Event()
_thread: threading.Thread | None = None


def system_idle_seconds() -> float:
    """Seconds since the last mouse/keyboard input (macOS HIDIdleTime).

    Uses `ioreg` (no special permissions). Returns a very large number if it can't
    be determined (non-macOS / parse failure) so scanning isn't blocked there.
    """
    try:
        out = subprocess.run(
            ["ioreg", "-c", "IOHIDSystem"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        ).stdout
        m = re.search(r'"HIDIdleTime"\s*=\s*(\d+)', out)
        if m:
            return int(m.group(1)) / 1_000_000_000  # nanoseconds → seconds
    except Exception as e:
        log.debug("could not read HIDIdleTime: %s", e)
    return 1e9


def _meta_from_preview(preview: dict) -> dict:
    """The light summary we keep alongside the raw capture (for display/lookup)."""
    return {
        "order_number": preview.get("order_number"),
        "customer": preview.get("customer"),
        "item_count": preview.get("item_count"),
        "total_units": preview.get("total_units"),
        "subtotal": preview.get("subtotal"),
        "parsed_total": preview.get("parsed_total"),
        "total_mismatch": preview.get("total_mismatch", False),
    }


def run_scan_step(
    driver,
    *,
    start: int = scanned_store.SCAN_START,
    capture_fn=capture_order,
    preview_fn=None,
) -> dict:
    """Capture the single next unscanned order. Returns {"action", "number"}.

    action ∈ {captured, not_found, incomplete, unavailable}. Only a successful
    capture stores to the cache (advancing the cursor); the others leave the cursor
    so the same number is retried.
    """
    if preview_fn is None:
        from pipeline import preview_order as preview_fn  # local import: avoids DB deps at import

    n = scanned_store.next_scan_number(start)
    try:
        text = capture_fn(str(n), driver)
    except OrderNotFound:
        return {"action": "not_found", "number": str(n)}
    except (AS400Disconnected, AS400ManualLoginRequired):
        return {"action": "unavailable", "number": str(n)}
    except CaptureError:
        # Partial / stalled capture (no END OF ORDER) — retry the same number later.
        return {"action": "incomplete", "number": str(n)}

    meta = _meta_from_preview(preview_fn(text))
    # Never cache junk: a real capture must parse to an order with items. The
    # 'Invalid Order Number, REENTER' screen (and any other non-order text) parses
    # to no number / no items — caching it floods the UI with empty 'Order #—'
    # cards. Treat it as not_found so the same number is retried later.
    if not meta.get("order_number") or not (meta.get("item_count") or 0):
        return {"action": "not_found", "number": str(n)}
    scanned_store.put(n, text, meta, source="auto_scan")
    return {"action": "captured", "number": str(n)}


def _wait_for(action: str) -> float:
    return {
        "captured": FOUND_NEXT_DELAY_SEC,
        "not_found": NOT_FOUND_WAIT_SEC,
        "incomplete": INCOMPLETE_RETRY_SEC,
        "unavailable": UNAVAILABLE_WAIT_SEC,
    }.get(action, NOT_FOUND_WAIT_SEC)


def _loop() -> None:
    log.info(
        "auto-scanner started (from #%s) — first capture in %.0fs",
        scanned_store.next_scan_number(),
        SCAN_INITIAL_DELAY_SEC,
    )
    _stop.wait(SCAN_INITIAL_DELAY_SEC)
    driver = None
    while not _stop.is_set():
        # Pause while the operator is actively using the computer, or while a manual
        # capture holds the lock — never fight the human for the keyboard.
        if system_idle_seconds() < IDLE_THRESHOLD_SEC:
            _stop.wait(IDLE_POLL_SEC)
            continue
        if not capture_lock.acquire(blocking=False):
            _stop.wait(IDLE_POLL_SEC)
            continue

        wait = NOT_FOUND_WAIT_SEC
        try:
            if driver is None:
                driver = MochaDriver()
            res = run_scan_step(driver)
            action = res["action"]
            wait = _wait_for(action)
            if action == "unavailable":
                # Try to (re)connect; if it works, retry promptly next iteration.
                try:
                    bootstrap_session(driver)
                    wait = FOUND_NEXT_DELAY_SEC
                except Exception as e:
                    log.info("auto-scan: AS400 not ready (%s)", e)
            elif action == "captured":
                log.info("auto-scan: cached order #%s", res["number"])
            else:
                log.info("auto-scan: %s on #%s (waiting %.0fs)", action, res["number"], wait)
        except Exception:
            log.exception("auto-scan step crashed")
            wait = UNAVAILABLE_WAIT_SEC
        finally:
            capture_lock.release()

        _stop.wait(wait)


def start_auto_scanner() -> None:
    """Start the background auto-scanner thread (idempotent). Gated by AUTO_SCAN."""
    global _thread
    if os.getenv("AUTO_SCAN", "1") not in ("1", "true", "True", "yes"):
        log.info("auto-scanner disabled (AUTO_SCAN is off)")
        return
    if _thread and _thread.is_alive():
        return
    _stop.clear()
    _thread = threading.Thread(target=_loop, daemon=True, name="auto-scanner")
    _thread.start()


def stop_auto_scanner() -> None:
    _stop.set()
