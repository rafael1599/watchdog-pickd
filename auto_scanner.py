"""
auto_scanner.py — Periodically capture new AS400 orders into the local scanned cache.

Design (confirmed with the operator):
  - Runs inside the capture-UI process (app.py), the single owner of the Mocha
    emulator, on a background thread every SCAN_INTERVAL_SEC seconds.
  - "Catch up": each cycle captures forward from the next unscanned number until it
    hits a number that doesn't exist yet (or a per-cycle cap). It then waits — the
    SAME number is retried next cycle (orders are issued in sequence).
  - "Cache only": captured orders are stored in scanned_store, NOT sent to PickD.
    Sending happens on a trigger (PDF drop / manual capture) that reuses the cache.
  - One keyboard at a time: a global capture_lock serializes AS400 access. A manual
    capture takes priority — it sets `manual_waiting`, and a running scan cycle yields
    after its current order so the human never fights the auto-scanner for focus.

Only the threaded runner touches macOS/Mocha; `run_scan_cycle` is pure (takes a
driver + injectable capture/preview fns) and is unit-tested with a fake driver.
"""

# PEP 563: defer annotations so "X | None" hints work on Python 3.9 (Bay 2 Mac).
from __future__ import annotations

import logging
import os
import threading

import scanned_store
from as400_capture import (
    AS400Disconnected,
    AS400ManualLoginRequired,
    CaptureError,
    MochaDriver,
    bootstrap_session,
    capture_order,
)

log = logging.getLogger("pickd-auto-scanner")

SCAN_INTERVAL_SEC = float(os.getenv("SCAN_INTERVAL_SEC", "1200"))  # 20 minutes
# Wait a bit after startup before the first cycle, so the UI loads and the operator
# sees it before the scanner takes over the Mocha keyboard.
SCAN_INITIAL_DELAY_SEC = float(os.getenv("SCAN_INITIAL_DELAY_SEC", "20"))
# Bound how many orders one cycle may capture, so a big backlog can't hold the
# keyboard for an unbounded stretch (it resumes next cycle).
SCAN_MAX_PER_CYCLE = int(os.getenv("SCAN_MAX_PER_CYCLE", "30"))

# Serializes all AS400/Mocha access between the auto-scanner and manual captures.
capture_lock = threading.Lock()
# A manual capture sets this so an in-flight scan cycle yields after its current order.
manual_waiting = threading.Event()

_stop = threading.Event()
_thread: threading.Thread | None = None


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


def run_scan_cycle(
    driver,
    *,
    start: int = scanned_store.SCAN_START,
    max_per_cycle: int = SCAN_MAX_PER_CYCLE,
    should_continue=None,
    capture_fn=capture_order,
    preview_fn=None,
) -> dict:
    """Capture forward from the next unscanned number, storing each success.

    Stops (and returns) when: an order number doesn't exist yet / the session is
    unavailable (we retry the same number next cycle), the per-cycle cap is hit, or
    `should_continue()` returns False (yield to a waiting manual capture). Never
    advances past a missing number and never sends anything.
    """
    if preview_fn is None:
        from pipeline import preview_order as preview_fn  # local import: avoids DB deps at import

    scanned = []
    for _ in range(max_per_cycle):
        if should_continue is not None and not should_continue():
            return {"scanned": scanned, "stopped": "yield_to_manual"}

        n = scanned_store.next_scan_number(start)
        try:
            text = capture_fn(str(n), driver)
        except (AS400Disconnected, AS400ManualLoginRequired) as e:
            return {"scanned": scanned, "stopped": "as400_unavailable", "detail": str(e)}
        except CaptureError as e:
            # The number isn't an order yet (or the screen stalled): stop and retry
            # this same number next cycle. Do NOT record it, so the cursor holds.
            return {"scanned": scanned, "stopped": "no_more_orders", "detail": str(e)}

        meta = _meta_from_preview(preview_fn(text))
        scanned_store.put(n, text, meta, source="auto_scan")
        scanned.append(str(n))
        log.info("auto-scan: cached order #%s (%s items)", n, meta.get("item_count"))

    return {"scanned": scanned, "stopped": "max_per_cycle"}


def _run_one_cycle() -> None:
    """One timer tick: yield if a manual capture holds the lock, else scan."""
    if not capture_lock.acquire(blocking=False):
        log.info("auto-scan: manual capture in progress — skipping this cycle")
        return
    try:
        driver = MochaDriver()
        try:
            bootstrap_session(driver)
        except (AS400Disconnected, AS400ManualLoginRequired) as e:
            log.info("auto-scan: AS400 not ready (%s) — skipping this cycle", e)
            return
        result = run_scan_cycle(driver, should_continue=lambda: not manual_waiting.is_set())
        if result["scanned"]:
            log.info("auto-scan cycle done: cached %s (%s)", result["scanned"], result["stopped"])
    finally:
        capture_lock.release()


def _loop(interval: float) -> None:
    log.info(
        "auto-scanner started (every %.0fs, from #%s) — first cycle in %.0fs",
        interval,
        scanned_store.next_scan_number(),
        SCAN_INITIAL_DELAY_SEC,
    )
    _stop.wait(SCAN_INITIAL_DELAY_SEC)  # let the UI come up before grabbing Mocha
    while not _stop.is_set():
        try:
            _run_one_cycle()
        except Exception:
            log.exception("auto-scan cycle crashed (will retry next interval)")
        _stop.wait(interval)


def start_auto_scanner(interval: float = SCAN_INTERVAL_SEC) -> None:
    """Start the background auto-scanner thread (idempotent). Gated by AUTO_SCAN."""
    global _thread
    if os.getenv("AUTO_SCAN", "1") not in ("1", "true", "True", "yes"):
        log.info("auto-scanner disabled (AUTO_SCAN is off)")
        return
    if _thread and _thread.is_alive():
        return
    _stop.clear()
    _thread = threading.Thread(target=_loop, args=(interval,), daemon=True, name="auto-scanner")
    _thread.start()


def stop_auto_scanner() -> None:
    _stop.set()
