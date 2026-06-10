"""Tests for the auto-scanner's single-step logic (run_scan_step) + helpers.

Pure logic only: a fake capture_fn decides which order numbers "exist" / how they
fail, and a stub preview_fn avoids the Supabase/pipeline import. No Mocha, no DB.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import auto_scanner  # noqa: E402
import scanned_store  # noqa: E402
from as400_capture import AS400Disconnected, CaptureError, OrderNotFound  # noqa: E402


@pytest.fixture(autouse=True)
def tmp_store(tmp_path, monkeypatch):
    monkeypatch.setenv("SCANNED_STORE_PATH", str(tmp_path / "scanned.json"))
    monkeypatch.setenv("SCAN_CURSOR_PATH", str(tmp_path / "scan_cursor"))


def _preview(text):
    return {
        "order_number": text.split()[1],
        "customer": "C",
        "item_count": 1,
        "total_units": 1,
        "subtotal": None,
        "parsed_total": 0.0,
        "total_mismatch": False,
    }


def test_step_captures_and_advances():
    def cap(n, driver):
        return f"ORDER {n} END OF ORDER"

    r = auto_scanner.run_scan_step(None, start=880112, capture_fn=cap, preview_fn=_preview)
    assert r == {"action": "captured", "number": "880112"}
    assert scanned_store.get("880112") is not None
    # Cursor advanced so the next step targets the following number.
    assert scanned_store.next_scan_number(880112) == 880113


def test_step_unparseable_capture_is_not_cached():
    # A capture that "succeeds" but parses to no order/items (e.g. an error screen
    # that slipped past the guards) must NOT be cached — junk in the cache floods
    # the UI with empty cards. Treated as not_found so the number is retried.
    def cap(n, driver):
        return "O R D E R   I N Q U I R Y\nInvalid Order Number, REENTER"

    def junk_preview(text):
        return {"order_number": None, "item_count": 0, "total_units": 0}

    r = auto_scanner.run_scan_step(None, start=880112, capture_fn=cap, preview_fn=junk_preview)
    assert r["action"] == "not_found"
    assert scanned_store.get("880112") is None
    assert scanned_store.next_scan_number(880112) == 880112


def test_step_not_found_does_not_advance():
    def cap(n, driver):
        raise OrderNotFound(f"order {n} doesn't exist yet")

    r = auto_scanner.run_scan_step(None, start=880112, capture_fn=cap, preview_fn=_preview)
    assert r["action"] == "not_found"
    assert scanned_store.get("880112") is None
    assert scanned_store.next_scan_number(880112) == 880112  # retried next time


def test_step_incomplete_does_not_advance():
    def cap(n, driver):
        raise CaptureError("screen didn't advance / no END OF ORDER")

    r = auto_scanner.run_scan_step(None, start=880112, capture_fn=cap, preview_fn=_preview)
    assert r["action"] == "incomplete"
    assert scanned_store.get("880112") is None
    assert scanned_store.next_scan_number(880112) == 880112  # same number retried


def test_step_unavailable_on_disconnect():
    def cap(n, driver):
        raise AS400Disconnected("host down")

    r = auto_scanner.run_scan_step(None, start=880112, capture_fn=cap, preview_fn=_preview)
    assert r["action"] == "unavailable"
    assert scanned_store.next_scan_number(880112) == 880112


def test_wait_per_action():
    assert auto_scanner._wait_for("captured") == auto_scanner.FOUND_NEXT_DELAY_SEC
    assert auto_scanner._wait_for("not_found") == auto_scanner.NOT_FOUND_WAIT_SEC
    assert auto_scanner._wait_for("incomplete") == auto_scanner.INCOMPLETE_RETRY_SEC
    assert auto_scanner._wait_for("unavailable") == auto_scanner.UNAVAILABLE_WAIT_SEC


def test_system_idle_seconds_parses_hididletime(monkeypatch):
    class _R:
        stdout = '  "HIDIdleTime" = 7500000000\n  "other" = 1'

    monkeypatch.setattr(auto_scanner.subprocess, "run", lambda *a, **k: _R())
    assert auto_scanner.system_idle_seconds() == pytest.approx(7.5, abs=0.01)


def test_system_idle_seconds_unknown_is_large(monkeypatch):
    def boom(*a, **k):
        raise FileNotFoundError("ioreg not found")

    monkeypatch.setattr(auto_scanner.subprocess, "run", boom)
    assert auto_scanner.system_idle_seconds() > 1e6  # treat as idle when unknown


# --- VOID / empty orders (complete screen, zero items) --------------------------

# Real capture of a voided order (operator-reported): valid ORDER INQUIRY screen,
# order number present, END OF ORDER reached, but ZERO line items.
VOID_SCREEN = """                            O R D E R   I N Q U I R Y

 Order Number: 880138                       Account Number: VOID

 Bill VOID VOID VOID VOID

 Quant  Quant  Stock #   W/H   Description                       Unit    Extend
   Ord   Ship                                                   Price

                                END OF ORDER                                .00
              Enter             Cmd6
              More Details       RETURN TO SELECT"""


def test_step_void_order_is_skipped_and_advances():
    # The killer bug: a VOID order parsed to 0 items was treated as not_found, so
    # the cursor never advanced and the scanner retried the SAME number forever.
    def cap(n, driver):
        return VOID_SCREEN

    def void_preview(text):
        return {"order_number": "880138", "item_count": 0, "total_units": 0, "is_last_page": True}

    r = auto_scanner.run_scan_step(None, start=880138, capture_fn=cap, preview_fn=void_preview)
    assert r == {"action": "empty_skipped", "number": "880138"}
    assert scanned_store.get("880138") is None  # never a candidate card
    assert scanned_store.next_scan_number(880138) == 880139  # moved past it


def test_step_void_order_with_real_parser():
    # End-to-end through the real preview_order: the 880138 screen must classify
    # as empty_skipped, not as a candidate and not as not_found.
    from pipeline import preview_order

    def cap(n, driver):
        return VOID_SCREEN

    r = auto_scanner.run_scan_step(None, start=880138, capture_fn=cap, preview_fn=preview_order)
    assert r["action"] == "empty_skipped"
    assert scanned_store.get("880138") is None
    assert scanned_store.next_scan_number(880138) == 880139


def test_step_incomplete_empty_capture_still_retried():
    # Zero items but NO 'END OF ORDER' → could be a stalled capture of a real
    # order; must keep retrying (not skip past it and lose the order).
    def cap(n, driver):
        return "ORDER 880140 partial screen"

    def partial_preview(text):
        return {"order_number": "880140", "item_count": 0, "total_units": 0, "is_last_page": False}

    r = auto_scanner.run_scan_step(None, start=880140, capture_fn=cap, preview_fn=partial_preview)
    assert r["action"] == "not_found"
    assert scanned_store.next_scan_number(880140) == 880140  # cursor untouched


def test_wait_for_empty_skipped_moves_on_quickly():
    assert auto_scanner._wait_for("empty_skipped") == auto_scanner.FOUND_NEXT_DELAY_SEC


# --- AS400 health beacon (UI dot) -----------------------------------------------


def test_health_unknown_before_any_signal(monkeypatch):
    monkeypatch.setitem(auto_scanner._as400_health, "ok", None)
    monkeypatch.setitem(auto_scanner._as400_health, "at", 0.0)
    assert auto_scanner.as400_health()["state"] == "unknown"


def test_health_ok_after_recent_success(monkeypatch):
    auto_scanner.note_as400(True)
    assert auto_scanner.as400_health()["state"] == "ok"
    auto_scanner.note_as400(False)
    assert auto_scanner.as400_health()["state"] == "err"


def test_health_degrades_to_unknown_when_stale(monkeypatch):
    import time

    auto_scanner.note_as400(True)
    monkeypatch.setitem(
        auto_scanner._as400_health, "at", time.time() - auto_scanner.AS400_HEALTH_MAX_AGE_SEC - 1
    )
    assert auto_scanner.as400_health()["state"] == "unknown"
