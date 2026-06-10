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
