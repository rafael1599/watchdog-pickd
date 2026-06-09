"""Tests for the auto-scanner's per-cycle catch-up logic (run_scan_cycle).

Pure logic only: a fake capture_fn decides which order numbers "exist", and a stub
preview_fn avoids the Supabase/pipeline import. No Mocha, no DB.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import auto_scanner  # noqa: E402
import scanned_store  # noqa: E402
from as400_capture import AS400Disconnected, CaptureError  # noqa: E402


@pytest.fixture(autouse=True)
def tmp_store(tmp_path, monkeypatch):
    monkeypatch.setenv("SCANNED_STORE_PATH", str(tmp_path / "scanned.json"))


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


def _capture_for(existing):
    def cap(n, driver):
        if str(n) in existing:
            return f"ORDER {n} END OF ORDER"
        raise CaptureError(f"order {n} doesn't exist")

    return cap


def test_catches_up_then_stops_at_gap():
    cap = _capture_for({"880112", "880113", "880114"})
    r = auto_scanner.run_scan_cycle(None, start=880112, capture_fn=cap, preview_fn=_preview)
    assert r["scanned"] == ["880112", "880113", "880114"]
    assert r["stopped"] == "no_more_orders"
    # The cursor holds at the first missing number, retried next cycle.
    assert scanned_store.next_scan_number(880112) == 880115


def test_does_not_advance_when_first_is_missing():
    cap = _capture_for(set())
    r = auto_scanner.run_scan_cycle(None, start=880112, capture_fn=cap, preview_fn=_preview)
    assert r["scanned"] == []
    assert scanned_store.next_scan_number(880112) == 880112


def test_respects_max_per_cycle():
    cap = _capture_for({str(n) for n in range(880112, 880200)})
    r = auto_scanner.run_scan_cycle(
        None, start=880112, max_per_cycle=3, capture_fn=cap, preview_fn=_preview
    )
    assert r["scanned"] == ["880112", "880113", "880114"]
    assert r["stopped"] == "max_per_cycle"


def test_yields_to_manual():
    cap = _capture_for({str(n) for n in range(880112, 880200)})
    calls = {"n": 0}

    def should_continue():
        calls["n"] += 1
        return calls["n"] <= 2  # let two orders through, then a manual capture waits

    r = auto_scanner.run_scan_cycle(
        None, start=880112, capture_fn=cap, preview_fn=_preview, should_continue=should_continue
    )
    assert r["stopped"] == "yield_to_manual"
    assert r["scanned"] == ["880112", "880113"]


def test_stops_on_disconnect_without_advancing():
    def cap(n, driver):
        raise AS400Disconnected("host down")

    r = auto_scanner.run_scan_cycle(None, start=880112, capture_fn=cap, preview_fn=_preview)
    assert r["stopped"] == "as400_unavailable"
    assert r["scanned"] == []
    assert scanned_store.next_scan_number(880112) == 880112
