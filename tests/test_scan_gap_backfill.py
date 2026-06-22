"""
Gap backfill contract (operator scenario, 2026-06-12): the auto-scanner had
captured up to order N when the operator manually captured an order ~5 ahead
(N+5) and sent it to PickD. The scanner must STILL scan N+1..N+4 — a manual
capture of an arbitrary number must never advance the scan position, not even
after the sent order is dropped from the cache. Protection shipped in #25;
these tests lock the contract.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import scanned_store  # noqa: E402

START = 880100


@pytest.fixture(autouse=True)
def tmp_store(tmp_path, monkeypatch):
    monkeypatch.setenv("SCANNED_STORE_PATH", str(tmp_path / "scanned.json"))
    monkeypatch.setenv("SCAN_CURSOR_PATH", str(tmp_path / "scan_cursor"))


def _auto(n):
    scanned_store.put(
        n, f"ORDER {n} END OF ORDER", {"order_number": str(n), "item_count": 1}, source="auto_scan"
    )


def test_manual_capture_ahead_never_advances_the_scan_position():
    _auto(START)  # scanner reached N → cursor at N+1
    assert scanned_store.next_scan_number(START) == START + 1

    # Operator captures an order 5 ahead via the capture box.
    scanned_store.put(
        START + 5,
        "ORDER 880105 END OF ORDER",
        {"order_number": str(START + 5), "item_count": 1},
        source="manual_capture",
    )
    assert scanned_store.next_scan_number(START) == START + 1  # gap preserved


def test_sending_the_manual_capture_keeps_the_gap():
    _auto(START)
    scanned_store.put(
        START + 5,
        "ORDER 880105 END OF ORDER",
        {"order_number": str(START + 5), "item_count": 1},
        source="manual_capture",
    )
    scanned_store.delete(START + 5)  # send() drops it from the cache

    assert scanned_store.next_scan_number(START) == START + 1  # still N+1


def test_scanner_backfills_the_gap_then_resumes_past_it():
    _auto(START)
    scanned_store.put(
        START + 5,
        "ORDER 880105 END OF ORDER",
        {"order_number": str(START + 5), "item_count": 1},
        source="manual_capture",
    )
    scanned_store.delete(START + 5)

    for n in range(START + 1, START + 5):  # the scanner walks N+1..N+4
        assert scanned_store.next_scan_number(START) == n
        _auto(n)

    # Gap closed: the scan position is now the manually-captured one. run_scan_step
    # skips it (already cached) instead of re-pulling it from AS400 — see
    # test_auto_scanner.test_step_backfills_gap_below_a_manual_capture_then_skips_it.
    assert scanned_store.next_scan_number(START) == START + 5


def test_auto_scans_do_advance_the_position():
    _auto(START)
    _auto(START + 1)
    assert scanned_store.next_scan_number(START) == START + 2
