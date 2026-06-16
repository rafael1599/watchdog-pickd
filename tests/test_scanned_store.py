"""Tests for the local scanned-orders cache (scanned_store)."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import scanned_store  # noqa: E402


@pytest.fixture(autouse=True)
def tmp_store(tmp_path, monkeypatch):
    monkeypatch.setenv("SCANNED_STORE_PATH", str(tmp_path / "scanned.json"))
    monkeypatch.setenv("SCAN_CURSOR_PATH", str(tmp_path / "scan_cursor"))


def test_cursor_advances_on_auto_scan_and_holds_through_delete():
    scanned_store.put("880112", "a", source="auto_scan")
    scanned_store.put("880113", "b", source="auto_scan")
    assert scanned_store.next_scan_number(880112) == 880114
    # Archiving/removing the latest must NOT rewind the scanner.
    scanned_store.delete("880113")
    assert scanned_store.next_scan_number(880112) == 880114


def test_manual_capture_does_not_advance_cursor():
    scanned_store.put("880200", "x", source="manual_capture")
    assert scanned_store.next_scan_number(880112) == 880112


def test_put_and_get():
    assert scanned_store.get("880112") is None
    scanned_store.put("880112", "RAW TEXT", {"customer": "X", "item_count": 3})
    e = scanned_store.get("880112")
    assert e["raw_text"] == "RAW TEXT"
    assert e["customer"] == "X"
    assert e["source"] == "auto_scan"
    assert "scanned_at" in e


def test_next_scan_number_empty_is_start():
    assert scanned_store.next_scan_number(880112) == 880112


def test_next_scan_number_advances_past_highest():
    scanned_store.put("880112", "a")
    scanned_store.put("880113", "b")
    assert scanned_store.next_scan_number(880112) == 880114


def test_next_scan_number_ignores_below_start_and_nonnumeric():
    scanned_store.put("123", "a")  # below start
    scanned_store.put("ABC", "b")  # non-numeric (e.g. a negative-counter order)
    assert scanned_store.next_scan_number(880112) == 880112


def test_count():
    scanned_store.put("880112", "a")
    scanned_store.put("880113", "b")
    assert scanned_store.count() == 2


def test_search_blank_returns_empty():
    scanned_store.put("880112", "a", {"customer": "ACME"})
    assert scanned_store.search("") == []
    assert scanned_store.search("   ") == []


def test_search_matches_by_number_and_customer():
    scanned_store.put("880112", "a", {"customer": "ACME BIKES", "item_count": 3})
    scanned_store.put("880113", "b", {"customer": "Globex", "item_count": 1})
    # Partial order number matches both.
    nums = {h["order_number"] for h in scanned_store.search("8801")}
    assert nums == {"880112", "880113"}
    # Customer match is case-insensitive and carries the lightweight meta.
    hits = scanned_store.search("acme")
    assert len(hits) == 1
    assert hits[0]["order_number"] == "880112"
    assert hits[0]["item_count"] == 3
    # Never leaks the full AS400 capture text to the client.
    assert "raw_text" not in hits[0]


def test_search_excludes_non_matches():
    scanned_store.put("880112", "a", {"customer": "ACME"})
    assert scanned_store.search("999") == []


def test_search_newest_first_and_limit():
    scanned_store.put("880100", "a", {"customer": "C", "scanned_at": "2026-06-01T00:00:00Z"})
    scanned_store.put("880200", "b", {"customer": "C", "scanned_at": "2026-06-10T00:00:00Z"})
    hits = scanned_store.search("C")
    assert [h["order_number"] for h in hits] == ["880200", "880100"]
    assert scanned_store.search("C", limit=1) == hits[:1]
