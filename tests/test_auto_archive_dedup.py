"""
Regression tests for the auto-archive runaway loop.

A stale UNSENT order must be archived exactly ONCE and removed from the scan
cache, so the UI's periodic /api/orders polls don't re-materialize and re-archive
it. The old behavior re-archived the same ~18 orders on every poll and grew the
local archive to 48 MB / 9,210 entries.
"""

import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import app as appmod  # noqa: E402
import scanned_store  # noqa: E402

HDR = {"Host": f"localhost:{appmod.PORT}"}

ORDER_TEXT = """                            O R D E R   I N Q U I R Y
 Order Number: 880009                       Account Number: 0003574 00
 Quant  Quant  Stock #   W/H   Description                       Unit    Extend
   1     1  03 3768 BL  N   DIVIDE S/O 12X27 2025 RIPTIDE   394.95    394.95
                                END OF ORDER                             394.95"""


@pytest.fixture
def client(tmp_path, monkeypatch):
    # Isolate every on-disk store to tmp so the test never touches real state.
    monkeypatch.setattr(appmod, "ARCHIVE_PATH", tmp_path / "arch.json")
    monkeypatch.setenv("SCANNED_STORE_PATH", str(tmp_path / "scan.json"))
    monkeypatch.setenv("SCAN_CURSOR_PATH", str(tmp_path / "cursor"))
    appmod._archive.clear()
    appmod._orders.clear()
    appmod._next_id = 1
    appmod.app.testing = True
    return appmod.app.test_client()


def _days_ago(n):
    return (datetime.now(timezone.utc) - timedelta(days=n)).isoformat()


def test_stale_cached_order_archived_once_and_dropped_from_cache(client):
    # A stale order sitting in the scan cache (older than the 8-day window).
    scanned_store.put("880009", ORDER_TEXT, meta={"scanned_at": _days_ago(9)})

    # Poll the list several times — the UI does this every ~8s.
    for _ in range(5):
        assert client.get("/api/orders", headers=HDR).status_code == 200

    # Archived exactly once (not once per poll) …
    matches = [v for v in appmod._archive.values() if v.get("order_number") == "880009"]
    assert len(matches) == 1
    # … and gone from the cache, so it can't re-materialize and re-archive.
    assert scanned_store.get("880009") is None


def test_archive_entry_dedupes_by_order_number(client):
    entry = appmod._add_order(ORDER_TEXT)
    with appmod._lock:
        appmod._archive_entry(entry)
        appmod._archive_entry(entry)  # same order again → must not pile up
    matches = [v for v in appmod._archive.values() if v.get("order_number") == "880009"]
    assert len(matches) == 1
