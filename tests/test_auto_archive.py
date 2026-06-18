"""
Tests for auto-archiving stale UNSENT candidate orders (older than
AUTO_ARCHIVE_DAYS) when /api/orders is served. Sent orders are never swept.
"""

import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import app as appmod  # noqa: E402

HDR = {"Host": f"localhost:{appmod.PORT}"}

ORDER_TEXT = """                            O R D E R   I N Q U I R Y
 Order Number: 880009                       Account Number: 0003574 00
 Quant  Quant  Stock #   W/H   Description                       Unit    Extend
   1     1  03 3768 BL  N   DIVIDE S/O 12X27 2025 RIPTIDE   394.95    394.95
                                END OF ORDER                             394.95"""


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(appmod, "ARCHIVE_PATH", tmp_path / "arch.json")
    appmod._archive.clear()
    appmod._orders.clear()
    appmod._next_id = 1
    appmod.app.testing = True
    return appmod.app.test_client()


def _days_ago(n):
    return (datetime.now(timezone.utc) - timedelta(days=n)).isoformat()


def test_stale_unsent_order_is_auto_archived(client):
    entry = appmod._add_order(ORDER_TEXT)
    entry["scanned_at"] = _days_ago(9)  # older than the 8-day threshold

    r = client.get("/api/orders", headers=HDR)
    assert r.status_code == 200
    assert entry["id"] not in appmod._orders  # swept out of the active list
    assert len(appmod._archive) == 1  # recoverable in the archive
    assert next(iter(appmod._archive.values()))["order_number"] == "880009"


def test_recent_unsent_order_is_not_archived(client):
    entry = appmod._add_order(ORDER_TEXT)
    entry["scanned_at"] = _days_ago(3)  # well within the window

    client.get("/api/orders", headers=HDR)
    assert entry["id"] in appmod._orders
    assert len(appmod._archive) == 0


def test_sent_order_is_not_auto_archived(client):
    entry = appmod._add_order(ORDER_TEXT)
    entry["scanned_at"] = _days_ago(30)  # very old…
    entry["sent"] = True  # …but already sent → must NOT be swept

    client.get("/api/orders", headers=HDR)
    assert entry["id"] in appmod._orders
    assert len(appmod._archive) == 0
