"""
Send to PickD double-click protection: the UI locks the button with a spinner,
and the server refuses a second send while one is in flight for the same order
(two parallel sends would race process_order_text).
"""

import os
import sys
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import app as appmod  # noqa: E402

HDR = {"Host": "localhost:5000"}

ORDER_TEXT = """                            O R D E R   I N Q U I R Y
 Order Number: 880300                       Account Number: 0000991 00
 Quant  Quant  Stock #   W/H   Description                       Unit    Extend
   1     1  03 3927 BK  N   CODA S2 21 2025 GLOSS BLACK     428.95    428.95
                                END OF ORDER                             428.95"""


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(appmod, "ARCHIVE_PATH", tmp_path / "arch.json")
    monkeypatch.setenv("SCANNED_STORE_PATH", str(tmp_path / "scanned.json"))
    monkeypatch.setenv("SCAN_CURSOR_PATH", str(tmp_path / "cursor"))
    appmod._archive.clear()
    appmod._orders.clear()
    appmod._sending.clear()
    appmod._next_id = 1
    appmod.app.testing = True
    return appmod.app.test_client()


def test_concurrent_send_is_rejected_while_in_flight(client):
    entry = appmod._add_order(ORDER_TEXT)
    appmod._sending.add(entry["id"])  # a send is mid-flight

    r = client.post(f"/api/orders/{entry['id']}/send", headers=HDR)

    assert r.status_code == 409
    assert "in progress" in r.get_json()["error"]


def test_in_flight_marker_is_cleared_after_a_send(client):
    entry = appmod._add_order(ORDER_TEXT)
    ok = {
        "status": "created",
        "order_number": "880300",
        "customer": "C",
        "item_count": 1,
        "needs_correction": False,
        "picking_list": None,
        "message": "Order #880300 (1 items).",
    }
    with patch("app.process_order_text", return_value=ok):
        r = client.post(f"/api/orders/{entry['id']}/send", headers=HDR)

    assert r.status_code == 200
    assert entry["id"] not in appmod._sending  # released for future actions


def test_in_flight_marker_is_cleared_even_on_failure(client):
    entry = appmod._add_order(ORDER_TEXT)
    with patch("app.process_order_text", side_effect=RuntimeError("boom")):
        r = client.post(f"/api/orders/{entry['id']}/send", headers=HDR)

    assert r.status_code == 500
    assert entry["id"] not in appmod._sending


def test_ui_send_button_locks_with_a_spinner():
    html = appmod.INDEX_HTML
    assert 'id="send-${o.id}"' in html
    assert 'class="spin"' in html
    assert "@keyframes spin" in html
