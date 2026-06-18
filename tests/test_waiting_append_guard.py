"""
A re-send (same order number) with NEW SKUs must never auto-append into an
order parked WAITING FOR INVENTORY (is_waiting_inventory = true): waiting
orders are off-limits for every automatic write, same operator rule as the
customer auto-combine exclusion. The pipeline returns status="waiting_locked"
without writing anything, and /api/orders/<id>/send surfaces it as a 409 so
the card stays pending. DB calls are mocked.
"""

import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import app as appmod  # noqa: E402
import pipeline  # noqa: E402

HDR = {"Host": f"localhost:{appmod.PORT}"}

CAPTURE = """                            O R D E R   I N Q U I R Y
 Order Number: 880092                       Account Number: 0000991 00
 Quant  Quant  Stock #   W/H   Description                       Unit    Extend
   1     1  03 3927 BK  N   CODA S2 21 2025 GLOSS BLACK     428.95    428.95
   1     1  01 0449     N   S/D ALLEGRO A3 15 INK           268.95    268.95
                                END OF ORDER                             697.90"""

WAITING_EXISTING = {
    "id": "uuid-1",
    "order_number": "880092",
    "status": "needs_correction",
    "is_waiting_inventory": True,
    "items": [{"sku": "03-3927BK", "raw_sku": "03 3927 BK", "pickingQty": 1}],
}

DELTA = [
    {
        "sku": "010449",
        "qty": 1,
        "raw_sku": "01 0449",
        "description": "S/D ALLEGRO A3 15 INK",
        "extend_price": 268.95,
    }
]


def test_resend_with_new_skus_never_appends_into_waiting_order():
    with (
        patch("pipeline.check_duplicate", return_value=None),
        patch("pipeline.find_existing_order", return_value=WAITING_EXISTING),
        patch("pipeline.get_client", return_value=MagicMock()),
        patch("pipeline.get_new_items_delta", return_value=DELTA),
        patch("pipeline.append_to_order") as mock_append,
        patch("pipeline.reopen_completed_order") as mock_reopen,
        patch("pipeline.create_order") as mock_create,
    ):
        result = pipeline.process_order_text(CAPTURE)

    assert result["status"] == "waiting_locked"
    assert "WAITING" in result["message"]
    assert "880092" in result["message"]
    assert "010449" in result["message"]  # the operator sees WHAT was not added
    mock_append.assert_not_called()
    mock_reopen.assert_not_called()
    mock_create.assert_not_called()


def test_resend_with_no_new_skus_into_waiting_is_a_plain_duplicate():
    with (
        patch("pipeline.check_duplicate", return_value=None),
        patch("pipeline.find_existing_order", return_value=WAITING_EXISTING),
        patch("pipeline.get_client", return_value=MagicMock()),
        patch("pipeline.get_new_items_delta", return_value=[]),
        patch("pipeline.append_to_order") as mock_append,
    ):
        result = pipeline.process_order_text(CAPTURE)

    assert result["status"] == "duplicate"  # harmless no-op either way
    mock_append.assert_not_called()


def test_non_waiting_order_still_auto_appends():
    existing = {**WAITING_EXISTING, "is_waiting_inventory": False}
    with (
        patch("pipeline.check_duplicate", return_value=None),
        patch("pipeline.find_existing_order", return_value=existing),
        patch("pipeline.get_client", return_value=MagicMock()),
        patch("pipeline.get_new_items_delta", return_value=DELTA),
        patch(
            "pipeline.append_to_order",
            return_value={"order_number": "880092", "items": [{"sku": "01-0449"}]},
        ) as mock_append,
    ):
        result = pipeline.process_order_text(CAPTURE)

    assert result["status"] == "appended"
    mock_append.assert_called_once()


def test_send_endpoint_returns_409_and_keeps_the_card_pending(tmp_path, monkeypatch):
    monkeypatch.setattr(appmod, "ARCHIVE_PATH", tmp_path / "arch.json")
    appmod._archive.clear()
    appmod._orders.clear()
    appmod._next_id = 1
    appmod.app.testing = True
    client = appmod.app.test_client()

    entry = appmod._add_order(CAPTURE)
    locked = {
        "status": "waiting_locked",
        "order_number": "880092",
        "customer": "Unknown",
        "item_count": 0,
        "needs_correction": False,
        "picking_list": None,
        "message": "Order #880092 is WAITING FOR INVENTORY in PickD — 1 new item(s) NOT added automatically (010449).",
    }
    with patch("app.process_order_text", return_value=locked):
        r = client.post(f"/api/orders/{entry['id']}/send", headers=HDR)

    assert r.status_code == 409
    assert "WAITING" in r.get_json()["error"]
    assert appmod._orders[entry["id"]]["sent"] is False  # card stays pending
