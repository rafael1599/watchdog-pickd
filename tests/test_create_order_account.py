"""
Tests that create_order carries the AS400 account through: the raw header value
lands in picking_lists.as400_account_number, the ship-to row returned by
_save_shipping_address lands in ship_to_address_id, the split account reaches
_resolve_customer and the suffix reaches _save_shipping_address. Both columns are
OMITTED (never written as NULL) when the capture has no account.

Modeled on test_create_order_source_date.py — mocks only, no DB.
"""

import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

SHIP = {
    "name": "TUCKER CYCLES",
    "street": "3544 ST JOHNS AVE",
    "city": "JACKSONVILLE",
    "state": "FL",
    "zip_code": "32205",
}


def _run_create(order_data, address_id="addr-uuid"):
    """Call create_order fully mocked; return (insert payload, resolve mock, save mock)."""
    from supabase_client import create_order

    mock_table = MagicMock()
    mock_table.insert.return_value.execute.return_value.data = [{"id": "uuid-1"}]

    with (
        patch("supabase_client.get_client") as mock_client,
        patch("supabase_client._to_cart_items", return_value=[{"sku": "X", "pickingQty": 1}]),
        patch("supabase_client._resolve_customer", return_value="cust-uuid") as resolve,
        patch("supabase_client._save_shipping_address", return_value=address_id) as save,
        patch("supabase_client._log_import"),
    ):
        mock_client.return_value.table.return_value = mock_table
        create_order(order_data, "hash123", "test_source")

    mock_table.insert.assert_called_once()
    return mock_table.insert.call_args[0][0], resolve, save


def _order(**extra):
    return {
        "order_number": "880036",
        "customer_name": "TUCKER CYCLES",
        "shipping": SHIP,
        "items": [{"sku": "X", "qty": 1}],
        **extra,
    }


def test_account_and_ship_to_address_in_payload():
    payload, resolve, save = _run_create(
        _order(account_number="0010495 00", as400_account="10495", as400_ship_to="00")
    )
    assert payload["as400_account_number"] == "0010495 00"  # raw, for audit
    assert payload["ship_to_address_id"] == "addr-uuid"
    assert payload["customer_id"] == "cust-uuid"
    assert resolve.call_args.kwargs["account"] == "10495"
    assert save.call_args.args[1:3] == ("cust-uuid", SHIP)
    assert save.call_args.kwargs["ship_to"] == "00"


def test_keys_omitted_without_account():
    payload, resolve, save = _run_create(_order(), address_id=None)
    assert "as400_account_number" not in payload
    assert "ship_to_address_id" not in payload
    assert resolve.call_args.kwargs["account"] is None
    assert save.call_args.kwargs["ship_to"] is None


def test_account_number_omitted_when_empty_string():
    payload, _, _ = _run_create(_order(account_number="", as400_account=None), address_id=None)
    assert "as400_account_number" not in payload


def test_ship_to_address_id_omitted_when_save_returns_none():
    # _save_shipping_address is non-blocking and returns None on failure: the
    # order is still created, with the raw header but without the link.
    payload, _, _ = _run_create(
        _order(account_number="0010495 00", as400_account="10495", as400_ship_to="00"),
        address_id=None,
    )
    assert payload["as400_account_number"] == "0010495 00"
    assert "ship_to_address_id" not in payload


def test_no_customer_means_no_address_and_no_link():
    payload, _, save = _run_create(
        {
            "order_number": "880036",
            "items": [{"sku": "X", "qty": 1}],
            "account_number": "0010495 00",
            "as400_account": "10495",
            "as400_ship_to": "00",
        }
    )
    save.assert_not_called()
    assert payload["as400_account_number"] == "0010495 00"
    assert "ship_to_address_id" not in payload
