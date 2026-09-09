"""
Tests that create_order writes the AS400 'Order Date' into the additive
picking_lists.source_order_date column when present (and omits it otherwise),
and that the optional group_id kwarg is included/omitted the same way.

Uses mocks for the Supabase client to avoid any DB dependency.
"""

import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


def _run_create(order_data, group_id=None):
    """Call create_order with everything mocked and return the insert payload."""
    from supabase_client import create_order

    mock_table = MagicMock()
    mock_table.insert.return_value.execute.return_value.data = [{"id": "uuid-1"}]

    with (
        patch("supabase_client.get_client") as mock_client,
        patch("supabase_client._to_cart_items", return_value=[{"sku": "X", "pickingQty": 1}]),
        patch("supabase_client._resolve_customer", return_value=None),
        patch("supabase_client._log_import"),
    ):
        mock_client.return_value.table.return_value = mock_table
        create_order(order_data, "hash123", "test_source", group_id=group_id)

    mock_table.insert.assert_called_once()
    return mock_table.insert.call_args[0][0]


def test_source_order_date_in_payload_when_present():
    payload = _run_create(
        {"order_number": "880009", "items": [{"sku": "X", "qty": 1}], "order_date": "2026-06-02"}
    )
    assert payload["source_order_date"] == "2026-06-02"


def test_source_order_date_omitted_when_absent():
    payload = _run_create({"order_number": "880009", "items": [{"sku": "X", "qty": 1}]})
    assert "source_order_date" not in payload


def test_source_order_date_omitted_when_none():
    payload = _run_create(
        {"order_number": "880009", "items": [{"sku": "X", "qty": 1}], "order_date": None}
    )
    assert "source_order_date" not in payload


def test_group_id_in_payload_when_provided():
    payload = _run_create(
        {"order_number": "880009", "items": [{"sku": "X", "qty": 1}]}, group_id="group-uuid"
    )
    assert payload["group_id"] == "group-uuid"


def test_group_id_omitted_when_absent():
    payload = _run_create({"order_number": "880009", "items": [{"sku": "X", "qty": 1}]})
    assert "group_id" not in payload


# ── picking_lists.source says where the text came from ───────────────────────


def test_source_is_as400_for_every_as400_path():
    # The Bay 2 UI, the folder watcher reusing a cached capture, and the door.
    from supabase_client import source_for

    assert source_for("as400_app") == "as400"
    assert source_for("scanned:881390") == "as400"
    assert source_for("pickd_door:881390") == "as400"


def test_source_stays_pdf_import_for_a_real_pdf():
    # Until 2026-09-08 everything said pdf_import; a dropped PDF still should.
    from supabase_client import source_for

    assert source_for("880300 MATTHEWS.pdf") == "pdf_import"
    assert source_for("") == "pdf_import"
    assert source_for(None) == "pdf_import"
