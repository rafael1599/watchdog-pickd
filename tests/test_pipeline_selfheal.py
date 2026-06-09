"""
Tests for the self-healing re-send in pipeline.process_order_text.

A re-send of the IDENTICAL capture (same content hash) must still top up an
existing order with SKUs that a newer parser now extracts — instead of bailing
on the content-hash duplicate check. It must NOT recreate or combine an order
whose content was already processed. DB calls are mocked.
"""

import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pipeline  # noqa: E402

# Two parseable item lines; enough for process_order_text to reach the
# existing-order branch (get_new_items_delta is mocked, so its real diffing
# logic is not under test here).
CAPTURE = """                            O R D E R   I N Q U I R Y
 Order Number: 880092                       Account Number: 0000991 00
 Quant  Quant  Stock #   W/H   Description                       Unit    Extend
   1     1  03 3927 BK  N   CODA S2 21 2025 GLOSS BLACK     428.95    428.95
   1     1  01 0449     N   S/D ALLEGRO A3 15 INK           268.95    268.95
                                END OF ORDER                             697.90"""

DUP_LOG = {"processed_at": "2026-06-09T13:08:47Z", "order_number": "880092"}

EXISTING = {
    "id": "uuid-1",
    "order_number": "880092",
    "status": "ready_to_double_check",
    "items": [{"sku": "03-3927BK", "raw_sku": "03 3927 BK", "pickingQty": 1}],
}


def test_resend_appends_missing_skus_despite_duplicate_hash():
    delta = [
        {
            "sku": "010449",
            "qty": 1,
            "raw_sku": "01 0449",
            "description": "S/D ALLEGRO A3 15 INK",
            "extend_price": 268.95,
        }
    ]
    with (
        patch("pipeline.check_duplicate", return_value=DUP_LOG),
        patch("pipeline.find_existing_order", return_value=EXISTING),
        patch("pipeline.get_client", return_value=MagicMock()),
        patch("pipeline.get_new_items_delta", return_value=delta),
        patch(
            "pipeline.append_to_order",
            return_value={
                "order_number": "880092",
                "items": EXISTING["items"] + [{"sku": "01-0449"}],
            },
        ) as mock_append,
    ):
        result = pipeline.process_order_text(CAPTURE)

    assert result["status"] == "appended"
    mock_append.assert_called_once()


def test_resend_with_no_new_skus_is_duplicate():
    with (
        patch("pipeline.check_duplicate", return_value=DUP_LOG),
        patch("pipeline.find_existing_order", return_value=EXISTING),
        patch("pipeline.get_client", return_value=MagicMock()),
        patch("pipeline.get_new_items_delta", return_value=[]),
        patch("pipeline.append_to_order") as mock_append,
    ):
        result = pipeline.process_order_text(CAPTURE)

    assert result["status"] == "duplicate"
    mock_append.assert_not_called()


def test_duplicate_content_without_existing_order_is_not_recreated():
    # Content seen before but the order no longer exists → report duplicate,
    # never silently recreate or combine it.
    with (
        patch("pipeline.check_duplicate", return_value=DUP_LOG),
        patch("pipeline.find_existing_order", return_value=None),
        patch("pipeline.create_order") as mock_create,
        patch("pipeline.combine_into_order") as mock_combine,
    ):
        result = pipeline.process_order_text(CAPTURE)

    assert result["status"] == "duplicate"
    mock_create.assert_not_called()
    mock_combine.assert_not_called()
