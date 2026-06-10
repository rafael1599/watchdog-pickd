"""
Tests for the shared "does this order already exist in PickD?" logic in
supabase_client: split_order_numbers, the combined-membership fallback in
find_existing_order, and the batched find_orders_in_pickd. Mocked Supabase.

This is plain Python code (not a DB function): both the send pipeline and the
watcher UI call these helpers, so combined orders ("880106 / 880107") match the
same way everywhere.
"""

import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from supabase_client import (  # noqa: E402
    find_existing_order,
    find_orders_in_pickd,
    split_order_numbers,
)


class _R:
    def __init__(self, data):
        self.data = data


def test_split_order_numbers():
    assert split_order_numbers("880106 / 880107") == ["880106", "880107"]
    assert split_order_numbers("880106") == ["880106"]
    assert split_order_numbers(None) == []
    assert split_order_numbers("  ") == []


@patch("supabase_client.get_client")
def test_find_existing_order_exact_match_first(mock_client):
    execute = mock_client.return_value.table.return_value.select.return_value
    execute.eq.return_value.order.return_value.limit.return_value.execute.return_value = _R(
        [{"id": "u1", "order_number": "880107"}]
    )
    row = find_existing_order("880107")
    assert row["id"] == "u1"


@patch("supabase_client.get_client")
def test_find_existing_order_matches_combined_membership(mock_client):
    sel = mock_client.return_value.table.return_value.select.return_value
    # eq lookup finds nothing; the LIKE fallback returns the combined order.
    sel.eq.return_value.order.return_value.limit.return_value.execute.return_value = _R([])
    sel.like.return_value.order.return_value.limit.return_value.execute.return_value = _R(
        [{"id": "u2", "order_number": "880106 / 880107"}]
    )
    row = find_existing_order("880107")
    assert row["id"] == "u2"


@patch("supabase_client.get_client")
def test_find_existing_order_rejects_substring_false_positive(mock_client):
    sel = mock_client.return_value.table.return_value.select.return_value
    sel.eq.return_value.order.return_value.limit.return_value.execute.return_value = _R([])
    # LIKE %880107% also matches '1880107' — membership check must reject it.
    sel.like.return_value.order.return_value.limit.return_value.execute.return_value = _R(
        [{"id": "u3", "order_number": "1880107"}]
    )
    assert find_existing_order("880107") is None


@patch("supabase_client.get_client")
def test_find_orders_in_pickd_exact_and_combined(mock_client):
    sel = mock_client.return_value.table.return_value.select.return_value
    sel.gte.return_value.execute.return_value = _R(
        [
            {"order_number": "880105", "status": "completed"},
            {"order_number": "880106 / 880107", "status": "active"},
            {"order_number": "880108", "status": "cancelled"},  # re-orderable → not counted
        ]
    )
    found = find_orders_in_pickd(["880105", "880107", "880108", "880199"])
    assert found == {"880105", "880107"}


def test_find_orders_in_pickd_empty_input_skips_query():
    # Must not even build a client for an empty candidate list.
    with patch("supabase_client.get_client") as mock_client:
        assert find_orders_in_pickd([]) == set()
        mock_client.assert_not_called()
