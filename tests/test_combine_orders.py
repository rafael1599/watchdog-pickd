"""
Tests for the auto-combine orders by customer feature.

Tests the logic in supabase_client.py:
- combine_into_order: links a new order to an existing one via group_id
  (order_groups insert + picking_lists.group_id), never mutating the target row
- find_combinable_order_by_customer: status filtering, 24h cutoff

Uses mocks for Supabase client to avoid DB dependency.
"""

import os
import sys
from unittest.mock import MagicMock, patch

# Add parent dir to path so we can import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


# ---------- combine_into_order tests (unit, mocked DB) ----------


class TestCombineIntoOrder:
    """Test the combine_into_order function logic."""

    @patch("supabase_client.create_order")
    @patch("supabase_client.get_client")
    def test_target_without_group_id_creates_group_and_links_it(
        self, mock_client, mock_create_order
    ):
        """No group_id on target: create an order_groups row, link target to it,
        and create the new order already carrying that group_id."""
        from supabase_client import combine_into_order

        target_order = {"id": "uuid-1", "order_number": "878279", "status": "ready_to_double_check"}
        new_order_data = {"order_number": "878280", "items": [{"sku": "A", "qty": 1}]}

        groups_table = MagicMock()
        groups_table.insert.return_value.execute.return_value.data = [{"id": "group-uuid"}]
        lists_table = MagicMock()

        def table(name):
            return groups_table if name == "order_groups" else lists_table

        mock_client.return_value.table.side_effect = table
        mock_create_order.return_value = {"id": "uuid-2", "order_number": "878280"}

        combine_into_order(target_order, new_order_data, "hash", "f.pdf")

        groups_table.insert.assert_called_once_with({"group_type": "general"})
        lists_table.update.assert_called_once_with({"group_id": "group-uuid"})
        lists_table.update.return_value.eq.assert_called_once_with("id", "uuid-1")
        mock_create_order.assert_called_once_with(
            new_order_data, "hash", "f.pdf", group_id="group-uuid"
        )

    @patch("supabase_client.create_order")
    @patch("supabase_client.get_client")
    def test_target_with_group_id_reuses_it(self, mock_client, mock_create_order):
        """Target already has a group_id: no new group, no update — just reuse it."""
        from supabase_client import combine_into_order

        target_order = {
            "id": "uuid-1",
            "order_number": "878279",
            "status": "ready_to_double_check",
            "group_id": "existing-group",
        }
        new_order_data = {"order_number": "878280", "items": [{"sku": "A", "qty": 1}]}

        mock_table = MagicMock()
        mock_client.return_value.table.return_value = mock_table
        mock_create_order.return_value = {"id": "uuid-2", "order_number": "878280"}

        combine_into_order(target_order, new_order_data, "hash", "f.pdf")

        mock_table.insert.assert_not_called()
        mock_table.update.assert_not_called()
        mock_create_order.assert_called_once_with(
            new_order_data, "hash", "f.pdf", group_id="existing-group"
        )

    @patch("supabase_client.create_order")
    @patch("supabase_client.get_client")
    def test_target_row_items_and_status_never_written(self, mock_client, mock_create_order):
        """The target row's items/status/checked_by must never be part of any write."""
        from supabase_client import combine_into_order

        target_order = {
            "id": "uuid-1",
            "order_number": "878279",
            "status": "double_checking",
            "checked_by": "checker-uuid",
            "items": [{"sku": "X", "pickingQty": 1}],
        }
        new_order_data = {"order_number": "878280", "items": [{"sku": "A", "qty": 1}]}

        groups_table = MagicMock()
        groups_table.insert.return_value.execute.return_value.data = [{"id": "group-uuid"}]
        lists_table = MagicMock()
        mock_client.return_value.table.side_effect = lambda name: (
            groups_table if name == "order_groups" else lists_table
        )
        mock_create_order.return_value = {"id": "uuid-2", "order_number": "878280"}

        combine_into_order(target_order, new_order_data, "hash", "f.pdf")

        update_payload = lists_table.update.call_args[0][0]
        assert update_payload == {"group_id": "group-uuid"}
        for key in ("items", "status", "checked_by", "order_number", "combine_meta"):
            assert key not in update_payload

    @patch("supabase_client.create_order")
    @patch("supabase_client.get_client")
    def test_returns_create_order_result(self, mock_client, mock_create_order):
        """combine_into_order returns whatever create_order returns, unmodified."""
        from supabase_client import combine_into_order

        target_order = {"id": "uuid-1", "order_number": "878279", "group_id": "g1"}
        new_order_data = {"order_number": "878280", "items": [{"sku": "A", "qty": 1}]}
        mock_create_order.return_value = {
            "id": "uuid-2",
            "order_number": "878280",
            "items": [{"sku": "A", "pickingQty": 1}],
        }

        result = combine_into_order(target_order, new_order_data, "hash", "f.pdf")

        assert result == mock_create_order.return_value


# ---------- find_combinable_order_by_customer tests ----------


class TestFindCombinableOrderByCustomer:
    """Test the query logic for finding combinable orders."""

    @patch("supabase_client.get_client")
    def test_finds_order_for_same_customer(self, mock_client):
        from supabase_client import find_combinable_order_by_customer

        mock_query = MagicMock()
        # Chain all query methods
        mock_query.select.return_value = mock_query
        mock_query.eq.return_value = mock_query
        mock_query.in_.return_value = mock_query
        mock_query.or_.return_value = mock_query
        mock_query.gte.return_value = mock_query
        mock_query.order.return_value = mock_query
        mock_query.limit.return_value = mock_query
        mock_query.neq.return_value = mock_query
        mock_query.execute.return_value.data = [{"id": "uuid-1", "order_number": "878279"}]

        mock_client.return_value.table.return_value = mock_query

        result = find_combinable_order_by_customer("customer-uuid-1")
        assert result is not None
        assert result["order_number"] == "878279"

    @patch("supabase_client.get_client")
    def test_returns_none_when_no_combinable_order(self, mock_client):
        from supabase_client import find_combinable_order_by_customer

        mock_query = MagicMock()
        mock_query.select.return_value = mock_query
        mock_query.eq.return_value = mock_query
        mock_query.in_.return_value = mock_query
        mock_query.or_.return_value = mock_query
        mock_query.gte.return_value = mock_query
        mock_query.order.return_value = mock_query
        mock_query.limit.return_value = mock_query
        mock_query.execute.return_value.data = []

        mock_client.return_value.table.return_value = mock_query

        result = find_combinable_order_by_customer("customer-uuid-1")
        assert result is None

    @patch("supabase_client.get_client")
    def test_excludes_specific_order_number(self, mock_client):
        from supabase_client import find_combinable_order_by_customer

        mock_query = MagicMock()
        mock_query.select.return_value = mock_query
        mock_query.eq.return_value = mock_query
        mock_query.in_.return_value = mock_query
        mock_query.or_.return_value = mock_query
        mock_query.gte.return_value = mock_query
        mock_query.order.return_value = mock_query
        mock_query.limit.return_value = mock_query
        mock_query.neq.return_value = mock_query
        mock_query.execute.return_value.data = []

        mock_client.return_value.table.return_value = mock_query

        find_combinable_order_by_customer("customer-uuid-1", exclude_order_number="878279")

        # Verify neq was called to exclude the order
        mock_query.neq.assert_called_once_with("order_number", "878279")

    @patch("supabase_client.get_client")
    def test_queries_only_combinable_statuses(self, mock_client):
        from supabase_client import COMBINABLE_STATUSES, find_combinable_order_by_customer

        mock_query = MagicMock()
        mock_query.select.return_value = mock_query
        mock_query.eq.return_value = mock_query
        mock_query.in_.return_value = mock_query
        mock_query.or_.return_value = mock_query
        mock_query.gte.return_value = mock_query
        mock_query.order.return_value = mock_query
        mock_query.limit.return_value = mock_query
        mock_query.execute.return_value.data = []

        mock_client.return_value.table.return_value = mock_query

        find_combinable_order_by_customer("customer-uuid-1")

        # Verify in_ was called with combinable statuses
        mock_query.in_.assert_called_once_with("status", COMBINABLE_STATUSES)
        assert "completed" not in COMBINABLE_STATUSES
        assert "cancelled" not in COMBINABLE_STATUSES
