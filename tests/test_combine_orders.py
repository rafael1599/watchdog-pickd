"""
Tests for the auto-combine orders by customer feature.

Tests the logic in supabase_client.py:
- combine_into_order: item tagging, merging, order number concatenation, combine_meta
- find_combinable_order_by_customer: status filtering, 24h cutoff

Uses mocks for Supabase client to avoid DB dependency.
"""

import sys
import os
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone, timedelta

# Add parent dir to path so we can import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


# ---------- combine_into_order tests (unit, mocked DB) ----------

class TestCombineIntoOrder:
    """Test the combine_into_order function logic."""

    @patch("supabase_client.get_client")
    @patch("supabase_client._to_cart_items")
    @patch("supabase_client._log_import")
    def test_basic_combine_tags_items_with_source_order(self, mock_log, mock_cart, mock_client):
        """Items from both orders should be tagged with their source_order."""
        from supabase_client import combine_into_order

        # Existing order
        target_order = {
            "id": "uuid-1",
            "order_number": "878279",
            "status": "ready_to_double_check",
            "items": [
                {"sku": "03-3684BL", "pickingQty": 4},
            ],
            "combine_meta": None,
            "created_at": "2026-03-17T10:00:00Z",
        }

        # New PDF data
        new_order_data = {
            "order_number": "878280",
            "items": [{"sku": "033994BR", "qty": 2}],
        }

        # Mock _to_cart_items to return converted items
        mock_cart.return_value = [
            {"sku": "03-3994BR", "pickingQty": 2},
        ]

        # Mock Supabase update
        mock_table = MagicMock()
        mock_table.update.return_value.eq.return_value.execute.return_value.data = [{
            "id": "uuid-1",
            "items": [
                {"sku": "03-3684BL", "pickingQty": 4, "source_order": "878279"},
                {"sku": "03-3994BR", "pickingQty": 2, "source_order": "878280"},
            ],
            "order_number": "878279 / 878280",
            "combine_meta": {"is_combined": True, "source_orders": []},
        }]
        mock_client.return_value.table.return_value = mock_table

        result = combine_into_order(target_order, new_order_data, "hash123", "test.pdf")

        # Verify update was called
        mock_table.update.assert_called_once()
        call_args = mock_table.update.call_args[0][0]

        # Check items are tagged
        merged_items = call_args["items"]
        assert len(merged_items) == 2
        assert merged_items[0]["source_order"] == "878279"
        assert merged_items[1]["source_order"] == "878280"

    @patch("supabase_client.get_client")
    @patch("supabase_client._to_cart_items")
    @patch("supabase_client._log_import")
    def test_combine_concatenates_order_numbers(self, mock_log, mock_cart, mock_client):
        """Combined order number should be "X / Y"."""
        from supabase_client import combine_into_order

        target_order = {
            "id": "uuid-1",
            "order_number": "878279",
            "status": "ready_to_double_check",
            "items": [],
            "combine_meta": None,
            "created_at": "2026-03-17T10:00:00Z",
        }

        new_order_data = {
            "order_number": "878280",
            "items": [{"sku": "033994BR", "qty": 2}],
        }

        mock_cart.return_value = [{"sku": "03-3994BR", "pickingQty": 2}]
        mock_table = MagicMock()
        mock_table.update.return_value.eq.return_value.execute.return_value.data = [{"id": "uuid-1"}]
        mock_client.return_value.table.return_value = mock_table

        combine_into_order(target_order, new_order_data, "hash123", "test.pdf")

        call_args = mock_table.update.call_args[0][0]
        assert call_args["order_number"] == "878279 / 878280"

    @patch("supabase_client.get_client")
    @patch("supabase_client._to_cart_items")
    @patch("supabase_client._log_import")
    def test_combine_builds_combine_meta(self, mock_log, mock_cart, mock_client):
        """combine_meta should track both source orders."""
        from supabase_client import combine_into_order

        target_order = {
            "id": "uuid-1",
            "order_number": "878279",
            "status": "ready_to_double_check",
            "items": [{"sku": "A", "pickingQty": 1}],
            "combine_meta": None,
            "created_at": "2026-03-17T10:00:00Z",
        }

        new_order_data = {
            "order_number": "878280",
            "items": [{"sku": "B", "qty": 1}],
        }

        mock_cart.return_value = [{"sku": "B", "pickingQty": 1}]
        mock_table = MagicMock()
        mock_table.update.return_value.eq.return_value.execute.return_value.data = [{"id": "uuid-1"}]
        mock_client.return_value.table.return_value = mock_table

        combine_into_order(target_order, new_order_data, "hash456", "order2.pdf")

        call_args = mock_table.update.call_args[0][0]
        meta = call_args["combine_meta"]

        assert meta["is_combined"] is True
        assert len(meta["source_orders"]) == 2
        assert meta["source_orders"][0]["order_number"] == "878279"
        assert meta["source_orders"][1]["order_number"] == "878280"
        assert meta["source_orders"][1]["pdf_hash"] == "hash456"

    @patch("supabase_client.get_client")
    @patch("supabase_client._to_cart_items")
    @patch("supabase_client._log_import")
    def test_combine_resets_double_checking_status(self, mock_log, mock_cart, mock_client):
        """If target was in double_checking, reset to ready_to_double_check and clear checker."""
        from supabase_client import combine_into_order

        target_order = {
            "id": "uuid-1",
            "order_number": "878279",
            "status": "double_checking",
            "checked_by": "checker-uuid",
            "items": [],
            "combine_meta": None,
            "created_at": "2026-03-17T10:00:00Z",
        }

        new_order_data = {"order_number": "878280", "items": [{"sku": "A", "qty": 1}]}
        mock_cart.return_value = [{"sku": "A", "pickingQty": 1}]
        mock_table = MagicMock()
        mock_table.update.return_value.eq.return_value.execute.return_value.data = [{"id": "uuid-1"}]
        mock_client.return_value.table.return_value = mock_table

        combine_into_order(target_order, new_order_data, "hash", "f.pdf")

        call_args = mock_table.update.call_args[0][0]
        assert call_args["status"] == "ready_to_double_check"
        assert call_args["checked_by"] is None

    @patch("supabase_client.get_client")
    @patch("supabase_client._to_cart_items")
    @patch("supabase_client._log_import")
    def test_combine_does_not_reset_active_status(self, mock_log, mock_cart, mock_client):
        """If target is active, do NOT change status (picker keeps working)."""
        from supabase_client import combine_into_order

        target_order = {
            "id": "uuid-1",
            "order_number": "878279",
            "status": "active",
            "items": [],
            "combine_meta": None,
            "created_at": "2026-03-17T10:00:00Z",
        }

        new_order_data = {"order_number": "878280", "items": [{"sku": "A", "qty": 1}]}
        mock_cart.return_value = [{"sku": "A", "pickingQty": 1}]
        mock_table = MagicMock()
        mock_table.update.return_value.eq.return_value.execute.return_value.data = [{"id": "uuid-1"}]
        mock_client.return_value.table.return_value = mock_table

        combine_into_order(target_order, new_order_data, "hash", "f.pdf")

        call_args = mock_table.update.call_args[0][0]
        assert "status" not in call_args  # Should NOT change status

    @patch("supabase_client.get_client")
    @patch("supabase_client._to_cart_items")
    @patch("supabase_client._log_import")
    def test_combine_same_sku_keeps_separate_items(self, mock_log, mock_cart, mock_client):
        """Same SKU from different orders should be kept as separate line items."""
        from supabase_client import combine_into_order

        target_order = {
            "id": "uuid-1",
            "order_number": "878279",
            "status": "ready_to_double_check",
            "items": [{"sku": "03-3684BL", "pickingQty": 4}],
            "combine_meta": None,
            "created_at": "2026-03-17T10:00:00Z",
        }

        new_order_data = {"order_number": "878280", "items": [{"sku": "033684BL", "qty": 2}]}
        mock_cart.return_value = [{"sku": "03-3684BL", "pickingQty": 2}]
        mock_table = MagicMock()
        mock_table.update.return_value.eq.return_value.execute.return_value.data = [{"id": "uuid-1"}]
        mock_client.return_value.table.return_value = mock_table

        combine_into_order(target_order, new_order_data, "hash", "f.pdf")

        call_args = mock_table.update.call_args[0][0]
        merged = call_args["items"]

        # Both items should exist (not merged into one)
        assert len(merged) == 2
        assert merged[0]["source_order"] == "878279"
        assert merged[0]["pickingQty"] == 4
        assert merged[1]["source_order"] == "878280"
        assert merged[1]["pickingQty"] == 2

    @patch("supabase_client.get_client")
    @patch("supabase_client._to_cart_items")
    @patch("supabase_client._log_import")
    def test_triple_combine_appends_to_existing_meta(self, mock_log, mock_cart, mock_client):
        """Third order combining into already-combined order should append to source_orders."""
        from supabase_client import combine_into_order

        target_order = {
            "id": "uuid-1",
            "order_number": "878279 / 878280",
            "status": "ready_to_double_check",
            "items": [
                {"sku": "A", "pickingQty": 1, "source_order": "878279"},
                {"sku": "B", "pickingQty": 2, "source_order": "878280"},
            ],
            "combine_meta": {
                "is_combined": True,
                "source_orders": [
                    {"order_number": "878279", "added_at": "2026-03-17T10:00:00Z", "item_count": 1},
                    {"order_number": "878280", "added_at": "2026-03-17T10:05:00Z", "item_count": 1},
                ],
            },
            "created_at": "2026-03-17T10:00:00Z",
        }

        new_order_data = {"order_number": "878281", "items": [{"sku": "C", "qty": 3}]}
        mock_cart.return_value = [{"sku": "C", "pickingQty": 3}]
        mock_table = MagicMock()
        mock_table.update.return_value.eq.return_value.execute.return_value.data = [{"id": "uuid-1"}]
        mock_client.return_value.table.return_value = mock_table

        combine_into_order(target_order, new_order_data, "hash3", "order3.pdf")

        call_args = mock_table.update.call_args[0][0]

        assert call_args["order_number"] == "878279 / 878280 / 878281"

        meta = call_args["combine_meta"]
        assert len(meta["source_orders"]) == 3
        assert meta["source_orders"][2]["order_number"] == "878281"


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
        from supabase_client import find_combinable_order_by_customer, COMBINABLE_STATUSES

        mock_query = MagicMock()
        mock_query.select.return_value = mock_query
        mock_query.eq.return_value = mock_query
        mock_query.in_.return_value = mock_query
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
