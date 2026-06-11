"""
find_combinable_order_by_customer must NEVER return a waiting order
(is_waiting_inventory = true): joining one is a manual, user-confirmed action
in PickD only (operator rule, 2026-06-11). Supabase mocked.
"""

import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from supabase_client import find_combinable_order_by_customer  # noqa: E402


@patch("supabase_client.get_client")
def test_query_filters_out_waiting_orders(mock_client):
    chain = MagicMock()
    # Every builder call returns the chain; execute yields no rows.
    for m in ("select", "eq", "in_", "or_", "gte", "order", "limit", "neq"):
        getattr(chain, m).return_value = chain
    chain.execute.return_value.data = []
    mock_client.return_value.table.return_value = chain

    find_combinable_order_by_customer("cust-1")

    # The waiting exclusion must be part of the query.
    chain.or_.assert_called_once_with("is_waiting_inventory.is.null,is_waiting_inventory.eq.false")
