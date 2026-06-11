"""
Tests for intake-time SKU matching in _to_cart_items (idea-101):
- format-only mismatches (dash/space) resolve to the canonical catalog SKU
- ambiguous normalized collisions are left UNRESOLVED (picker decides)
Supabase fully mocked.
"""

import os
import sys
from unittest.mock import MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from supabase_client import _to_cart_items  # noqa: E402


def _client_with_catalog(catalog_skus):
    """Mock client: sku_metadata returns the given SKUs; inventory is empty."""
    client = MagicMock()

    def table(name):
        t = MagicMock()
        if name == "sku_metadata":
            t.select.return_value.range.return_value.execute.return_value.data = [
                {"sku": s} for s in catalog_skus
            ]
        else:  # inventory
            chain = t.select.return_value.in_.return_value.eq.return_value.eq.return_value
            chain.execute.return_value.data = []
        return t

    client.table.side_effect = table
    return client


def test_format_only_mismatch_resolves_to_canonical():
    # PDF gives '034664BR'; catalog has '03-4664BR' → must NOT be sku_not_found.
    client = _client_with_catalog(["03-4664BR"])
    items = _to_cart_items(client, [{"sku": "034664BR", "qty": 1, "description": "d"}])
    assert items[0]["sku"] == "03-4664BR"
    assert items[0]["sku_not_found"] is False


def test_ambiguous_normalized_collision_stays_manual():
    # Two catalog SKUs share the normalized form '034666BR' — substituting either
    # would be a guess, so the item must stay unresolved for the picker.
    client = _client_with_catalog(["03-4666BR", "034-666-BR"])
    items = _to_cart_items(client, [{"sku": "034666BR", "qty": 1, "description": "d"}])
    assert items[0]["sku_not_found"] is True


def test_duplicate_identical_catalog_rows_are_not_a_collision():
    client = _client_with_catalog(["03-4664BR", "03-4664BR"])
    items = _to_cart_items(client, [{"sku": "034664BR", "qty": 1, "description": "d"}])
    assert items[0]["sku"] == "03-4664BR"
    assert items[0]["sku_not_found"] is False


# --- finish/variant suffix: let the CATALOG decide (operator-reported 2026-06-11) --


def test_suffix_kept_when_catalog_has_it():
    # AS400 line '03 3769 BLD'; the parser truncates to '033769BL' but the catalog
    # SKU really is '03-3769BLD' — the full raw form must win.
    client = _client_with_catalog(["03-3769BLD"])
    items = _to_cart_items(
        client, [{"sku": "033769BL", "raw_sku": "03 3769 BLD", "qty": 1, "description": "d"}]
    )
    assert items[0]["sku"] == "03-3769BLD"
    assert items[0]["sku_not_found"] is False


def test_suffix_dropped_when_catalog_lacks_it():
    # Same source shape, but this catalog entry has no 3rd letter → fall back to
    # the 2-letter-color canonical guess (previous behavior preserved).
    client = _client_with_catalog(["03-3768BL"])
    items = _to_cart_items(
        client, [{"sku": "033768BL", "raw_sku": "03 3768 BLD", "qty": 1, "description": "d"}]
    )
    assert items[0]["sku"] == "03-3768BL"
    assert items[0]["sku_not_found"] is False


def test_full_suffixed_match_beats_truncated_when_both_exist():
    # If BOTH '03-3769BL' and '03-3769BLD' exist, the source said BLD → the more
    # specific full match must be chosen, never the silently-truncated one.
    client = _client_with_catalog(["03-3769BL", "03-3769BLD"])
    items = _to_cart_items(
        client, [{"sku": "033769BL", "raw_sku": "03 3769 BLD", "qty": 1, "description": "d"}]
    )
    assert items[0]["sku"] == "03-3769BLD"
