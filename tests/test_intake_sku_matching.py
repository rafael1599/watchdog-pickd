"""
Tests for intake-time SKU matching in _to_cart_items (idea-101):
- format-only mismatches (dash/space) resolve to the canonical catalog SKU
- ambiguous normalized collisions are left UNRESOLVED (picker decides)
- variant siblings ('03-3768BL' / '03-3768BLD') are chosen by STOCK, not spelling
Supabase fully mocked.
"""

import os
import sys
from unittest.mock import MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from supabase_client import _to_cart_items  # noqa: E402


def _client_with_catalog(catalog_skus, inventory=None, active_lists=None):
    """Mock client: sku_metadata returns the given SKUs; `inventory` rows are
    returned as-is for the LUDLOW active query (the .in_ filter is not applied,
    so pass only the rows the test is about); `active_lists` feeds the
    reservation pass as [{"items": [...]}]."""
    client = MagicMock()

    def table(name):
        t = MagicMock()
        if name == "sku_metadata":
            t.select.return_value.range.return_value.execute.return_value.data = [
                {"sku": s} for s in catalog_skus
            ]
        elif name == "inventory":
            chain = t.select.return_value.in_.return_value.eq.return_value.eq.return_value
            chain.execute.return_value.data = list(inventory or [])
        else:  # picking_lists (reservations)
            t.select.return_value.in_.return_value.execute.return_value.data = list(
                active_lists or []
            )
        return t

    client.table.side_effect = table
    return client


def _row(sku, location, qty):
    return {
        "sku": sku,
        "location": location,
        "quantity": qty,
        "distribution": [],
        "location_hint": None,
        "item_name": "DIVIDE S/O",
        "sublocation": None,
    }


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


def test_full_suffixed_match_beats_truncated_when_neither_has_stock():
    # If BOTH '03-3769BL' and '03-3769BLD' exist and NEITHER has stock, the
    # source said BLD → the more specific full match is the name the line stays
    # flagged under, never the silently-truncated one.
    client = _client_with_catalog(["03-3769BL", "03-3769BLD"])
    items = _to_cart_items(
        client, [{"sku": "033769BL", "raw_sku": "03 3769 BLD", "qty": 1, "description": "d"}]
    )
    assert items[0]["sku"] == "03-3769BLD"
    assert items[0]["insufficient_stock"] is True


# --- variant siblings: the SAME bike under two catalog names — stock decides ------
#
# The operator renames the inventory row between '03-3768BL' and '03-3768BLD'
# (3 times in 2026); the old sku_metadata row stays because qty-0 inventory rows
# reference it. Matching "the first name that exists in the catalog" kept landing
# on the dead one: order 881288 (2026-08-26) arrived LOW STOCK for '03 3768 BLD'
# with 145 units on ROW 43 under '03-3768BL'.


def test_sibling_with_stock_beats_dead_exact_match():
    # Source 'BLD' exists in the catalog but is empty; the 2-letter sibling holds
    # the bikes → the line must resolve to the sibling, located, not flagged.
    client = _client_with_catalog(
        ["03-3768BL", "03-3768BLD"], inventory=[_row("03-3768BL", "ROW 43", 145)]
    )
    items = _to_cart_items(
        client, [{"sku": "033768BL", "raw_sku": "03 3768 BLD", "qty": 1, "description": "d"}]
    )
    assert items[0]["sku"] == "03-3768BL"
    assert items[0]["location"] == "ROW 43"
    assert items[0]["insufficient_stock"] is False
    assert items[0]["sku_not_found"] is False


def test_sibling_with_stock_beats_dead_canonical_match():
    # Source '03 3769 BLT': no 'BLT' in the catalog, canonical 'BL' exists but is
    # empty, 'BLD' (not even a candidate spelling) holds 76 → pick 'BLD'.
    client = _client_with_catalog(
        ["03-3769BL", "03-3769BLD"], inventory=[_row("03-3769BLD", "ROW 41", 76)]
    )
    items = _to_cart_items(
        client, [{"sku": "033769BL", "raw_sku": "03 3769 BLT", "qty": 1, "description": "d"}]
    )
    assert items[0]["sku"] == "03-3769BLD"
    assert items[0]["location"] == "ROW 41"
    assert items[0]["insufficient_stock"] is False


def test_source_spelling_wins_when_it_has_stock_too():
    # Both siblings stocked → the source's own spelling is kept (no gratuitous swap).
    client = _client_with_catalog(
        ["03-3768BL", "03-3768BLD"],
        inventory=[_row("03-3768BL", "ROW 43", 145), _row("03-3768BLD", "ROW 41", 3)],
    )
    items = _to_cart_items(
        client, [{"sku": "033768BL", "raw_sku": "03 3768 BLD", "qty": 2, "description": "d"}]
    )
    assert items[0]["sku"] == "03-3768BLD"
    assert items[0]["insufficient_stock"] is False


def test_sibling_that_covers_the_line_beats_one_that_does_not():
    # Source spelling has 2, the line needs 3, the sibling has 10 → the sibling.
    client = _client_with_catalog(
        ["03-3768BL", "03-3768BLD"],
        inventory=[_row("03-3768BLD", "ROW 41", 2), _row("03-3768BL", "ROW 43", 10)],
    )
    items = _to_cart_items(
        client, [{"sku": "033768BL", "raw_sku": "03 3768 BLD", "qty": 3, "description": "d"}]
    )
    assert items[0]["sku"] == "03-3768BL"
    assert items[0]["insufficient_stock"] is False


def test_partial_stock_still_prefers_the_fuller_sibling():
    # Nobody covers the line: take the sibling with the most stock and flag it.
    client = _client_with_catalog(
        ["03-3768BL", "03-3768BLD"],
        inventory=[_row("03-3768BLD", "ROW 41", 1), _row("03-3768BL", "ROW 43", 4)],
    )
    items = _to_cart_items(
        client, [{"sku": "033768BL", "raw_sku": "03 3768 BLD", "qty": 6, "description": "d"}]
    )
    assert items[0]["sku"] == "03-3768BL"
    assert items[0]["insufficient_stock"] is True
    assert items[0]["available_qty"] == 4


def test_reservations_count_against_a_sibling():
    # 'BL' has 1 physical but an active order already reserves it → 0 effective;
    # 'BLD' has 5 free → the line goes to 'BLD'.
    client = _client_with_catalog(
        ["03-3768BL", "03-3768BLD"],
        inventory=[_row("03-3768BL", "ROW 43", 1), _row("03-3768BLD", "ROW 41", 5)],
        active_lists=[{"items": [{"sku": "03-3768BL", "location": "ROW 43", "pickingQty": 1}]}],
    )
    items = _to_cart_items(
        client, [{"sku": "033768BL", "raw_sku": "03 3768 BL", "qty": 1, "description": "d"}]
    )
    assert items[0]["sku"] == "03-3768BLD"
    assert items[0]["insufficient_stock"] is False


def test_sibling_rule_is_for_bike_style_skus_only():
    # A part number has no colour, hence no family: '01-0449' must never be
    # resolved to a stocked '01-0449A' just because it looks like a sibling.
    client = _client_with_catalog(
        ["01-0449", "01-0449A"], inventory=[_row("01-0449A", "ROW 22", 9)]
    )
    items = _to_cart_items(
        client, [{"sku": "010449", "raw_sku": "01 0449", "qty": 1, "description": "d"}]
    )
    assert items[0]["sku"] == "01-0449"
    assert items[0]["insufficient_stock"] is True


def test_two_letter_colour_is_not_a_sibling_of_a_different_colour():
    # 'BK' and 'BL' share 'B' but are different bikes — no family relationship.
    client = _client_with_catalog(
        ["03-4070BL", "03-4070BK"], inventory=[_row("03-4070BK", "ROW 43", 20)]
    )
    items = _to_cart_items(
        client, [{"sku": "034070BL", "raw_sku": "03 4070 BL", "qty": 1, "description": "d"}]
    )
    assert items[0]["sku"] == "03-4070BL"
    assert items[0]["insufficient_stock"] is True
