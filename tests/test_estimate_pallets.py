"""
Tests for the watcher-side pallet estimate ("2 pallets · 20 units" on the card):
pipeline.estimate_pallets (faithful port of PickD's pallet COUNT rule) and the
cached bike catalog (supabase_client.get_bike_skus). Supabase mocked.
"""

import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import supabase_client  # noqa: E402
from pipeline import BIKES_PER_PALLET, estimate_pallets  # noqa: E402

BIKES = {"033684BR", "094802BK"}


def test_no_items_is_zero_pallets():
    assert estimate_pallets([], BIKES) == 0


def test_parts_only_is_always_one_pallet():
    items = [{"sku": "990001", "qty": 7}, {"sku": "990002", "qty": 30}]
    assert estimate_pallets(items, BIKES) == 1


def test_bikes_ceil_by_twelve():
    assert estimate_pallets([{"sku": "033684BR", "qty": 12}], BIKES) == 1
    assert estimate_pallets([{"sku": "033684BR", "qty": 13}], BIKES) == 2
    assert estimate_pallets([{"sku": "033684BR", "qty": 24}], BIKES) == 2
    assert BIKES_PER_PALLET == 12  # PickD's pickingLogic.ts constant


def test_parts_stack_on_bike_pallets_without_adding_one():
    items = [
        {"sku": "033684BR", "qty": 10},  # bikes → 1 pallet
        {"sku": "990001", "qty": 25},  # parts ride on the last bike pallet
    ]
    assert estimate_pallets(items, BIKES) == 1


def test_skus_match_normalized():
    # The parser may emit the raw spaced form; matching must normalize it.
    items = [{"sku": "03 3684 BR", "qty": 13}]
    assert estimate_pallets(items, BIKES) == 2


def test_get_bike_skus_is_cached(monkeypatch):
    monkeypatch.setitem(supabase_client._bike_skus_cache, "at", 0.0)
    monkeypatch.setitem(supabase_client._bike_skus_cache, "skus", None)
    fake = MagicMock()
    fake.table.return_value.select.return_value.eq.return_value.execute.return_value.data = [
        {"sku": "03-3684BR"},
        {"sku": "09-4802BK"},
    ]
    with patch.object(supabase_client, "get_client", return_value=fake) as gc:
        first = supabase_client.get_bike_skus()
        second = supabase_client.get_bike_skus()
    assert first == {"033684BR", "094802BK"}  # normalized
    assert second is first
    assert gc.call_count == 1  # one query per TTL window
