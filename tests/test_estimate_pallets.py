"""
The cached bike catalog (supabase_client.get_bike_skus). Supabase mocked.

estimate_pallets used to live here too; it was the watcher's port of Pickd's
pallet rule, drifted from it, and only fed a line of text on a card. Pickd
counts pallets on the board and in Double Check. The catalog cache stays: door.py
embeds is_bike into every capture it publishes.
"""

import supabase_client


def test_get_bike_skus_is_cached(monkeypatch):
    calls = []

    class Q:
        def select(self, *a, **k):
            return self

        def eq(self, *a, **k):
            return self

        def execute(self):
            calls.append(1)
            return type("R", (), {"data": [{"sku": "03-3684BR"}, {"sku": "01-0530"}]})()

    class C:
        def table(self, name):
            return Q()

    monkeypatch.setattr(supabase_client, "get_client", lambda: C())
    supabase_client._bike_skus_cache.update({"skus": None, "at": 0.0})

    first = supabase_client.get_bike_skus()
    second = supabase_client.get_bike_skus()

    assert first == {"033684BR", "010530"}  # normalized, the way the parser spells them
    assert second is first and len(calls) == 1
