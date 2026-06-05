"""
Tests for pipeline.resolve_order_items — the read-only pick-location resolution
used by the capture UI's order-detail preview. It must parse the order text and
hand the items to the same resolver as send (_to_cart_items) WITHOUT creating or
reserving anything. Supabase is monkeypatched out.
"""

import pipeline

ONE_ITEM = """                            O R D E R   I N Q U I R Y
 Order Number: 880009                       Account Number: 0003574 00
 Quant  Quant  Stock #   W/H   Description                       Unit    Extend
   3     3  03 4068 BK  F   EXPLORER A2 17 2025 GLOSS BLAC  388.95   1166.85
                                END OF ORDER"""


def test_no_items_returns_empty_without_touching_supabase(monkeypatch):
    # Must short-circuit before any client/resolver call.
    def boom(*a, **k):
        raise AssertionError("Supabase should not be touched when there are no items")

    monkeypatch.setattr(pipeline, "get_client", boom)
    monkeypatch.setattr(pipeline, "_to_cart_items", boom)
    assert pipeline.resolve_order_items("nothing parseable here") == []


def test_passes_parsed_items_to_readonly_resolver(monkeypatch):
    captured = {}

    monkeypatch.setattr(pipeline, "get_client", lambda: "CLIENT")

    def fake_cart(client, items):
        captured["client"] = client
        captured["items"] = items
        return [{"sku": items[0]["sku"], "pickingQty": items[0]["qty"], "location": "ROW 32"}]

    monkeypatch.setattr(pipeline, "_to_cart_items", fake_cart)

    out = pipeline.resolve_order_items(ONE_ITEM)

    # Parsed the canonical SKU and forwarded it to the resolver with the client.
    assert captured["client"] == "CLIENT"
    assert captured["items"][0]["sku"] == "034068BK"
    assert captured["items"][0]["qty"] == 3
    # Returns whatever the resolver produced (locations etc.), untouched.
    assert out == [{"sku": "034068BK", "pickingQty": 3, "location": "ROW 32"}]
