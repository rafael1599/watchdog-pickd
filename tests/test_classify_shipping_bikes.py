"""
The shipping class counts BIKES, not units.

Third mirror of one rule. The other two are Pickd's
`src/utils/shippingClassification.ts` — which says it in as many words, "Parts
never make an order 'regular' on their own: an order of 50 small parts still
ships FedEx" — and the DB's `classify_picking_list_fedex`. That file even
carries a "keep both in sync" note this side never saw, and this side drifted:
it counted every unit, so five pedals were a truck.

Spotted by Rafael on 2026-09-08, comparing the watcher against Double Check View.
"""

from pipeline import classify_shipping, count_bike_units, preview_order

BIKES = {"033684BR", "033933BK"}


def _items(*pairs):
    return [{"sku": sku, "qty": qty} for sku, qty in pairs]


def test_five_bikes_are_a_truck():
    assert classify_shipping(None, 5) == "regular"


def test_five_parts_are_not():
    # THE bug. Parts never make an order a truck on their own.
    assert count_bike_units(_items(("99-3604", 5)), BIKES) == 0
    assert classify_shipping(None, count_bike_units(_items(("99-3604", 5)), BIKES)) == "fedex"


def test_fifty_parts_are_still_not():
    assert classify_shipping(None, count_bike_units(_items(("99-3604", 50)), BIKES)) == "fedex"


def test_a_few_bikes_among_many_parts_stay_fedex():
    items = _items(("033684BR", 2), ("99-3604", 40))
    assert count_bike_units(items, BIKES) == 2
    assert classify_shipping(None, count_bike_units(items, BIKES)) == "fedex"


def test_bikes_across_lines_add_up_to_a_truck():
    items = _items(("033684BR", 3), ("033933BK", 2), ("99-3604", 9))
    assert count_bike_units(items, BIKES) == 5
    assert classify_shipping(None, count_bike_units(items, BIKES)) == "regular"


def test_a_named_carrier_still_wins_over_the_count():
    # Ship Via is empty on every order we capture (§3.0), so this is inert in
    # the normal flow — but a recaptured, already-shipped order does carry it.
    assert classify_shipping("FEDEX GROUND", 12) == "fedex"
    assert classify_shipping("R&L", 1) == "regular"


ORDER = """ O R D E R  I N Q U I R Y
 Order Number: 881310  Account Number: 0010495 00
 Bill MATTHEWS BICYCLE MART, INC
 Quant Quant  Stock #      W/H  Description          Unit     Extend
 Ord   Ship
     5     5  99 3604 XX   N    TOOL HYENA DIAGNOSTIC  10.00     50.00
 END OF ORDER                                                    50.00
"""


def test_the_preview_says_which_rule_it_used():
    # An explicit set means the real rule ran; the flag exists so a fallback
    # can never be mistaken for an answer.
    p = preview_order(ORDER, bike_skus=BIKES)
    assert p["shipping_type_basis"] == "bikes"
    assert p["shipping_type"] == "fedex"  # five parts, not five bikes


def test_an_unreachable_catalog_falls_back_and_admits_it():
    # Better a labelled fallback than a preview that fails, or one that pretends.
    p = preview_order(ORDER, bike_skus=None)
    assert p["shipping_type_basis"] in ("bikes", "units-fallback")
