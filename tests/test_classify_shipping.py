"""
Tests for pipeline.classify_shipping — local-only FedEx/regular classification
using BOTH the AS400 'Ship Via' carrier and the units heuristic fallback.
"""

from pipeline import classify_shipping, preview_order


def test_fedex_via_returns_fedex():
    assert classify_shipping("FEDEX", 1) == "fedex"
    assert classify_shipping("FDX HOME", 99) == "fedex"  # carrier wins over units


def test_ups_ground_returns_regular():
    assert classify_shipping("UPS GROUND", 1) == "regular"


def test_freight_carriers_return_regular():
    assert classify_shipping("ABF FREIGHT", 1) == "regular"
    assert classify_shipping("LTL", 1) == "regular"
    assert classify_shipping("TRUCK", 1) == "regular"


def test_no_ship_via_uses_units_heuristic():
    # No carrier hint → fall back to the 5-units rule.
    assert classify_shipping(None, 6) == "regular"
    assert classify_shipping(None, 2) == "fedex"
    assert classify_shipping("", 5) == "regular"  # boundary: 5 → regular


def test_unknown_carrier_falls_back_to_units():
    # A carrier we don't map → heuristic decides.
    assert classify_shipping("SOME COURIER", 6) == "regular"
    assert classify_shipping("SOME COURIER", 2) == "fedex"


PREVIEW_TEXT = """                            O R D E R   I N Q U I R Y
 Order Number: 880009                       Account Number: 0003574 00
 Quant  Quant  Stock #   W/H   Description                       Unit    Extend
   1     1  03 3768 BL  N   DIVIDE S/O 12X27 2025 RIPTIDE   394.95    394.95
           Ship Via    FEDEX           Shipped From Florida
                                END OF ORDER                             394.95"""


def test_preview_order_exposes_shipping_type():
    assert preview_order(PREVIEW_TEXT)["shipping_type"] == "fedex"
