"""
Tests for pipeline.preview_order's item_count vs total_units.

item_count = number of distinct line items (SKUs); total_units = sum of the
ordered quantities across those lines. They differ whenever any line has qty > 1.
"""

from pipeline import preview_order

ORDER_TEXT = """                            O R D E R   I N Q U I R Y
 Order Number: 880009                       Account Number: 0003574 00
 Quant  Quant  Stock #   W/H   Description                       Unit    Extend
   3     3  03 4068 BK  F   EXPLORER A2 17 2025 GLOSS BLAC  388.95   1166.85
   2     2  03 3768 BL  N   DIVIDE S/O 12X27 2025 RIPTIDE   394.95    789.90
   1     0  03 3769 BL  N   DIVIDE S/O 14X27 2025 RIPTIDE   394.95    394.95
                                END OF ORDER                            2351.70"""


def test_total_units_sums_ordered_quantities():
    p = preview_order(ORDER_TEXT)
    assert p["item_count"] == 3  # three distinct line items
    assert p["total_units"] == 6  # 3 + 2 + 1 (backordered line still counts ordered qty)


def test_total_units_zero_for_no_items():
    p = preview_order("no items here")
    assert p["item_count"] == 0
    assert p["total_units"] == 0


# --- Sub-Total reconciliation guard -------------------------------------------

# A capture whose Sub-Total matches the sum of the parsed line extends.
RECONCILED = """                            O R D E R   I N Q U I R Y
 Order Number: 880009                       Account Number: 0003574 00
          Sub-Total          789.90
 Quant  Quant  Stock #   W/H   Description                       Unit    Extend
   1     1  03 3768 BL  N   DIVIDE S/O 12X27 2025 RIPTIDE   394.95    394.95
   1     1  03 3769 BL  N   DIVIDE S/O 14X27 2025 RIPTIDE   394.95    394.95
                                END OF ORDER                             789.90"""


def test_total_matches_subtotal_no_mismatch():
    p = preview_order(RECONCILED)
    assert p["subtotal"] == 789.90
    assert p["parsed_total"] == 789.90
    assert p["total_mismatch"] is False


def test_dropped_line_trips_mismatch():
    # Drop the second line: parsed total (394.95) no longer matches Sub-Total.
    text = "\n".join(line for line in RECONCILED.splitlines() if "3769" not in line)
    p = preview_order(text)
    assert p["item_count"] == 1
    assert p["parsed_total"] == 394.95
    assert p["subtotal"] == 789.90
    assert p["total_mismatch"] is True


def test_no_subtotal_means_no_mismatch():
    # When the header has no Sub-Total/Order Total, we can't reconcile → never warn.
    p = preview_order(ORDER_TEXT)
    assert p["subtotal"] is None
    assert p["total_mismatch"] is False
