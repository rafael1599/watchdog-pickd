"""
The capture UI shows the Ship-to name on its cards; the Bill-to stays as
`customer` because it drives customers.name in PickD and the auto-archive rule
(EBAY PART SALES is a Bill-to). Both travel side by side from preview_order
through the scan-cache meta.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from auto_scanner import _meta_from_preview  # noqa: E402
from pipeline import preview_order  # noqa: E402

HEADER = """                            O R D E R   I N Q U I R Y
 Order Number: 880036                       Account Number: 0010495 00

 Bill JAMIS CONSUMER ALL ACCESS        Ship JOHN SMITH
      123 MAIN ST                           45 OCEAN DR

      HACKENSACK      NJ  07601             VERO BEACH      FL  32963

 Terms: 28        Cr Lim:        .00   Invoice Comments
 Order Date: 060426 P/O: SO1610
   Ord   Ship
      1     1  03 4664 BR   F  DAKAR A1 17 2025 BROWN       1234.00    1234.00
                                   END OF ORDER   1234.00
"""


def test_preview_carries_ship_to_next_to_bill_to():
    preview = preview_order(HEADER)
    assert preview["customer"] == "JAMIS CONSUMER ALL ACCESS"
    assert preview["ship_to"] == "JOHN SMITH"


def test_preview_ship_to_is_none_without_a_ship_block():
    preview = preview_order("Order Number: 1 \n Bill ACME\n END OF ORDER")
    assert preview["customer"] == "ACME"
    assert preview["ship_to"] is None


def test_meta_keeps_ship_to_for_the_cache():
    meta = _meta_from_preview({"order_number": "1", "customer": "BILL", "ship_to": "SHIP"})
    assert meta["customer"] == "BILL"
    assert meta["ship_to"] == "SHIP"
