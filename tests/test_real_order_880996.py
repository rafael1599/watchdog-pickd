"""
A second real order, on purpose: #880996 (SHREWSBURY BICYCLES, 2026-07-31).

Rafael sent it with a warning — "para que no tomes esa anterior como patrón
repetitivo definitivo". Where 881310 left most of the header blank, this one is
filled in, because it has ALREADY SHIPPED: the carrier is picked at shipping time,
so Ship Via, COD Tag No, Carton Count, freight and the invoice fields only exist
on an order the watchdog would never meet on intake. It is here as the second
shape of the same screen (see docs/as400-screen-map.md §3.0), and it carries the
three screens that matter:

  HEADER  — Ship Via with a carrier (R&L), a COD Tag No, a Carton Count, Freight
            & Misc, an Invoice No/Date, and a right-hand 'Invoice Comments'
            column the parser never looks at (NET 30 DAYS, BOL# 130636156).
  ITEMS   — three lines that reconcile with the Sub-Total.
  EMPTY   — what ENTER draws AFTER the last page: the same screen with no lines
            and END OF ORDER still showing. That screen is the reason
            run_scan_step no longer trusts "number + zero items + last page".

Only trailing 80-column padding was removed.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import auto_scanner  # noqa: E402
import scanned_store  # noqa: E402
from as400_capture import _is_order_header_screen  # noqa: E402
from parser import parse_order  # noqa: E402
from pipeline import classify_shipping, meaningful_note, preview_order  # noqa: E402

HEADER = """                            O R D E R   I N Q U I R Y

 Order Number: 880996                       Account Number: 0009981 00

 Bill SHREWSBURY BICYCLES INC.         Ship SHREWSBURY BICYCLES INC.
      765 BROAD STREET                      765 BROAD STREET

      SHREWSBURY      NJ  07702             SHREWSBURY      NJ  07702

 Terms: 28        Cr Lim:        .00   Invoice Comments
 Sales ID: 179    Order Taken By: HAM  NET 30 DAYS
 Credit Hold:     Carton Count:    10  100% FRT DEDUCT IF PD W/IN TERMS
 Order Date: 073126 P/O: CAPRI/TAX24   BOL# 130636156

           Ship Via    R&L             Shipped From New Jersey
         COD Tag No    AK2064222       Ship Date    07/31/26
          Sub-Total         3965.50
     Freight & Misc          660.00
        Order Total         4625.50
         Invoice No          862809
           Inv Date         7/31/26
 Order Comments: CLOSED TUESDAY. FF 10 BIKBACKYARD PROGRAM
                                   Cmd5            Cmd6                 Cmd7
                                    DETAILS         RETURN TO SELECT     EXIT"""

ITEMS = """                            O R D E R   I N Q U I R Y


 Order Number: 880996                       Account Number: 0009981 00

 Bill SHREWSBURY BICYCLES INC.

 Quant  Quant  Stock #   W/H   Description                       Unit    Extend
   Ord   Ship                                                   Price
     4      4  06 4731 BK  N   TAXI 24" 2026 GLOSS BLACK       449.95   1799.80
     3      3  07 3689 WH  N   JUV CAPRI 2.4 2026 VANILLA      360.95   1082.85
     3      3  07 3690 BL  N   JUV CAPRI 2.4 2026 SKY BLUE     360.95   1082.85




                                END OF ORDER                            3965.50

              Enter             Cmd6
              More Details       RETURN TO SELECT"""

# One ENTER past the last items page: same screen, no lines, END OF ORDER intact.
EMPTY_PAGE_AFTER_ENTER = """                            O R D E R   I N Q U I R Y


 Order Number: 880996                       Account Number: 0009981 00

 Bill SHREWSBURY BICYCLES INC.

 Quant  Quant  Stock #   W/H   Description                       Unit    Extend
   Ord   Ship                                                   Price




                                END OF ORDER                            3965.50

              Enter             Cmd6
              More Details       RETURN TO SELECT"""

CAPTURE = HEADER + "\n" + ITEMS


@pytest.fixture(autouse=True)
def tmp_store(tmp_path, monkeypatch):
    monkeypatch.setenv("SCANNED_STORE_PATH", str(tmp_path / "scanned.json"))
    monkeypatch.setenv("SCAN_CURSOR_PATH", str(tmp_path / "scan_cursor"))


# ── the header, filled in ────────────────────────────────────────────────────


def test_header_fields():
    d = parse_order(CAPTURE)
    assert d["order_number"] == "880996"
    assert (d["as400_account"], d["as400_ship_to"]) == ("9981", "00")
    assert d["customer_name"] == "SHREWSBURY BICYCLES INC."
    assert d["order_date"] == "2026-07-31"
    assert d["subtotal"] == 3965.50  # NOT the Order Total (4625.50 with freight)


def test_a_populated_ship_via_survives_the_shipped_from_cut():
    # The fix for the empty field must not break the filled one.
    assert parse_order(CAPTURE)["ship_via"] == "R&L"


def test_rl_is_a_truck_carrier_not_a_parcel_one():
    # R&L is LTL freight. Without the hint this only landed on 'regular' through
    # the units fallback, so a small R&L order would have been coloured FedEx.
    assert classify_shipping("R&L", 10) == "regular"
    assert classify_shipping("R&L", 2) == "regular"


def test_delivery_instruction_survives_the_noise_filter():
    # 'CLOSED TUESDAY' is exactly the kind of note the floor needs to see.
    note = meaningful_note(parse_order(CAPTURE)["order_comments"])
    assert note is not None and "CLOSED TUESDAY" in note


# ── the items ────────────────────────────────────────────────────────────────


def test_three_lines_ten_units_reconciled():
    d = parse_order(CAPTURE)
    assert [i["sku"] for i in d["items"]] == ["064731BK", "073689WH", "073690BL"]
    assert sum(i["qty"] for i in d["items"]) == 10
    assert preview_order(CAPTURE)["total_mismatch"] is False


def test_a_quoted_inch_mark_in_the_description_is_kept():
    # '06 4731 BK  N   TAXI 24" 2026 GLOSS BLACK' — the inch mark must not break
    # the line regex nor leak into the SKU.
    taxi = parse_order(CAPTURE)["items"][0]
    assert taxi["description"] == 'TAXI 24" 2026 GLOSS BLACK'


# ── the page after the last page ─────────────────────────────────────────────


def test_header_detector_rejects_both_item_views():
    assert _is_order_header_screen(HEADER, "880996")
    assert not _is_order_header_screen(ITEMS, "880996")
    assert not _is_order_header_screen(EMPTY_PAGE_AFTER_ENTER, "880996")


def test_the_empty_page_is_not_mistaken_for_a_void_order():
    # It has an order number, zero items and END OF ORDER — the exact shape the
    # scanner used to read as "VOID/empty, skip this number forever". The header's
    # Sub-Total is what says otherwise: 3965.50 declared, 0.00 parsed.
    captured = HEADER + "\n" + EMPTY_PAGE_AFTER_ENTER
    preview = preview_order(captured)
    assert preview["item_count"] == 0
    assert preview["is_last_page"] is True
    assert preview["total_mismatch"] is True  # this is what saves it

    res = auto_scanner.run_scan_step(
        None,
        start=880996,
        capture_fn=lambda n, driver: captured,
        preview_fn=preview_order,
    )
    assert res["action"] != "empty_skipped"
    # The cursor must NOT have moved past a real order.
    assert scanned_store.next_scan_number(880996) == 880996


def test_a_genuinely_empty_order_is_still_skipped():
    # No Sub-Total on the header → nothing to reconcile → still a VOID/empty order,
    # and the scanner must still advance past it or it retries forever.
    void_like = """                            O R D E R   I N Q U I R Y
 Order Number: 880997                       Account Number: 0009981 00
 Bill SOMEBODY
                                END OF ORDER"""
    res = auto_scanner.run_scan_step(
        None,
        start=880997,
        capture_fn=lambda n, driver: void_like,
        preview_fn=preview_order,
    )
    assert res["action"] == "empty_skipped"
    assert scanned_store.next_scan_number(880997) == 880998
