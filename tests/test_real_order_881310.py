"""
A real order, screen for screen: #881310 (MIAMI BEACH BICYCLE CENTER, 2026-08-28).

This is the shape the watchdog actually meets: an order that has NOT shipped yet
(Ship Via, COD Tag No, Invoice No and Inv Date all blank, Carton Count 0, no
freight). Compare with test_real_order_880996, the same screen after shipping.

Pasted by Rafael from the live terminal on 2026-09-01. Everything the repo knew
about these two screens came from screens trimmed by hand; this is the first
byte-for-byte pair (header + items) in the suite, so it is the fixture that
pins:

  - the header/items discriminator behind "continue from the order already on
    screen" — the items page repeats the very same 'Order Number:' line, and
    mistaking it for a header would capture an order starting mid-way;
  - the Sub-Total guard, which is what would catch a lost items page if the
    capture ever pages too fast;
  - parse_ship_via with an EMPTY Ship Via field (this order has none), which is
    how the neighbouring 'Shipped From' column used to become the carrier.

Only trailing 80-column padding was removed; every internal column gap is as
the terminal drew it.
"""

from as400_capture import (
    STATE_ORDER_INQUIRY,
    _is_order_header_screen,
    _is_void_order,
    classify_screen,
)
from parser import parse_order
from pipeline import preview_order

HEADER = """                           O R D E R   I N Q U I R Y

 Order Number: 881310                       Account Number: 0008672 00

 Bill MIAMI BEACH BICYCLE CENTER INC   Ship MIAMI BEACH BICYCLE CENTER INC
      746-5TH STREET                        746-5TH STREET

      MIAMI BEACH     FL  33139             MIAMI BEACH     FL  33139

 Terms: 28        Cr Lim:        .00   Invoice Comments
 Sales ID: 215    Order Taken By: MAX
 Credit Hold:     Carton Count:     0
 Order Date: 082826 P/O: ALEX 8/28

           Ship Via                    Shipped From New Jersey
         COD Tag No                    Ship Date    08/28/26
          Sub-Total         6776.05
     Freight & Misc             .00
        Order Total         6776.05
         Invoice No
           Inv Date
 Order Comments: FREE FREIGHT NET 60      PART 1 BUY-IN PREMIER
                                   Cmd5            Cmd6                 Cmd7
                                    DETAILS         RETURN TO SELECT     EXIT"""

ITEMS = """                            O R D E R   I N Q U I R Y


 Order Number: 881310                       Account Number: 0008672 00

 Bill MIAMI BEACH BICYCLE CENTER INC

 Quant  Quant  Stock #   W/H   Description                       Unit    Extend
   Ord   Ship                                                   Price
     1      1  03 3732 BL  N   DURANGO A2 19 2026 MIDNIGHT BL  416.95    416.95
     1      1  03 3978 BL  N   CITIZEN 2 17 2026 MONTEREY BLU  403.95    403.95
     1      1  03 3980 BL  N   CITIZEN 2 19 2026 MONTEREY BLU  403.95    403.95
     3      3  03 4805 RD  N   ALLEGRO A3 15 2026 CAYENNE      346.95   1040.85
     1      1  03 4807 RD  N   ALLEGRO A3 17 2026 CAYENNE      346.95    346.95
     7      7  03 4809 RD  N   ALLEGRO A3 19 2026 CAYENNE      346.95   2428.65
     5      5  03 4811 RD  N   ALLEGRO A3 21 2026 CAYENNE      346.95   1734.75




                                END OF ORDER                            6776.05

              Enter             Cmd6
              More Details       RETURN TO SELECT"""

CAPTURE = HEADER + "\n" + ITEMS


# ── the screens themselves ───────────────────────────────────────────────────


def test_both_screens_are_a_ready_order_view():
    assert classify_screen(HEADER) == STATE_ORDER_INQUIRY
    assert classify_screen(ITEMS) == STATE_ORDER_INQUIRY


def test_header_is_recognised_and_items_page_is_not():
    # The whole "continue from the order already on screen" shortcut rests here.
    assert _is_order_header_screen(HEADER, "881310")
    assert not _is_order_header_screen(ITEMS, "881310")
    # ...and never for a different order.
    assert not _is_order_header_screen(HEADER, "881311")


def test_a_normal_order_is_not_void():
    assert not _is_void_order(HEADER)


# ── what the parser gets out of it ───────────────────────────────────────────


def test_header_fields():
    d = parse_order(CAPTURE)
    assert d["order_number"] == "881310"
    assert d["account_number"] == "0008672 00"
    assert (d["as400_account"], d["as400_ship_to"]) == ("8672", "00")
    assert d["customer_name"] == "MIAMI BEACH BICYCLE CENTER INC"
    assert d["order_date"] == "2026-08-28"
    assert d["subtotal"] == 6776.05
    assert d["is_last_page"] is True


def test_ship_to_address():
    ship = parse_order(CAPTURE)["shipping"]
    assert ship["name"] == "MIAMI BEACH BICYCLE CENTER INC"
    assert ship["city"] == "MIAMI BEACH"
    assert ship["state"] == "FL"
    assert ship["zip_code"] == "33139"


def test_empty_ship_via_is_none_not_the_next_column():
    # This order's Ship Via is blank; 'Shipped From New Jersey' sits to its right.
    assert parse_order(CAPTURE)["ship_via"] is None


def test_all_seven_lines_and_nineteen_units():
    items = parse_order(CAPTURE)["items"]
    assert len(items) == 7
    assert sum(i["qty"] for i in items) == 19
    assert [i["sku"] for i in items] == [
        "033732BL",
        "033978BL",
        "033980BL",
        "034805RD",
        "034807RD",
        "034809RD",
        "034811RD",
    ]


def test_subtotal_reconciles_so_no_page_was_lost():
    # The guard that would catch a capture that paged too fast: the header's
    # Sub-Total against the sum of the parsed lines.
    assert preview_order(CAPTURE)["total_mismatch"] is False
