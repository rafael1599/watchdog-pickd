"""
Tests for header parsing added for AS400 captures:
Order Comments → notes, and the Ship-to block (right column of the header).
These only import `parser` (no Supabase deps).
"""

from parser import parse_order, parse_order_comments, parse_shipping_address

HEADER = """                            O R D E R   I N Q U I R Y
 Order Number: 880013                       Account Number: 0020045 00
 Bill DEALER WARRANTY 2009             Ship CHICAGO LAND BICYCLES
                                            10355 SOUTH KEDZIE
      NORTHVALE       NJ  07647             CHICAGO         IL  60655
 Terms: 14        Cr Lim:        .00   Invoice Comments
 Sales ID: 125    Order Taken By: JON
 Order Comments: SEE EMAIL FOR CC PAYMENT
                            O R D E R   I N Q U I R Y
 Quant  Quant  Stock #   W/H   Description
     1      1  99 3406     N   JRP DER HNGR
                                END OF ORDER                              14.95"""


def test_parse_order_comments():
    assert parse_order_comments(HEADER) == "SEE EMAIL FOR CC PAYMENT"


def test_parse_order_comments_absent():
    assert parse_order_comments("Order Number: 1\n Terms: 5") is None


def test_parse_order_comments_ignores_invoice_comments():
    assert parse_order_comments("Invoice Comments: ignore me") is None


def test_parse_shipping_address_collects_right_column():
    assert (
        parse_shipping_address(HEADER)
        == "CHICAGO LAND BICYCLES, 10355 SOUTH KEDZIE, CHICAGO IL 60655"
    )


def test_parse_shipping_address_stops_before_items_and_end():
    # No 'Terms:' footer: must still stop at END OF ORDER, not swallow it.
    assert parse_shipping_address("Bill X   Ship ACME CO\n  END OF ORDER") == "ACME CO"


def test_parse_shipping_ignores_ship_via_and_date():
    text = "           Ship Via                    Shipped From New Jersey\n         Ship Date    06/03/26"
    assert parse_shipping_address(text) is None


def test_parse_order_exposes_new_fields():
    d = parse_order(HEADER)
    assert d["order_comments"] == "SEE EMAIL FOR CC PAYMENT"
    assert d["shipping_address"].startswith("CHICAGO LAND BICYCLES")
