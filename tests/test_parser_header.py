"""
Tests for header parsing added for AS400 captures:
Order Comments → notes, and the Ship-to block (right column of the header).
These only import `parser` (no Supabase deps).
"""

from parser import (
    parse_customer_name,
    parse_order,
    parse_order_comments,
    parse_shipping_address,
    parse_shipping_address_struct,
)

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


def test_parse_customer_name_from_as400_header():
    # AS400 indents 'Bill' and puts 'Ship' on the same line — must return only
    # the Bill-to name, not the Ship-to column.
    assert parse_customer_name(HEADER) == "DEALER WARRANTY 2009"


def test_parse_customer_name_from_pdf_line_start():
    assert parse_customer_name("Bill MATTHEWS BICYCLE MART, INC") == "MATTHEWS BICYCLE MART, INC"


def test_parse_customer_name_absent():
    assert parse_customer_name("Order Number: 1\n Terms: 5") is None


def test_parse_customer_name_blank_bill_returns_none():
    # Bill column empty, only a Ship-to name present — must not return 'Ship NAME'.
    assert parse_customer_name(" Bill                       Ship CAMP HIGH ROCKS") is None


def test_parse_order_comments():
    assert parse_order_comments(HEADER) == "SEE EMAIL FOR CC PAYMENT"


def test_parse_order_comments_strips_as400_cmd_legend():
    # AS400 renders the function-key legend on the same row; it is not a comment.
    assert parse_order_comments("Order Comments: Cmd5 Cmd6 Cmd7") is None
    assert parse_order_comments("Order Comments: CALL FIRST  Cmd5 Cmd6") == "CALL FIRST"


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


def test_parse_shipping_struct_splits_fields():
    assert parse_shipping_address_struct(HEADER) == {
        "name": "CHICAGO LAND BICYCLES",
        "street": "10355 SOUTH KEDZIE",
        "city": "CHICAGO",
        "state": "IL",
        "zip_code": "60655",
    }


def test_parse_shipping_struct_handles_zip_plus_four():
    text = (
        " Bill X CORP                           Ship ACME CO\n"
        "                                            5 MAIN ST\n"
        "                                            RENO            NV  89501-1234\n"
        " Terms: 1"
    )
    s = parse_shipping_address_struct(text)
    assert s["zip_code"] == "89501-1234"
    assert s["city"] == "RENO" and s["state"] == "NV"


def test_parse_shipping_struct_none_when_absent():
    assert parse_shipping_address_struct("Order Number: 1\n Terms: 5") is None


def test_parse_order_exposes_new_fields():
    d = parse_order(HEADER)
    assert d["order_comments"] == "SEE EMAIL FOR CC PAYMENT"
    assert d["shipping_address"].startswith("CHICAGO LAND BICYCLES")
    assert d["shipping"]["city"] == "CHICAGO"


# A full real AS400 capture (order #880036): indented Bill on the same line as
# Ship, a blank line inside the Ship-to block, and the Cmd legend after the
# Order Comments. Guards the customer / comments / shipping parsing end to end.
REAL_880036 = """   O R D E R   I N Q U I R Y

 Order Number: 880036                       Account Number: 0010495 00

 Bill TUCKER CYCLES                    Ship TUCKER CYCLES
      3544 ST JOHNS AVE                     3544 ST JOHNS AVE

      JACKSONVILLE    FL  32205             JACKSONVILLE    FL  32205

 Terms: 28        Cr Lim:        .00   Invoice Comments
 Sales ID: 223    Order Taken By: ANN
 Order Date: 060426 P/O: SO1610
 Order Comments: FREE FREIGHT
                                   Cmd5            Cmd6                 Cmd7
                                    DETAILS         RETURN TO SELECT     EXIT
"""


def test_real_capture_880036():
    d = parse_order(REAL_880036)
    assert d["customer_name"] == "TUCKER CYCLES"
    assert d["order_comments"] == "FREE FREIGHT"
    assert d["shipping"] == {
        "name": "TUCKER CYCLES",
        "street": "3544 ST JOHNS AVE",
        "city": "JACKSONVILLE",
        "state": "FL",
        "zip_code": "32205",
    }
