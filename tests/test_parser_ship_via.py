"""
Tests for parser.parse_ship_via — the shipping carrier from the 'Ship Via'
header field, captured verbatim (FEDEX, UPS GROUND, …) without swallowing the
neighbouring 'Shipped From …' column.
"""

from parser import parse_order, parse_ship_via

HEADER_FEDEX = """                            O R D E R   I N Q U I R Y
 Order Number: 880009                       Account Number: 0003574 00
 Bill BIKES AND MORE                   Ship BIKES AND MORE
 Order Date: 060226 P/O: SO1608
           Ship Via    FEDEX           Shipped From Florida
         COD Tag No    447424067133    Ship Date    06/03/26"""


def test_parses_fedex_verbatim():
    assert parse_ship_via(HEADER_FEDEX) == "FEDEX"


def test_does_not_swallow_shipped_from_column():
    # The 'Shipped From Florida' column sits to the right behind a 2+ space gap.
    assert "SHIPPED" not in (parse_ship_via(HEADER_FEDEX) or "").upper()


def test_multiword_carrier_kept_intact():
    line = "           Ship Via    UPS GROUND        Shipped From Florida"
    assert parse_ship_via(line) == "UPS GROUND"


def test_absent_ship_via_returns_none():
    assert parse_ship_via("Order Number: 880009\n Bill SOMEONE") is None


def test_parse_order_exposes_ship_via():
    assert parse_order(HEADER_FEDEX)["ship_via"] == "FEDEX"
