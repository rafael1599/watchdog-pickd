"""
Tests for parser.parse_order_date — the AS400 'Order Date' header field
(6-digit MMDDYY) returned as an ISO 'YYYY-MM-DD' string, defensively.
"""

from parser import parse_order, parse_order_date

HEADER = """                            O R D E R   I N Q U I R Y
 Order Number: 880009                       Account Number: 0003574 00
 Bill BIKES AND MORE                   Ship BIKES AND MORE
 Order Date: 060226 P/O: SO1608
           Ship Via    FEDEX           Shipped From Florida"""


def test_parses_mmddyy_to_iso():
    # 060226 → month 06, day 02, year 2026.
    assert parse_order_date(HEADER) == "2026-06-02"


def test_other_real_date():
    assert parse_order_date(" Order Date: 060426 P/O: SO1610") == "2026-06-04"


def test_missing_order_date_returns_none():
    assert parse_order_date("Order Number: 880009\n Bill SOMEONE") is None


def test_invalid_month_returns_none():
    # Month 13 is not a real month.
    assert parse_order_date(" Order Date: 130126") is None


def test_invalid_day_returns_none():
    # Day 00 / day 32 are not real days.
    assert parse_order_date(" Order Date: 060026") is None
    assert parse_order_date(" Order Date: 063226") is None


def test_garbage_returns_none():
    assert parse_order_date(" Order Date: ABCDEF") is None
    assert parse_order_date(" Order Date:") is None


def test_parse_order_exposes_order_date():
    assert parse_order(HEADER)["order_date"] == "2026-06-02"
