"""
Tests for the AS400 account header split. 'Account Number: 0010495 00' is a
7-digit bill-to account plus a 2-digit ship-to suffix, and FedEx Ship Manager's
Recipient ID is the account without leading zeros followed by the suffix
('1049500'). parse_order exposes both halves so the watcher can seal them onto
the customer and the ship-to address. Only imports `parser` (no Supabase deps).
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from test_parser_header import HEADER, REAL_880036  # noqa: E402

from parser import parse_account_number, parse_order, split_account_number  # noqa: E402


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("0010495 00", ("10495", "00")),
        ("0000991 00", ("991", "00")),
        ("0007099 01", ("7099", "01")),
        ("0010495   00", ("10495", "00")),  # extra spaces between the halves
        ("  0010495 00  ", ("10495", "00")),
        ("0010495", ("10495", None)),  # no ship-to suffix
        ("0010495 1", ("10495", None)),  # a suffix is exactly two digits
        ("0000000 00", (None, "00")),  # an all-zero account is no account
        ("0000000", (None, None)),
        ("VOID", (None, None)),
        ("", (None, None)),
        (None, (None, None)),
    ],
)
def test_split_account_number(raw, expected):
    assert split_account_number(raw) == expected


def test_split_keeps_suffix_zeros():
    # '00' is a real slot (the dealer's first store); it must never collapse to ''.
    assert split_account_number("0010495 00")[1] == "00"


def test_split_rejects_account_longer_than_seven_digits():
    # Not an AS400 account (they are 7 digits) and it would fail the DB CHECK.
    assert split_account_number("123456789 00") == (None, "00")


def test_parse_order_exposes_account_and_ship_to():
    d = parse_order(HEADER)
    assert d["account_number"] == "0020045 00"  # the raw header value, unchanged
    assert d["as400_account"] == "20045"
    assert d["as400_ship_to"] == "00"


def test_parse_order_real_capture_880036():
    assert parse_account_number(REAL_880036) == "0010495 00"
    d = parse_order(REAL_880036)
    assert (d["as400_account"], d["as400_ship_to"]) == ("10495", "00")


def test_parse_order_without_header_yields_none():
    d = parse_order("Order Number: 1\n Terms: 5")
    assert d["account_number"] is None
    assert d["as400_account"] is None
    assert d["as400_ship_to"] is None
