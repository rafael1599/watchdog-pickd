"""
canonical_sku — the SAME case table lives in pickd's SQL migration
20260826220000 (validated against prod) and in src/utils/__tests__/
skuNormalize.test.ts. Change one, change all three.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from parser import canonical_sku  # noqa: E402

CASES = [
    ("01-530", "01-0530"),
    ("01 0530", "01-0530"),
    ("033768BLD", "03-3768BLD"),
    ("03 3768 BLD", "03-3768BLD"),
    ("700106BK", "70-0106BK"),
    ("128353", "12-8353"),
    ("  033768bld ", "03-3768BLD"),
    ("03-3768BL", "03-3768BL"),
    ("01-0530", "01-0530"),
    ("PKD-252HEX", "PKD-252HEX"),
    ("23-00146A", "23-00146A"),
    ("792282670112", "792282670112"),
    ("Y22B010415", "Y22B010415"),
    ("brakes", "BRAKES"),
    ("03", "03"),
    ("ABC123", "ABC123"),
    ("01-093]", "01-093]"),
    ("S/D03-3826GY", "S/D03-3826GY"),
    ("", ""),
]


@pytest.mark.parametrize("raw,expected", CASES)
def test_canonical_sku(raw, expected):
    assert canonical_sku(raw) == expected


@pytest.mark.parametrize("raw,expected", CASES)
def test_canonical_sku_is_idempotent(raw, expected):
    assert canonical_sku(expected) == expected


def test_none_is_empty():
    assert canonical_sku(None) == ""
