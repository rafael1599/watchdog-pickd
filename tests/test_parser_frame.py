"""
test_parser_frame.py — Tabla canónica de casos para looks_like_frame (bug-037).

La MISMA tabla de 19 casos vive en docs/prds/catalog-writers-study.md §6(a),
en pickd (src/utils/__tests__/bikeDetection.test.ts) y en la migración SQL de catálogo.
Cambiar uno es cambiar los tres.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from parser import looks_like_frame  # noqa: E402

CASES = [
    # 11 casos positivos (§6(a))
    ("FRAME RENEGADE S1 48 2024 CHARCOAL", True),
    ("FRAME RENEGADE S1 UDH 54 2025 CHARCOAL", True),
    ("JRP FRAME HARDLINE C1 2024", True),
    ("JRP FRAME RENEGADE S1 54 2024 CHARCOAL", True),
    ("RENEGADE S1 UDH FRAMEKIT 54 CHARCOAL", True),
    ("RENEGADE S1 FRAMEKIT Charcoal 56", True),
    ("WARRANTY FRAME", True),
    ("Faultline A1 Frame", True),
    ("ENDURA FRAME 700 x 54", True),
    ("PORTAL C2 FRAME", True),
    ("RENEGADE C1 FRAMESET 56", True),
    # 8 casos negativos (§6(a))
    ("TRAIL X1", False),
    ("FAULTLINE A1", False),
    ("RENEGADE S3", False),
    ('EXPLORER A2 19" Gloss Black', False),
    ("CODA S2 L16 2026 GLOSS BLACK", False),
    ("PEDAL TAXI 2020 20 GLOSS BLACK", False),
    ("CITIZEN 2", False),
    ("BOSS CRUISER 7", False),
]


@pytest.mark.parametrize("description,expected", CASES)
def test_looks_like_frame_table(description, expected):
    assert looks_like_frame(description) is expected


def test_looks_like_frame_empty_and_none():
    assert looks_like_frame(None) is False
    assert looks_like_frame("") is False
    assert looks_like_frame("   ") is False
