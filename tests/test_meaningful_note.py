"""
Tests for pipeline.meaningful_note — mirror of pickd's meaningfulNote filter.
Freight boilerplate is noise; real instructions and unknown notes are kept.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from pipeline import meaningful_note  # noqa: E402


def test_drops_pure_freight_noise():
    assert meaningful_note("FREE FREIGHT") is None
    assert meaningful_note("Freight $65.00") is None
    assert meaningful_note("  FREIGHT  ") is None
    assert meaningful_note("PREPAID") is None
    assert meaningful_note("FOB") is None


def test_keeps_real_instructions_even_with_freight():
    assert meaningful_note("FREE FREIGHT - DO NOT SHIP UNTIL MONDAY") is not None
    assert meaningful_note("FREIGHT $65 - hold for pickup") is not None
    assert meaningful_note("wait for inventory") == "wait for inventory"
    assert meaningful_note("CALL BEFORE SHIPPING") is not None


def test_keeps_unknown_notes():
    assert meaningful_note("Leave at dock 3") == "Leave at dock 3"
    assert meaningful_note("Customer prefers UPS") == "Customer prefers UPS"


def test_blank_and_none_are_none():
    assert meaningful_note(None) is None
    assert meaningful_note("   ") is None


def test_not_uses_word_boundary():
    # 'notation' must NOT trigger the keep rule, so a freight note stays noise.
    assert meaningful_note("FREE FREIGHT notation") is None
    assert meaningful_note("FREE FREIGHT, not ready") is not None
