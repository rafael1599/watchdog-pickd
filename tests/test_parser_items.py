"""
Tests for line-item / SKU parsing in parser.parse_items.

Focus: the canonical SKU is `<dept><number><2-letter color>`. A finish/variant
letter the PDF glues onto the color (e.g. '03 3768 BLD') must NOT pollute the
SKU — it stays in raw_sku only. These import `parser` only (no Supabase deps).
"""

from parser import parse_items


def _one(line):
    items = parse_items(line)
    assert len(items) == 1, f"expected 1 item, got {len(items)}: {items}"
    return items[0]


def test_extra_variant_letter_does_not_pollute_sku():
    # Reported bug: '03 3768 BLD' (3rd letter from a finish/variant column) was
    # parsed as SKU '033768BLD' → not found. Canonical SKU is the 2-letter color.
    it = _one("1   1  03 3768 BLD  N   DIVIDE S/O 12X27 2025 RIPTIDE   394.95   394.95")
    assert it["sku"] == "033768BL"
    assert it["warehouse"] == "N"
    assert it["description"] == "DIVIDE S/O 12X27 2025 RIPTIDE"
    # The variant letter is preserved verbatim for the record, not in the SKU.
    assert it["raw_sku"] == "03 3768 BLD"


def test_reported_second_line():
    it = _one("1   1  03 3769 BLD  N   DIVIDE S/O 14X27 2025 RIPTIDE   394.95   394.95")
    assert it["sku"] == "033769BL"
    assert it["raw_sku"] == "03 3769 BLD"


def test_clean_two_letter_color_unchanged():
    it = _one("1   1  03 3768 BL   N   DIVIDE S/O 12X27 2025 RIPTIDE   394.95   394.95")
    assert it["sku"] == "033768BL"
    assert it["raw_sku"] == "03 3768 BL"


def test_trailing_t_variant_canonicalizes():
    # '...BLT' previously relied on a downstream fuzzy 'T' strip; now it parses to
    # the canonical 2-letter-color SKU directly.
    it = _one("1   1  03 3994 BLT  N   SOMEBIKE 2025 COLOR  1299.95  1299.95")
    assert it["sku"] == "033994BL"
    assert it["raw_sku"] == "03 3994 BLT"


def test_docstring_sample_still_parses():
    it = _one("4     4      03 3684 BR   N    FAULTLINE A1 17 2025 SANDSTONE    1299.95  5199.80")
    assert it["sku"] == "033684BR"
    assert it["qty"] == 4
    assert it["unit_price"] == 1299.95
    assert it["extend_price"] == 5199.80


def test_zero_padded_prices():
    it = _one("2   0  03 3685 GY  N   BACKORDER BIKE 2025  .00  .00")
    assert it["sku"] == "033685GY"
    assert it["qty"] == 2
    assert it["unit_price"] == 0.0


def test_non_item_lines_are_ignored():
    text = """ Quant  Quant  Stock #   W/H   Description                       Unit    Extend
 Order Number: 880009
                                END OF ORDER                             388.95"""
    assert parse_items(text) == []
