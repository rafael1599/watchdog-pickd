"""Stock Inquiry: real screens kept by the watcher on 13 Nov 2026 (as400_screens,
classified stock_inquiry), one per case that was read wrong or never checked."""

from pathlib import Path

from parser import parse_stock_inquiry

FIX = Path(__file__).parent / "fixtures"


def screen(sku: str) -> dict:
    return parse_stock_inquiry((FIX / f"stock_{sku}.txt").read_text())


def test_a_negative_is_written_with_the_sign_behind():
    # «Available      1-      16        0» — more committed than on hand in NJ
    s = screen("06-4572GY")
    assert s["on_hand"] == {"NJ": 0, "FL": 16, "CA": 0}
    assert s["on_order"] == {"NJ": 1, "FL": 0, "CA": 0}
    assert s["available"] == {"NJ": -1, "FL": 16, "CA": 0}


def test_a_negative_in_another_warehouse_column():
    # «Available     21        3-       0» — FL is the negative one
    assert screen("37-1374BK")["available"] == {"NJ": 21, "FL": -3, "CA": 0}


def test_the_price_breaks_are_not_moved_by_the_sign():
    s = screen("06-4572GY")
    assert s["price_breaks"][0] == {"label": "Each", "price": 354.95, "qty": 49}


def test_an_empty_vendor_is_none_not_its_colon():
    s = screen("70-0105BK")
    assert s["vendor_no"] is None
    assert s["model_year"] is None  # «Model Year:» empty too
    assert s["bin_location"] is None


def test_a_vendor_with_a_number():
    assert screen("03-3827GY")["vendor_no"] == "0065"


def test_bin_and_stock_location_both_filled():
    s = screen("03-3827GY")
    assert s["bin_location"] == "R40" and s["stock_location"] == "R42"


def test_an_empty_unit_measure():
    s = screen("45-1036")
    assert s["unit_meas"] is None and s["bin_location"] == "E20"


def test_an_empty_bin_beside_an_empty_status_code():
    s = screen("02-3054TL")
    assert s["bin_location"] is None and s["status_code"] is None
