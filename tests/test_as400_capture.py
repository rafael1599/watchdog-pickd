"""
Tests for the AS400 capture LOOP logic (pure, no GUI / no macOS).

A FakeDriver feeds canned screens so we can assert the F5/ENTER/END OF ORDER
flow and the multi-page accumulation without driving Mocha.
"""

import pytest

from as400_capture import CaptureError, capture_order


class FakeDriver:
    """Replays a list of screens and records the keys pressed."""

    def __init__(self, screens):
        self._screens = list(screens)
        self._idx = -1  # first copy_screen returns screens[0]
        self.keys = []
        self.typed = None
        self.focused = False

    def focus(self):
        self.focused = True

    def type_text(self, text):
        self.typed = text

    def key(self, name):
        self.keys.append(name.lower())

    def copy_screen(self):
        self._idx += 1
        return self._screens[self._idx]


def test_single_item_page_stops_immediately():
    # header, then one items page already containing END OF ORDER
    driver = FakeDriver(["CUSTOMER HEADER", "ITEM 1\nITEM 2\nEND OF ORDER"])
    text = capture_order("880005", driver, page_wait=0)

    assert driver.focused
    assert driver.typed == "880005"
    # F5 once to enter items, no ENTER because END OF ORDER was on the first items page
    assert driver.keys == ["f5"]
    assert "CUSTOMER HEADER" in text
    assert "END OF ORDER" in text


def test_multi_page_pages_with_enter_until_marker():
    driver = FakeDriver(
        [
            "CUSTOMER HEADER",  # page 1 (header)
            "ITEM 1\nITEM 2",  # items page 1 (no marker)
            "ITEM 3\nITEM 4",  # items page 2 (no marker)
            "ITEM 5\nEND OF ORDER",  # items page 3 (marker)
        ]
    )
    text = capture_order("880006", driver, page_wait=0)

    # F5 once, then ENTER between item pages (2 ENTERs for 3 item pages)
    assert driver.keys == ["f5", "enter", "enter"]
    for chunk in ["CUSTOMER HEADER", "ITEM 1", "ITEM 3", "ITEM 5", "END OF ORDER"]:
        assert chunk in text


def test_marker_is_case_insensitive():
    driver = FakeDriver(["HEADER", "item\nend of order"])
    text = capture_order("1", driver, page_wait=0)
    assert "end of order" in text
    assert driver.keys == ["f5"]


def test_raises_when_marker_never_appears():
    driver = FakeDriver(["HEADER"] + ["ITEMS NO MARKER"] * 50)
    with pytest.raises(CaptureError):
        capture_order("999", driver, page_wait=0, max_pages=5)
