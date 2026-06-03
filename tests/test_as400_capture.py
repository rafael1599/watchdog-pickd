"""
Tests for the AS400 capture LOOP logic (pure, no GUI / no macOS).

A FakeDriver feeds canned screens so we can assert the F5/ENTER/END OF ORDER
flow and the multi-page accumulation without driving Mocha.
"""

import pytest

from as400_capture import CaptureError, _has_end_marker, capture_order, run_login


class FakeDriver:
    """Replays a list of screens and records the keys pressed."""

    def __init__(self, screens=None):
        self._screens = list(screens or [])
        self._idx = -1  # first copy_screen returns screens[0]
        self.keys = []
        self.typed = None
        self.focused = False
        self.launched = False
        self.actions = []  # ordered ("text"|"key", value) for login assertions

    def launch(self):
        self.launched = True

    def focus(self):
        self.focused = True

    def type_text(self, text):
        self.typed = text
        self.actions.append(("text", text))

    def key(self, name):
        self.keys.append(name.lower())
        self.actions.append(("key", name.lower()))

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


def test_marker_ignores_extra_whitespace():
    # 5250 captures may space out letters/words
    assert _has_end_marker("... E N D  O F  O R D E R")
    assert _has_end_marker("items\nEND   OF\nORDER")
    assert not _has_end_marker("END OF LINE")


def test_stops_with_spaced_out_marker():
    driver = FakeDriver(["HEADER", "ITEM 1", "E N D   O F   O R D E R"])
    capture_order("7", driver, page_wait=0)
    assert driver.keys == ["f5", "enter"]  # one ENTER, then stop on spaced marker


def test_raises_when_marker_never_appears():
    driver = FakeDriver(["HEADER"] + ["ITEMS NO MARKER"] * 50)
    with pytest.raises(CaptureError):
        capture_order("999", driver, page_wait=0, max_pages=5)


def test_login_macro_replays_steps_in_order():
    driver = FakeDriver()
    run_login(driver, step_wait=0)
    # Default macro: ROMAN, TAB, STOU, ENTER, ENTER, 3, ENTER
    assert driver.actions == [
        ("text", "ROMAN"),
        ("key", "tab"),
        ("text", "STOU"),
        ("key", "enter"),
        ("key", "enter"),
        ("text", "3"),
        ("key", "enter"),
    ]


def test_login_supports_custom_steps_and_wait():
    driver = FakeDriver()
    run_login(driver, login_steps=[("text", "USER"), ("wait", 0), ("key", "enter")], step_wait=0)
    assert driver.actions == [("text", "USER"), ("key", "enter")]
