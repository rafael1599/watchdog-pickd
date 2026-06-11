"""
Tests for the AS400 capture LOOP logic (pure, no GUI / no macOS).

A FakeDriver feeds canned screens so we can assert the F5/ENTER/END OF ORDER
flow and the multi-page accumulation without driving Mocha.
"""

import pytest

from as400_capture import (
    STATE_DISCONNECTED,
    STATE_LOGIN,
    STATE_MENU,
    STATE_MESSAGE,
    STATE_ORDER_INQUIRY,
    STATE_ORDER_SEARCH,
    STATE_UNKNOWN,
    AS400Disconnected,
    AS400ManualLoginRequired,
    CaptureError,
    OrderVoidSkip,
    _has_end_marker,
    _is_invalid_order,
    _is_message_info_screen,
    bootstrap_session,
    capture_order,
    classify_screen,
    run_login,
)

# The 'ADDITIONAL MESSAGE INFORMATION' dead-end a VOID order can route to (operator
# report 2026-06-11): a BAS-#### error + 'No matching key' + an 'Option:' prompt.
ADDL_MSG_SCREEN = """                                                              XP
                         ADDITIONAL MESSAGE INFORMATION
    BAS-5065  Options (  23 )
    Line 1800: No matching key
    There is no additional information for this message.
    Option:"""

# A logged-in "order search" screen, used as the pre-capture check screen.
READY = "O R D E R   N U M B E R: ______"

# Real captures from the login flow (see docs §3.1), used to lock classify_screen.
SIGN_ON_SCREEN = """                                   Sign On
                                               System  . . . . . :   S104DPPM
                User  . . . . . . . . . . . . . .
                Password  . . . . . . . . . . . .
                Program/procedure . . . . . . . .
                                        (C) COPYRIGHT IBM CORP. 1980, 1999."""

MESSAGE_SCREEN = """                               Message Display                                XP
 SYS-7300     also routed to W1 from jobname - XP150044       Time- 20:02:55
  The 3 option was taken to this message
                          Press Enter to continue"""

MENU_SCREEN = """ COMMAND                      SALESN Options                                 XP
  01. Customer Inquiry
  02. Stock File Inquiry
  03. Order Inquiry
  04. Accounts Receivable Inquiry
 Ready for option number or command"""

# Empty order-search screen (after picking 3 from the menu).
ORDER_SEARCH_SCREEN = """                            O R D E R   I N Q U I R Y
 Order Number:                              Account Number:
 Alpha Search:                                     Invoice:"""

# Header page after typing an order number.
ORDER_HEADER_SCREEN = """                            O R D E R   I N Q U I R Y
 Order Number: 880009                       Account Number: 0003574 00
 Bill BIKES AND MORE                   Ship BIKES AND MORE
      2133 NW 6TH STREET                    2133 NW 6TH STREET
 Order Total          453.95
                                   Cmd5            Cmd6                 Cmd7
                                    DETAILS         RETURN TO SELECT     EXIT"""

# Items page after F5, with the END OF ORDER marker.
ORDER_ITEMS_END_SCREEN = """                            O R D E R   I N Q U I R Y
 Order Number: 880009                       Account Number: 0003574 00
 Quant  Quant  Stock #   W/H   Description                       Unit    Extend
     1      1  03 4068 BK  F   EXPLORER A2 17 2025 GLOSS BLAC  388.95    388.95
                                END OF ORDER                             388.95"""


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
    # pre-check ready screen, header, then one items page with END OF ORDER
    driver = FakeDriver(
        [READY, "O R D E R  I N Q U I R Y\nCUSTOMER HEADER", "ITEM 1\nITEM 2\nEND OF ORDER"]
    )
    text = capture_order("880005", driver, page_wait=0)

    assert driver.focused
    assert driver.typed == "880005"
    # F6 (new search), F5 once to enter items, no ENTER (marker on first items page)
    assert driver.keys == ["f6", "f5"]
    assert "O R D E R  I N Q U I R Y\nCUSTOMER HEADER" in text
    assert "END OF ORDER" in text


def test_multi_page_pages_with_enter_until_marker():
    driver = FakeDriver(
        [
            READY,  # pre-capture check
            "O R D E R  I N Q U I R Y\nCUSTOMER HEADER",  # page 1 (header)
            "ITEM 1\nITEM 2",  # items page 1 (no marker)
            "ITEM 3\nITEM 4",  # items page 2 (no marker)
            "ITEM 5\nEND OF ORDER",  # items page 3 (marker)
        ]
    )
    text = capture_order("880006", driver, page_wait=0)

    # F6 (new search), F5 once, then ENTER between item pages (2 ENTERs for 3 pages)
    assert driver.keys == ["f6", "f5", "enter", "enter"]
    for chunk in [
        "O R D E R  I N Q U I R Y\nCUSTOMER HEADER",
        "ITEM 1",
        "ITEM 3",
        "ITEM 5",
        "END OF ORDER",
    ]:
        assert chunk in text


def test_marker_is_case_insensitive():
    driver = FakeDriver([READY, "ORDER INQUIRY", "item\nend of order"])
    text = capture_order("1", driver, page_wait=0)
    assert "end of order" in text
    assert driver.keys == ["f6", "f5"]


def test_marker_ignores_extra_whitespace():
    # 5250 captures may space out letters/words
    assert _has_end_marker("... E N D  O F  O R D E R")
    assert _has_end_marker("items\nEND   OF\nORDER")
    assert not _has_end_marker("END OF LINE")


def test_stops_with_spaced_out_marker():
    driver = FakeDriver([READY, "ORDER INQUIRY", "ITEM 1", "E N D   O F   O R D E R"])
    capture_order("7", driver, page_wait=0)
    assert driver.keys == ["f6", "f5", "enter"]  # F6 search, F5 items, one ENTER, then stop


def test_raises_when_marker_never_appears():
    # Distinct pages so the loop keeps advancing until it hits the max_pages cap.
    driver = FakeDriver([READY, "ORDER INQUIRY"] + [f"ITEMS PAGE {n}" for n in range(50)])
    with pytest.raises(CaptureError):
        capture_order("999", driver, page_wait=0, max_pages=5)


class LaggyTerminalDriver:
    """Models a 5250 terminal: copy_screen() returns the CURRENT page, which only
    changes after F5/ENTER. With lag>0 the screen stays stale for `lag` reads after
    a transition, simulating a not-yet-refreshed screen (the source of skipped
    items when the old loop paged forward on a stale copy)."""

    def __init__(self, header, item_pages, lag=0, precheck=READY):
        self.header = header
        self.item_pages = list(item_pages)
        self.lag = lag
        self.precheck = precheck
        self._did_precheck = False
        self.cur = header
        self._pending = None
        self._stale_left = 0
        self.item_idx = -1
        self.keys = []
        self.typed = None

    def focus(self):
        pass

    def launch(self):
        pass

    def type_text(self, t):
        self.typed = t

    def key(self, name):
        name = name.lower()
        self.keys.append(name)
        if name == "f5":
            self.item_idx = 0
            self._transition(self.item_pages[0])
        elif name == "enter":
            self.item_idx += 1
            nxt = (
                self.item_pages[self.item_idx] if self.item_idx < len(self.item_pages) else self.cur
            )
            self._transition(nxt)

    def _transition(self, new):
        self._pending = new
        self._stale_left = self.lag

    def copy_screen(self):
        if not self._did_precheck:
            self._did_precheck = True
            return self.precheck
        if self._pending is not None:
            if self._stale_left > 0:
                self._stale_left -= 1
                return self.cur  # stale: previous page still on screen
            self.cur = self._pending
            self._pending = None
        return self.cur


def test_paging_waits_for_refresh_so_no_items_are_skipped():
    # Two item pages; the screen lags one read behind on each transition. The old
    # loop would copy the stale page, see no marker, and ENTER again — skipping the
    # second page. The fixed loop waits for the real page before paging.
    items1 = "  O R D E R   I N Q U I R Y\n Order Number: 880009\n ITEM ONE\n ITEM TWO"
    items2 = "  O R D E R   I N Q U I R Y\n Order Number: 880009\n ITEM THREE\n END OF ORDER"
    driver = LaggyTerminalDriver(ORDER_HEADER_SCREEN, [items1, items2], lag=1)
    text = capture_order("880009", driver, page_wait=0, poll_interval=0.001, refresh_timeout=0.05)

    assert "ITEM ONE" in text and "ITEM TWO" in text
    assert "ITEM THREE" in text  # the page that used to get skipped
    assert _has_end_marker(text)
    assert driver.keys == ["f6", "f5", "enter"]  # exactly one ENTER for two pages


def test_paging_raises_when_screen_never_advances():
    items1 = "  O R D E R   I N Q U I R Y\n Order Number: 880009\n ITEM ONE"  # no marker
    driver = LaggyTerminalDriver(ORDER_HEADER_SCREEN, [items1, items1], lag=100)
    with pytest.raises(CaptureError):
        capture_order("880009", driver, page_wait=0, poll_interval=0.001, refresh_timeout=0.01)


def test_rejects_wrong_view_without_looping():
    # Ready at pre-check, but after typing we land on a menu (order doesn't exist):
    # bail out after the header, never press F5.
    driver = FakeDriver([READY, "MAIN MENU\n1. Inventory\n2. Customers\n3. Selection"])
    with pytest.raises(CaptureError):
        capture_order("880013", driver, page_wait=0)
    assert driver.keys == ["f6"]  # F6 only; no F5, no ENTER loop


# Search screen showing the rejection for a not-yet-registered order number.
INVALID_ORDER_SCREEN = """                            O R D E R   I N Q U I R Y
 Order Number:                              Account Number:
 Invalid Order Number, REENTER"""


def test_is_invalid_order_detects_message():
    assert _is_invalid_order(INVALID_ORDER_SCREEN)
    assert _is_invalid_order("...  I n v a l i d  O r d e r  N u m b e r , REENTER")
    assert not _is_invalid_order(ORDER_HEADER_SCREEN)


def test_invalid_order_number_stops_without_paging():
    # After typing a number that isn't a real order yet, AS400 shows 'Invalid Order
    # Number, REENTER'. Capture must stop (so the scanner retries it later), never F5.
    driver = FakeDriver([READY, INVALID_ORDER_SCREEN])
    with pytest.raises(CaptureError):
        capture_order("999999", driver, page_wait=0)
    assert driver.keys == ["f6"]  # fresh search only; no F5/ENTER paging


def test_capture_aborts_before_typing_when_disconnected():
    # Pre-check sees a dead session → raise immediately, never touch the keyboard.
    driver = FakeDriver(["Cannot connect to host 47.22.32.213 , port 23"])
    with pytest.raises(AS400Disconnected):
        capture_order("880013", driver, page_wait=0)
    assert driver.keys == []
    assert driver.typed is None


def test_capture_aborts_before_typing_when_not_logged_in():
    driver = FakeDriver(["Sign On\nUser . . .\nPassword . . ."])
    with pytest.raises(AS400ManualLoginRequired):
        capture_order("880013", driver, page_wait=0)
    assert driver.keys == []
    assert driver.typed is None


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


# --- classify_screen ----------------------------------------------------------


def test_classify_disconnected_from_real_error():
    # The exact message the user reported when the host is down.
    assert classify_screen("Cannot connect to host 47.22.32.213 , port 23") == STATE_DISCONNECTED


@pytest.mark.parametrize(
    "text",
    ["Connection refused", "Session ended", "DISCONNECTED", "the connection timed out"],
)
def test_classify_disconnected_variants(text):
    assert classify_screen(text) == STATE_DISCONNECTED


def test_classify_order_screens():
    assert classify_screen("O R D E R  I N Q U I R Y") == STATE_ORDER_INQUIRY
    assert classify_screen("ORDER NUMBER: ____") == STATE_ORDER_SEARCH


def test_classify_login_and_unknown():
    assert classify_screen("Sign On\nPassword . . .") == STATE_LOGIN
    assert classify_screen("MAIN MENU\n1. Inventory") == STATE_UNKNOWN
    assert classify_screen("") == STATE_UNKNOWN


def test_classify_real_login_flow_screens():
    assert classify_screen(SIGN_ON_SCREEN) == STATE_LOGIN
    assert classify_screen(MESSAGE_SCREEN) == STATE_MESSAGE
    # The menu lists "03. Order Inquiry" but must NOT be read as an order view.
    assert classify_screen(MENU_SCREEN) == STATE_MENU


def test_classify_real_order_screens_are_ready():
    # All three real ORDER INQUIRY screens (empty search, header, items) are ready.
    assert classify_screen(ORDER_SEARCH_SCREEN) == STATE_ORDER_INQUIRY
    assert classify_screen(ORDER_HEADER_SCREEN) == STATE_ORDER_INQUIRY
    assert classify_screen(ORDER_ITEMS_END_SCREEN) == STATE_ORDER_INQUIRY
    # The items page carries the stop marker.
    assert _has_end_marker(ORDER_ITEMS_END_SCREEN)


def test_capture_full_flow_with_real_screens():
    # pre-check (search) → type number → header → F5 → items page with END OF ORDER.
    driver = FakeDriver([ORDER_SEARCH_SCREEN, ORDER_HEADER_SCREEN, ORDER_ITEMS_END_SCREEN])
    text = capture_order("880009", driver, page_wait=0)
    assert driver.typed == "880009"
    assert driver.keys == ["f6", "f5"]  # fresh search, switch to items, marker on first page
    assert "EXPLORER A2 17 2025 GLOSS BLAC" in text
    assert _has_end_marker(text)


def test_classify_disconnected_wins_over_stale_order_text():
    # A dead session showing leftover order text must still read as disconnected.
    assert classify_screen("ORDER NUMBER\nCannot connect to host, port 23") == STATE_DISCONNECTED


# --- bootstrap_session (connect) ---------------------------------------------


class StatefulDriver(FakeDriver):
    """FakeDriver where copy_screen() always returns the same current screen."""

    def __init__(self, screen):
        super().__init__()
        self.screen = screen

    def copy_screen(self):
        return self.screen


def test_bootstrap_raises_when_disconnected_without_typing():
    driver = StatefulDriver("Cannot connect to host 47.22.32.213 , port 23")
    with pytest.raises(AS400Disconnected):
        bootstrap_session(driver, launch_wait=0, step_wait=0)
    assert driver.actions == []  # never replayed the login macro into a dead screen


def test_bootstrap_skips_login_when_already_logged_in():
    driver = StatefulDriver("ORDER NUMBER: ____")
    state = bootstrap_session(driver, launch_wait=0, step_wait=0)
    assert state == STATE_ORDER_SEARCH
    assert driver.actions == []  # no re-login


def test_bootstrap_unknown_screen_requires_manual_login():
    driver = StatefulDriver("MAIN MENU\n1. Inventory\n2. Customers")
    with pytest.raises(AS400ManualLoginRequired):
        bootstrap_session(driver, launch_wait=0, step_wait=0)
    assert driver.actions == []


def test_bootstrap_runs_login_then_verifies_order_screen():
    # Login screen first, then the macro lands us on the order-search screen.
    screens = iter(["Sign On\nPassword", "ORDER NUMBER: ____"])
    driver = FakeDriver()
    driver.copy_screen = lambda: next(screens)
    state = bootstrap_session(driver, launch_wait=0, step_wait=0)
    assert state == STATE_ORDER_SEARCH
    # The login macro WAS replayed (we were on the sign-on screen).
    assert ("text", "ROMAN") in driver.actions


def test_bootstrap_raises_if_login_does_not_reach_order_screen():
    screens = iter(["Sign On\nPassword", "MAIN MENU"])
    driver = FakeDriver()
    driver.copy_screen = lambda: next(screens)
    with pytest.raises(AS400ManualLoginRequired):
        bootstrap_session(driver, launch_wait=0, step_wait=0)


def test_bootstrap_navigates_full_login_flow():
    # sign-on → Message Display → SALESN menu → order search, each step verified.
    screens = iter([SIGN_ON_SCREEN, MESSAGE_SCREEN, MENU_SCREEN, READY])
    driver = FakeDriver()
    driver.copy_screen = lambda: next(screens)
    state = bootstrap_session(driver, launch_wait=0, step_wait=0)
    assert state == STATE_ORDER_SEARCH
    # Login macro ran, the Message Display was dismissed, and "3" picked Order Inquiry.
    assert ("text", "ROMAN") in driver.actions
    assert ("text", "3") in driver.actions


def test_bootstrap_navigates_from_menu_only():
    # Already past login, sitting on the SALESN menu: just pick 3 → order search.
    screens = iter([MENU_SCREEN, READY])
    driver = FakeDriver()
    driver.copy_screen = lambda: next(screens)
    state = bootstrap_session(driver, launch_wait=0, step_wait=0)
    assert state == STATE_ORDER_SEARCH
    assert ("text", "ROMAN") not in driver.actions  # no re-login
    assert driver.actions == [("text", "3"), ("key", "enter")]


# --- VOID order routes to the 'ADDITIONAL MESSAGE INFORMATION' screen (F6 recover) ---


def test_message_info_screen_detector():
    assert _is_message_info_screen(ADDL_MSG_SCREEN)
    assert not _is_message_info_screen("O R D E R  I N Q U I R Y\nITEM 1")
    assert not _is_message_info_screen(MESSAGE_SCREEN)  # the Press-Enter one is different


def test_void_message_screen_as_header_presses_f6_and_skips():
    driver = FakeDriver([READY, ADDL_MSG_SCREEN])
    with pytest.raises(OrderVoidSkip):
        capture_order("880150", driver, page_wait=0)
    # Initial F6 (new search) + the recovery F6 on the message screen. No F5/ENTER.
    assert driver.keys == ["f6", "f6"]


def test_void_message_screen_after_f5_presses_f6_and_skips():
    driver = FakeDriver([READY, "O R D E R  I N Q U I R Y\nHEADER", ADDL_MSG_SCREEN])
    with pytest.raises(OrderVoidSkip):
        capture_order("880151", driver, page_wait=0)
    # F6 (search) → F5 (items) → F6 (recover from the dead-end message screen).
    assert driver.keys == ["f6", "f5", "f6"]
