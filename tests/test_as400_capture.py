"""
Tests for the AS400 capture LOOP logic (pure, no GUI / no macOS).

A FakeDriver feeds canned screens so we can assert the F5/ENTER/END OF ORDER
flow and the multi-page accumulation without driving Mocha.
"""

import shutil
import subprocess

import pytest

from as400_capture import (
    KEY_CODES,
    OSASCRIPT_TIMEOUT_DEFAULT,
    PAGE_WAIT_DEFAULT,
    STATE_CUSTOMER_DISPLAY,
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
    MochaDriver,
    OrderNotFound,
    OrderVoidSkip,
    _env_float,
    _has_end_marker,
    _is_invalid_order,
    _is_message_info_screen,
    _is_order_header_screen,
    _is_void_order,
    bootstrap_session,
    build_copy_screen_script,
    build_focus_script,
    capture_order,
    classify_screen,
    run_login,
)

# A VOID order's header (operator report 2026-06-11). Pressing F5 on it is what
# routes to the dead-end message screen — so we must skip on the HEADER, pre-F5.
VOID_HEADER = """                            O R D E R   I N Q U I R Y
 Order Number: 880138                       Account Number: VOID
 Bill VOID VOID VOID VOID
                                   Cmd5            Cmd6
                                    DETAILS         RETURN TO SELECT"""

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


# The FULL SALESN menu (Rafael, 2026-09-01). The old fixture only listed the first
# four options; six more exist, including Order Entry and Sign Off.
FULL_MENU_SCREEN = """ COMMAND                      SALESN Options                                 XT


  01. Customer Inquiry
  02. Stock File Inquiry
  03. Order Inquiry
  04. Accounts Receivable Inquiry

  06. Display Print Status
  07. Set Terminal Functions


  09. Order Entry
  10. Order Turn - California



      24. Sign Off


 Ready for option number or command"""

# CUSTOMER DISPLAY — menu option 01 (Rafael, 2026-09-01). This is where the dealer's
# phone and e-mail live; the order screens carry neither. Reached with 1 → ENTER →
# customer number → TAB → 00 → ENTER. Its own legend gives Cmd7 EXIT as the way out.
CUSTOMER_DISPLAY_SCREEN = """                       C U S T O M E R    D I S P L A Y
   cfvdet01
  Account Number: 0009981 00

  Name           SHREWSBURY BICYCLES INC.
  Address        765 BROAD STREET

  City           SHREWSBURY       NJ  07702
  Phone No       732 7412799
  Fax No                       Salesman ID   179 LAMBERT/PARSONS
  EMAIL Address  INFO@SHREWSBURYBICYCLES.COM
  Cr Limit - Bikes     10000.00
  Cr Limit - Parts          .00
  Terms Code         28 NET 30 DAYS

    Size of Store:
    # of Locations:
    Bike Buyer:      ACT# 2385  ROUT# 0353
    Parts Buyer:
    Other Buyer:

 ---- Lines ---
 Cmd1    Cmd2  Cmd3        Cmd4     Cmd5     Cmd10  Cmd11  Cmd12    Cmd6   Cmd7
  Product Comp  Closest Dlr POP Info CallBack Top10  Commit PreSeas  Prior  EXIT"""


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
        # The fake used to accept ANY key name, so code could press something the
        # real driver has no key code for and every test still passed. That is
        # exactly how the F6·F6·F7 recovery shipped broken: MochaDriver knew only
        # F5 and F6, and it died on "Unknown key: f7" in front of the operator.
        assert name.lower() in KEY_CODES, f"MochaDriver cannot press {name!r} — see KEY_CODES"
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


# ── Continuing from an order already on screen (operator report 2026-09-01) ──
# "Cuando busco una orden, aunque ya esté en AS400 y el watcher la vea, la vuelve a
# buscar, retrocediendo un paso en el proceso." The pre-check read IS the header;
# F6 + re-typing walks back to the search screen to reach the page already shown.


def test_is_order_header_screen_detector():
    assert _is_order_header_screen(ORDER_HEADER_SCREEN, "880009")
    # A different order on screen is not ours.
    assert not _is_order_header_screen(ORDER_HEADER_SCREEN, "880010")
    # Same order, but the ITEMS view — continuing from it would capture an order
    # that starts in the middle.
    assert not _is_order_header_screen(ORDER_ITEMS_END_SCREEN, "880009")
    # Screens with no order on them.
    assert not _is_order_header_screen(ORDER_SEARCH_SCREEN, "880009")
    assert not _is_order_header_screen(INVALID_ORDER_SCREEN, "999999")
    assert not _is_order_header_screen(READY, "880009")
    assert not _is_order_header_screen(ORDER_HEADER_SCREEN, None)
    # 5250 letter-spacing must not break the match.
    assert _is_order_header_screen("O r d e r  N u m b e r :  8 8 0 0 0 9\n Bill ACME", "880009")


def test_continues_from_the_order_already_on_screen():
    driver = FakeDriver([ORDER_HEADER_SCREEN, "ITEM 1\nEND OF ORDER"])
    text = capture_order("880009", driver, page_wait=0)

    assert driver.keys == ["f5"]  # straight to the items view — no F6
    assert driver.typed is None  # nothing re-typed
    assert "Order Number: 880009" in text
    assert "END OF ORDER" in text


def test_does_not_continue_from_a_different_order():
    driver = FakeDriver(
        [ORDER_HEADER_SCREEN, "O R D E R  I N Q U I R Y\nCUSTOMER HEADER", "ITEM\nEND OF ORDER"]
    )
    capture_order("880010", driver, page_wait=0)

    assert driver.keys == ["f6", "f5"]
    assert driver.typed == "880010"


def test_does_not_continue_from_an_items_page_of_the_same_order():
    # The screen shows order 880009 but we are mid-order: reusing it would drop
    # every line above the current page.
    driver = FakeDriver([ORDER_ITEMS_END_SCREEN, ORDER_HEADER_SCREEN, "ITEM\nEND OF ORDER"])
    capture_order("880009", driver, page_wait=0)

    assert driver.keys == ["f6", "f5"]
    assert driver.typed == "880009"


def test_continuing_can_be_turned_off():
    # AS400_REUSE_HEADER=0 on Bay 2 restores the old path without a deploy.
    driver = FakeDriver([ORDER_HEADER_SCREEN, ORDER_HEADER_SCREEN, "ITEM\nEND OF ORDER"])
    capture_order("880009", driver, page_wait=0, reuse_header=False)

    assert driver.keys == ["f6", "f5"]
    assert driver.typed == "880009"


def test_void_order_already_on_screen_still_skips_before_f5():
    # The guards run on the reused screen exactly as on a freshly typed one.
    driver = FakeDriver([VOID_HEADER])
    with pytest.raises(OrderVoidSkip):
        capture_order("880138", driver, page_wait=0, reuse_header=True)
    assert "f5" not in driver.keys


# ── Tunability and guards (plan F1) ─────────────────────────────────────────


def test_env_float_reads_a_value_and_falls_back_on_garbage(monkeypatch):
    monkeypatch.setenv("AS400_TEST_KNOB", "1.25")
    assert _env_float("AS400_TEST_KNOB", 9.0) == 1.25
    monkeypatch.setenv("AS400_TEST_KNOB", "  ")
    assert _env_float("AS400_TEST_KNOB", 9.0) == 9.0
    # Garbage must not take the capture down — it warns and uses the default.
    monkeypatch.setenv("AS400_TEST_KNOB", "muy rapido")
    assert _env_float("AS400_TEST_KNOB", 9.0) == 9.0
    monkeypatch.delenv("AS400_TEST_KNOB")
    assert _env_float("AS400_TEST_KNOB", 9.0) == 9.0


def test_the_defaults_are_the_values_that_were_hardcoded():
    # Turning the waits into knobs must not change behaviour by itself.
    assert PAGE_WAIT_DEFAULT == 0.8
    assert OSASCRIPT_TIMEOUT_DEFAULT == 10.0


def test_page_wait_comes_from_the_environment_when_not_passed(monkeypatch):
    monkeypatch.setenv("AS400_PAGE_WAIT", "0")
    slept = []
    monkeypatch.setattr("as400_capture.time.sleep", lambda s: slept.append(s))
    driver = FakeDriver([READY, "ORDER INQUIRY", "item\nEND OF ORDER"])
    capture_order("880005", driver)  # no page_wait argument
    assert slept and all(s == 0 for s in slept)


def test_a_non_numeric_order_number_never_reaches_the_terminal():
    driver = FakeDriver([READY])
    with pytest.raises(OrderNotFound):
        capture_order('88" or so', driver, page_wait=0)
    assert driver.keys == []
    assert driver.typed is None


def test_type_text_escapes_a_quote_instead_of_breaking_the_script():
    driver = MochaDriver()
    sent = []
    driver._osascript = lambda script: sent.append(script)
    driver.type_text('88"13')
    assert sent == ['tell application "System Events" to keystroke "88\\"13"']


# ── One osascript per screen read (plan F2/F3) ──────────────────────────────

_SCRIPTS = {
    "copy by name": build_copy_screen_script("Mocha TN5250", None, 0.4, 0.15, 0.2),
    "copy by bundle id": build_copy_screen_script("x", "com.mochasoft.tn5250", 0.4, 0.15, 0.2),
    "focus by name": build_focus_script("Mocha TN5250", None, 0.4),
    "focus by bundle id": build_focus_script("x", "com.mochasoft.tn5250", 0.4),
    "delays at zero": build_copy_screen_script("Mocha TN5250", None, 0, 0, 0),
}


@pytest.mark.skipif(shutil.which("osacompile") is None, reason="macOS only")
@pytest.mark.parametrize("name", sorted(_SCRIPTS))
def test_the_generated_applescript_compiles(name, tmp_path):
    """Compiles it — never runs it (running would grab the keyboard of whatever
    machine the suite is on). A syntax error here would only ever show up as a
    dead capture on the Bay 2 Mac."""
    result = subprocess.run(
        ["osacompile", "-o", str(tmp_path / "check.scpt"), "-e", _SCRIPTS[name]],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_a_bundle_id_is_resolved_at_run_time_not_compile_time():
    # 'tell application id "..."' resolves when the script COMPILES, so on a
    # machine without that bundle installed the whole read would fail — and this
    # script is the read now, not just the focus.
    script = _SCRIPTS["copy by bundle id"]
    assert "tell application id" not in script
    assert "every application process whose bundle identifier is" in script
    assert "isn't running" in script


def test_the_read_only_activates_when_the_emulator_is_not_in_front():
    script = _SCRIPTS["copy by name"]
    assert (
        'if (name of first application process whose frontmost is true) is not "Mocha TN5250"'
        in script
    )
    assert 'set frontmost of process "Mocha TN5250" to true' in script
    assert "delay 0.4" in script  # the settle pause only runs inside the if


def test_an_app_name_with_a_quote_is_refused_instead_of_building_a_broken_script():
    with pytest.raises(CaptureError):
        build_focus_script('Mocha "TN5250"', None, 0.4)


class _FakeRun:
    def __init__(self, stdout=""):
        self.stdout = stdout


def test_a_screen_read_is_one_osascript_call(monkeypatch):
    # Three processes used to do this: ~140 ms each just to start, and ~140 ms of
    # daylight between the select-all and the copy for another app to steal focus.
    driver = MochaDriver(app_name="Mocha TN5250")
    scripts = []
    driver._osascript = scripts.append
    monkeypatch.setattr("as400_capture.subprocess.run", lambda *a, **k: _FakeRun("SCREEN TEXT"))

    assert driver.copy_screen() == "SCREEN TEXT"
    assert len(scripts) == 1
    assert 'keystroke "a" using command down' in scripts[0]
    assert 'keystroke "c" using command down' in scripts[0]
    assert "frontmost" in scripts[0]


def test_the_old_three_call_shape_is_one_env_var_away(monkeypatch):
    # AS400_SINGLE_SCRIPT=0 is both the revert lever and how the BEFORE gets
    # measured on Bay 2: everything ships on main, so update.sh brings every phase
    # at once and only a switch keeps the baseline honest.
    monkeypatch.setenv("AS400_SINGLE_SCRIPT", "0")
    monkeypatch.setattr("as400_capture.time.sleep", lambda s: None)
    driver = MochaDriver(app_name="Mocha TN5250")
    scripts = []
    driver._osascript = scripts.append
    monkeypatch.setattr("as400_capture.subprocess.run", lambda *a, **k: _FakeRun("SCREEN TEXT"))

    assert driver.copy_screen() == "SCREEN TEXT"
    assert len(scripts) == 3  # activate, Cmd+A, Cmd+C
    assert "frontmost" not in scripts[0] or "is not" not in scripts[0]  # unconditional activate


def test_a_read_that_did_not_copy_still_raises(monkeypatch):
    # The sentinel is what makes skipping the activation safe: if Cmd+A/Cmd+C went
    # anywhere else, the clipboard still holds it and we fail loudly instead of
    # parsing whatever another app had.
    driver = MochaDriver(app_name="Mocha TN5250")
    driver._osascript = lambda script: None
    written = {}

    def fake_run(cmd, **kwargs):
        if cmd == ["pbcopy"]:
            written["sentinel"] = kwargs["input"]
            return _FakeRun()
        return _FakeRun(written["sentinel"])  # clipboard never changed

    monkeypatch.setattr("as400_capture.subprocess.run", fake_run)
    with pytest.raises(CaptureError):
        driver.copy_screen()


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
        assert name in KEY_CODES, f"MochaDriver cannot press {name!r} — see KEY_CODES"
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


def test_every_key_the_flow_presses_has_a_real_key_code():
    # F7 was missing (2026-09-02): the recovery written against the operator's
    # F6·F6·F7 recipe could never run. Presses are asserted through the fakes now,
    # but this states the contract directly.
    for name in ("enter", "tab", "f5", "f6", "f7"):
        assert name in KEY_CODES
    with pytest.raises(ValueError):
        MochaDriver(app_name="Mocha TN5250").key("f99")


def test_bootstrap_unknown_screen_tries_the_operator_way_out_once():
    # An unrecognized screen is no longer an immediate give-up: F6·F6·F7 is the
    # operator's own way back to the SALESN menu from anywhere (Rafael 2026-09-01).
    # This screen never changes, so after ONE attempt it still asks for a human —
    # and it must not keep hammering keys at a terminal nobody is watching.
    driver = StatefulDriver("MAIN MENU\n1. Inventory\n2. Customers")
    with pytest.raises(AS400ManualLoginRequired):
        bootstrap_session(driver, launch_wait=0, step_wait=0)
    assert driver.keys == ["f6", "f6", "f7"]
    assert driver.typed is None


def test_bootstrap_unsticks_an_unknown_screen_back_to_the_order_view():
    # Unknown screen → F6·F6·F7 lands on the menu → 3 → order search.
    screens = iter(["SOME OTHER PROGRAM", MENU_SCREEN, READY])
    driver = FakeDriver()
    driver.copy_screen = lambda: next(screens)
    assert bootstrap_session(driver, launch_wait=0, step_wait=0) == STATE_ORDER_SEARCH
    assert driver.keys[:3] == ["f6", "f6", "f7"]


def test_bootstrap_never_hammers_the_dead_end_message_screen():
    # 'ADDITIONAL MESSAGE INFORMATION': no key works, only a re-login escapes it.
    # Trying F6·F6·F7 here would be pressing keys into a screen that cannot answer.
    driver = StatefulDriver(ADDL_MSG_SCREEN)
    with pytest.raises(AS400ManualLoginRequired):
        bootstrap_session(driver, launch_wait=0, step_wait=0)
    assert driver.keys == []


def test_customer_display_is_classified_and_exited_with_f7():
    assert classify_screen(CUSTOMER_DISPLAY_SCREEN) == STATE_CUSTOMER_DISPLAY
    screens = iter([CUSTOMER_DISPLAY_SCREEN, MENU_SCREEN, READY])
    driver = FakeDriver()
    driver.copy_screen = lambda: next(screens)
    assert bootstrap_session(driver, launch_wait=0, step_wait=0) == STATE_ORDER_SEARCH
    assert driver.keys[0] == "f7"  # EXIT, per the screen's own legend


def test_full_menu_still_classifies_as_the_menu():
    # Ten options instead of four — the markers must not depend on the short list.
    assert classify_screen(FULL_MENU_SCREEN) == STATE_MENU


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
    # Third screen: after the login macro the view is still unknown, the one
    # unstick attempt is spent on it, and it stays unknown → a human is needed.
    screens = iter(["Sign On\nPassword", "MAIN MENU", "MAIN MENU"])
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


# --- VOID prevention: skip on the header BEFORE F5 (never enter the dead screen) ---


def test_void_order_detector():
    assert _is_void_order(VOID_HEADER)
    assert _is_void_order("Account Number:   VOID")
    assert not _is_void_order(ORDER_HEADER_SCREEN)  # a normal order
    assert not _is_void_order("Bill AVOID COLLISIONS LLC")  # 'VOID' inside a word, once


def test_void_header_skips_before_pressing_f5():
    driver = FakeDriver([READY, VOID_HEADER])
    with pytest.raises(OrderVoidSkip):
        capture_order("880138", driver, page_wait=0)
    # Initial F6 (search) + recovery F6. Crucially NO F5 — we never trigger the
    # dead-end message screen.
    assert driver.keys == ["f6", "f6"]
    assert "f5" not in driver.keys
