"""
as400_capture.py — Drive Mocha TN5250 to capture an order's screens into text.

Follows the confirmed AS400 flow (see docs/AS400_AUTOMATION.md §3.3):
    1. type order number  → page 1 = customer header (address + info)
    2. F5                  → switches to the ITEMS view
    3. loop: copy screen; if it contains "END OF ORDER" stop, else ENTER for next page

The keystroke/clipboard layer (MochaDriver) only works on macOS. The capture LOOP
(capture_order) is pure and testable with any driver implementing the same interface.
"""

import logging
import os
import re
import subprocess
import time

log = logging.getLogger("pickd-as400")

NO_COPY_MESSAGE = (
    "Cmd+A/Cmd+C didn't copy the AS400 screen (the clipboard didn't change). "
    "Make sure Mocha stays in front and copies with Cmd+A+Cmd+C; "
    "if Mocha copies a different way (Edit menu), let me know which."
)

END_OF_ORDER_MARKER = "END OF ORDER"
MOCHA_APP_NAME = os.getenv("MOCHA_APP_NAME", "Mocha TN5250")

# App/document to open to start the emulator. Either an app name (open -a)
# or a full path to a .app / saved session file (open <path>).
AS400_LAUNCH_TARGET = os.getenv("AS400_LAUNCH_TARGET", MOCHA_APP_NAME)

# Seconds to wait after launching before the emulator is ready to receive keys.
LAUNCH_WAIT = float(os.getenv("AS400_LAUNCH_WAIT", "5"))

# ── Tunables (plan F1) ───────────────────────────────────────────────────────
# Every wait the capture makes lives here, and every one is read at CALL time,
# not at import: a .env loaded later still wins, and the Bay 2 Mac can be retuned
# with an edit + restart instead of a deploy — which is a trip to Bay 2. The
# defaults ARE the values that were hardcoded until now, so turning them into
# knobs changed no behaviour by itself.
PAGE_WAIT_DEFAULT = 0.8  # after F6, after typing the number, after each ENTER
POLL_INTERVAL_DEFAULT = 0.3  # between re-reads while the screen hasn't refreshed
REFRESH_TIMEOUT_DEFAULT = 5.0  # how long to wait for the screen to advance
# A hung osascript runs on the capture thread WITH capture_lock held, so it
# freezes the scanner and every manual capture queued behind it.
OSASCRIPT_TIMEOUT_DEFAULT = 10.0
# Inside the screen-read script (plan F2/F3): settle after bringing Mocha up, and
# the two pauses around Cmd+A / Cmd+C. They used to be Python sleeps between three
# separate osascript processes; now they run inside the one script.
# Waiting for the screen instead of for the clock (plan F4). OFF by default: it
# is the one phase that changes WHEN we trust a screen, and everything ships on
# main, so it must not ride along on an unrelated update.
SETTLED_READS_DEFAULT = 2  # identical reads in a row before a new page is trusted
SETTLE_POLL_DEFAULT = 0.0  # extra pause between those reads; the read IS the interval
REFRESH_DEADLINE_DEFAULT = 25.0  # wall clock — see the comment in _await_changed_page
FOCUS_SETTLE_DEFAULT = 0.4
SELECT_DELAY_DEFAULT = 0.15
# Nothing sleeps after Cmd+C any more: the clipboard is polled until it stops
# being the sentinel, which is an answer instead of a guess (see _read_clipboard).
COPY_DELAY_DEFAULT = 0.0
CLIP_TIMEOUT_DEFAULT = 1.0  # five times more patient than the 0.2s sleep it replaces
CLIP_POLL_DEFAULT = 0.02
# Doing the whole read inside one osascript — sentinel, keys, clipboard wait and
# the text itself — instead of spawning pbcopy/pbpaste around it (plan F7).
READ_IN_SCRIPT_DEFAULT = False


def _env_float(name: str, default: float) -> float:
    """Read a float from the environment, falling back loudly on garbage."""
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        log.warning("%s=%r is not a number — using %s", name, raw, default)
        return default


# Login macro: replicates "ROMAN + TAB + STOU + ENTER + ENTER + 3 + ENTER" to reach the
# order-search screen. Each step is (kind, value): kind in {"text", "key", "wait"}.
# Edit here if the real sequence/timing differs.
DEFAULT_LOGIN_STEPS = [
    ("text", "ROMAN"),
    ("key", "tab"),
    ("text", "STOU"),
    ("key", "enter"),
    ("key", "enter"),
    ("text", "3"),
    ("key", "enter"),
]


# --- AS400 screen states -------------------------------------------------
# We read and classify the emulator screen BEFORE driving it, so we never type
# into a dead/unknown view (e.g. the host being down) and never falsely report
# "connected". Markers are whitespace-insensitive (5250 captures space out text).
STATE_DISCONNECTED = "disconnected"  # Mocha can't reach the host / session ended
STATE_LOGIN = "login"  # AS400 sign-on screen
STATE_MENU = "menu"  # SALESN options menu (pick 3 = Order Inquiry)
STATE_MESSAGE = "message"  # transient "Message Display / Press Enter to continue"
STATE_CUSTOMER_DISPLAY = "customer_display"  # CUSTOMER DISPLAY (menu option 01)
STATE_STOCK_INQUIRY = "stock_inquiry"  # STOCK INQUIRY (menu option 02) — detail OR its NOTES
STATE_ORDER_SEARCH = "order_search"  # logged in, ready to type an order number
STATE_ORDER_INQUIRY = "order_inquiry"  # viewing an order
STATE_UNKNOWN = "unknown"  # unrecognized → ask the user to log in manually

# Whitespace-stripped, upper-cased markers. The disconnected set is the strongest
# signal (the user reported "Cannot connect to host ... , port 23"). The others are
# taken from real `Peek screen` captures of the login flow.
DISCONNECTED_MARKERS = (
    "CANNOTCONNECT",
    "CONNECTIONREFUSED",
    "CONNECTIONTIMEDOUT",
    "CONNECTIONCLOSED",
    "PORT23",
    "DISCONNECTED",
    "NOTCONNECTED",
    "SESSIONENDED",
    "HOSTUNREACHABLE",
)
LOGIN_MARKERS = ("SIGNON", "PASSWORD")
# The SALESN options menu lists "03. Order Inquiry" — so it MUST be matched before
# the order-screen substring checks, or the menu would look like an order view.
MENU_MARKERS = ("SALESNOPTIONS", "READYFOROPTION")
# macOS virtual key codes. The AS400 legends go up to Cmd12, so all twelve
# function keys are here even though the flow only presses a few: the map had
# just F5 and F6, so the F6·F6·F7 recovery written for it died on "Unknown key:
# f7" the first time the operator tried it (2026-09-02). The fake drivers in the
# tests validate against THIS table, so a key the real driver can't press now
# fails in the suite instead of on the floor.
KEY_CODES = {
    "enter": 36,
    "return": 36,
    "tab": 48,
    "f1": 122,
    "f2": 120,
    "f3": 99,
    "f4": 118,
    "f5": 96,
    "f6": 97,
    "f7": 98,
    "f8": 100,
    "f9": 101,
    "f10": 109,
    "f11": 103,
    "f12": 111,
}

MESSAGE_MARKERS = ("PRESSENTERTOCONTINUE",)
# Option 01 of the SALESN menu. Reachable by hand (it holds the dealer's phone and
# e-mail), so the daemon can find the terminal parked here. Its own legend says
# Cmd7 EXIT, which is the way back to the menu.
CUSTOMER_MARKERS = ("CUSTOMERDISPLAY",)
# Option 02 of the SALESN menu: the catalogue name, the AS400 weight and its own
# stock by warehouse (docs/as400-screen-map.md §2.12).
#
# ⚠️ This marker matches TWO screens. Cmd10 NOTES (§2.12b) repaints the same title
# with NONE of the fields — no Description:, no Weight:, no On Hand. Classifying is
# enough to recognize and leave; it is NOT enough to read from. Anything that reads
# a field must call `is_stock_detail_screen` first (see it for why).
STOCK_MARKERS = ("STOCKINQUIRY",)

# States from which a capture can start (logged in, on an order view).
_READY_STATES = (STATE_ORDER_SEARCH, STATE_ORDER_INQUIRY)


def classify_screen(text: str) -> str:
    """Best-effort classification of the current AS400 screen.

    Whitespace-insensitive. Order of checks matters: disconnected first (strongest
    signal); then login / menu / message (the SALESN menu literally contains the
    text "Order Inquiry", so it must win over the order-screen checks below);
    finally the actual order views. Anything else is UNKNOWN, which callers treat
    as "needs manual login".
    """
    norm = re.sub(r"\s+", "", (text or "").upper())
    if not norm:
        return STATE_UNKNOWN
    if any(m in norm for m in DISCONNECTED_MARKERS):
        return STATE_DISCONNECTED
    if any(m in norm for m in LOGIN_MARKERS):
        return STATE_LOGIN
    if any(m in norm for m in MENU_MARKERS):
        return STATE_MENU
    if any(m in norm for m in MESSAGE_MARKERS):
        return STATE_MESSAGE
    if any(m in norm for m in CUSTOMER_MARKERS):
        return STATE_CUSTOMER_DISPLAY
    if any(m in norm for m in STOCK_MARKERS):
        return STATE_STOCK_INQUIRY
    if "ORDERINQUIRY" in norm:
        return STATE_ORDER_INQUIRY
    if "ORDERNUMBER" in norm:
        return STATE_ORDER_SEARCH
    return STATE_UNKNOWN


def is_stock_detail_screen(text: str) -> bool:
    """True only for the STOCK INQUIRY screen that actually carries the fields.

    R10. The title is NOT a discriminator: Cmd10 NOTES (docs/as400-screen-map.md
    §2.12b) repaints the same "S T O C K   I N Q U I R Y" heading and the same
    Stock Number, with none of the data — no labelled Description, no Weight, no
    On Hand. Something that landed there believing it was on the detail screen
    would read `Weight` as ABSENT rather than as "I am not where I think I am",
    and write a bike's weight from a screen that never had one.

    So the test is two fields that exist on the detail and on nothing else. The
    labelled `Description:` matters specifically: NOTES prints the description
    too, but bare on the Stock Number line, with no label.
    """
    norm = _norm_screen(text)
    return "DESCRIPTION:" in norm and "WEIGHT:" in norm


def sku_screen_fields(sku: str):
    """Split a canonical SKU into what the two entry fields want.

    `03-3933BK` → ("033933", "BK"). Rafael, 2026-09-02: type the numeric part
    WITHOUT the dash, TAB, then the 2- (or 3-) character colour code, TAB, X.
    A SKU with no colour suffix takes a blank TAB — the 126 bikes shaped like
    `01-0169` are in the queue, not excluded.

    Returns None for anything that isn't an AS400 stock number: the FedEx/USPS
    tracking numbers and the serials, which AS400 does not know (R6). The SHAPE
    is the filter, so no list of exceptions can go stale.
    """
    m = re.fullmatch(r"(\d{2})-(\d{4})([A-Z]{0,3})", (sku or "").strip().upper())
    if not m:
        return None
    return m.group(1) + m.group(2), m.group(3)


def _norm_screen(text: str) -> str:
    """Whitespace-stripped, upper-cased screen text, for change detection."""
    return re.sub(r"\s+", "", (text or "").upper())


def _has_end_marker(text: str) -> bool:
    """Whitespace-insensitive match for the END OF ORDER marker.

    The 5250 screen capture can include extra spacing between characters, so we
    strip all whitespace before checking (e.g. 'E N D  O F  O R D E R' still matches).
    """
    return "ENDOFORDER" in re.sub(r"\s+", "", text.upper())


def _is_invalid_order(text: str) -> bool:
    """True if AS400 rejected the order number ('Invalid Order Number, REENTER').

    This is the screen for a number that isn't a real order yet — the search screen
    still shows 'Order Number', so classify_screen would call it a valid order view.
    We detect the error message explicitly so the auto-scanner stops on it (and
    retries the same number next cycle) instead of trying to page through nothing.
    """
    norm = re.sub(r"\s+", "", text.upper())
    return "INVALIDORDERNUMBER" in norm or "INVALIDORDER" in norm


def _is_void_order(text: str) -> bool:
    """True if the ORDER header is a VOID order ('Account Number: VOID', the Ship/
    Bill name is 'VOID VOID VOID …').

    Pressing F5 (DETAILS) on a VOID order is what routes the terminal to the
    dead-end 'ADDITIONAL MESSAGE INFORMATION' screen, where NO key works and the
    operator has to close the session and log back in. So we detect VOID on the
    HEADER and skip BEFORE pressing F5 — never entering that screen at all.

    Normalized to alphanumerics only so the ':' after 'Account Number' and the
    5250 letter-spacing don't break the match.
    """
    norm = re.sub(r"[^A-Z0-9]", "", text.upper())
    return "ACCOUNTNUMBERVOID" in norm or "BILLVOIDVOID" in norm or "SHIPVOIDVOID" in norm


def _is_message_info_screen(text: str) -> bool:
    """True on the AS400 'ADDITIONAL MESSAGE INFORMATION' message-detail screen.

    A VOID order can route here after F5 (it shows a BAS-#### error like 'No
    matching key' and an 'Option:' prompt). It's a dead end for capture — paging
    with ENTER never reaches END OF ORDER — so we detect it explicitly to bail out
    and skip the number instead of looping on it.
    """
    norm = re.sub(r"\s+", "", text.upper())
    return "ADDITIONALMESSAGEINFORMATION" in norm


# Column heading of the ITEMS view ('Quant Quant Stock # W/H Description ...'),
# normalized to alphanumerics. The items page repeats the very same 'Order Number:'
# line as the header, so this is what tells the two views apart.
_ITEMS_VIEW_MARKERS = ("QUANTQUANT", "STOCKWH")


def _alnum(text: str) -> str:
    """Alphanumerics only, upper-cased — 5250 spaces letters out ('O r d e r')."""
    return re.sub(r"[^A-Za-z0-9]", "", text or "").upper()


def _screen_order_number(text: str):
    """The order number this screen is showing, or None (empty search screen,
    'Invalid Order Number' — both clear the field, so there are no digits)."""
    m = re.search(r"ORDERNUMBER(\d+)", _alnum(text))
    return m.group(1) if m else None


def _is_order_header_screen(text: str, order_number) -> bool:
    """True when the screen is ALREADY the header (page 1) of exactly this order.

    Both halves matter:
      - it names THIS order number (a stale screen showing the previous order is
        not ours), and
      - it is the HEADER, not an items page. The items view carries the same
        'Order Number:' line, and continuing from one would capture an order that
        starts in the middle — so its column heading disqualifies the screen.
    """
    if not order_number:
        return False
    if _screen_order_number(text) != str(order_number).strip():
        return False
    norm = _alnum(text)
    return not any(m in norm for m in _ITEMS_VIEW_MARKERS)


def _single_script_enabled() -> bool:
    """Whether a screen read is one osascript (plan F2/F3) or the old three.

    The revert lever for the biggest change to how the terminal is driven, and the
    way to measure the BEFORE on the Bay 2 Mac: with AS400_SINGLE_SCRIPT=0 the F0
    log records the old shape, so "half the time" is a comparison and not a claim.
    """
    return os.getenv("AS400_SINGLE_SCRIPT", "1").strip().lower() in ("1", "true", "yes", "on")


def _read_in_script_enabled() -> bool:
    """Whether a screen read is one osascript end to end (plan F7).

    Off by default: it takes the screen text out through AppleScript's own
    clipboard access instead of pbpaste, and text is exactly the thing worth
    checking against a real terminal before trusting it everywhere.
    """
    return os.getenv("AS400_READ_IN_SCRIPT", "0").strip().lower() in ("1", "true", "yes", "on")


def _wait_for_settled_enabled() -> bool:
    """Whether paging waits for the screen (plan F4) or sleeps a fixed time."""
    return os.getenv("AS400_WAIT_FOR_SETTLED", "0").strip().lower() in ("1", "true", "yes", "on")


def _reuse_header_default() -> bool:
    """Whether capture_order may continue from an order already on screen.

    Read at call time (not import) so a .env loaded later still wins, and so the
    Bay 2 machine can switch it off without a deploy: AS400_REUSE_HEADER=0.
    """
    return os.getenv("AS400_REUSE_HEADER", "1").strip().lower() in ("1", "true", "yes", "on")


def _await_changed_page(read_fn, prev_norm, *, settled_reads, poll, deadline, on_stale=None):
    """Read until the screen is BOTH new and finished painting. Returns (text, ok).

    Two conditions, and both are the same bug seen from opposite sides:

      (a) different from `prev_norm` — the invariant won on 2026-06-05. Acting on a
          frame we already acted on makes the loop press ENTER twice and skip the
          genuinely new page, losing its items with no error anywhere.
      (b) the same text `settled_reads` times in a row — what a fixed sleep never
          actually promised. `sleep(0.8)` says 800 ms passed, not that the 5250
          finished painting; accepting a half-drawn page loses the lines that had
          not arrived yet. Two identical reads say it is done.

    The deadline is WALL CLOCK on purpose. The old loop added up poll intervals
    while each re-read cost far more than one, so `refresh_timeout=5.0` really
    tolerated ~25s of a slow AS400 — and it would have silently shrunk to ~10s the
    moment reads got cheaper. A number that changes meaning when unrelated code
    gets faster is not a timeout. The default is that same effective ~25s.
    """
    started = time.monotonic()
    page = read_fn()
    norm = _norm_screen(page)
    streak = 1
    while True:
        if norm != prev_norm and streak >= settled_reads:
            return page, True
        if time.monotonic() - started >= deadline:
            return page, norm != prev_norm
        if norm == prev_norm and on_stale is not None:
            on_stale()
        if poll:
            time.sleep(poll)
        page = read_fn()
        fresh = _norm_screen(page)
        streak = streak + 1 if fresh == norm else 1
        norm = fresh


def _looks_like_order_screen(text: str) -> bool:
    """True if the captured text looks like an order view (search or inquiry).

    Used to bail out (instead of looping) when we're on a different AS400 view
    (e.g. a menu) or the order doesn't exist.
    """
    return classify_screen(text) in _READY_STATES


class CaptureError(Exception):
    """Raised when a capture cannot complete (e.g. END OF ORDER never appears)."""


class OrderNotFound(CaptureError):
    """The order number isn't a real order (yet): AS400 rejects it ('Invalid Order
    Number, REENTER') or we're not on an order view. Distinct from a partial/stalled
    capture so the auto-scanner can wait longer before retrying the same number."""


class AS400Disconnected(CaptureError):
    """The emulator isn't connected to the host (server down / session ended)."""


class AS400ManualLoginRequired(CaptureError):
    """The screen is unrecognized or not logged in — a human must log in first."""


class StockSkuNotFound(CaptureError):
    """AS400 has no stock record for that number — mark it and never ask again (R7)."""


class StockScreenMismatch(CaptureError):
    """We are not on the stock DETAIL screen for the SKU we asked for.

    Either the NOTES screen (same title, no fields — R10), some other view, or
    the right kind of screen showing somebody else's record. Nothing is read
    from it and nothing is written.
    """


class OrderVoidSkip(CaptureError):
    """Capture dead-ended on the AS400 'ADDITIONAL MESSAGE INFORMATION' screen
    (e.g. a VOID order routes here after F5, prompting for an Option with no valid
    key). There is nothing to capture and paging can't recover. We press F6 to
    return to order search and the auto-scanner SKIPS this number (advances the
    cursor) instead of retrying it forever. Distinct from OrderNotFound, which does
    NOT advance (the number may become a real order later)."""


def frontmost_app_name():
    """Name of the macOS app that currently has focus, or None (off-macOS/error).

    Snapshotted before a manual capture so the operator's browser can get the
    focus back once Mocha is done.
    """
    try:
        out = subprocess.run(
            [
                "osascript",
                "-e",
                'tell application "System Events" to get name of '
                "first application process whose frontmost is true",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
        return out.stdout.strip() or None
    except Exception as e:  # noqa: BLE001
        # Visible on purpose: a silent None here disables the focus restore and
        # usually means macOS denied the Automation permission for System Events.
        log.warning("focus: could not read the frontmost app (Automation permission?): %s", e)
        return None


def activate_app(name: str) -> None:
    """Bring a macOS app to the front by process name. Caller handles errors.

    Tries System Events first (works on process names); falls back to a direct
    `tell application … activate` (more reliable for some apps/Spaces). Names
    containing double quotes are ignored (AppleScript injection guard).
    """
    if not name or '"' in name:
        return
    try:
        subprocess.run(
            [
                "osascript",
                "-e",
                f'tell application "System Events" to set frontmost of process "{name}" to true',
            ],
            check=True,
            capture_output=True,
            timeout=5,
        )
        return
    except Exception as e:  # noqa: BLE001
        log.info("focus: System Events activate failed for %s (%s) — trying app activate", name, e)
    subprocess.run(
        ["osascript", "-e", f'tell application "{name}" to activate'],
        check=True,
        capture_output=True,
        timeout=5,
    )


def _applescript_literal(value: str) -> str:
    """An app name safe to interpolate into an AppleScript string literal.

    Names come from the environment (MOCHA_APP_NAME / AS400_LAUNCH_TARGET), so a
    quote there is a misconfiguration, not an attack — but it would build a broken
    script that fails with an AppleScript syntax error nobody can read.
    """
    if not value or '"' in value or "\\" in value:
        raise CaptureError(
            f"{value!r} can't be used as an app name in AppleScript (it contains a quote or a "
            "backslash). Check MOCHA_APP_NAME / AS400_LAUNCH_TARGET."
        )
    return value


def _activation_lines(app_name, bundle_id, settle: float) -> str:
    """AppleScript that brings the emulator up ONLY if it isn't already in front.

    Re-activating on every screen read cost an Apple event plus a settle sleep even
    though Mocha had been in front for the whole capture (plan F3). Asking first is
    one property read inside the script we are already running.
    """
    if bundle_id:
        bid = _applescript_literal(bundle_id)
        # Deliberately NOT `tell application id "..." to activate`: AppleScript
        # resolves that at COMPILE time, so on a machine where the bundle isn't
        # installed the whole script fails to compile — and this script is now the
        # screen read itself, not just the focus. Asking System Events for the
        # process resolves at run time and fails with a sentence instead.
        return (
            f'  set targets to (every application process whose bundle identifier is "{bid}")\n'
            f'  if targets is {{}} then error "The emulator ({bid}) isn\'t running."\n'
            "  set target to item 1 of targets\n"
            "  if not (frontmost of target) then\n"
            "    set frontmost of target to true\n"
            f"    delay {settle}\n"
            "  end if"
        )
    name = _applescript_literal(app_name)
    return (
        f'  if (name of first application process whose frontmost is true) is not "{name}" then\n'
        f'    set frontmost of process "{name}" to true\n'
        f"    delay {settle}\n"
        "  end if"
    )


def build_focus_script(app_name, bundle_id, settle: float) -> str:
    """Bring the emulator to the front if it isn't already. One osascript."""
    return (
        'tell application "System Events"\n'
        + _activation_lines(app_name, bundle_id, settle)
        + "\nend tell"
    )


def _step_lines(steps) -> str:
    """The F6 / order number / F5 / ENTER that precede a read, as AppleScript.

    Each of these used to be its own osascript — and on the Bay 2 Mac an Apple
    Event to System Events costs ~300 ms while starting a process costs 6. Six
    calls per capture was 1.8s of pure round trip for keys we already knew we were
    going to press in that exact order.
    """
    out = []
    for kind, value in steps:
        if kind == "key":
            code = KEY_CODES.get(str(value).lower())
            if code is None:
                raise ValueError(f"Unknown key: {value}")
            out.append(f"  key code {code}")
        elif kind == "text":
            digits = str(value)
            if not digits.isdigit():
                raise ValueError(f"{value!r} isn't an AS400 order number (they are digits).")
            out.append(f'  keystroke "{digits}"')
        elif kind == "wait":
            out.append(f"  delay {float(value)}")
        else:
            raise ValueError(f"Unknown step kind: {kind}")
    return ("\n".join(out) + "\n") if out else ""


def build_read_screen_script(
    app_name,
    bundle_id,
    settle: float,
    select_delay: float,
    sentinel: str,
    timeout: float,
    steps=(),
) -> str:
    """The ENTIRE screen read as one osascript, text included (plan F7).

    On the Bay 2 MacBook Air a process costs ~0.52s to start, so polling the
    clipboard by spawning pbpaste was costing ~0.4s of the 1.09s a read took —
    more than the copy itself. AppleScript can watch its own clipboard and hand
    the text back on stdout, so the read becomes one process instead of three.

    The sentinel logic is identical, just moved inside: the clipboard is stamped,
    the keys are pressed, and the script waits for the stamp to be replaced. If it
    never is, it returns the sentinel and the caller raises exactly as before.
    """
    _applescript_literal(sentinel)
    ticks = max(1, int(timeout / 0.05))
    return (
        f'set the clipboard to "{sentinel}"\n'
        'tell application "System Events"\n'
        + _activation_lines(app_name, bundle_id, settle)
        + "\n"
        + _step_lines(steps)
        + '  keystroke "a" using command down\n'
        f"  delay {select_delay}\n"
        '  keystroke "c" using command down\n'
        "end tell\n"
        f'set screenText to "{sentinel}"\n'
        f"repeat {ticks} times\n"
        "  try\n"
        "    set screenText to (the clipboard as text)\n"
        "  end try\n"
        f'  if screenText is not "{sentinel}" then exit repeat\n'
        "  delay 0.05\n"
        "end repeat\n"
        "return screenText"
    )


def build_copy_screen_script(
    app_name, bundle_id, settle: float, select_delay: float, copy_delay: float
) -> str:
    """Focus-if-needed + Cmd+A + Cmd+C as ONE script (plan F2).

    Three separate osascript processes used to do this. Each one costs ~140 ms just
    to start, and between the select-all and the copy there were ~140 ms of daylight
    in which another app could take the focus and receive the Cmd+C. One script
    closes that window and removes the biggest constant cost of a screen read.
    """
    return (
        'tell application "System Events"\n'
        + _activation_lines(app_name, bundle_id, settle)
        + '\n  keystroke "a" using command down\n'
        f"  delay {select_delay}\n"
        '  keystroke "c" using command down\n'
        f"  delay {copy_delay}\n"
        "end tell"
    )


# ── whose hands were those? ──────────────────────────────────────────────────
#
# The driver types with `System Events keystroke`, which posts REAL HID events —
# so every key the watchdog sends resets macOS's `HIDIdleTime`, the same clock
# the scanner reads to decide whether a person is at the keyboard. The watchdog
# was impersonating the operator, and then standing down for him.
#
# It showed up as the catalogue queue never chaining two lookups: one lookup
# types, idle drops to zero, the next check says "the operator is back". On
# 11 sep 2026 the manual run read exactly the seven SKUs that fitted inside its
# two-minute grace window and then stopped — at a quarter past three in the
# afternoon, with nobody near the Mac, and again at eleven at night.
#
# `HIDIdleTime` is time since the LAST event, so the test is exact: if it is
# meaningfully SMALLER than the time since our own last keystroke, a newer event
# happened and it was not ours.
_last_self_input = 0.0


def note_self_input() -> None:
    """Stamp that WE just posted an input event."""
    global _last_self_input
    _last_self_input = time.monotonic()


def seconds_since_self_input() -> float:
    return time.monotonic() - _last_self_input


class MochaDriver:
    # Can take the keystrokes that precede a read inside the same script.
    supports_steps = True

    """
    macOS keystroke/clipboard driver for Mocha TN5250 using AppleScript (osascript)
    and pbpaste. Only functional on macOS with the emulator open and logged in.
    """

    def __init__(self, app_name: str = None, launch_target: str = None):
        # Resolve at instantiation (request time) so a .env loaded after import
        # is still honored.
        self.app_name = app_name or os.getenv("MOCHA_APP_NAME", "Mocha TN5250")
        self.launch_target = launch_target or os.getenv("AS400_LAUNCH_TARGET") or self.app_name
        # If the launch target is a bundle id, use it to activate the app reliably
        # (sandboxed apps can't be resolved by name via `tell application "<name>"`).
        bundle_re = r"^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)+$"
        self.bundle_id = self.launch_target if re.match(bundle_re, self.launch_target) else None

    def _osascript(self, script: str, capture: bool = False):
        """Run one AppleScript — always with a timeout.

        Without it, a hung osascript (Mocha showing a dialog, macOS asking for an
        Automation permission) blocked the capture thread forever while it held
        capture_lock: the scanner went silent and manual captures hung behind it,
        with nothing in the log to say why.
        """
        timeout = _env_float("AS400_OSASCRIPT_TIMEOUT", OSASCRIPT_TIMEOUT_DEFAULT)
        try:
            result = subprocess.run(
                ["osascript", "-e", script],
                check=True,
                timeout=timeout,
                capture_output=capture,
                text=capture,
            )
            return result.stdout if capture else None
        except subprocess.TimeoutExpired as e:
            raise CaptureError(
                f"AppleScript didn't answer in {timeout:.0f}s. Mocha may be showing a dialog, "
                "or macOS is asking for an Automation permission — check the screen on the "
                "Bay 2 Mac."
            ) from e

    def launch(self):
        """Launch (or focus, if already running) the emulator via `open`.

        Robust alternative to Spotlight. The launch target may be:
          - a path (contains "/")        → open <path>
          - a bundle id (reverse-DNS)    → open -b <id>   (App Store / sandboxed apps)
          - an application name          → open -a <name>
        """
        target = self.launch_target
        if "/" in target:
            subprocess.run(["open", target], check=True)
        elif self.bundle_id:
            subprocess.run(["open", "-b", target], check=True)
        else:
            subprocess.run(["open", "-a", target], check=True)

    def close_window(self) -> None:
        """Cmd+W, then ENTER. Both, always, in ONE script.

        Rafael, 2026-09-08: "el command w solo cierra con el enter después" —
        Cmd+W raises a confirmation and the Enter is what answers it. That makes
        this the one place in the driver that deliberately opens a dialog, and a
        dialog blocks EVERY Apple event after it: leaving one up would trade a
        stuck terminal for an emulator nobody can drive at all.

        So the two keys travel inside a single osascript with the delay between
        them. Nothing of ours can run in between, and there is no path through
        this method that sends the Cmd+W without the Enter.
        """
        confirm = _env_float("AS400_CLOSE_CONFIRM_DELAY", 0.15)
        self.focus()
        self._osascript(
            'tell application "System Events"\n'
            '  keystroke "w" using command down\n'
            f"  delay {confirm}\n"
            "  keystroke return\n"
            "end tell"
        )

    def previous_window(self) -> None:
        """Ctrl+Shift+Tab — back one window (Rafael's own way to reach the dead
        one after opening a fresh session)."""
        self.focus()
        self._osascript(
            'tell application "System Events" to keystroke tab using {control down, shift down}'
        )

    def next_window(self) -> None:
        """Ctrl+Tab — forward one window. The way back when the window we landed
        on turns out not to be the one we meant to close."""
        self.focus()
        self._osascript('tell application "System Events" to keystroke tab using {control down}')

    def quit(self) -> bool:
        """Ask the emulator to quit. Returns True once it is really gone.

        The polite `quit` first, because a session Mocha closes itself is a
        session the host sees closed. If it is still up after the grace period
        we escalate to a TERM signal: the whole reason we are here is a screen
        that answers nothing, and a Mocha showing a modal would ignore the
        Apple event exactly the same way.

        Never called on its own — see `hard_restart`, which owns the guards.
        """
        target = (
            f'application id "{self.bundle_id}"'
            if self.bundle_id
            else f'application "{self.app_name}"'
        )
        try:
            self._osascript(f"tell {target} to quit")
        except Exception as e:  # a dialog, a hung app: fall through to the signal
            log.info("AS400 restart: the polite quit didn't take (%s)", e)

        deadline = time.time() + _env_float("AS400_QUIT_GRACE", 8.0)
        while time.time() < deadline:
            if not self._is_running():
                return True
            time.sleep(0.5)

        log.info("AS400 restart: still running after the grace period — sending TERM")
        subprocess.run(["pkill", "-x", self.app_name], check=False)
        time.sleep(1.5)
        return not self._is_running()

    def _is_running(self) -> bool:
        out = subprocess.run(
            ["pgrep", "-x", self.app_name], capture_output=True, text=True, check=False
        )
        return bool(out.stdout.strip())

    def focus(self):
        # Activate by bundle id when available (reliable for sandboxed apps);
        # otherwise bring the running process to the front by name. Only when it
        # isn't already in front — and the settle pause runs inside the script, so
        # an emulator that was already up costs one Apple event and no sleep.
        settle = _env_float("AS400_FOCUS_SETTLE", FOCUS_SETTLE_DEFAULT)
        if not _single_script_enabled():
            self._focus_the_old_way(settle)
            return
        self._osascript(build_focus_script(self.app_name, self.bundle_id, settle))

    def _focus_the_old_way(self, settle: float) -> None:
        """Pre-F3: activate unconditionally, then sleep in Python."""
        if self.bundle_id:
            self._osascript(f'tell application id "{self.bundle_id}" to activate')
        else:
            self._osascript(
                f'tell application "System Events" to set frontmost of process "{self.app_name}" to true'
            )
        time.sleep(settle)

    def type_text(self, text: str):
        # Escaped for the AppleScript string literal: a quote or a backslash in the
        # value used to break the script and surface as an unexplained 500.
        safe = str(text).replace("\\", "\\\\").replace('"', '\\"')
        note_self_input()
        self._osascript(f'tell application "System Events" to keystroke "{safe}"')

    def key(self, name: str):
        """Press a special key by name — see KEY_CODES."""
        code = KEY_CODES.get(name.lower())
        if code is None:
            raise ValueError(f"Unknown key: {name}")
        note_self_input()
        self._osascript(f'tell application "System Events" to key code {code}')

    def copy_screen(self, steps=()) -> str:
        """Read the whole AS400 screen: focus if needed, Cmd+A, Cmd+C, clipboard.

        ONE osascript does the keyboard half (plan F2/F3). Writes a sentinel to the
        clipboard first; if Cmd+A/Cmd+C didn't replace it, the copy went nowhere
        (wrong window focused, or Mocha doesn't copy via Cmd+A) — we raise instead
        of looping forever on stale clipboard content. That sentinel is exactly what
        makes the fast path safe: skipping the activation can only fail loudly.
        """
        clip_timeout = _env_float("AS400_OSASCRIPT_TIMEOUT", OSASCRIPT_TIMEOUT_DEFAULT)
        sentinel = f"__PICKD_NO_COPY__{time.time()}"

        if _read_in_script_enabled():
            # One process for the whole read: stamp, keys, wait, and the text back
            # on stdout. On a Mac where a process costs half a second, spawning
            # pbcopy and pbpaste around the script cost more than the copy did.
            out = self._osascript(
                build_read_screen_script(
                    self.app_name,
                    self.bundle_id,
                    _env_float("AS400_FOCUS_SETTLE", FOCUS_SETTLE_DEFAULT),
                    _env_float("AS400_SELECT_DELAY", SELECT_DELAY_DEFAULT),
                    sentinel,
                    _env_float("AS400_CLIP_TIMEOUT", CLIP_TIMEOUT_DEFAULT),
                    steps,
                ),
                capture=True,
            )
            out = out[:-1] if out.endswith("\n") else out  # osascript adds one
            if out == sentinel:
                raise CaptureError(NO_COPY_MESSAGE)
            return out

        # Without the one-process read there is nowhere to put the prelude: the
        # caller replays it key by key instead (see _keys_then_read).
        subprocess.run(["pbcopy"], input=sentinel, text=True, check=True, timeout=clip_timeout)

        settle = _env_float("AS400_FOCUS_SETTLE", FOCUS_SETTLE_DEFAULT)
        select_delay = _env_float("AS400_SELECT_DELAY", SELECT_DELAY_DEFAULT)
        copy_delay = _env_float("AS400_COPY_DELAY", COPY_DELAY_DEFAULT)
        if _single_script_enabled():
            self._osascript(
                build_copy_screen_script(
                    self.app_name, self.bundle_id, settle, select_delay, copy_delay
                )
            )
        else:
            # Pre-F2: three processes and three Python sleeps.
            self._focus_the_old_way(settle)
            self._osascript('tell application "System Events" to keystroke "a" using command down')
            time.sleep(select_delay)
            self._osascript('tell application "System Events" to keystroke "c" using command down')
            if copy_delay:
                time.sleep(copy_delay)

        return self._read_clipboard(sentinel, clip_timeout)

    def _read_clipboard(self, sentinel: str, run_timeout: float) -> str:
        """Wait for the clipboard to stop being the sentinel — ask, don't guess.

        The old shape slept a flat 0.2s after Cmd+C and then read once: too long
        when the copy lands in 30 ms, and a hard failure when it takes 250. Polling
        returns as soon as the copy is really there AND waits five times longer
        before giving up, so it is both faster and more forgiving than the sleep.
        """
        deadline = time.monotonic() + _env_float("AS400_CLIP_TIMEOUT", CLIP_TIMEOUT_DEFAULT)
        poll = _env_float("AS400_CLIP_POLL", CLIP_POLL_DEFAULT)
        while True:
            out = subprocess.run(
                ["pbpaste"], capture_output=True, text=True, check=True, timeout=run_timeout
            ).stdout
            if out != sentinel:
                return out
            if time.monotonic() >= deadline:
                raise CaptureError(NO_COPY_MESSAGE)
            if poll:
                time.sleep(poll)


def run_login(driver, login_steps=DEFAULT_LOGIN_STEPS, step_wait: float = 0.6):
    """Replay the login macro on a focused emulator window.

    Each step is (kind, value): "text" types a string, "key" presses a special
    key, "wait" sleeps for `value` seconds. A short pause follows every step.
    """
    for kind, value in login_steps:
        log.info("login step: %s %r", kind, value)
        if kind == "text":
            driver.type_text(value)
        elif kind == "key":
            driver.key(value)
        elif kind == "wait":
            time.sleep(float(value))
            continue
        else:
            raise ValueError(f"Unknown login step kind: {kind}")
        time.sleep(step_wait)


def unstick_to_menu(driver, step_wait: float = 0.6) -> None:
    """The operator's own way out of any screen: F6, F6, then F7.

    Rafael, 2026-09-01: "para salir al menú principal es presionando primero F6 dos
    veces y después F7, desde cualquier menú en el que se esté". F6 is RETURN TO
    SELECT and F7 is EXIT, so this walks back out of whatever view the terminal is
    parked in and lands on the SALESN options list.

    It does NOT rescue the 'ADDITIONAL MESSAGE INFORMATION' dead end, where no key
    works at all — the caller checks for that screen first and never gets here.
    """
    for key in ("f6", "f6", "f7"):
        driver.key(key)
        time.sleep(step_wait)


def _unstick_tries() -> int:
    """How many times F6·F6·F7 is worth trying before giving up.

    Rafael, 2026-09-02: "F6 F6 F7 se puede reintentar hasta 3 veces antes de
    rendirse en caso de atascamiento". Read at call time so Bay 2 can retune it
    with a .env edit and a restart instead of a deploy.
    """
    return max(1, int(_env_float("AS400_UNSTICK_TRIES", 3)))


def return_to_order_search(driver, step_wait: float = 0.6, read_fn=None) -> str:
    """Put the terminal back on the order-search screen, and PROVE it by reading.

    The return trip is part of the step, not a tidy-up: a step that ends anywhere
    else costs the scanner its next order. Walks back with the operator's own way
    out (F6·F6·F7 → menu → 3), re-reading between attempts, up to
    AS400_UNSTICK_TRIES. Raises rather than leaving the terminal parked somewhere
    the next capture won't recognize.

    Never touches the ADDITIONAL MESSAGE INFORMATION dead end, where no key works.
    """
    read = read_fn or driver.copy_screen
    for _ in range(_unstick_tries()):
        screen = read()
        state = classify_screen(screen)
        if state == STATE_DISCONNECTED:
            raise AS400Disconnected("The AS400 dropped while returning to the order search.")
        if state == STATE_ORDER_SEARCH:
            return screen
        if _is_message_info_screen(screen):
            raise AS400ManualLoginRequired(
                "The AS400 is on the 'ADDITIONAL MESSAGE INFORMATION' screen, where no key "
                "works. Close the session and log back in."
            )
        if state == STATE_MENU:
            driver.type_text("3")  # 03. Order Inquiry
            time.sleep(step_wait)
            driver.key("enter")
        else:
            unstick_to_menu(driver, step_wait=step_wait)
        time.sleep(step_wait)

    raise AS400ManualLoginRequired(
        f"Couldn't get back to the order search after {_unstick_tries()} tries of F6·F6·F7."
    )


def return_to_search(driver, step_wait: float = 0.6) -> None:
    """Cmd7 from a stock detail lands back on the Stock Inquiry SEARCH form.

    Rafael, 11 sep 2026: "cmd7 regresa a la pantalla de busqueda, viene vacia".
    Blank is what makes this safe to do blind — nothing to clear, so the next
    SKU cannot concatenate onto the last one.

    One key and no read on purpose. The next lookup's own result is the check,
    and a wrong guess costs that lookup and nothing else: `capture_stock_inquiry`
    refuses to mark a SKU unknown while it is navigating optimistically.
    """
    driver.key("f7")
    time.sleep(step_wait)


def return_to_menu(driver, step_wait: float = 0.6, read_fn=None) -> str:
    """Put the terminal on the SALESN menu, and PROVE it by reading.

    The short way home, for when the next thing is another SKU. Going all the
    way back to the order search between two lookups means typing 3 to enter it
    and F7 to leave it again — the terminal walks out of the menu to walk
    straight back in (Rafael, 11 sep 2026: "no quiero volver a ver que se sale
    de la búsqueda de sku a propósito en vez de seguir con el siguiente"). The
    menu is a valid starting point for `capture_stock_inquiry`, so stopping here
    skips both keys.

    The FULL return to the order search still happens once, when the run ends:
    a terminal left anywhere else costs the scanner its next order.
    """
    read = read_fn or driver.copy_screen
    for _ in range(_unstick_tries()):
        screen = read()
        state = classify_screen(screen)
        if state == STATE_DISCONNECTED:
            raise AS400Disconnected("The AS400 dropped while returning to the menu.")
        if state == STATE_MENU:
            return screen
        if _is_message_info_screen(screen):
            raise AS400ManualLoginRequired(
                "The AS400 is on the 'ADDITIONAL MESSAGE INFORMATION' screen, where no key "
                "works. Close the session and log back in."
            )
        unstick_to_menu(driver, step_wait=step_wait)
        time.sleep(step_wait)

    raise AS400ManualLoginRequired(
        f"Couldn't get back to the menu after {_unstick_tries()} tries of F6·F6·F7."
    )


def _advance_toward_order_screen(
    driver, state, login_steps, step_wait, allow_unstick=False
) -> bool:
    """Take the known keystroke for `state` to move one step toward the order screen.

    Confirmed login flow (see docs §3.1): sign-on → Message Display → SALESN menu
    → Order Inquiry. Returns True if it acted, False if the state has no known move
    (caller then asks for manual login).

    `allow_unstick` lets an UNRECOGNIZED screen try the operator's F6·F6·F7 way back
    to the menu instead of giving up. It is offered once per session bootstrap: if
    the screen is still unknown afterwards, a human really is needed.
    """
    if state == STATE_LOGIN:
        run_login(driver, login_steps=login_steps, step_wait=step_wait)
    elif state == STATE_MENU:
        driver.type_text("3")  # 03. Order Inquiry
        time.sleep(step_wait)
        driver.key("enter")
    elif state == STATE_MESSAGE:
        driver.key("enter")  # "Press Enter to continue"
    elif state == STATE_CUSTOMER_DISPLAY:
        driver.key("f7")  # EXIT, per the screen's own legend → back to the menu
    elif state == STATE_STOCK_INQUIRY:
        # Cmd7 EXIT on both forms — the detail screen's footer legend and the
        # NOTES screen's "(Cmd7-Exit)" corner say the same key.
        driver.key("f7")
    elif state == STATE_UNKNOWN and allow_unstick:
        unstick_to_menu(driver, step_wait=step_wait)
    else:
        return False
    return True


# The last hard restart, so a stuck terminal can't turn into a relaunch loop.
_last_hard_restart = 0.0


def hard_restart_enabled() -> bool:
    """Whether the daemon may try to rescue itself from the dead-end screen.

    Off until somebody has watched it do it once. The mechanism is Cmd+N, which
    Rafael uses by hand and which takes nothing away from anyone, so this is
    expected to go on — the switch exists so that the first time it fires,
    somebody is standing there.
    """
    return os.getenv("AS400_HARD_RESTART", "0") in ("1", "true", "True", "yes")


def quit_and_relaunch_enabled() -> bool:
    """The deeper fallback: closing the emulator outright. Stays off unless asked.

    Cmd+N (below) should be enough and costs nothing. This one closes an
    application a person shares, can raise a confirmation dialog — and a dialog
    blocks every Apple event after it, trading a stuck terminal for one nobody
    can drive at all — and depends on Mocha reconnecting to the host by itself,
    which nobody has verified.
    """
    return os.getenv("AS400_HARD_RESTART_QUIT", "0") in ("1", "true", "True", "yes")


def hard_restart(driver, idle_fn=None) -> bool:
    """Get out of the screen no key escapes. The last resort, and only that.

    The ADDITIONAL MESSAGE INFORMATION screen answers NO key (§2.10), so once
    the terminal lands there the daemon is finished until a person re-opens the
    session. On 2026-09-08 that cost 36 minutes of a working day and, on a
    Friday evening, would have cost the weekend.

    Two ways out, cheapest first:

      1. **Cmd+N** — a fresh session window. Rafael, 2026-09-08. It is an
         APPLICATION command rather than a 5250 keystroke, which is exactly why
         it escapes a screen that ignores what the session receives.
      2. **Ctrl+Shift+Tab, then Cmd+W + ENTER** — step back to the dead window
         and close it, so corpses don't pile up behind the live session. Cmd+W
         only ASKS; the Enter answers ("el command w solo cierra con el enter
         después"). Tidy-up, not recovery: it is best effort and verifies the
         window before closing anything.
      3. **Quit and relaunch** — off by default (`AS400_HARD_RESTART_QUIT`), for
         a Cmd+N that doesn't work at all.

    Opening before closing is what makes this safe: there are always at least
    two windows when the Cmd+W lands, so it can never be the one that closes
    Mocha altogether — the case that would need the application reopened.

    Either way it refuses unless the Mac has been untouched for
    AS400_HARD_RESTART_IDLE_SEC (a new window steals focus, and focus stolen
    mid-order is somebody's work interrupted) and unless the last attempt was
    longer ago than the cooldown, so a terminal that cannot be saved is not
    poked every five minutes all night.

    It does NOT log in — the caller's bootstrap does, verifying every screen, so
    a recovery that lands somewhere unexpected still ends as an honest "a human
    is needed" rather than a loop.
    """
    global _last_hard_restart

    if not hard_restart_enabled():
        return False

    idle = (idle_fn or _system_idle_seconds)()
    needed = _env_float("AS400_HARD_RESTART_IDLE_SEC", 300.0)
    if idle < needed:
        log.info(
            "AS400 recovery: not while somebody is using the Mac (idle %.0fs < %.0fs)", idle, needed
        )
        return False

    since = time.time() - _last_hard_restart
    cooldown = _env_float("AS400_HARD_RESTART_COOLDOWN_SEC", 1800.0)
    if since < cooldown:
        log.info("AS400 recovery: already tried %.0fs ago — leaving it for a person", since)
        return False

    _last_hard_restart = time.time()

    # 1. A fresh session window FIRST. This is the recovery; everything after
    #    it is tidying up. Opening first also means there are always at least
    #    two windows, so the Cmd+W below can never be the one that closes Mocha
    #    altogether — the case Rafael warned about stops existing.
    log.warning("AS400 recovery: dead-end screen — opening a new session window (Cmd+N)")
    try:
        driver.new_window()
        time.sleep(_env_float("AS400_NEW_WINDOW_WAIT", 3.0))
    except Exception as e:
        log.warning("AS400 recovery: Cmd+N didn't work (%s)", e)
        if not quit_and_relaunch_enabled():
            log.error("AS400 recovery: and quitting is off. A person has to step in.")
            return False
        return _quit_and_relaunch(driver)

    # 2. Tidy up: step back to the dead window and close it, so the corpses
    #    don't pile up behind the live session. Best effort — the recovery has
    #    already happened, and none of this may put it at risk.
    _close_the_dead_window(driver)
    return True


def _close_the_dead_window(driver) -> None:
    """Ctrl+Shift+Tab back to the dead window and close it. Best effort.

    VERIFIES before closing, like everything else that drives this terminal. If
    the window we land on is not the dead end — the operator had others open,
    the shortcut behaved differently — nothing is closed and we step forward
    again. Closing somebody's window to tidy up would be a far worse bug than
    leaving a dead one behind.
    """
    try:
        driver.previous_window()
        time.sleep(_env_float("AS400_WINDOW_SWITCH_WAIT", 1.0))
        screen = driver.copy_screen()
    except Exception as e:
        log.info("AS400 recovery: couldn't reach the old window to close it (%s)", e)
        return

    if not _is_message_info_screen(screen):
        log.info(
            "AS400 recovery: the window behind isn't the dead one — leaving it alone (state=%s)",
            classify_screen(screen),
        )
        try:
            driver.next_window()
            time.sleep(_env_float("AS400_WINDOW_SWITCH_WAIT", 1.0))
        except Exception as e:
            log.warning("AS400 recovery: couldn't step back to the new window (%s)", e)
        return

    try:
        # Cmd+W asks; the Enter answers. Both live inside one script — see
        # close_window — so a confirmation dialog can never be left standing.
        driver.close_window()
        time.sleep(_env_float("AS400_CLOSE_WINDOW_WAIT", 1.5))
        log.info("AS400 recovery: the dead window is closed")
    except Exception as e:
        log.warning("AS400 recovery: couldn't close the dead window (%s) — it stays behind", e)


def _quit_and_relaunch(driver) -> bool:
    """The deepest fallback, behind its own switch. See quit_and_relaunch_enabled."""
    log.warning("AS400 recovery: closing the emulator")
    if not driver.quit():
        log.error("AS400 recovery: the emulator would not close. A person has to do it.")
        return False
    time.sleep(_env_float("AS400_RESTART_PAUSE", 3.0))
    driver.launch()
    time.sleep(_env_float("AS400_LAUNCH_WAIT", LAUNCH_WAIT))
    log.warning("AS400 recovery: emulator reopened — it still has to log itself back in")
    return True


def _system_idle_seconds() -> float:
    """Seconds since the last input. Mirrors auto_scanner's reader; imported
    lazily so this module keeps having no local dependencies."""
    try:
        from auto_scanner import system_idle_seconds

        return system_idle_seconds()
    except Exception:
        return 0.0  # can't tell → assume somebody is there, and don't restart


def bootstrap_session(
    driver,
    launch_wait: float = LAUNCH_WAIT,
    login_steps=DEFAULT_LOGIN_STEPS,
    step_wait: float = 0.6,
    max_steps: int = 8,
):
    """Open the emulator and ensure we end logged in at the order screen.

    Unlike a blind macro replay, this VERIFIES the screen at every step so it
    never falsely reports success. It reads the screen, then drives one confirmed
    keystroke toward the order view, repeating until ready:
      - host down      → AS400Disconnected (never type into a dead screen)
      - already in     → return the state without re-typing anything
      - sign-on / menu / Message Display → take the known step (login macro,
        pick 3, or Press Enter), re-read, and continue
      - unrecognized   → AS400ManualLoginRequired (ask for manual login)

    Returns the final screen state (STATE_ORDER_SEARCH or STATE_ORDER_INQUIRY).
    """
    driver.launch()
    time.sleep(launch_wait)
    driver.focus()

    unsticks = 0
    for _ in range(max_steps):
        screen = driver.copy_screen()
        state = classify_screen(screen)
        log.info("AS400 connect: screen state=%s", state)

        if state == STATE_DISCONNECTED:
            raise AS400Disconnected(
                "The AS400 isn't connected (the emulator can't reach the host). "
                "Connect and log in manually in Mocha, then try again."
            )
        if state in _READY_STATES:
            return state
        if _is_message_info_screen(screen):
            # The one screen no key escapes (operator, 2026-06-11). Don't hammer
            # it — but closing the emulator outright is not a keystroke, and it
            # is the only thing that has ever recovered this without a person
            # (Rafael, 2026-09-08). Guarded to the teeth; see hard_restart.
            if hard_restart(driver):
                continue
            raise AS400ManualLoginRequired(
                "The AS400 is on the 'ADDITIONAL MESSAGE INFORMATION' screen, where no "
                "key works. Close the session and log back in, then try again."
            )

        allow_unstick = state == STATE_UNKNOWN and unsticks < _unstick_tries()
        if not _advance_toward_order_screen(
            driver, state, login_steps, step_wait, allow_unstick=allow_unstick
        ):
            raise AS400ManualLoginRequired(
                "I don't recognize the current AS400 screen. Log in manually "
                "to the order-search screen, then try again."
            )
        if allow_unstick:
            unsticks += 1
            log.info(
                "AS400 connect: unknown screen — tried F6·F6·F7 back to the menu (%d/%d)",
                unsticks,
                _unstick_tries(),
            )
        time.sleep(step_wait)

    raise AS400ManualLoginRequired(
        "Couldn't reach the order-search screen after several steps. "
        "Log in manually, then try again."
    )


def capture_stock_inquiry(
    sku: str,
    driver,
    page_wait=None,
    step_wait: float = 0.6,
    read_fn=None,
    on_search_screen: bool = False,
) -> str:
    """Drive the terminal to STOCK INQUIRY for `sku` and return the screen text.

    READ ONLY. It types the SKU into a lookup and copies what comes back; it
    never presses a key that writes. It does NOT bring the terminal home either
    — the caller owns that (`return_to_order_search`), because the return trip
    has to run whether this succeeded or raised.

    The route (docs/as400-screen-map.md §2.12, Rafael 2026-09-02):

        order search ──F7──▶ menu ──2 + ENTER──▶ Stock Inquiry
            ──digits, TAB, colour, TAB, X──▶ the detail screen

    The X is the last key: it submits on its own, and nothing follows it.

    Raises StockSkuNotFound when the SKU has a shape AS400 cannot look up (R6)
    or the lookup does not land on a stock screen, and StockScreenMismatch when
    it lands on one that is either the NOTES form (R10) or somebody else's
    record.
    """
    if page_wait is None:
        page_wait = _env_float("AS400_PAGE_WAIT", PAGE_WAIT_DEFAULT)
    read = read_fn or driver.copy_screen

    fields = sku_screen_fields(sku)
    if fields is None:
        # The FedEx/USPS tracking numbers and the serials. The SHAPE is the
        # filter, so no hand-kept list of exceptions can go stale.
        raise StockSkuNotFound(f"{sku!r} isn't an AS400 stock number — nothing to look up.")
    digits, colour = fields

    # `on_search_screen`: the caller knows we are already on the search form,
    # because the last lookup ended with Cmd7 and Cmd7 lands there. No read, no
    # menu, no option 2 — type the next SKU where we stand (Rafael, 11 sep 2026:
    # "que asuma que sigue en la misma pagina de busqueda"). The cost of being
    # wrong is handled below, where it would otherwise be expensive.
    if on_search_screen:
        return _type_stock_lookup(sku, driver, digits, colour, page_wait, read, optimistic=True)

    # Verify before driving, exactly like a capture: never type into a dead or
    # unrecognized screen.
    screen = read()
    state = classify_screen(screen)
    if state == STATE_DISCONNECTED:
        raise AS400Disconnected("The AS400 isn't connected — not looking up a SKU.")
    if state not in _READY_STATES and state != STATE_MENU:
        raise AS400ManualLoginRequired(
            "The AS400 isn't on the order search or the menu, so a SKU lookup would be "
            "typing into an unknown screen."
        )

    if state != STATE_MENU:
        driver.key("f7")  # EXIT → SALESN menu
        time.sleep(step_wait)
        if classify_screen(read()) != STATE_MENU:
            raise StockScreenMismatch("F7 didn't land on the SALESN menu — not typing further.")

    # 02, never 03. Rafael, 2026-09-08: "el 2 es para stock inquiry, el 3 es
    # para órdenes". Option 3 is only ever typed on the way BACK, by
    # return_to_order_search, which is its actual job.
    driver.type_text("2")  # 02. Stock File Inquiry
    time.sleep(step_wait)
    driver.key("enter")
    time.sleep(page_wait)

    # Confirm we are INSIDE the stock program before typing a SKU into it, and
    # this guard is not ceremony. Without it, a `2` that never took — the menu
    # missed it, the screen was slow, we were somewhere else entirely — means
    # the SKU gets typed into whatever is in front, the check further down finds
    # no stock screen, and the step reports StockSkuNotFound. That marks the SKU
    # as one AS400 does not have (R7) and removes it from the queue FOR EVER,
    # although AS400 knows it perfectly well. A navigation failure would poison
    # the queue one good SKU at a time, silently.
    if classify_screen(read()) != STATE_STOCK_INQUIRY:
        raise StockScreenMismatch(
            "Option 2 didn't open Stock Inquiry — not typing a SKU into an unknown screen."
        )

    return _type_stock_lookup(sku, driver, digits, colour, page_wait, read, optimistic=False)


def _type_stock_lookup(sku, driver, digits, colour, page_wait, read, *, optimistic: bool) -> str:
    """Type one SKU into the Stock Inquiry search form and read what comes back.

    A SKU with no colour suffix takes a blank TAB — that is the 126 bikes shaped
    like `01-0169`, and they are in the queue, not out.
    """
    driver.type_text(digits)
    driver.key("tab")
    if colour:
        driver.type_text(colour)
    driver.key("tab")
    # The X is the last key. Rafael, 2026-09-08: "después de presionar X no se
    # debe presionar otra tecla, solo copiar de frente". It submits by itself,
    # and the ENTER that used to follow it was a guess of mine.
    driver.type_text("X")
    time.sleep(page_wait)

    screen = read()
    if classify_screen(screen) != STATE_STOCK_INQUIRY:
        if optimistic:
            # The trap this whole branch has to avoid. Verified, landing
            # nowhere means AS400 has no such stock number, and the caller
            # marks it so the queue stops asking (R7) — FOR EVER. Optimistic,
            # it much more likely means we were not on the search form after
            # all, and reporting it as "AS400 doesn't have this" would poison
            # the queue one good SKU at a time, silently. So: a navigation
            # problem, which costs this lookup and nothing else.
            raise StockScreenMismatch(
                f"Assumed the search form for {sku} and landed elsewhere — not marking it."
            )
        # We were on the stock program a moment ago, so this is the SKU's own
        # answer: whatever AS400 shows for a stock number it doesn't have. Nobody
        # has seen that screen — F2 logs it so it can be mapped.
        raise StockSkuNotFound(f"The lookup for {sku} didn't land on a stock screen.")
    if not is_stock_detail_screen(screen):
        # R10: same title, no fields. Reading here would report `Weight` as
        # absent instead of as "I am not where I think I am".
        raise StockScreenMismatch(
            f"Landed on the STOCK INQUIRY NOTES form for {sku} — no fields to read."
        )
    return screen


def capture_order(
    order_number: str,
    driver,
    page_wait=None,
    max_pages: int = 25,
    poll_interval=None,
    refresh_timeout=None,
    reuse_header=None,
) -> str:
    """
    Capture a full order from the AS400 screen into one text blob.

    Args:
        order_number: the order number to type.
        driver:       object with focus(), type_text(str), key(str), copy_screen()->str.
        page_wait:    seconds to wait for the screen to refresh before copying.
                      None → AS400_PAGE_WAIT, default 0.8 (same for poll_interval
                      → AS400_POLL_INTERVAL and refresh_timeout →
                      AS400_REFRESH_TIMEOUT). Passing a number wins, so tests stay
                      independent of the environment.
        max_pages:    safety cap on item pages to avoid an infinite loop if the
                      END OF ORDER marker never appears.

    Returns the concatenated text of the header page + all item pages.
    Raises CaptureError if END OF ORDER is not seen within max_pages.

    Paging waits for the screen to ACTUALLY change before deciding what to do.
    A fixed sleep alone is racy: if the 5250 screen hasn't refreshed yet after an
    ENTER, copy_screen() returns the previous page (no END OF ORDER), the loop
    presses ENTER again, and the genuinely-new page is skipped — losing its items.
    """
    if page_wait is None:
        page_wait = _env_float("AS400_PAGE_WAIT", PAGE_WAIT_DEFAULT)
    if poll_interval is None:
        poll_interval = _env_float("AS400_POLL_INTERVAL", POLL_INTERVAL_DEFAULT)
    if refresh_timeout is None:
        refresh_timeout = _env_float("AS400_REFRESH_TIMEOUT", REFRESH_TIMEOUT_DEFAULT)
    wait_for_settled = _wait_for_settled_enabled()
    settled_reads = max(1, int(_env_float("AS400_SETTLED_READS", SETTLED_READS_DEFAULT)))
    settle_poll = _env_float("AS400_SETTLE_POLL", SETTLE_POLL_DEFAULT)
    refresh_deadline = _env_float("AS400_REFRESH_DEADLINE", REFRESH_DEADLINE_DEFAULT)

    # AS400 order numbers are digits. Anything else is a caller mistake, and it used
    # to reach the terminal as keystrokes inside an AppleScript string.
    if not str(order_number).strip().isdigit():
        raise OrderNotFound(f"{order_number!r} isn't an AS400 order number (they are digits).")

    # ── measurement (plan F0) ────────────────────────────────────────────────
    # Where the time actually goes, per capture, in the log of the machine that
    # matters. No phase after this one tunes a wait without a number from here.
    started = time.monotonic()
    # wait_s is the whole page transition (the pause plus the read that follows),
    # not just the stall: "3.15s waiting for refresh, 0 stale reads" read as a
    # contradiction in the first real log line it produced.
    stats = {"reads": 0, "read_s": 0.0, "wait_s": 0.0, "stale": 0}

    def read_screen(steps=()) -> str:
        stats["reads"] += 1
        at = time.monotonic()
        try:
            return driver.copy_screen(steps) if steps else driver.copy_screen()
        finally:
            stats["read_s"] += time.monotonic() - at

    def send_keys(steps) -> None:
        for kind, value in steps:
            if kind == "key":
                driver.key(value)
            elif kind == "text":
                driver.type_text(str(value))
            elif kind == "wait":
                time.sleep(float(value))

    def send_and_read(steps) -> str:
        """The keys that lead to a screen, then that screen — in ONE process when
        the driver can do it. On Bay 2 each Apple Event to System Events costs
        ~300 ms, so F6 + typing + reading as three calls was 0.9s of round trip
        for a sequence we already knew in advance. Identical keys, identical
        waits; only the number of processes changes. Any driver without
        `supports_steps` (every fake in the tests) replays it key by key."""
        if steps and _read_in_script_enabled() and getattr(driver, "supports_steps", False):
            return read_screen(steps)
        send_keys(steps)
        return read_screen()

    def log_timing(outcome: str, item_pages: int) -> None:
        total = time.monotonic() - started
        log.info(
            "AS400 #%s %s in %.2fs — %d reads (%.2fs), %d item pages, "
            "%.2fs on page transitions, %d stale reads",
            order_number,
            outcome,
            total,
            stats["reads"],
            stats["read_s"],
            item_pages,
            stats["wait_s"],
            stats["stale"],
        )

    # No focus() here: the read below brings the emulator up itself if it isn't
    # already, in the same script. Calling both meant two Apple events asking the
    # same question at the start of every capture.

    # Verify the screen BEFORE driving it: never type an order number into a
    # disconnected or unrecognized view (that's how a dead session silently
    # swallowed keystrokes and still looked "fine").
    screen = read_screen()
    state = classify_screen(screen)
    if state == STATE_DISCONNECTED:
        raise AS400Disconnected(
            "The AS400 isn't connected. Log in manually in Mocha, then try again."
        )
    if state not in _READY_STATES:
        raise AS400ManualLoginRequired(
            "The AS400 isn't on the order-search screen (not logged in or an "
            "unknown view). Log in manually, then try again."
        )

    if reuse_header is None:
        reuse_header = _reuse_header_default()

    if reuse_header and _is_order_header_screen(screen, order_number):
        # The screen we just read IS this order's header — the operator looked the
        # order up in Mocha before hitting capture, or a previous attempt left it
        # there. Pressing F6 and re-typing the number walks BACK to the search
        # screen to arrive at the page already in front of us: a third of the
        # capture, and two keystrokes into a terminal shared with a person. Carry
        # on from here; every guard below still runs on this same text (VOID,
        # message screen, invalid number, wrong view), so nothing is skipped —
        # only the round trip is.
        header = screen
        log.info(
            "AS400 #%s already on screen — continuing from it (no F6 / re-type)",
            order_number,
        )
    else:
        # Page 1: customer header. Typing the last digit surfaces it automatically.
        # F6 opens a fresh order search before each new order; it, the number and
        # the read that follows are one script when the driver allows it.
        if wait_for_settled:
            # Waiting for the header to REPLACE the search screen, not just for
            # 800 ms: reading too early here would hand the search screen to the
            # checks below, which would happily call it an order view and press F5.
            search_norm = _norm_screen(screen)
            send_keys([("key", "f6"), ("wait", page_wait), ("text", str(order_number))])
            header, _ = _await_changed_page(
                read_screen,
                search_norm,
                settled_reads=settled_reads,
                poll=settle_poll,
                deadline=refresh_deadline,
                on_stale=lambda: stats.__setitem__("stale", stats["stale"] + 1),
            )
        else:
            header = send_and_read(
                [
                    ("key", "f6"),
                    ("wait", page_wait),
                    ("text", str(order_number)),
                    ("wait", page_wait),
                ]
            )

    pages = [header]
    log.info("AS400 header captured: %d chars", len(header))

    # The number isn't a registered order yet: AS400 answers 'Invalid Order Number,
    # REENTER' on the search screen. That screen still says 'Order Number', so the
    # generic order-screen check below would pass — detect the rejection explicitly
    # and stop here (the auto-scanner retries this same number next cycle).
    if _is_invalid_order(header):
        raise OrderNotFound(
            f"Order {order_number} doesn't exist yet (AS400: 'Invalid Order Number, REENTER')."
        )

    # PREVENTION (primary): a VOID order header. Pressing F5 on it routes to the
    # dead-end message screen where no key works (full re-login needed), so we
    # SKIP here — before F5 — and never enter that screen. F6 (return to select)
    # works from the header view; the next capture starts clean.
    if _is_void_order(header):
        driver.key("f6")
        raise OrderVoidSkip(f"Order {order_number} is VOID (header) — skipped before F5.")

    # DEFENSE IN DEPTH: if we somehow already landed on the message screen, still
    # try F6 and skip rather than page-looping (no key may work here, but the next
    # capture's initial F6 / a re-login is the operator's recovery).
    if _is_message_info_screen(header):
        driver.key("f6")
        raise OrderVoidSkip(
            f"Order {order_number} routed to the AS400 message screen (likely VOID) — skipped."
        )

    # Guard: if this isn't an order view we're on the wrong screen (a menu, etc.)
    # or the order doesn't exist. Bail out now instead of pressing F5 and looping
    # through pages that will never show END OF ORDER.
    if not _looks_like_order_screen(header):
        raise OrderNotFound(
            f"The screen isn't an order inquiry (wrong view, or order "
            f"{order_number} doesn't exist). Go to order search (F7 → 3) and try again."
        )

    # The last page we acted on. We only page forward once the screen differs from
    # this, so a not-yet-refreshed (stale) copy never triggers a second ENTER.
    prev_norm = _norm_screen(header)

    # F5 switches to the items view (once); ENTER pages from there. The key now
    # travels in the same script as the read that follows it.
    pending_key = "f5"

    for i in range(1, max_pages + 1):
        waiting_from = time.monotonic()
        if wait_for_settled:
            send_keys([("key", pending_key)])
            page, _ = _await_changed_page(
                read_screen,
                prev_norm,
                settled_reads=settled_reads,
                poll=settle_poll,
                deadline=refresh_deadline,
                on_stale=lambda: stats.__setitem__("stale", stats["stale"] + 1),
            )
        else:
            page = send_and_read([("key", pending_key), ("wait", page_wait)])

            # Wait for the screen to actually advance before trusting it. If it's
            # still showing the previous page, keep polling instead of paging again.
            waited = 0.0
            while _norm_screen(page) == prev_norm and waited < refresh_timeout:
                stats["stale"] += 1
                time.sleep(poll_interval)
                waited += poll_interval
                page = read_screen()
        stats["wait_s"] += time.monotonic() - waiting_from

        # A VOID order can route to the message-detail screen after F5. Recover by
        # pressing F6 (back to order search) and skip — never page-loop on it.
        if _is_message_info_screen(page):
            driver.key("f6")
            raise OrderVoidSkip(
                f"Order {order_number} routed to the AS400 message screen (likely VOID) — skipped."
            )

        stale = _norm_screen(page) == prev_norm
        found = _has_end_marker(page)
        log.info(
            "AS400 items page %d: %d chars, end_marker=%s, stale=%s | tail=%r",
            i,
            len(page),
            found,
            stale,
            page[-60:].replace("\n", "\\n"),
        )
        if found:
            log.info("END OF ORDER found on page %d — stopping.", i)
            log_timing("captured", i)
            return "\n".join(pages + [page])
        if stale:
            # Screen never changed within the timeout: the order likely ended
            # without an END OF ORDER marker (or the session stalled). Stop here
            # rather than paging blindly and risk skipping/duplicating content.
            log_timing("stalled", i)
            raise CaptureError(
                f"The AS400 screen didn't advance for order {order_number} "
                f"(no '{END_OF_ORDER_MARKER}' and no new page appeared). Capture aborted."
            )

        pages.append(page)
        prev_norm = _norm_screen(page)
        pending_key = "enter"

    log_timing("hit the page cap", max_pages)
    raise CaptureError(
        f"'{END_OF_ORDER_MARKER}' didn't appear after {max_pages} pages for order "
        f"{order_number}. Capture aborted."
    )
