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
FOCUS_SETTLE_DEFAULT = 0.4
SELECT_DELAY_DEFAULT = 0.15
COPY_DELAY_DEFAULT = 0.2


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
MESSAGE_MARKERS = ("PRESSENTERTOCONTINUE",)
# Option 01 of the SALESN menu. Reachable by hand (it holds the dealer's phone and
# e-mail), so the daemon can find the terminal parked here. Its own legend says
# Cmd7 EXIT, which is the way back to the menu.
CUSTOMER_MARKERS = ("CUSTOMERDISPLAY",)

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
    if "ORDERINQUIRY" in norm:
        return STATE_ORDER_INQUIRY
    if "ORDERNUMBER" in norm:
        return STATE_ORDER_SEARCH
    return STATE_UNKNOWN


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


def _reuse_header_default() -> bool:
    """Whether capture_order may continue from an order already on screen.

    Read at call time (not import) so a .env loaded later still wins, and so the
    Bay 2 machine can switch it off without a deploy: AS400_REUSE_HEADER=0.
    """
    return os.getenv("AS400_REUSE_HEADER", "1").strip().lower() in ("1", "true", "yes", "on")


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


class MochaDriver:
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

    def _osascript(self, script: str):
        """Run one AppleScript — always with a timeout.

        Without it, a hung osascript (Mocha showing a dialog, macOS asking for an
        Automation permission) blocked the capture thread forever while it held
        capture_lock: the scanner went silent and manual captures hung behind it,
        with nothing in the log to say why.
        """
        timeout = _env_float("AS400_OSASCRIPT_TIMEOUT", OSASCRIPT_TIMEOUT_DEFAULT)
        try:
            subprocess.run(["osascript", "-e", script], check=True, timeout=timeout)
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

    def focus(self):
        # Activate by bundle id when available (reliable for sandboxed apps);
        # otherwise bring the running process to the front by name. Only when it
        # isn't already in front — and the settle pause runs inside the script, so
        # an emulator that was already up costs one Apple event and no sleep.
        self._osascript(
            build_focus_script(
                self.app_name,
                self.bundle_id,
                _env_float("AS400_FOCUS_SETTLE", FOCUS_SETTLE_DEFAULT),
            )
        )

    def type_text(self, text: str):
        # Escaped for the AppleScript string literal: a quote or a backslash in the
        # value used to break the script and surface as an unexplained 500.
        safe = str(text).replace("\\", "\\\\").replace('"', '\\"')
        self._osascript(f'tell application "System Events" to keystroke "{safe}"')

    def key(self, name: str):
        """Press a special key: 'enter', 'tab', 'f5' or 'f6'."""
        key_codes = {"enter": 36, "return": 36, "tab": 48, "f5": 96, "f6": 97}
        code = key_codes.get(name.lower())
        if code is None:
            raise ValueError(f"Unknown key: {name}")
        self._osascript(f'tell application "System Events" to key code {code}')

    def copy_screen(self) -> str:
        """Read the whole AS400 screen: focus if needed, Cmd+A, Cmd+C, clipboard.

        ONE osascript does the keyboard half (plan F2/F3). Writes a sentinel to the
        clipboard first; if Cmd+A/Cmd+C didn't replace it, the copy went nowhere
        (wrong window focused, or Mocha doesn't copy via Cmd+A) — we raise instead
        of looping forever on stale clipboard content. That sentinel is exactly what
        makes the fast path safe: skipping the activation can only fail loudly.
        """
        clip_timeout = _env_float("AS400_OSASCRIPT_TIMEOUT", OSASCRIPT_TIMEOUT_DEFAULT)
        sentinel = f"__PICKD_NO_COPY__{time.time()}"
        subprocess.run(["pbcopy"], input=sentinel, text=True, check=True, timeout=clip_timeout)

        self._osascript(
            build_copy_screen_script(
                self.app_name,
                self.bundle_id,
                _env_float("AS400_FOCUS_SETTLE", FOCUS_SETTLE_DEFAULT),
                _env_float("AS400_SELECT_DELAY", SELECT_DELAY_DEFAULT),
                _env_float("AS400_COPY_DELAY", COPY_DELAY_DEFAULT),
            )
        )

        out = subprocess.run(
            ["pbpaste"], capture_output=True, text=True, check=True, timeout=clip_timeout
        ).stdout
        if out == sentinel:
            raise CaptureError(
                "Cmd+A/Cmd+C didn't copy the AS400 screen (the clipboard didn't change). "
                "Make sure Mocha stays in front and copies with Cmd+A+Cmd+C; "
                "if Mocha copies a different way (Edit menu), let me know which."
            )
        return out


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
    elif state == STATE_UNKNOWN and allow_unstick:
        unstick_to_menu(driver, step_wait=step_wait)
    else:
        return False
    return True


def bootstrap_session(
    driver,
    launch_wait: float = LAUNCH_WAIT,
    login_steps=DEFAULT_LOGIN_STEPS,
    step_wait: float = 0.6,
    max_steps: int = 6,
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

    unstick_used = False
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
            # The one screen no key escapes (operator, 2026-06-11). Don't hammer it.
            raise AS400ManualLoginRequired(
                "The AS400 is on the 'ADDITIONAL MESSAGE INFORMATION' screen, where no "
                "key works. Close the session and log back in, then try again."
            )

        allow_unstick = state == STATE_UNKNOWN and not unstick_used
        if not _advance_toward_order_screen(
            driver, state, login_steps, step_wait, allow_unstick=allow_unstick
        ):
            raise AS400ManualLoginRequired(
                "I don't recognize the current AS400 screen. Log in manually "
                "to the order-search screen, then try again."
            )
        if allow_unstick:
            unstick_used = True
            log.info("AS400 connect: unknown screen — tried F6·F6·F7 back to the menu")
        time.sleep(step_wait)

    raise AS400ManualLoginRequired(
        "Couldn't reach the order-search screen after several steps. "
        "Log in manually, then try again."
    )


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

    # AS400 order numbers are digits. Anything else is a caller mistake, and it used
    # to reach the terminal as keystrokes inside an AppleScript string.
    if not str(order_number).strip().isdigit():
        raise OrderNotFound(f"{order_number!r} isn't an AS400 order number (they are digits).")

    # ── measurement (plan F0) ────────────────────────────────────────────────
    # Where the time actually goes, per capture, in the log of the machine that
    # matters. No phase after this one tunes a wait without a number from here.
    started = time.monotonic()
    stats = {"reads": 0, "read_s": 0.0, "wait_s": 0.0, "stale": 0}

    def read_screen() -> str:
        stats["reads"] += 1
        at = time.monotonic()
        try:
            return driver.copy_screen()
        finally:
            stats["read_s"] += time.monotonic() - at

    def log_timing(outcome: str, item_pages: int) -> None:
        total = time.monotonic() - started
        log.info(
            "AS400 #%s %s in %.2fs — %d reads (%.2fs), %d item pages, "
            "%.2fs waiting for refresh, %d stale reads",
            order_number,
            outcome,
            total,
            stats["reads"],
            stats["read_s"],
            item_pages,
            stats["wait_s"],
            stats["stale"],
        )

    driver.focus()

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
        # F6 opens a fresh order search before each new order.
        driver.key("f6")
        time.sleep(page_wait)

        # Page 1: customer header. Typing the last digit surfaces it automatically.
        driver.type_text(str(order_number))
        time.sleep(page_wait)
        header = read_screen()

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

    # Switch to the items view (once).
    driver.key("f5")

    # The last page we acted on. We only page forward once the screen differs from
    # this, so a not-yet-refreshed (stale) copy never triggers a second ENTER.
    prev_norm = _norm_screen(header)

    for i in range(1, max_pages + 1):
        time.sleep(page_wait)
        page = read_screen()

        # Wait for the screen to actually advance before trusting it. If it's still
        # showing the previous page, keep polling instead of paging again.
        waited = 0.0
        waiting_from = time.monotonic()
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
        driver.key("enter")

    log_timing("hit the page cap", max_pages)
    raise CaptureError(
        f"'{END_OF_ORDER_MARKER}' didn't appear after {max_pages} pages for order "
        f"{order_number}. Capture aborted."
    )
