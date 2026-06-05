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
    if "ORDERINQUIRY" in norm:
        return STATE_ORDER_INQUIRY
    if "ORDERNUMBER" in norm:
        return STATE_ORDER_SEARCH
    return STATE_UNKNOWN


def _has_end_marker(text: str) -> bool:
    """Whitespace-insensitive match for the END OF ORDER marker.

    The 5250 screen capture can include extra spacing between characters, so we
    strip all whitespace before checking (e.g. 'E N D  O F  O R D E R' still matches).
    """
    return "ENDOFORDER" in re.sub(r"\s+", "", text.upper())


def _looks_like_order_screen(text: str) -> bool:
    """True if the captured text looks like an order view (search or inquiry).

    Used to bail out (instead of looping) when we're on a different AS400 view
    (e.g. a menu) or the order doesn't exist.
    """
    return classify_screen(text) in _READY_STATES


class CaptureError(Exception):
    """Raised when a capture cannot complete (e.g. END OF ORDER never appears)."""


class AS400Disconnected(CaptureError):
    """The emulator isn't connected to the host (server down / session ended)."""


class AS400ManualLoginRequired(CaptureError):
    """The screen is unrecognized or not logged in — a human must log in first."""


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
        subprocess.run(["osascript", "-e", script], check=True)

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
        # otherwise bring the running process to the front by name.
        if self.bundle_id:
            self._osascript(f'tell application id "{self.bundle_id}" to activate')
        else:
            self._osascript(
                f'tell application "System Events" to set frontmost of process "{self.app_name}" to true'
            )
        time.sleep(0.4)

    def type_text(self, text: str):
        self._osascript(f'tell application "System Events" to keystroke "{text}"')

    def key(self, name: str):
        """Press a special key: 'enter', 'tab', 'f5' or 'f6'."""
        key_codes = {"enter": 36, "return": 36, "tab": 48, "f5": 96, "f6": 97}
        code = key_codes.get(name.lower())
        if code is None:
            raise ValueError(f"Unknown key: {name}")
        self._osascript(f'tell application "System Events" to key code {code}')

    def copy_screen(self) -> str:
        """Focus Mocha, select all (Cmd+A), copy (Cmd+C), read the clipboard.

        Writes a sentinel to the clipboard first; if Cmd+A/Cmd+C didn't replace it,
        the copy went nowhere (wrong window focused, or Mocha doesn't copy via
        Cmd+A) — we raise instead of looping forever on stale clipboard content.
        """
        self.focus()
        sentinel = f"__PICKD_NO_COPY__{time.time()}"
        subprocess.run(["pbcopy"], input=sentinel, text=True, check=True)

        self._osascript('tell application "System Events" to keystroke "a" using command down')
        time.sleep(0.15)
        self._osascript('tell application "System Events" to keystroke "c" using command down')
        time.sleep(0.2)

        out = subprocess.run(["pbpaste"], capture_output=True, text=True, check=True).stdout
        if out == sentinel:
            raise CaptureError(
                "Cmd+A/Cmd+C no copió la pantalla del AS400 (el portapapeles no cambió). "
                "Verifica que Mocha quede al frente y que copie con Cmd+A+Cmd+C; "
                "si en Mocha se copia de otra forma (menú Edit), dime cuál."
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


def _advance_toward_order_screen(driver, state, login_steps, step_wait) -> bool:
    """Take the known keystroke for `state` to move one step toward the order screen.

    Confirmed login flow (see docs §3.1): sign-on → Message Display → SALESN menu
    → Order Inquiry. Returns True if it acted, False if the state has no known move
    (caller then asks for manual login).
    """
    if state == STATE_LOGIN:
        run_login(driver, login_steps=login_steps, step_wait=step_wait)
    elif state == STATE_MENU:
        driver.type_text("3")  # 03. Order Inquiry
        time.sleep(step_wait)
        driver.key("enter")
    elif state == STATE_MESSAGE:
        driver.key("enter")  # "Press Enter to continue"
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

    for _ in range(max_steps):
        state = classify_screen(driver.copy_screen())
        log.info("AS400 connect: screen state=%s", state)

        if state == STATE_DISCONNECTED:
            raise AS400Disconnected(
                "El AS400 no está conectado (el emulador no alcanza el host). "
                "Conéctate e inicia sesión manualmente en Mocha y vuelve a intentar."
            )
        if state in _READY_STATES:
            return state

        if not _advance_toward_order_screen(driver, state, login_steps, step_wait):
            raise AS400ManualLoginRequired(
                "No reconozco la pantalla actual del AS400. Inicia sesión manualmente "
                "hasta la pantalla de búsqueda de orden y vuelve a intentar."
            )
        time.sleep(step_wait)

    raise AS400ManualLoginRequired(
        "No llegué a la pantalla de búsqueda de orden tras varios pasos. "
        "Inicia sesión manualmente y vuelve a intentar."
    )


def capture_order(
    order_number: str,
    driver,
    page_wait: float = 0.8,
    max_pages: int = 25,
) -> str:
    """
    Capture a full order from the AS400 screen into one text blob.

    Args:
        order_number: the order number to type.
        driver:       object with focus(), type_text(str), key(str), copy_screen()->str.
        page_wait:    seconds to wait for the screen to refresh before copying.
        max_pages:    safety cap on item pages to avoid an infinite loop if the
                      END OF ORDER marker never appears.

    Returns the concatenated text of the header page + all item pages.
    Raises CaptureError if END OF ORDER is not seen within max_pages.
    """
    driver.focus()

    # Verify the screen BEFORE driving it: never type an order number into a
    # disconnected or unrecognized view (that's how a dead session silently
    # swallowed keystrokes and still looked "fine").
    state = classify_screen(driver.copy_screen())
    if state == STATE_DISCONNECTED:
        raise AS400Disconnected(
            "El AS400 no está conectado. Inicia sesión manualmente en Mocha y reintenta."
        )
    if state not in _READY_STATES:
        raise AS400ManualLoginRequired(
            "El AS400 no está en la pantalla de búsqueda de orden (sesión no iniciada o "
            "vista desconocida). Inicia sesión manualmente y reintenta."
        )

    # F6 opens a fresh order search before each new order.
    driver.key("f6")
    time.sleep(page_wait)

    # Page 1: customer header. Typing the last digit surfaces it automatically.
    driver.type_text(str(order_number))
    time.sleep(page_wait)
    header = driver.copy_screen()
    pages = [header]
    log.info("AS400 header captured: %d chars", len(header))

    # Guard: if this isn't an order view we're on the wrong screen (a menu, etc.)
    # or the order doesn't exist. Bail out now instead of pressing F5 and looping
    # through pages that will never show END OF ORDER.
    if not _looks_like_order_screen(header):
        raise CaptureError(
            f"La pantalla no es una consulta de orden (vista incorrecta o la orden "
            f"{order_number} no existe). Ve a búsqueda de orden (F7 → 3) e intenta de nuevo."
        )

    # Switch to the items view (once).
    driver.key("f5")

    for i in range(1, max_pages + 1):
        time.sleep(page_wait)
        page = driver.copy_screen()
        pages.append(page)
        found = _has_end_marker(page)
        log.info(
            "AS400 items page %d: %d chars, end_marker=%s | tail=%r",
            i,
            len(page),
            found,
            page[-60:].replace("\n", "\\n"),
        )
        if found:
            log.info("END OF ORDER found on page %d — stopping.", i)
            return "\n".join(pages)
        driver.key("enter")

    raise CaptureError(
        f"'{END_OF_ORDER_MARKER}' no apareció tras {max_pages} páginas para la orden "
        f"{order_number}. Captura abortada."
    )
