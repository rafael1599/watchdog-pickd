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


def _has_end_marker(text: str) -> bool:
    """Whitespace-insensitive match for the END OF ORDER marker.

    The 5250 screen capture can include extra spacing between characters, so we
    strip all whitespace before checking (e.g. 'E N D  O F  O R D E R' still matches).
    """
    return "ENDOFORDER" in re.sub(r"\s+", "", text.upper())


def _looks_like_order_screen(text: str) -> bool:
    """True if the captured text looks like an ORDER INQUIRY screen.

    Used to bail out (instead of looping) when we're on a different AS400 view
    (e.g. a menu) or the order doesn't exist. Whitespace-insensitive so the
    spaced-out 'O R D E R  I N Q U I R Y' title still matches.
    """
    norm = re.sub(r"\s+", "", text.upper())
    return "ORDERINQUIRY" in norm or "ORDERNUMBER" in norm


class CaptureError(Exception):
    """Raised when a capture cannot complete (e.g. END OF ORDER never appears)."""


class MochaDriver:
    """
    macOS keystroke/clipboard driver for Mocha TN5250 using AppleScript (osascript)
    and pbpaste. Only functional on macOS with the emulator open and logged in.
    """

    def __init__(self, app_name: str = None, launch_target: str = None):
        # Resolve at instantiation (request time) so a .env loaded after import
        # is still honored.
        self.app_name = app_name or os.getenv("MOCHA_APP_NAME", "Mocha TN5250")
        self.launch_target = (
            launch_target or os.getenv("AS400_LAUNCH_TARGET") or self.app_name
        )
        # If the launch target is a bundle id, use it to activate the app reliably
        # (sandboxed apps can't be resolved by name via `tell application "<name>"`).
        bundle_re = r"^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)+$"
        self.bundle_id = (
            self.launch_target if re.match(bundle_re, self.launch_target) else None
        )

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


def bootstrap_session(
    driver,
    launch_wait: float = LAUNCH_WAIT,
    login_steps=DEFAULT_LOGIN_STEPS,
    step_wait: float = 0.6,
):
    """Open the emulator and log in, leaving the session at the order-search screen.

    Sequence: launch → wait for the app → focus → run the login macro.
    """
    driver.launch()
    time.sleep(launch_wait)
    driver.focus()
    run_login(driver, login_steps=login_steps, step_wait=step_wait)


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

    # F6 opens a fresh order search before each new order.
    driver.key("f6")
    time.sleep(page_wait)

    # Page 1: customer header. Typing the last digit surfaces it automatically.
    driver.type_text(str(order_number))
    time.sleep(page_wait)
    header = driver.copy_screen()
    pages = [header]
    log.info("AS400 header captured: %d chars", len(header))

    # Guard: if this isn't an ORDER INQUIRY screen we're on the wrong view (a menu,
    # etc.) or the order doesn't exist. Bail out now instead of pressing F5 and
    # looping through pages that will never show END OF ORDER.
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
