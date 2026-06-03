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
import subprocess
import time

log = logging.getLogger("pickd-as400")

END_OF_ORDER_MARKER = "END OF ORDER"
MOCHA_APP_NAME = os.getenv("MOCHA_APP_NAME", "Mocha TN5250")


class CaptureError(Exception):
    """Raised when a capture cannot complete (e.g. END OF ORDER never appears)."""


class MochaDriver:
    """
    macOS keystroke/clipboard driver for Mocha TN5250 using AppleScript (osascript)
    and pbpaste. Only functional on macOS with the emulator open and logged in.
    """

    def __init__(self, app_name: str = MOCHA_APP_NAME):
        self.app_name = app_name

    def _osascript(self, script: str):
        subprocess.run(["osascript", "-e", script], check=True)

    def focus(self):
        self._osascript(f'tell application "{self.app_name}" to activate')
        time.sleep(0.3)

    def type_text(self, text: str):
        self._osascript(f'tell application "System Events" to keystroke "{text}"')

    def key(self, name: str):
        """Press a special key: 'enter' or 'f5'."""
        key_codes = {"enter": 36, "f5": 96, "return": 36}
        code = key_codes.get(name.lower())
        if code is None:
            raise ValueError(f"Unknown key: {name}")
        self._osascript(f'tell application "System Events" to key code {code}')

    def copy_screen(self) -> str:
        """Select all (Cmd+A), copy (Cmd+C), then read the clipboard via pbpaste."""
        self._osascript('tell application "System Events" to keystroke "a" using command down')
        time.sleep(0.1)
        self._osascript('tell application "System Events" to keystroke "c" using command down')
        time.sleep(0.1)
        result = subprocess.run(["pbpaste"], capture_output=True, text=True, check=True)
        return result.stdout


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

    # Page 1: customer header. Typing the last digit surfaces it automatically.
    driver.type_text(str(order_number))
    time.sleep(page_wait)
    pages = [driver.copy_screen()]

    # Switch to the items view (once).
    driver.key("f5")

    for _ in range(max_pages):
        time.sleep(page_wait)
        page = driver.copy_screen()
        pages.append(page)
        if END_OF_ORDER_MARKER in page.upper():
            return "\n".join(pages)
        driver.key("enter")

    raise CaptureError(
        f"'{END_OF_ORDER_MARKER}' no apareció tras {max_pages} páginas para la orden "
        f"{order_number}. Captura abortada."
    )
