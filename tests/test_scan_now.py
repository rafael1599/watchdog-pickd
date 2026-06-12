"""
"Get orders now" (operator's manual kick): trigger_scan_now wakes the scanner
loop immediately and bypasses the operator-activity gate for one pass. The
/api/scan-now endpoint reports honestly when the scanner isn't running.
"""

import os
import sys
import threading
import time
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import app as appmod  # noqa: E402
import auto_scanner  # noqa: E402

HDR = {"Host": "localhost:5000"}


@pytest.fixture(autouse=True)
def clean_kick():
    auto_scanner._kick.clear()
    yield
    auto_scanner._kick.clear()


class _AliveThread:
    @staticmethod
    def is_alive() -> bool:
        return True


def test_trigger_returns_false_when_scanner_not_running(monkeypatch):
    monkeypatch.setattr(auto_scanner, "_thread", None)
    assert auto_scanner.trigger_scan_now() is False
    assert not auto_scanner._kick.is_set()


def test_trigger_sets_the_kick_when_scanner_runs(monkeypatch):
    monkeypatch.setattr(auto_scanner, "_thread", _AliveThread())
    assert auto_scanner.trigger_scan_now() is True
    assert auto_scanner._kick.is_set()


def test_interruptible_wait_returns_early_on_kick():
    start = time.monotonic()
    waiter = threading.Thread(target=lambda: auto_scanner._interruptible_wait(10))
    waiter.start()
    auto_scanner._kick.set()
    waiter.join(timeout=3)
    assert not waiter.is_alive()  # woke up, did not sleep the full 10s
    assert time.monotonic() - start < 3


def test_scan_now_endpoint_ok():
    appmod.app.testing = True
    client = appmod.app.test_client()
    with patch("auto_scanner.trigger_scan_now", return_value=True):
        r = client.post("/api/scan-now", headers=HDR)
    assert r.status_code == 200
    assert "kicked" in r.get_json()["message"].lower()


def test_scan_now_endpoint_409_when_scanner_off():
    appmod.app.testing = True
    client = appmod.app.test_client()
    with patch("auto_scanner.trigger_scan_now", return_value=False):
        r = client.post("/api/scan-now", headers=HDR)
    assert r.status_code == 409
    assert "not running" in r.get_json()["error"]


def test_menu_has_the_get_orders_now_button():
    assert "Get orders now" in appmod.INDEX_HTML
    assert "doScanNow" in appmod.INDEX_HTML
