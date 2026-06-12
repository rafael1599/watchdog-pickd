"""
A manual /api/capture drives Mocha, which steals macOS focus from the
operator's browser. The endpoint snapshots the frontmost app before driving
Mocha and hands the focus back when the capture finishes — success or failure.
The cached path never touches Mocha, so it never touches focus either.
Also locks the sticky topbar markup of the watcher UI.
"""

import os
import sys
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import app as appmod  # noqa: E402
import scanned_store  # noqa: E402
from as400_capture import CaptureError  # noqa: E402

HDR = {"Host": "localhost:5000"}

CAPTURE = """                            O R D E R   I N Q U I R Y
 Order Number: 880092                       Account Number: 0000991 00
 Quant  Quant  Stock #   W/H   Description                       Unit    Extend
   1     1  03 3927 BK  N   CODA S2 21 2025 GLOSS BLACK     428.95    428.95
                                END OF ORDER                             428.95"""


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(appmod, "ARCHIVE_PATH", tmp_path / "arch.json")
    monkeypatch.setenv("SCANNED_STORE_PATH", str(tmp_path / "scanned.json"))
    monkeypatch.setenv("SCAN_CURSOR_PATH", str(tmp_path / "cursor"))
    appmod._archive.clear()
    appmod._orders.clear()
    appmod._next_id = 1
    appmod.app.testing = True
    return appmod.app.test_client()


def test_focus_returns_to_previous_app_after_capture(client):
    restored = []
    with (
        patch("app.frontmost_app_name", return_value="Google Chrome"),
        patch("app.activate_app", side_effect=restored.append),
        patch("app.capture_order", return_value=CAPTURE),
    ):
        r = client.post("/api/capture", json={"order_number": "880092"}, headers=HDR)

    assert r.status_code == 200
    assert restored == ["Google Chrome"]


def test_focus_restored_even_when_capture_fails(client):
    restored = []
    with (
        patch("app.frontmost_app_name", return_value="Safari"),
        patch("app.activate_app", side_effect=restored.append),
        patch("app.capture_order", side_effect=CaptureError("stalled")),
    ):
        r = client.post("/api/capture", json={"order_number": "880093"}, headers=HDR)

    assert r.status_code == 422
    assert restored == ["Safari"]


def test_focus_failure_never_breaks_the_response(client):
    with (
        patch("app.frontmost_app_name", return_value="Arc"),
        patch("app.activate_app", side_effect=RuntimeError("osascript died")),
        patch("app.capture_order", return_value=CAPTURE),
    ):
        r = client.post("/api/capture", json={"order_number": "880092"}, headers=HDR)

    assert r.status_code == 200  # best-effort: capture result still delivered


def test_cached_capture_never_touches_focus(client):
    scanned_store.put("880092", CAPTURE, {"order_number": "880092"}, source="auto_scan")
    with (
        patch("app.frontmost_app_name", side_effect=AssertionError("must not query focus")),
        patch("app.activate_app", side_effect=AssertionError("must not activate")),
    ):
        r = client.post("/api/capture", json={"order_number": "880092"}, headers=HDR)

    assert r.status_code == 200


def test_topbar_is_sticky_and_wraps_the_capture_controls():
    html = appmod.INDEX_HTML
    assert 'id="topbar"' in html
    assert "position: sticky" in html
    topbar_start = html.index('id="topbar"')
    capture_input = html.index('id="num"')
    overlay = html.index('id="vboard-overlay"')
    assert topbar_start < capture_input < overlay  # input inside, overlay outside
