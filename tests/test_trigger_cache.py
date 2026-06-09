"""
Tests for the trigger flows reusing the scanned cache:
  - watcher.process_pdf prefers a cached AS400 capture over the dropped PDF.
  - app's /api/capture reuses the cache instead of driving Mocha.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import app as appmod  # noqa: E402
import scanned_store  # noqa: E402
import watcher  # noqa: E402

HDR = {"Host": "localhost:5000"}
PDF_TEXT = " Order Number: 880009 \n some pdf body"


@pytest.fixture(autouse=True)
def tmp_store(tmp_path, monkeypatch):
    monkeypatch.setenv("SCANNED_STORE_PATH", str(tmp_path / "scanned.json"))


# --- watcher PDF routing -----------------------------------------------------


def _fake_result(**kw):
    base = {
        "status": "created",
        "order_number": "880009",
        "customer": "X",
        "item_count": 1,
        "needs_correction": False,
    }
    base.update(kw)
    return base


def test_process_pdf_prefers_cached_capture(monkeypatch):
    scanned_store.put("880009", "AS400 RAW CAPTURE", {"order_number": "880009"})
    monkeypatch.setattr(watcher, "extract_text", lambda p: PDF_TEXT)
    monkeypatch.setattr(watcher, "move_file", lambda *a, **k: "moved")
    seen = {}

    def fake_process(text, source_name=None):
        seen["text"] = text
        seen["source"] = source_name
        return _fake_result()

    monkeypatch.setattr(watcher, "process_order_text", fake_process)
    watcher.process_pdf("/tmp/fake.pdf")
    assert seen["text"] == "AS400 RAW CAPTURE"  # used the cache, not the PDF text
    assert seen["source"] == "scanned:880009"


def test_process_pdf_falls_back_to_pdf_when_not_cached(monkeypatch):
    monkeypatch.setattr(watcher, "extract_text", lambda p: PDF_TEXT)
    monkeypatch.setattr(watcher, "move_file", lambda *a, **k: "moved")
    seen = {}

    def fake_process(text, source_name=None):
        seen["text"] = text
        return _fake_result()

    monkeypatch.setattr(watcher, "process_order_text", fake_process)
    watcher.process_pdf("/tmp/fake.pdf")
    assert seen["text"] == PDF_TEXT  # nothing cached → PDF text used


# --- app /api/capture cache reuse --------------------------------------------


@pytest.fixture
def client():
    appmod._orders.clear()
    appmod._next_id = 1
    appmod.app.testing = True
    return appmod.app.test_client()


CAPTURE_TEXT = """ Order Number: 880009
 Quant  Quant  Stock #   W/H   Description                       Unit    Extend
   1     1  03 3768 BL  N   DIVIDE S/O 12X27 2025 RIPTIDE   394.95    394.95
                                END OF ORDER                             394.95"""


def test_capture_reuses_cache_without_driving_mocha(client, monkeypatch):
    scanned_store.put("880009", CAPTURE_TEXT, {"order_number": "880009"})

    def boom(*a, **k):
        raise AssertionError("should not drive Mocha when the order is cached")

    monkeypatch.setattr(appmod, "capture_order", boom)
    r = client.post("/api/capture", json={"order_number": "880009"}, headers=HDR)
    assert r.status_code == 200
    data = r.get_json()
    assert data["from_cache"] is True
    assert data["order_number"] == "880009"


def test_capture_drives_and_caches_when_not_cached(client, monkeypatch):
    monkeypatch.setattr(appmod, "capture_order", lambda num, drv: CAPTURE_TEXT)
    monkeypatch.setattr(appmod, "MochaDriver", lambda *a, **k: object())
    r = client.post("/api/capture", json={"order_number": "880009"}, headers=HDR)
    assert r.status_code == 200
    assert not r.get_json().get("from_cache")
    # The manual capture is now recorded in the cache for next time.
    assert scanned_store.get("880009") is not None
