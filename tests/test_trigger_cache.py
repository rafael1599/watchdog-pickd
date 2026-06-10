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
    monkeypatch.setenv("SCAN_CURSOR_PATH", str(tmp_path / "scan_cursor"))


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
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(appmod, "ARCHIVE_PATH", tmp_path / "archived.json")
    appmod._orders.clear()
    appmod._archive.clear()
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


# --- unified order list (auto-scanned + manual, deduped, full sendable cards) ---

CAPTURE_112 = CAPTURE_TEXT.replace("880009", "880112")


def test_list_orders_surfaces_scanned_as_full_cards(client):
    scanned_store.put("880112", CAPTURE_112, {"order_number": "880112"}, source="auto_scan")
    r = client.get("/api/orders", headers=HDR)
    assert r.status_code == 200
    data = r.get_json()
    o = next((o for o in data if o["order_number"] == "880112"), None)
    assert o is not None
    assert o["item_count"] >= 1  # full card fields present (not just a stub)
    assert o["from_cache"] is True
    assert "raw_text" not in o


def test_list_orders_dedupes_manual_and_scanned(client):
    # Same order present both in _orders (manual) and the cache → shows once.
    appmod._add_order(CAPTURE_TEXT)  # 880009 → _orders
    scanned_store.put("880009", CAPTURE_TEXT, {"order_number": "880009"})
    nums = [o["order_number"] for o in client.get("/api/orders", headers=HDR).get_json()]
    assert nums.count("880009") == 1


# Real AS400 screen for a number that isn't an order — parses to no order/items.
INVALID_SCREEN = """                            O R D E R   I N Q U I R Y
 Order Number:                              Account Number:
 Invalid Order Number, REENTER              BEN BUSCHBACHER
 ORDER# CUSTOMER P/O ORD DATE   Inv#     Date      Total   Ship Source   CRH"""


def test_junk_cache_entry_is_purged_not_surfaced(client):
    # Reported bug: junk captures (invalid-order screens) in the cache rendered as
    # endless duplicated 'Order #—' cards — the dedup by order_number never matched
    # because the parsed number is None. They must be purged, not shown.
    scanned_store.put("880119", INVALID_SCREEN, {"order_number": "880119"}, source="auto_scan")
    data = client.get("/api/orders", headers=HDR).get_json()
    assert all(o.get("order_number") for o in data)  # no 'Order #—' cards
    assert scanned_store.get("880119") is None  # junk purged from the cache


def test_junk_does_not_duplicate_across_refreshes(client):
    scanned_store.put("880119", INVALID_SCREEN, {"order_number": "880119"}, source="auto_scan")
    for _ in range(3):  # simulate the UI's periodic refresh
        data = client.get("/api/orders", headers=HDR).get_json()
    assert data == []  # nothing surfaced, nothing accumulated


def test_manual_capture_of_junk_is_not_cached(client, monkeypatch):
    monkeypatch.setattr(appmod, "capture_order", lambda num, drv: INVALID_SCREEN)
    monkeypatch.setattr(appmod, "MochaDriver", lambda *a, **k: object())
    client.post("/api/capture", json={"order_number": "999999"}, headers=HDR)
    assert scanned_store.get("999999") is None  # junk never re-enters the cache


def test_send_drops_order_from_cache(client, monkeypatch):
    scanned_store.put("880009", CAPTURE_TEXT, {"order_number": "880009"})
    client.get("/api/orders", headers=HDR)  # materialize into _orders
    oid = next(iter(appmod._orders))
    monkeypatch.setattr(
        appmod,
        "process_order_text",
        lambda *a, **k: {
            "status": "created",
            "order_number": "880009",
            "customer": "X",
            "item_count": 1,
            "needs_correction": False,
        },
    )
    r = client.post(f"/api/orders/{oid}/send", headers=HDR)
    assert r.status_code == 200
    assert scanned_store.get("880009") is None  # won't re-sync after a restart


def test_archive_by_id_removes_from_cache_keeps_cursor(client):
    scanned_store.put("880112", CAPTURE_112, {"order_number": "880112"}, source="auto_scan")
    cursor_before = scanned_store.next_scan_number(880112)
    client.get("/api/orders", headers=HDR)  # materialize
    oid = next(iter(appmod._orders))
    r = client.post(f"/api/orders/{oid}/archive", headers=HDR)
    assert r.status_code == 200
    assert scanned_store.get("880112") is None
    assert len(appmod._archive) == 1
    assert scanned_store.next_scan_number(880112) == cursor_before  # no rewind


def test_remove_by_id_removes_from_cache(client):
    scanned_store.put("880112", CAPTURE_112, {"order_number": "880112"})
    client.get("/api/orders", headers=HDR)
    oid = next(iter(appmod._orders))
    r = client.delete(f"/api/orders/{oid}", headers=HDR)
    assert r.status_code == 200
    assert scanned_store.get("880112") is None  # removal sticks (no re-sync)
