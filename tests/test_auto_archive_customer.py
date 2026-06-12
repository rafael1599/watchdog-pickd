"""
Orders billed to parts-only customers (AUTO_ARCHIVE_CUSTOMERS, e.g. EBAY PART
SALES) are archived on arrival: they never appear in the pending queue, their
scanned-cache copy is purged (so the auto-scan sweep can't re-archive a
duplicate), and an explicit Restore pulls them back without re-archiving.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import app as appmod  # noqa: E402
import scanned_store  # noqa: E402

HDR = {"Host": "localhost:5000"}

EBAY_ORDER = """                            O R D E R   I N Q U I R Y
 Order Number: 880100                       Account Number: 0099999 00
 Bill EBAY PART SALES                  Ship SOME EBAY BUYER
 Quant  Quant  Stock #   W/H   Description                       Unit    Extend
   1     1  03 3768 BL  N   DIVIDE S/O 12X27 2025 RIPTIDE   394.95    394.95
                                END OF ORDER                             394.95"""

REGULAR_ORDER = """                            O R D E R   I N Q U I R Y
 Order Number: 880101                       Account Number: 0003574 00
 Bill MATTHEWS BICYCLE MART, INC       Ship MATTHEWS BICYCLE MART
 Quant  Quant  Stock #   W/H   Description                       Unit    Extend
   1     1  03 3768 BL  N   DIVIDE S/O 12X27 2025 RIPTIDE   394.95    394.95
                                END OF ORDER                             394.95"""


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


def test_ebay_order_is_archived_on_arrival(client):
    entry = appmod._add_order(EBAY_ORDER)

    assert entry["auto_archived"] is True
    assert entry["id"] not in appmod._orders  # never a pending card
    assert len(appmod._archive) == 1
    arch = next(iter(appmod._archive.values()))
    assert arch["order_number"] == "880100"
    assert arch["customer"] == "EBAY PART SALES"


def test_regular_customer_still_goes_to_pending(client):
    entry = appmod._add_order(REGULAR_ORDER)

    assert not entry.get("auto_archived")
    assert entry["id"] in appmod._orders
    assert len(appmod._archive) == 0


def test_customer_match_is_case_spacing_and_substring_tolerant(client):
    assert appmod._is_auto_archive_customer("EBAY PART SALES")
    assert appmod._is_auto_archive_customer("ebay  Part   Sales")
    assert appmod._is_auto_archive_customer("EBAY PART SALES #2")
    assert not appmod._is_auto_archive_customer("MATTHEWS BICYCLE MART, INC")
    assert not appmod._is_auto_archive_customer(None)
    assert not appmod._is_auto_archive_customer("")


def test_ebay_order_purges_its_scanned_cache_copy(client):
    scanned_store.put("880100", EBAY_ORDER, {"order_number": "880100"}, source="auto_scan")
    assert scanned_store.get("880100") is not None

    appmod._add_order(EBAY_ORDER)

    assert scanned_store.get("880100") is None  # sweep can't re-archive it


def test_auto_scan_sweep_archives_ebay_once_without_duplicates(client):
    scanned_store.put("880100", EBAY_ORDER, {"order_number": "880100"}, source="auto_scan")

    r = client.get("/api/orders", headers=HDR)
    assert r.status_code == 200
    assert all(o["order_number"] != "880100" for o in r.get_json())  # not pending
    assert len(appmod._archive) == 1

    # Second refresh: cache copy is gone, so no duplicate archive entry.
    client.get("/api/orders", headers=HDR)
    assert len(appmod._archive) == 1


def test_restore_wins_over_the_auto_archive_rule(client):
    appmod._add_order(EBAY_ORDER)
    aid = next(iter(appmod._archive))

    r = client.post(f"/api/archived/{aid}/restore", headers=HDR)
    assert r.status_code == 200
    data = r.get_json()
    assert data["order_number"] == "880100"
    assert not data.get("auto_archived")
    assert len(appmod._archive) == 0
    assert any(o["order_number"] == "880100" for o in appmod._orders.values())
