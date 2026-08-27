"""
Tests for the local "archive" feature in app.py: archiving a pending order to a
local JSON file (persisted across restarts), listing/restoring it, and the
re-capture warning that compares a fresh scan against the archived copy.

These exercise the Flask endpoints + pure helpers; preview_order is parse-only,
so no Supabase access is needed.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import app as appmod  # noqa: E402

HDR = {"Host": f"localhost:{appmod.PORT}"}  # satisfy app's loopback-only guard

ORDER_TEXT = """                            O R D E R   I N Q U I R Y
 Order Number: 880009                       Account Number: 0003574 00
 Quant  Quant  Stock #   W/H   Description                       Unit    Extend
   1     1  03 3768 BL  N   DIVIDE S/O 12X27 2025 RIPTIDE   394.95    394.95
                                END OF ORDER                             394.95"""


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(appmod, "ARCHIVE_PATH", tmp_path / "arch.json")
    appmod._archive.clear()
    appmod._orders.clear()
    appmod._next_id = 1
    appmod.app.testing = True
    return appmod.app.test_client()


# --- _compare_items ----------------------------------------------------------


def test_compare_items_identical():
    r = appmod._compare_items([{"sku": "X", "qty": 1}], [{"sku": "X", "qty": 1}])
    assert r["identical"] is True
    assert r["summary"] == "identical"


def test_compare_items_added_sku():
    r = appmod._compare_items(
        [{"sku": "X", "qty": 1}], [{"sku": "X", "qty": 1}, {"sku": "Y", "qty": 2}]
    )
    assert r["identical"] is False
    assert "new SKU" in r["summary"]


def test_compare_items_changed_qty():
    r = appmod._compare_items([{"sku": "X", "qty": 1}], [{"sku": "X", "qty": 3}])
    assert r["identical"] is False
    assert "quantity" in r["summary"]


# --- archive / list / restore -----------------------------------------------


def test_archive_and_restore_roundtrip(client):
    entry = appmod._add_order(ORDER_TEXT)
    oid = entry["id"]

    r = client.post(f"/api/orders/{oid}/archive", headers=HDR)
    assert r.status_code == 200
    assert oid not in appmod._orders  # removed from pending

    r = client.get("/api/archived", headers=HDR)
    arch = r.get_json()
    assert len(arch) == 1
    assert arch[0]["order_number"] == "880009"
    assert "raw_text" not in arch[0]  # raw_text is stripped from the payload
    assert appmod.ARCHIVE_PATH.exists()  # persisted to disk

    aid = arch[0]["aid"]
    r = client.post(f"/api/archived/{aid}/restore", headers=HDR)
    assert r.status_code == 200
    assert len(appmod._archive) == 0
    assert any(o["order_number"] == "880009" for o in appmod._orders.values())


def test_archive_survives_restart(client):
    entry = appmod._add_order(ORDER_TEXT)
    client.post(f"/api/orders/{entry['id']}/archive", headers=HDR)

    # Simulate an app restart: drop in-memory state, reload from disk.
    appmod._archive.clear()
    appmod._load_archive()
    assert len(appmod._archive) == 1
    assert next(iter(appmod._archive.values()))["order_number"] == "880009"


def test_archive_missing_order_is_404(client):
    r = client.post("/api/orders/999/archive", headers=HDR)
    assert r.status_code == 404


def test_restore_missing_archive_is_404(client):
    r = client.post("/api/archived/deadbeef/restore", headers=HDR)
    assert r.status_code == 404


# --- re-capture warning ------------------------------------------------------


def test_recapture_of_archived_flags_identical_match(client):
    entry = appmod._add_order(ORDER_TEXT)
    client.post(f"/api/orders/{entry['id']}/archive", headers=HDR)

    # Re-capturing the same order number must surface the archived copy.
    again = appmod._add_order(ORDER_TEXT)
    assert again["archived_match"] is not None
    assert again["archived_match"]["identical"] is True


def test_recapture_with_different_content_flags_diff(client):
    entry = appmod._add_order(ORDER_TEXT)
    client.post(f"/api/orders/{entry['id']}/archive", headers=HDR)

    # Same order number, an extra line → the match must report a difference.
    changed = ORDER_TEXT.replace(
        "                                END OF ORDER                             394.95",
        "   1     1  03 3769 BL  N   DIVIDE S/O 14X27 2025 RIPTIDE   394.95    394.95\n"
        "                                END OF ORDER                             789.90",
    )
    again = appmod._add_order(changed)
    assert again["archived_match"] is not None
    assert again["archived_match"]["identical"] is False


def test_no_archived_match_when_number_differs(client):
    entry = appmod._add_order(ORDER_TEXT)
    client.post(f"/api/orders/{entry['id']}/archive", headers=HDR)

    other = appmod._add_order(ORDER_TEXT.replace("880009", "880010"))
    assert other["archived_match"] is None
