"""Una orden que el AS400 no abre se aparta y el cursor sigue (#882082, 28 oct 2026)."""

from datetime import datetime, timedelta, timezone

import pytest

import as400_capture
import auto_scanner
import operator_sentinel
import scanned_store
from as400_capture import AS400SessionClosed, OrderNotFound

EMPTY_SEARCH = (
    "                            O R D E R   I N Q U I R Y\n\n"
    " Order Number:                              Account Number:\n\n"
    " Alpha Search:                                     Invoice:\n"
)


@pytest.fixture(autouse=True)
def _state(tmp_path, monkeypatch):
    monkeypatch.setenv("SCANNED_STORE_PATH", str(tmp_path / "scanned.json"))
    monkeypatch.setenv("SCAN_CURSOR_PATH", str(tmp_path / "scan_cursor"))
    monkeypatch.setenv("SCAN_PARKED_PATH", str(tmp_path / "parked.json"))
    as400_capture._reset_restart_backoff_for_tests()
    operator_sentinel._reset_for_tests()
    monkeypatch.setattr(auto_scanner, "_last_operator_input", None)
    monkeypatch.setattr(as400_capture, "_system_idle_seconds", lambda: 120.0)
    yield
    operator_sentinel._reset_for_tests()
    as400_capture._reset_restart_backoff_for_tests()


def _preview(text):
    return {"order_number": text.split()[1], "customer": "C", "item_count": 1, "total_units": 1}


class Driver:
    def new_window(self):
        pass


def _seed_last_captured():
    scanned_store.put("882081", "ORDER 882081", {"order_number": "882081", "item_count": 1})


def _step(cap, restarts):
    return auto_scanner.run_scan_step(
        Driver(),
        start=880112,
        capture_fn=cap,
        preview_fn=_preview,
        restart_fn=lambda d, dead_screen=None: restarts.append(1),
    )


def test_order_that_will_not_open_is_parked_without_restart():
    _seed_last_captured()
    asked = []

    def cap(n, drv):
        asked.append(n)
        if n == "882082":
            raise AS400SessionClosed("empty", screen=EMPTY_SEARCH)
        return f"ORDER {n}"

    restarts = []
    r = _step(cap, restarts)
    assert r == {"action": "parked", "number": "882082"}
    assert asked == ["882082", "882081"]  # la prueba con la última que sí abrió
    assert restarts == []
    assert scanned_store.next_scan_number(880112) == 882083
    assert "882082" in scanned_store.parked()

    # El paso siguiente va a la 882083, no a la aparcada.
    r2 = _step(cap, restarts)
    assert r2 == {"action": "captured", "number": "882083"}


def test_empty_search_on_a_dead_session_still_restarts():
    _seed_last_captured()

    def cap(n, drv):
        raise AS400SessionClosed("empty", screen=EMPTY_SEARCH)

    restarts = []
    r = _step(cap, restarts)
    assert r == {"action": "unavailable", "number": "882082"}
    assert restarts == [1]
    assert scanned_store.parked() == {}
    assert scanned_store.next_scan_number(880112) == 882082


def test_sign_on_screen_is_never_parked():
    _seed_last_captured()

    def cap(n, drv):
        raise AS400SessionClosed("sign on", screen="Sign On  User . . . .")

    restarts = []
    r = _step(cap, restarts)
    assert r["action"] == "unavailable"
    assert scanned_store.parked() == {}


def test_nothing_captured_yet_cannot_prove_the_session():
    def cap(n, drv):
        raise AS400SessionClosed("empty", screen=EMPTY_SEARCH)

    r = _step(cap, [])
    assert r["action"] == "unavailable"
    assert scanned_store.parked() == {}


def test_parked_order_is_revisited_and_captured_when_it_opens():
    _seed_last_captured()
    past = datetime.now(timezone.utc) - timedelta(minutes=20)
    scanned_store.park("882082", now=past)

    asked = []

    def cap(n, drv):
        asked.append(n)
        return f"ORDER {n}"

    r = _step(cap, [])
    assert r == {"action": "captured", "number": "882082"}
    assert asked == ["882082"]
    assert scanned_store.get("882082") is not None
    assert scanned_store.parked() == {}


def test_parked_order_still_closed_is_rescheduled_without_restart():
    _seed_last_captured()
    past = datetime.now(timezone.utc) - timedelta(minutes=20)
    scanned_store.park("882082", now=past)

    def cap(n, drv):
        if n == "882082":
            raise AS400SessionClosed("empty", screen=EMPTY_SEARCH)
        raise OrderNotFound("not yet")

    restarts = []
    r = _step(cap, restarts)
    assert r == {"action": "parked", "number": "882082"}
    assert restarts == []
    assert scanned_store.parked()["882082"]["tries"] == 2
    # Recién repasada: el paso siguiente vuelve a la secuencia.
    assert _step(cap, restarts) == {"action": "not_found", "number": "882083"}


def test_parked_orders_expire():
    old = datetime.now(timezone.utc) - timedelta(days=4)
    scanned_store.park("882082", now=old)
    assert scanned_store.due_parked(900, 3 * 86400) is None
    assert scanned_store.parked() == {}


def test_manual_capture_clears_the_parked_entry():
    scanned_store.park("882082")
    scanned_store.put("882082", "ORDER 882082", {"order_number": "882082"}, source="manual_capture")
    assert scanned_store.parked() == {}
