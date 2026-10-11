"""Tests for AS400 auto-restart policies, backoff series, and operator guards.

Rafael, 11 oct 2026.
"""

import time
from unittest.mock import MagicMock

import pytest

import as400_capture
import auto_scanner
import operator_sentinel
import scanned_store
from as400_capture import (
    AS400SessionClosed,
    auto_restart_enabled,
    hard_restart,
    is_restart_backing_off,
    keep_restart_screen,
    operator_allows_restart,
    record_restart_failure,
    record_restart_success,
    restart_backoff_sec,
    restart_cooldown_remaining,
)


@pytest.fixture(autouse=True)
def _reset_state(tmp_path, monkeypatch):
    monkeypatch.setenv("SCANNED_STORE_PATH", str(tmp_path / "scanned.json"))
    monkeypatch.setenv("SCAN_CURSOR_PATH", str(tmp_path / "scan_cursor"))
    as400_capture._reset_restart_backoff_for_tests()
    operator_sentinel._reset_for_tests()
    monkeypatch.setattr(auto_scanner, "_last_operator_input", None)
    monkeypatch.setattr(as400_capture, "_system_idle_seconds", lambda: 120.0)
    monkeypatch.setattr(as400_capture, "_last_hard_restart", 0.0)
    monkeypatch.delenv("AS400_AUTO_RESTART", raising=False)
    monkeypatch.delenv("AS400_RESTART_IDLE_SEC", raising=False)
    monkeypatch.delenv("AS400_HARD_RESTART_IDLE_SEC", raising=False)
    yield
    operator_sentinel._reset_for_tests()
    as400_capture._reset_restart_backoff_for_tests()


def _preview(text):
    parts = text.split()
    num = parts[1] if len(parts) > 1 else "880112"
    return {
        "order_number": num,
        "customer": "C",
        "item_count": 1,
        "total_units": 1,
        "subtotal": None,
        "parsed_total": 0.0,
        "total_mismatch": False,
    }


class FakeDriverWithRestart:
    def new_window(self):
        pass


# ── Backoff Series ────────────────────────────────────────────────────────────


def test_restart_backoff_series():
    # 0 fallos → sin espera
    assert restart_backoff_sec(0) == 0.0
    assert restart_backoff_sec(-1) == 0.0

    # 1 → 4 min (240s)
    assert restart_backoff_sec(1) == 4 * 60.0
    # 2 → 8 min (480s)
    assert restart_backoff_sec(2) == 8 * 60.0
    # 3 → 16 min (960s)
    assert restart_backoff_sec(3) == 16 * 60.0
    # 4 → 32 min (1920s)
    assert restart_backoff_sec(4) == 32 * 60.0
    # 5 → 64 min (3840s, tope)
    assert restart_backoff_sec(5) == 64 * 60.0
    # 9 → 64 min (3840s, sigue en tope)
    assert restart_backoff_sec(9) == 64 * 60.0


def test_restart_backoff_success_resets_to_4_min():
    # Record failures advancing to 16 min (3 failures)
    record_restart_failure(now=1000.0)  # 1 -> 4m
    record_restart_failure(now=1000.0)  # 2 -> 8m
    delay3 = record_restart_failure(now=1000.0)  # 3 -> 16m
    assert delay3 == 16 * 60.0

    # Un reinicio que sale bien vuelve la cuenta a 4
    record_restart_success()
    assert is_restart_backing_off(now=1000.0) is False

    # The next failure starts fresh at 4 minutes (240s)
    delay_after_success = record_restart_failure(now=1000.0)
    assert delay_after_success == 4 * 60.0


def test_step_skips_restart_during_backoff():
    # Put the system in backoff (1st failure = 240s backoff)
    now = time.time()
    record_restart_failure(now=now)
    assert is_restart_backing_off(now=now) is True
    assert restart_cooldown_remaining(now=now) > 0.0

    restarted = []

    def fake_restart(drv):
        restarted.append(True)

    def cap(n, drv):
        raise AS400SessionClosed("Session closed")

    r = auto_scanner.run_scan_step(
        FakeDriverWithRestart(),
        start=880112,
        capture_fn=cap,
        preview_fn=_preview,
        restart_fn=fake_restart,
    )
    # During backoff, no Cmd+N is attempted; step yields unavailable
    assert r == {"action": "unavailable", "number": "880112"}
    assert len(restarted) == 0


# ── Inactivity & Operator Sentinel Guard (60s) ────────────────────────────────


def test_inactivity_guard_thresholds():
    # 59 s → no reinicia
    assert operator_allows_restart(idle_fn=lambda: 59.0) is False

    # 60 s → sí reinicia
    assert operator_allows_restart(idle_fn=lambda: 60.0) is True

    # 120 s → sí reinicia
    assert operator_allows_restart(idle_fn=lambda: 120.0) is True


def test_inactivity_guard_sentinel_blocks():
    # Even if idle is large (1000s), if operator sentinel is active (hands off), no restart
    operator_sentinel.on_signal("MOUSE")
    try:
        assert operator_sentinel.hands_off() is True
        assert operator_allows_restart(idle_fn=lambda: 1000.0) is False
    finally:
        operator_sentinel._reset_for_tests()


def test_step_respects_inactivity_guard(monkeypatch):
    restarted = []

    def fake_restart(drv):
        restarted.append(True)

    def cap(n, drv):
        raise AS400SessionClosed("Session closed")

    # When someone was active 59s ago:
    monkeypatch.setattr(as400_capture, "_system_idle_seconds", lambda: 59.0)
    r = auto_scanner.run_scan_step(
        FakeDriverWithRestart(),
        start=880112,
        capture_fn=cap,
        preview_fn=_preview,
        restart_fn=fake_restart,
    )
    assert r == {"action": "unavailable", "number": "880112"}
    assert len(restarted) == 0

    # When untouched for 60s:
    monkeypatch.setattr(as400_capture, "_system_idle_seconds", lambda: 60.0)
    # capture succeeds on second try
    calls = []

    def cap_recover(n, drv):
        calls.append(n)
        if len(calls) == 1:
            raise AS400SessionClosed("Session closed")
        return f"ORDER {n} END OF ORDER"

    r2 = auto_scanner.run_scan_step(
        FakeDriverWithRestart(),
        start=880112,
        capture_fn=cap_recover,
        preview_fn=_preview,
        restart_fn=fake_restart,
    )
    assert r2 == {"action": "captured", "number": "880112"}
    assert len(restarted) == 1


# ── Flag AS400_AUTO_RESTART=0 ────────────────────────────────────────────────


def test_auto_restart_disabled_flag(monkeypatch):
    monkeypatch.setenv("AS400_AUTO_RESTART", "0")
    assert auto_restart_enabled() is False

    restarted = []

    def fake_restart(drv):
        restarted.append(True)

    def cap(n, drv):
        raise AS400SessionClosed("Session closed")

    r = auto_scanner.run_scan_step(
        FakeDriverWithRestart(),
        start=880112,
        capture_fn=cap,
        preview_fn=_preview,
        restart_fn=fake_restart,
    )
    assert r == {"action": "unavailable", "number": "880112"}
    assert len(restarted) == 0


def test_auto_restart_enabled_by_default(monkeypatch):
    monkeypatch.delenv("AS400_AUTO_RESTART", raising=False)
    assert auto_restart_enabled() is True


# ── Hard restart uses same 60s rule ──────────────────────────────────────────


def test_hard_restart_uses_unified_60s_rule(monkeypatch):
    monkeypatch.setenv("AS400_HARD_RESTART", "1")
    monkeypatch.setenv("AS400_NEW_WINDOW_WAIT", "0")
    monkeypatch.setenv("AS400_CLOSE_WINDOW_WAIT", "0")

    class FakeEmulator:
        def __init__(self):
            self.events = []

        def new_window(self):
            self.events.append("cmd+n")

        def previous_window(self):
            self.events.append("ctrl+shift+tab")

        def copy_screen(self):
            return "ADDITIONAL MESSAGE INFORMATION\n BAS-1234 No matching key\n Option:"

        def close_window(self):
            self.events.append("cmd+w+enter")

    d = FakeEmulator()
    # 59s → no reinicia
    assert hard_restart(d, idle_fn=lambda: 59.0) is False
    assert d.events == []

    # 60s → sí reinicia
    assert hard_restart(d, idle_fn=lambda: 60.0) is True
    assert "cmd+n" in d.events

    # Sentinel activo → no reinicia
    monkeypatch.setattr(as400_capture, "_last_hard_restart", 0.0)
    operator_sentinel.on_signal("MOUSE")
    try:
        assert hard_restart(d, idle_fn=lambda: 100.0) is False
    finally:
        operator_sentinel._reset_for_tests()


# ── Registration in as400_screens ─────────────────────────────────────────────


def test_keep_restart_screen_records_row():
    recorded = []

    class FakeClient:
        def table(self, name):
            assert name == "as400_screens"
            return self

        def insert(self, row):
            recorded.append(row)
            return self

        def execute(self):
            return MagicMock(data=[])

    ok = keep_restart_screen(
        label="880112",
        after="ok",
        raw="DEAD SCREEN TEXT",
        client=FakeClient(),
    )
    assert ok is True
    assert len(recorded) == 1
    row = recorded[0]
    assert row["sku"] == "880112"
    assert row["classified"] == "session_restart"
    assert row["after"] == "ok"
    assert row["raw"] == "DEAD SCREEN TEXT"


def test_keep_restart_screen_swallows_db_errors():
    class FailingClient:
        def table(self, name):
            raise RuntimeError("Database connection timed out")

    # A failure to write to the database must never raise or abort
    ok = keep_restart_screen(
        label="880112",
        after="fail: error",
        raw="SCREEN",
        client=FailingClient(),
    )
    assert ok is False


def test_step_continues_if_screen_registration_fails(monkeypatch):
    # Mock keep_restart_screen to fail (e.g. database error)
    monkeypatch.setattr(as400_capture, "keep_restart_screen", lambda *a, **kw: False)

    restarted = []

    def fake_restart(drv):
        restarted.append(True)

    calls = []

    def cap(n, drv):
        calls.append(n)
        if len(calls) == 1:
            raise AS400SessionClosed("Session died")
        return f"ORDER {n} END OF ORDER"

    r = auto_scanner.run_scan_step(
        FakeDriverWithRestart(),
        start=880112,
        capture_fn=cap,
        preview_fn=_preview,
        restart_fn=fake_restart,
    )
    assert r == {"action": "captured", "number": "880112"}
    assert len(restarted) == 1
    assert scanned_store.get("880112") is not None
