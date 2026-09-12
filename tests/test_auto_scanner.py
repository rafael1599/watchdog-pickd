"""Tests for the auto-scanner's single-step logic (run_scan_step) + helpers.

Pure logic only: a fake capture_fn decides which order numbers "exist" / how they
fail, and a stub preview_fn avoids the Supabase/pipeline import. No Mocha, no DB.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import auto_scanner  # noqa: E402
import scanned_store  # noqa: E402
from as400_capture import AS400Disconnected, CaptureError, OrderNotFound  # noqa: E402


@pytest.fixture(autouse=True)
def tmp_store(tmp_path, monkeypatch):
    monkeypatch.setenv("SCANNED_STORE_PATH", str(tmp_path / "scanned.json"))
    monkeypatch.setenv("SCAN_CURSOR_PATH", str(tmp_path / "scan_cursor"))


def _preview(text):
    return {
        "order_number": text.split()[1],
        "customer": "C",
        "item_count": 1,
        "total_units": 1,
        "subtotal": None,
        "parsed_total": 0.0,
        "total_mismatch": False,
    }


def test_step_captures_and_advances():
    def cap(n, driver):
        return f"ORDER {n} END OF ORDER"

    r = auto_scanner.run_scan_step(None, start=880112, capture_fn=cap, preview_fn=_preview)
    assert r == {"action": "captured", "number": "880112"}
    assert scanned_store.get("880112") is not None
    # Cursor advanced so the next step targets the following number.
    assert scanned_store.next_scan_number(880112) == 880113


def test_step_skips_an_already_cached_next_number():
    # Operator bug (2026-06): "Get orders now" re-pulled an order already in the
    # list. The scan position landed on a number the operator had MANUALLY captured
    # (a manual capture doesn't advance the cursor), and the step drove AS400 for it
    # again. The step must skip cached numbers and capture the first NEW one instead.
    scanned_store.put(
        880112,
        "ORDER 880112 END OF ORDER",
        {"order_number": "880112", "item_count": 1},
        source="manual_capture",
    )
    assert scanned_store.next_scan_number(880112) == 880112  # would re-pull it

    pulled = []

    def cap(n, driver):
        pulled.append(n)
        return f"ORDER {n} END OF ORDER"

    r = auto_scanner.run_scan_step(None, start=880112, capture_fn=cap, preview_fn=_preview)
    assert pulled == ["880113"]  # 880112 skipped — AS400 driven only for the new one
    assert r == {"action": "captured", "number": "880113"}
    assert scanned_store.get("880113") is not None


def test_step_backfills_gap_below_a_manual_capture_then_skips_it():
    # Gap-backfill contract preserved: a manual capture a few ahead must NOT make
    # the scanner skip the numbers in the gap below it — only the cached manual one
    # is skipped (not re-pulled) when the scanner reaches it.
    scanned_store.put(
        880115,
        "ORDER 880115 END OF ORDER",
        {"order_number": "880115", "item_count": 1},
        source="manual_capture",
    )
    pulled = []

    def cap(n, driver):
        pulled.append(n)
        return f"ORDER {n} END OF ORDER"

    for expected in ("880112", "880113", "880114"):  # the gap below the manual one
        r = auto_scanner.run_scan_step(None, start=880112, capture_fn=cap, preview_fn=_preview)
        assert r == {"action": "captured", "number": expected}

    # Next step lands on the cached manual 880115 → skipped → captures 880116.
    r = auto_scanner.run_scan_step(None, start=880112, capture_fn=cap, preview_fn=_preview)
    assert r == {"action": "captured", "number": "880116"}
    assert "880115" not in pulled  # the manual order is never re-pulled from AS400


def test_step_unparseable_capture_is_not_cached():
    # A capture that "succeeds" but parses to no order/items (e.g. an error screen
    # that slipped past the guards) must NOT be cached — junk in the cache floods
    # the UI with empty cards. Treated as not_found so the number is retried.
    def cap(n, driver):
        return "O R D E R   I N Q U I R Y\nInvalid Order Number, REENTER"

    def junk_preview(text):
        return {"order_number": None, "item_count": 0, "total_units": 0}

    r = auto_scanner.run_scan_step(None, start=880112, capture_fn=cap, preview_fn=junk_preview)
    assert r["action"] == "not_found"
    assert scanned_store.get("880112") is None
    assert scanned_store.next_scan_number(880112) == 880112


def test_step_not_found_does_not_advance():
    def cap(n, driver):
        raise OrderNotFound(f"order {n} doesn't exist yet")

    r = auto_scanner.run_scan_step(None, start=880112, capture_fn=cap, preview_fn=_preview)
    assert r["action"] == "not_found"
    assert scanned_store.get("880112") is None
    assert scanned_store.next_scan_number(880112) == 880112  # retried next time


def test_step_incomplete_does_not_advance():
    def cap(n, driver):
        raise CaptureError("screen didn't advance / no END OF ORDER")

    r = auto_scanner.run_scan_step(None, start=880112, capture_fn=cap, preview_fn=_preview)
    assert r["action"] == "incomplete"
    assert scanned_store.get("880112") is None
    assert scanned_store.next_scan_number(880112) == 880112  # same number retried


def test_step_unavailable_on_disconnect():
    def cap(n, driver):
        raise AS400Disconnected("host down")

    r = auto_scanner.run_scan_step(None, start=880112, capture_fn=cap, preview_fn=_preview)
    assert r["action"] == "unavailable"
    assert scanned_store.next_scan_number(880112) == 880112


def test_wait_per_action():
    assert auto_scanner._wait_for("captured") == auto_scanner.FOUND_NEXT_DELAY_SEC
    assert auto_scanner._wait_for("not_found") == auto_scanner.NOT_FOUND_WAIT_SEC
    assert auto_scanner._wait_for("incomplete") == auto_scanner.INCOMPLETE_RETRY_SEC
    assert auto_scanner._wait_for("unavailable") == auto_scanner.UNAVAILABLE_WAIT_SEC


def test_system_idle_seconds_parses_hididletime(monkeypatch):
    class _R:
        stdout = '  "HIDIdleTime" = 7500000000\n  "other" = 1'

    monkeypatch.setattr(auto_scanner.subprocess, "run", lambda *a, **k: _R())
    assert auto_scanner.system_idle_seconds() == pytest.approx(7.5, abs=0.01)


def test_system_idle_seconds_unknown_is_large(monkeypatch):
    def boom(*a, **k):
        raise FileNotFoundError("ioreg not found")

    monkeypatch.setattr(auto_scanner.subprocess, "run", boom)
    assert auto_scanner.system_idle_seconds() > 1e6  # treat as idle when unknown


# --- VOID / empty orders (complete screen, zero items) --------------------------

# Real capture of a voided order (operator-reported): valid ORDER INQUIRY screen,
# order number present, END OF ORDER reached, but ZERO line items.
VOID_SCREEN = """                            O R D E R   I N Q U I R Y

 Order Number: 880138                       Account Number: VOID

 Bill VOID VOID VOID VOID

 Quant  Quant  Stock #   W/H   Description                       Unit    Extend
   Ord   Ship                                                   Price

                                END OF ORDER                                .00
              Enter             Cmd6
              More Details       RETURN TO SELECT"""


def test_step_void_order_is_skipped_and_advances():
    # The killer bug: a VOID order parsed to 0 items was treated as not_found, so
    # the cursor never advanced and the scanner retried the SAME number forever.
    def cap(n, driver):
        return VOID_SCREEN

    def void_preview(text):
        return {"order_number": "880138", "item_count": 0, "total_units": 0, "is_last_page": True}

    r = auto_scanner.run_scan_step(None, start=880138, capture_fn=cap, preview_fn=void_preview)
    assert r == {"action": "empty_skipped", "number": "880138"}
    assert scanned_store.get("880138") is None  # never a candidate card
    assert scanned_store.next_scan_number(880138) == 880139  # moved past it


def test_step_void_order_with_real_parser():
    # End-to-end through the real preview_order: the 880138 screen must classify
    # as empty_skipped, not as a candidate and not as not_found.
    from pipeline import preview_order

    def cap(n, driver):
        return VOID_SCREEN

    r = auto_scanner.run_scan_step(None, start=880138, capture_fn=cap, preview_fn=preview_order)
    assert r["action"] == "empty_skipped"
    assert scanned_store.get("880138") is None
    assert scanned_store.next_scan_number(880138) == 880139


def test_step_incomplete_empty_capture_still_retried():
    # Zero items but NO 'END OF ORDER' → could be a stalled capture of a real
    # order; must keep retrying (not skip past it and lose the order).
    def cap(n, driver):
        return "ORDER 880140 partial screen"

    def partial_preview(text):
        return {"order_number": "880140", "item_count": 0, "total_units": 0, "is_last_page": False}

    r = auto_scanner.run_scan_step(None, start=880140, capture_fn=cap, preview_fn=partial_preview)
    assert r["action"] == "not_found"
    assert scanned_store.next_scan_number(880140) == 880140  # cursor untouched


def test_wait_for_empty_skipped_moves_on_quickly():
    assert auto_scanner._wait_for("empty_skipped") == auto_scanner.FOUND_NEXT_DELAY_SEC


# --- AS400 health beacon (UI dot) -----------------------------------------------


def test_health_unknown_before_any_signal(monkeypatch):
    monkeypatch.setitem(auto_scanner._as400_health, "ok", None)
    monkeypatch.setitem(auto_scanner._as400_health, "at", 0.0)
    assert auto_scanner.as400_health()["state"] == "unknown"


def test_health_ok_after_recent_success(monkeypatch):
    auto_scanner.note_as400(True)
    assert auto_scanner.as400_health()["state"] == "ok"
    auto_scanner.note_as400(False)
    assert auto_scanner.as400_health()["state"] == "err"


def test_health_degrades_to_unknown_when_stale(monkeypatch):
    import time

    auto_scanner.note_as400(True)
    monkeypatch.setitem(
        auto_scanner._as400_health, "at", time.time() - auto_scanner.AS400_HEALTH_MAX_AGE_SEC - 1
    )
    assert auto_scanner.as400_health()["state"] == "unknown"


def test_step_void_message_screen_is_skipped(monkeypatch):
    # capture_order raises OrderVoidSkip (after pressing F6) when a VOID order
    # dead-ends on the AS400 message screen → advance past it, cache nothing.
    from as400_capture import OrderVoidSkip

    def cap(n, driver):
        raise OrderVoidSkip("routed to message screen")

    r = auto_scanner.run_scan_step(None, start=880150, capture_fn=cap, preview_fn=_preview)
    assert r == {"action": "empty_skipped", "number": "880150"}
    assert scanned_store.get("880150") is None
    assert scanned_store.next_scan_number(880150) == 880151


# ── the operator's terminal is not a stuck terminal ──────────────────────────
#
# Rafael, 11 sep 2026: "cuando tomo control al watcher no le importa si se
# crashea capturando la misma pantalla en bucle".


def test_the_screens_a_person_navigates_to_are_not_treated_as_a_jam():
    # Unsticking one of these means F6·F6·F7 on somebody mid-lookup, and then a
    # retry five seconds later — the watcher taking the terminal back in a loop.
    from as400_capture import (
        STATE_CUSTOMER_DISPLAY,
        STATE_STOCK_INQUIRY,
        STATE_UNKNOWN,
    )

    assert STATE_CUSTOMER_DISPLAY in auto_scanner.OPERATOR_SCREENS
    assert STATE_STOCK_INQUIRY in auto_scanner.OPERATOR_SCREENS
    # A jam is not a person: UNKNOWN still gets the unstick.
    assert STATE_UNKNOWN not in auto_scanner.OPERATOR_SCREENS


def test_the_hold_is_finite_so_a_forgotten_screen_comes_back():
    # Standing down for ever would mean one abandoned lookup stops the orders
    # for the rest of the day.
    assert 0 < auto_scanner.OPERATOR_HOLD_SEC <= 3600


# ── the watchdog was impersonating the operator ──────────────────────────────
#
# The driver types with `System Events keystroke`, which posts real HID events,
# so every key it sends resets the very clock it reads to decide whether a
# person is at the keyboard. On 11 sep 2026 the catalogue run read the seven
# SKUs that fitted in its grace window and stopped — at 3pm, with nobody there.


def test_our_own_typing_opens_the_gate_instead_of_closing_it(monkeypatch):
    """The bug this function was written for, and then still had.

    Right after the watchdog types, BOTH clocks read zero — the raw idle
    because the event just landed, and the time since our own stamp for the
    same reason. Returning either of them says "somebody is at the keyboard",
    which is how the queue spent eight hours standing down for itself at three
    in the morning. What has to be remembered is when a PERSON last typed.
    """
    monkeypatch.setattr(auto_scanner, "_last_operator_input", None)
    # A person (or the restart) touched it 5 minutes ago and nothing since.
    monkeypatch.setattr(auto_scanner, "system_idle_seconds", lambda: 300.0)
    monkeypatch.setattr(auto_scanner, "seconds_since_self_input", lambda: 1e9)
    assert auto_scanner.operator_idle_seconds() >= 300.0

    # Now WE type: the raw clock drops to zero, and so does our own stamp.
    monkeypatch.setattr(auto_scanner, "system_idle_seconds", lambda: 0.2)
    monkeypatch.setattr(auto_scanner, "seconds_since_self_input", lambda: 0.2)
    # The person is still five minutes idle, and the gate stays OPEN.
    assert auto_scanner.operator_idle_seconds() >= 300.0


def test_a_real_hand_is_newer_than_our_last_keystroke(monkeypatch):
    # We typed 40s ago and the machine says the last event was 1s ago — that is
    # somebody else, and the gate has to shut.
    monkeypatch.setattr(auto_scanner, "_last_operator_input", None)
    monkeypatch.setattr(auto_scanner, "system_idle_seconds", lambda: 1.0)
    monkeypatch.setattr(auto_scanner, "seconds_since_self_input", lambda: 40.0)
    assert auto_scanner.operator_idle_seconds() < 5.0


def test_the_operator_ages_out_again_once_they_stop(monkeypatch):
    import time as _t

    monkeypatch.setattr(auto_scanner, "_last_operator_input", None)
    monkeypatch.setattr(auto_scanner, "system_idle_seconds", lambda: 1.0)
    monkeypatch.setattr(auto_scanner, "seconds_since_self_input", lambda: 40.0)
    auto_scanner.operator_idle_seconds()  # a hand, remembered
    base = _t.monotonic()
    monkeypatch.setattr(auto_scanner, "_last_operator_input", base - 120.0)
    # They walked off and we have been driving since: idle keeps growing.
    monkeypatch.setattr(auto_scanner, "system_idle_seconds", lambda: 0.1)
    monkeypatch.setattr(auto_scanner, "seconds_since_self_input", lambda: 0.1)
    assert auto_scanner.operator_idle_seconds() >= 120.0


def test_the_slack_absorbs_the_lag_between_asking_and_the_event_landing(monkeypatch):
    # The keystroke lands a moment after we asked for it, so the raw clock can
    # read a touch lower than our own stamp without a person being involved.
    monkeypatch.setattr(auto_scanner, "_last_operator_input", None)
    monkeypatch.setattr(auto_scanner, "system_idle_seconds", lambda: 600.0)
    monkeypatch.setattr(auto_scanner, "seconds_since_self_input", lambda: 1e9)
    auto_scanner.operator_idle_seconds()
    monkeypatch.setattr(auto_scanner, "system_idle_seconds", lambda: 4.5)
    monkeypatch.setattr(auto_scanner, "seconds_since_self_input", lambda: 5.0)
    assert auto_scanner.operator_idle_seconds() >= 600.0


# ── staying awake for the weekend ────────────────────────────────────────────


def test_the_wake_assertion_dies_with_this_process(monkeypatch):
    # `-w <pid>`: an update restart or a crash must never leave the Mac pinned
    # awake with nobody there to notice.
    seen = {}

    class FakeProc:
        def poll(self):
            return None

        def terminate(self):
            seen["terminated"] = True

    monkeypatch.setattr(auto_scanner, "_awake_proc", None)
    monkeypatch.setattr(
        auto_scanner.subprocess,
        "Popen",
        lambda argv, **kw: seen.setdefault("argv", argv) or FakeProc(),
    )
    auto_scanner.hold_awake()
    assert seen["argv"][0] == "caffeinate"
    assert "-w" in seen["argv"] and str(os.getpid()) in seen["argv"]


def test_it_only_prevents_sleep_on_ac_power(monkeypatch):
    # `-s`, not `-i`: on battery the Mac still sleeps. That is what the lid gets
    # closed for.
    seen = {}

    class FakeProc:
        def poll(self):
            return None

    monkeypatch.setattr(auto_scanner, "_awake_proc", None)
    monkeypatch.setattr(
        auto_scanner.subprocess,
        "Popen",
        lambda argv, **kw: seen.setdefault("argv", argv) or FakeProc(),
    )
    auto_scanner.hold_awake()
    assert "-s" in seen["argv"]
    assert "-u" not in seen["argv"]  # -u fakes user activity and would stop the run


def test_holding_awake_twice_does_not_spawn_two(monkeypatch):
    calls = []

    class FakeProc:
        def poll(self):
            return None

    monkeypatch.setattr(auto_scanner, "_awake_proc", None)
    monkeypatch.setattr(
        auto_scanner.subprocess, "Popen", lambda argv, **kw: calls.append(argv) or FakeProc()
    )
    auto_scanner.hold_awake()
    auto_scanner.hold_awake()
    assert len(calls) == 1


def test_it_can_be_switched_off_without_a_deploy(monkeypatch):
    calls = []
    monkeypatch.setenv("SCAN_HOLD_AWAKE", "0")
    monkeypatch.setattr(auto_scanner, "_awake_proc", None)
    monkeypatch.setattr(auto_scanner.subprocess, "Popen", lambda *a, **k: calls.append(1))
    auto_scanner.hold_awake()
    assert calls == []
