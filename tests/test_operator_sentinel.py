"""A person can always stop the watcher (operator_sentinel.py, Rafael 29 sep 2026:
«nadie puede parar al watcher… usar la tecla delete dos veces»)."""

import pytest

import as400_capture
import operator_sentinel as osn
from as400_capture import MochaDriver, OperatorTookOver


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    osn._reset_for_tests()
    monkeypatch.setenv("SCAN_IDLE_THRESHOLD_SEC", "60")
    monkeypatch.setenv("OPERATOR_EMERGENCY_STOP_SEC", "1800")
    yield
    osn._reset_for_tests()
    as400_capture._automation.on = False
    as400_capture.set_hands_off_check(None)


def test_a_mouse_move_is_a_person_for_the_idle_threshold():
    assert osn.on_signal("MOUSE\n", now=100.0) == "operator"
    assert osn.hands_off(now=130.0)
    assert not osn.hands_off(now=161.0)


def test_delete_twice_within_two_seconds_is_the_emergency_stop():
    assert osn.on_signal("DELETE", now=100.0) == "operator"
    assert osn.on_signal("DELETE", now=101.5) == "emergency_stop"
    assert osn.hands_off(now=100.0 + 29 * 60)
    assert not osn.hands_off(now=101.5 + 1800 + 61)


def test_two_deletes_far_apart_are_not_a_stop():
    osn.on_signal("DELETE", now=100.0)
    assert osn.on_signal("DELETE", now=105.0) == "operator"
    assert osn.emergency_remaining(now=106.0) == 0


def test_resume_lifts_the_emergency_stop():
    osn.on_signal("DELETE", now=100.0)
    osn.on_signal("DELETE", now=100.5)
    osn.resume()
    assert osn.emergency_remaining(now=101.0) == 0


class _Ran(Exception):
    pass


def _driver_that_would_type(monkeypatch):
    def fake_run(*a, **k):
        raise _Ran()

    monkeypatch.setattr(as400_capture.subprocess, "run", fake_run)
    return MochaDriver()


def test_the_automated_thread_does_not_type_while_a_person_has_the_mac(monkeypatch):
    driver = _driver_that_would_type(monkeypatch)
    as400_capture.mark_automated_thread()
    as400_capture.set_hands_off_check(lambda: True)
    with pytest.raises(OperatorTookOver):
        driver._osascript('tell application "System Events" to keystroke "3"')


def test_a_capture_the_operator_asks_for_is_never_stopped(monkeypatch):
    # The Bay 2 UI runs on another thread: it is not marked automated.
    driver = _driver_that_would_type(monkeypatch)
    as400_capture.set_hands_off_check(lambda: True)
    with pytest.raises(_Ran):  # it reached osascript
        driver._osascript('tell application "System Events" to keystroke "3"')


def test_the_scanner_idle_clock_hears_the_sentinel(monkeypatch):
    import auto_scanner

    monkeypatch.setattr(auto_scanner, "system_idle_seconds", lambda: 1e6)
    monkeypatch.setattr(auto_scanner, "_last_operator_input", None)
    monkeypatch.setattr(as400_capture, "_last_self_input", None)
    auto_scanner.operator_idle_seconds()  # settles the HID baseline
    osn.on_signal("MOUSE")
    assert auto_scanner.operator_idle_seconds() < 1.0
    osn.on_signal("DELETE")
    osn.on_signal("DELETE")
    assert auto_scanner.operator_idle_seconds() == 0.0


def test_the_sentinel_script_ships_with_the_repo():
    assert osn.SCRIPT.exists()
    text = osn.SCRIPT.read_text()
    assert "MOUSE" in text and "DELETE" in text
