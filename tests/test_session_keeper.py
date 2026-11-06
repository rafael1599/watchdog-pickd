"""Someone else took the AS400 user: Bay 2 waits its grace, then takes it back."""

import pytest

import as400_capture as cap
import session_keeper as sk

SIGN_ON = "Sign On  System . . . : JAMIS  User . . . .  Password . . ."
READY = "O R D E R  I N Q U I R Y  Order Number: ______"
GONE = "Connection closed by host"


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    sk.back()
    monkeypatch.setenv("AS400_RETAKE_WAIT_SEC", "180")
    monkeypatch.delenv("AS400_RETAKE_MODE", raising=False)
    monkeypatch.delenv("AS400_SESSION_KEEPER", raising=False)
    yield
    sk.back()


class Driver:
    def __init__(self, screen, running=True, windows=1):
        self.screen = screen
        self.running = running
        self.windows = windows

    def window_count(self):
        return self.windows

    def _is_running(self):
        return self.running

    def copy_screen(self, steps=()):
        if isinstance(self.screen, Exception):
            raise self.screen
        return self.screen


def run(driver, now, calls):
    gaps, health = [], []
    out = sk.check_once(
        driver,
        note_gap=gaps.append,
        note_as400=lambda ok, parked=None: health.append((ok, parked)),
        wake=lambda: calls.append("wake"),
        now=now,
    )
    return out, gaps, health


@pytest.fixture
def calls(monkeypatch):
    seen = []
    monkeypatch.setattr(cap, "bootstrap_session", lambda d, **k: seen.append("same") or "ok")
    monkeypatch.setattr(
        cap, "restart_as400_session", lambda d, dead_screen=None, **k: seen.append("new") or "ok"
    )
    return seen


def test_a_sign_on_waits_the_grace_then_logs_in_on_the_same_window(calls):
    out, gaps, health = run(Driver(SIGN_ON), 1000.0, calls)
    assert out == "lost" and calls == []
    assert gaps == ["session: lost (login), taking it back in 180s"]
    assert health == [(False, "login")]

    out, *_ = run(Driver(SIGN_ON), 1179.0, calls)
    assert out == "lost" and calls == []

    out, gaps, health = run(Driver(SIGN_ON), 1180.0, calls)
    assert out == "retaken"
    assert calls == ["same", "wake"]
    assert gaps[-1] == "session: taken back (same)"
    assert health[-1] == (True, None)
    assert sk.grace_left(1181.0) == 0.0


def test_new_mode_opens_a_fresh_window(calls, monkeypatch):
    monkeypatch.setenv("AS400_RETAKE_MODE", "new")
    run(Driver(SIGN_ON), 0.0, calls)
    out, *_ = run(Driver(SIGN_ON), 180.0, calls)
    assert out == "retaken" and calls == ["new", "wake"]


def test_same_mode_on_a_disconnected_window_falls_back_to_new(calls, monkeypatch):
    def boom(d, **k):
        calls.append("same")
        raise cap.AS400Disconnected("gone")

    monkeypatch.setattr(cap, "bootstrap_session", boom)
    run(Driver(GONE), 0.0, calls)
    out, *_ = run(Driver(GONE), 180.0, calls)
    assert out == "retaken" and calls == ["same", "new", "wake"]


def test_a_failed_retake_waits_another_grace(calls, monkeypatch):
    monkeypatch.setattr(
        cap, "bootstrap_session", lambda d, **k: (_ for _ in ()).throw(RuntimeError("no"))
    )
    run(Driver(SIGN_ON), 0.0, calls)
    out, gaps, _ = run(Driver(SIGN_ON), 180.0, calls)
    assert out == "retake_failed" and "failed" in gaps[-1]
    assert sk.grace_left(180.0) == 180.0


def test_someone_logging_in_again_ends_the_grace(calls):
    run(Driver(SIGN_ON), 0.0, calls)
    out, gaps, _ = run(Driver(READY), 10.0, calls)
    assert out == "ready" and gaps == ["session: back"]
    assert sk.grace_left(11.0) == 0.0


def test_a_strange_screen_is_left_to_the_scanner_but_said(calls):
    out, gaps, health = run(Driver("SOMETHING ELSE ENTIRELY"), 0.0, calls)
    assert out == "unknown" and health == [] and calls == []
    assert gaps == ["session: unknown screen «SOMETHING ELSE ENTIRELY»"]


def test_an_unreadable_screen_does_nothing_but_says_so(calls):
    out, gaps, _ = run(Driver(cap.CaptureError("covered")), 0.0, calls)
    assert out == "unreadable" and calls == []
    assert gaps == ["session: could not read the screen (CaptureError: covered)"]


def test_the_scanner_does_not_log_in_during_the_grace():
    assert sk.blocks_restart(SIGN_ON, now=0.0) is True
    assert sk.blocks_restart(SIGN_ON, now=179.0) is True
    assert sk.blocks_restart(SIGN_ON, now=180.0) is False


def test_other_dead_screens_restart_as_before():
    assert sk.blocks_restart("ADDITIONAL MESSAGE INFORMATION", now=0.0) is False
    assert sk.blocks_restart(None, now=0.0) is False


def test_switched_off_never_blocks(monkeypatch):
    monkeypatch.setenv("AS400_SESSION_KEEPER", "0")
    assert sk.blocks_restart(SIGN_ON, now=0.0) is False


def test_mocha_closed_is_a_lost_session_and_is_opened_again(calls, monkeypatch):
    monkeypatch.setenv("AS400_RETAKE_MODE", "new")  # closed always opens + logs in
    out, gaps, health = run(Driver(RuntimeError("not running"), running=False), 0.0, calls)
    assert out == "lost" and gaps == ["session: lost (closed), taking it back in 180s"]
    assert health == [(False, "closed")]
    out, *_ = run(Driver(RuntimeError("not running"), running=False), 180.0, calls)
    assert out == "retaken" and calls == ["same", "wake"]


def test_a_read_that_errors_any_way_is_not_a_verdict(calls):
    out, *_ = run(Driver(RuntimeError("osascript exited 1")), 0.0, calls)
    assert out == "unreadable" and calls == []


def test_a_wrong_process_name_never_says_closed_while_the_screen_reads(calls):
    out, *_ = run(Driver(READY, running=False), 0.0, calls)
    assert out == "ready" and calls == []


def test_mocha_with_no_window_is_lost_and_opened_again(calls):
    nothing = cap.CaptureError("the clipboard didn't change")
    out, gaps, health = run(Driver(nothing, windows=0), 0.0, calls)
    assert out == "lost" and gaps == ["session: lost (no_window), taking it back in 180s"]
    assert health == [(False, "no_window")]
    out, *_ = run(Driver(nothing, windows=0), 180.0, calls)
    assert out == "retaken" and calls == ["same", "wake"]


class WindowlessMocha:
    """bootstrap_session on a Mocha with no window: Cmd+N first, then the login."""

    def __init__(self):
        self.windows = 0
        self.log = []

    def launch(self):
        self.log.append("launch")

    def window_count(self):
        return self.windows

    def new_window(self):
        self.log.append("cmd+n")
        self.windows = 1

    def focus(self):
        pass

    def copy_screen(self, steps=()):
        return READY


def test_bootstrap_opens_a_window_when_mocha_has_none(monkeypatch):
    monkeypatch.setenv("AS400_NEW_WINDOW_WAIT", "0")
    d = WindowlessMocha()
    cap.bootstrap_session(d, launch_wait=0)
    assert d.log == ["launch", "cmd+n"]


def test_bootstrap_leaves_an_open_window_alone(monkeypatch):
    d = WindowlessMocha()
    d.windows = 1
    cap.bootstrap_session(d, launch_wait=0)
    assert d.log == ["launch"]
