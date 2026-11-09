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
    monkeypatch.setattr(cap, "bootstrap_session", lambda d, **k: seen.append("open") or "ok")
    monkeypatch.setattr(
        cap, "restart_as400_session", lambda d, dead_screen=None, **k: seen.append("new") or "ok"
    )
    return seen


def test_a_sign_on_waits_the_grace_then_focus_cmd_n_and_verify(calls):
    out, gaps, health = run(Driver(SIGN_ON), 1000.0, calls)
    assert out == "lost" and calls == []
    assert gaps == ["session: lost (login), taking it back in 180s"]
    assert health == [(False, "login")]

    out, *_ = run(Driver(SIGN_ON), 1179.0, calls)
    assert out == "lost" and calls == []

    out, gaps, health = run(Driver(SIGN_ON), 1180.0, calls)
    assert out == "retaken"
    assert calls == ["new", "wake"]
    assert gaps[-1] == "session: taken back"
    assert health[-1] == (True, None)
    assert sk.grace_left(1181.0) == 0.0


def test_a_disconnected_window_takes_the_same_road(calls):
    run(Driver(GONE), 0.0, calls)
    out, *_ = run(Driver(GONE), 180.0, calls)
    assert out == "retaken" and calls == ["new", "wake"]


def test_a_failed_retake_waits_another_grace(calls, monkeypatch):
    monkeypatch.setattr(
        cap,
        "restart_as400_session",
        lambda d, **k: (_ for _ in ()).throw(RuntimeError("no")),
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


def test_mocha_closed_is_a_lost_session_and_is_opened_again(calls):
    out, gaps, health = run(Driver(RuntimeError("not running"), running=False), 0.0, calls)
    assert out == "lost" and gaps == ["session: lost (closed), taking it back in 180s"]
    assert health == [(False, "closed")]
    out, *_ = run(Driver(RuntimeError("not running"), running=False), 180.0, calls)
    assert out == "retaken" and calls == ["open", "wake"]


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
    assert out == "retaken" and calls == ["new", "wake"]


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


# ── an unknown screen: close Mocha, wait, open it again (9 nov 2026) ──────────

CANCELED = (
    "Input-Output XP BASICR OPVOF,,40 SYS-5580 System operator canceled job. "
    "Press Enter SYS-7212 Cmd7 not allowed now"
)


class Mocha(Driver):
    def __init__(self, screen, closes=True):
        super().__init__(screen)
        self.closes = closes
        self.quits = 0

    def quit(self):
        self.quits += 1
        return self.closes


@pytest.fixture
def relaunch(monkeypatch, calls):
    cap._reset_restart_backoff_for_tests()
    sk.known()
    monkeypatch.setenv("AS400_UNKNOWN_RESTART_SEC", "120")
    monkeypatch.setenv("AS400_RELAUNCH_PAUSE_SEC", "5")
    monkeypatch.delenv("AS400_UNKNOWN_RELAUNCH", raising=False)
    monkeypatch.setattr(cap, "operator_allows_restart", lambda idle_fn=None: True)
    yield calls
    cap._reset_restart_backoff_for_tests()
    sk.known()


def look(driver, now, calls, slept):
    gaps, health = [], []
    out = sk.check_once(
        driver,
        note_gap=gaps.append,
        note_as400=lambda ok, parked=None: health.append((ok, parked)),
        wake=lambda: calls.append("wake"),
        now=now,
        sleep=slept.append,
    )
    return out, gaps, health


def test_the_canceled_job_screen_is_unknown():
    assert cap.classify_screen(CANCELED) == cap.STATE_UNKNOWN


def test_an_unknown_screen_waits_then_closes_pauses_and_reopens(relaunch):
    mocha, slept = Mocha(CANCELED), []
    out, gaps, _ = look(mocha, 1000.0, relaunch, slept)
    assert out == "unknown" and mocha.quits == 0
    assert gaps[0].startswith("session: unknown screen «Input-Output")

    out, *_ = look(mocha, 1119.0, relaunch, slept)
    assert out == "unknown" and mocha.quits == 0

    out, gaps, health = look(mocha, 1120.0, relaunch, slept)
    assert out == "relaunched"
    assert mocha.quits == 1 and slept == [5.0]
    assert relaunch == ["open", "wake"]
    assert gaps == ["session: unknown screen for 120s, closing Mocha", "session: Mocha reopened"]
    assert health == [(True, None)]


def test_a_known_screen_forgets_the_unknown_one(relaunch):
    mocha, slept = Mocha(CANCELED), []
    look(mocha, 0.0, relaunch, slept)
    mocha.screen = READY
    assert look(mocha, 60.0, relaunch, slept)[0] == "ready"
    mocha.screen = CANCELED
    assert look(mocha, 130.0, relaunch, slept)[0] == "unknown"
    assert look(mocha, 249.0, relaunch, slept)[0] == "unknown"
    assert look(mocha, 250.0, relaunch, slept)[0] == "relaunched"


def test_nobody_closes_mocha_under_somebodys_hands(relaunch, monkeypatch):
    monkeypatch.setattr(cap, "operator_allows_restart", lambda idle_fn=None: False)
    mocha, slept = Mocha(CANCELED), []
    look(mocha, 0.0, relaunch, slept)
    out, gaps, _ = look(mocha, 500.0, relaunch, slept)
    assert out == "unknown" and mocha.quits == 0 and relaunch == []
    assert gaps == ["session: unknown screen for 500s, waiting for the Mac to be free"]


def test_a_failed_reopen_feeds_the_backoff(relaunch):
    mocha, slept = Mocha(CANCELED, closes=False), []
    look(mocha, 0.0, relaunch, slept)
    out, gaps, _ = look(mocha, 120.0, relaunch, slept)
    assert out == "relaunch_failed" and relaunch == [] and slept == []
    assert gaps[-1] == "session: reopening Mocha failed (RuntimeError: Mocha would not close)"
    assert cap.is_restart_backing_off(120.0)

    # Inside the 4 min the screen counts again from zero and the backoff holds it.
    mocha.closes = True
    look(mocha, 130.0, relaunch, slept)
    out, gaps, _ = look(mocha, 250.0, relaunch, slept)
    assert out == "unknown" and mocha.quits == 1
    assert gaps == ["session: unknown screen for 120s, backing off 110s"]

    out, *_ = look(mocha, 360.0, relaunch, slept)
    assert out == "relaunched" and mocha.quits == 2
    assert not cap.is_restart_backing_off(361.0)


def test_a_bootstrap_that_fails_counts_too(relaunch, monkeypatch):
    def boom(d, **k):
        raise RuntimeError("no Sign On")

    monkeypatch.setattr(cap, "bootstrap_session", boom)
    mocha, slept = Mocha(CANCELED), []
    look(mocha, 0.0, relaunch, slept)
    out, gaps, _ = look(mocha, 120.0, relaunch, slept)
    assert out == "relaunch_failed" and mocha.quits == 1 and slept == [5.0]
    assert gaps[-1] == "session: reopening Mocha failed (RuntimeError: no Sign On)"
    assert cap.is_restart_backing_off(121.0)


def test_the_switch_turns_only_this_off(relaunch, monkeypatch):
    monkeypatch.setenv("AS400_UNKNOWN_RELAUNCH", "0")
    mocha, slept = Mocha(CANCELED), []
    look(mocha, 0.0, relaunch, slept)
    assert look(mocha, 9999.0, relaunch, slept)[0] == "unknown"
    assert mocha.quits == 0
