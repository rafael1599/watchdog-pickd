"""
Tests for the way out of the screen that answers no key.

The ADDITIONAL MESSAGE INFORMATION screen ignores every keystroke, so once the
terminal lands there the daemon is finished until a person re-opens the session
— 36 minutes of a working day on 2026-09-08, and a whole weekend if it lands
there on a Friday evening.

The way out is **Cmd+W then Cmd+N** — close the dead window, open a fresh
session — and it works for a reason worth keeping in a test: those are
APPLICATION commands, not 5250 keystrokes. The dead screen has no say over what
Mocha does with its own menu shortcuts.

In that order, never the other way round: after Cmd+N the NEW window is in
front, so a Cmd+W then would close the good one. And Cmd+W on the LAST window
closes Mocha altogether, which is why what follows is reopening the application
rather than asking a closed one for a window.
"""

import pytest

import as400_capture
from as400_capture import AS400ManualLoginRequired, bootstrap_session, hard_restart

ADDL_MSG = "ADDITIONAL MESSAGE INFORMATION\n BAS-1234 No matching key\n Option:"
SIGN_ON = "Sign On\nPassword"
MENU = "SALESN Options\n Ready for option number or command"
READY = "Order Number: ____"


class FakeEmulator:
    """Records the lifecycle calls, and can refuse them like a real one."""

    supports_steps = False

    def __init__(
        self, screens=None, *, cmd_n_works=True, cmd_w_works=True, last_window=False, closes=True
    ):
        self.events = []
        self.cmd_n_works = cmd_n_works
        self.cmd_w_works = cmd_w_works
        # Cmd+W on the LAST window closes Mocha altogether (Rafael, 2026-09-08).
        self.last_window = last_window
        self._up = True
        self.closes = closes
        self._screens = list(screens or [ADDL_MSG])
        self.keys = []
        self.actions = []

    def close_window(self):
        self.events.append("cmd+w")
        if not self.cmd_w_works:
            raise RuntimeError("System Events refused")
        if self.last_window:
            self._up = False

    def is_running(self):
        return self._up

    def new_window(self):
        self.events.append("cmd+n")
        if not self.cmd_n_works:
            raise RuntimeError("System Events refused")

    def quit(self):
        self.events.append("quit")
        return self.closes

    def launch(self):
        self.events.append("launch")
        self._up = True

    def focus(self):
        pass

    def copy_screen(self, steps=()):
        return self._screens.pop(0) if len(self._screens) > 1 else self._screens[0]

    def key(self, name):
        self.keys.append(name)

    def type_text(self, text):
        self.actions.append(text)


@pytest.fixture(autouse=True)
def _fast_and_fresh(monkeypatch):
    # Module state: without the reset, the first test to recover would silence
    # every test after it through the cooldown.
    monkeypatch.setattr(as400_capture, "_last_hard_restart", 0.0)
    monkeypatch.setenv("AS400_NEW_WINDOW_WAIT", "0")
    monkeypatch.setenv("AS400_CLOSE_WINDOW_WAIT", "0")
    monkeypatch.setenv("AS400_RESTART_PAUSE", "0")
    monkeypatch.setenv("AS400_LAUNCH_WAIT", "0")


def test_it_is_off_until_somebody_has_watched_it_once(monkeypatch):
    monkeypatch.delenv("AS400_HARD_RESTART", raising=False)
    d = FakeEmulator()
    assert hard_restart(d, idle_fn=lambda: 1e9) is False
    assert d.events == []


def test_it_never_steals_focus_from_somebody_working(monkeypatch):
    # A new window comes to the front, and focus taken mid-order is somebody's
    # work interrupted. Five minutes untouched, five times the normal gate.
    monkeypatch.setenv("AS400_HARD_RESTART", "1")
    d = FakeEmulator()
    assert hard_restart(d, idle_fn=lambda: 120.0) is False
    assert d.events == []


def test_the_way_out_is_the_application_menu_not_a_keystroke(monkeypatch):
    # Cmd+W and Cmd+N are handled by Mocha, not by the session, which is exactly
    # why they escape a screen that ignores what the session receives.
    #
    # The ORDER is the point: the dead window is the one in front, because we
    # just read it, so it is closed first. Cmd+N first would put the NEW window
    # in front and the Cmd+W after it would close the good one.
    monkeypatch.setenv("AS400_HARD_RESTART", "1")
    d = FakeEmulator()
    assert hard_restart(d, idle_fn=lambda: 1e9) is True
    assert d.events == ["cmd+w", "cmd+n"]


def test_closing_the_last_window_closes_mocha_so_it_is_reopened(monkeypatch):
    # Rafael, 2026-09-08: "cuando solo queda una ventana abierta cierra por
    # completo el AS400 y toca recuperarlo abriéndolo de nuevo". Asking a dead
    # application for a new window would do nothing at all.
    monkeypatch.setenv("AS400_HARD_RESTART", "1")
    d = FakeEmulator(last_window=True)
    assert hard_restart(d, idle_fn=lambda: 1e9) is True
    assert d.events == ["cmd+w", "launch"]  # no pointless Cmd+N at a closed app


def test_a_window_that_will_not_close_still_gets_a_new_one(monkeypatch):
    # Failing to tidy up is not a reason to stay stuck.
    monkeypatch.setenv("AS400_HARD_RESTART", "1")
    d = FakeEmulator(cmd_w_works=False)
    assert hard_restart(d, idle_fn=lambda: 1e9) is True
    assert d.events == ["cmd+w", "cmd+n"]


def test_a_terminal_that_cannot_be_saved_is_not_poked_all_night(monkeypatch):
    monkeypatch.setenv("AS400_HARD_RESTART", "1")
    d = FakeEmulator()
    assert hard_restart(d, idle_fn=lambda: 1e9) is True
    assert hard_restart(d, idle_fn=lambda: 1e9) is False  # same episode
    assert d.events == ["cmd+w", "cmd+n"]  # exactly one attempt


def test_closing_the_emulator_stays_off_unless_asked(monkeypatch):
    # Quitting shares an application with a person, can raise a dialog that
    # blocks every Apple event after it, and assumes Mocha reconnects by itself.
    # If Cmd+N fails, the honest answer is to ask for a human.
    monkeypatch.setenv("AS400_HARD_RESTART", "1")
    monkeypatch.delenv("AS400_HARD_RESTART_QUIT", raising=False)
    d = FakeEmulator(cmd_n_works=False)
    assert hard_restart(d, idle_fn=lambda: 1e9) is False
    assert d.events == ["cmd+w", "cmd+n"]  # never quit


def test_the_deeper_fallback_closes_and_reopens_when_switched_on(monkeypatch):
    monkeypatch.setenv("AS400_HARD_RESTART", "1")
    monkeypatch.setenv("AS400_HARD_RESTART_QUIT", "1")
    d = FakeEmulator(cmd_n_works=False)
    assert hard_restart(d, idle_fn=lambda: 1e9) is True
    assert d.events == ["cmd+w", "cmd+n", "quit", "launch"]


def test_an_emulator_that_will_not_close_is_left_to_a_person(monkeypatch):
    # Relaunching one that never closed would just add a second window.
    monkeypatch.setenv("AS400_HARD_RESTART", "1")
    monkeypatch.setenv("AS400_HARD_RESTART_QUIT", "1")
    d = FakeEmulator(cmd_n_works=False, closes=False)
    assert hard_restart(d, idle_fn=lambda: 1e9) is False
    assert d.events == ["cmd+w", "cmd+n", "quit"]  # never launched


def test_the_dead_end_still_asks_for_a_human_while_this_is_off(monkeypatch):
    # Unchanged, and the important half of it: not one keystroke is sent into
    # the screen that answers none.
    monkeypatch.delenv("AS400_HARD_RESTART", raising=False)
    d = FakeEmulator([ADDL_MSG])
    with pytest.raises(AS400ManualLoginRequired):
        bootstrap_session(d, launch_wait=0, step_wait=0)
    assert d.keys == []


def test_the_dead_end_recovers_itself_when_this_is_on(monkeypatch):
    # The whole point: dead end → new window → sign on → menu → order search,
    # with nobody involved.
    monkeypatch.setenv("AS400_HARD_RESTART", "1")
    monkeypatch.setattr(as400_capture, "_system_idle_seconds", lambda: 1e9)
    d = FakeEmulator([ADDL_MSG, SIGN_ON, MENU, READY])
    assert bootstrap_session(d, launch_wait=0, step_wait=0) == "order_search"
    assert "cmd+n" in d.events
    assert "ROMAN" in d.actions  # it logged itself back in


def test_it_gives_up_honestly_if_the_new_window_lands_nowhere(monkeypatch):
    # A new window that does NOT reach the host must end as "a human is needed",
    # not as a loop. bootstrap verifies every screen, which is what makes that
    # true without this function knowing anything about hosts.
    monkeypatch.setenv("AS400_HARD_RESTART", "1")
    monkeypatch.setattr(as400_capture, "_system_idle_seconds", lambda: 1e9)
    d = FakeEmulator([ADDL_MSG, "Cannot connect to host 47.22.32.213 , port 23"])
    with pytest.raises(as400_capture.AS400Disconnected):
        bootstrap_session(d, launch_wait=0, step_wait=0)
    assert d.events == ["launch", "cmd+w", "cmd+n"]  # tried once, then stopped
