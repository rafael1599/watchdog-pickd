"""
Tests for the last resort: closing the emulator and opening it again.

The ADDITIONAL MESSAGE INFORMATION screen answers no key, so once the terminal
lands there the daemon is finished until a person re-opens the session — 36
minutes of a working day on 2026-09-08, and a whole weekend if it happens on a
Friday evening.

This is the only thing in the system that closes an application a PERSON is
sharing. Every test here is about it refusing to.
"""

import pytest

import as400_capture
from as400_capture import (
    AS400ManualLoginRequired,
    bootstrap_session,
    hard_restart,
)

ADDL_MSG = "ADDITIONAL MESSAGE INFORMATION\n BAS-1234 No matching key\n Option:"
SIGN_ON = "Sign On\nPassword"
MENU = "SALESN Options\n Ready for option number or command"
READY = "Order Number: ____"


class FakeEmulator:
    """Records the lifecycle calls, and can refuse to close like a real one."""

    supports_steps = False

    def __init__(self, screens=None, closes=True):
        self.events = []
        self.closes = closes
        self._screens = list(screens or [ADDL_MSG])
        self.keys = []
        self.actions = []

    def quit(self):
        self.events.append("quit")
        return self.closes

    def launch(self):
        self.events.append("launch")

    def focus(self):
        pass

    def copy_screen(self, steps=()):
        return self._screens.pop(0) if len(self._screens) > 1 else self._screens[0]

    def key(self, name):
        self.keys.append(name)

    def type_text(self, text):
        self.actions.append(text)


@pytest.fixture(autouse=True)
def _reset_cooldown(monkeypatch):
    # Module state: without this the first test to restart would silence the rest.
    monkeypatch.setattr(as400_capture, "_last_hard_restart", 0.0)


def test_it_is_off_until_somebody_turns_it_on(monkeypatch):
    # Two things it cannot assume and Bay 2 has to confirm: that Mocha
    # reconnects on its own, and that quitting raises no dialog — a dialog
    # blocks every Apple event after it, trading a stuck terminal for one
    # nobody can drive at all.
    monkeypatch.delenv("AS400_HARD_RESTART", raising=False)
    d = FakeEmulator()
    assert hard_restart(d, idle_fn=lambda: 1e9) is False
    assert d.events == []


def test_it_never_closes_the_emulator_under_somebody_s_hands(monkeypatch):
    # THE guard. Quitting Mocha while the operator is reading an order would
    # throw that order off their screen. Five minutes untouched, five times the
    # scanner's normal gate.
    monkeypatch.setenv("AS400_HARD_RESTART", "1")
    d = FakeEmulator()
    assert hard_restart(d, idle_fn=lambda: 120.0) is False
    assert d.events == []


def test_it_closes_and_reopens_when_the_mac_is_really_idle(monkeypatch):
    monkeypatch.setenv("AS400_HARD_RESTART", "1")
    monkeypatch.setenv("AS400_RESTART_PAUSE", "0")
    monkeypatch.setenv("AS400_LAUNCH_WAIT", "0")
    d = FakeEmulator()
    assert hard_restart(d, idle_fn=lambda: 1e9) is True
    assert d.events == ["quit", "launch"]


def test_a_terminal_that_cannot_be_saved_is_not_relaunched_all_night(monkeypatch):
    # The cooldown. Without it a dead session would be reopened every five
    # minutes until morning.
    monkeypatch.setenv("AS400_HARD_RESTART", "1")
    monkeypatch.setenv("AS400_RESTART_PAUSE", "0")
    monkeypatch.setenv("AS400_LAUNCH_WAIT", "0")
    d = FakeEmulator()
    assert hard_restart(d, idle_fn=lambda: 1e9) is True
    assert hard_restart(d, idle_fn=lambda: 1e9) is False  # same episode
    assert d.events == ["quit", "launch"]  # exactly one round trip


def test_an_emulator_that_will_not_close_is_left_to_a_person(monkeypatch):
    # If it won't even quit, relaunching would just add a second window.
    monkeypatch.setenv("AS400_HARD_RESTART", "1")
    d = FakeEmulator(closes=False)
    assert hard_restart(d, idle_fn=lambda: 1e9) is False
    assert d.events == ["quit"]  # never launched


def test_the_dead_end_still_asks_for_a_human_while_this_is_off(monkeypatch):
    # Unchanged behaviour, and the important half of it: not one keystroke is
    # sent into the screen that answers none.
    monkeypatch.delenv("AS400_HARD_RESTART", raising=False)
    d = FakeEmulator([ADDL_MSG])
    with pytest.raises(AS400ManualLoginRequired):
        bootstrap_session(d, launch_wait=0, step_wait=0)
    assert d.keys == []


def test_the_dead_end_recovers_itself_when_this_is_on(monkeypatch):
    # The whole point: dead end → close → reopen → sign on → menu → order search,
    # with no person involved.
    monkeypatch.setenv("AS400_HARD_RESTART", "1")
    monkeypatch.setenv("AS400_RESTART_PAUSE", "0")
    monkeypatch.setenv("AS400_LAUNCH_WAIT", "0")
    monkeypatch.setattr(as400_capture, "_system_idle_seconds", lambda: 1e9)
    d = FakeEmulator([ADDL_MSG, SIGN_ON, MENU, READY])
    assert bootstrap_session(d, launch_wait=0, step_wait=0) == "order_search"
    # bootstrap opens the emulator first, as it always did; the restart is the
    # quit + launch pair inside it.
    assert d.events == ["launch", "quit", "launch"]
    assert "ROMAN" in d.actions  # it logged itself back in


def test_it_gives_up_honestly_if_the_relaunch_lands_nowhere(monkeypatch):
    # A reopened emulator that does NOT reconnect is the unknown this feature
    # is switched off for. It must end as "a human is needed", not as a loop.
    monkeypatch.setenv("AS400_HARD_RESTART", "1")
    monkeypatch.setenv("AS400_RESTART_PAUSE", "0")
    monkeypatch.setenv("AS400_LAUNCH_WAIT", "0")
    monkeypatch.setattr(as400_capture, "_system_idle_seconds", lambda: 1e9)
    d = FakeEmulator([ADDL_MSG, "Cannot connect to host 47.22.32.213 , port 23"])
    with pytest.raises(as400_capture.AS400Disconnected):
        bootstrap_session(d, launch_wait=0, step_wait=0)
    assert d.events == ["launch", "quit", "launch"]  # tried once, then stopped
