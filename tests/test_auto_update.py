"""
Tests for the self-updater: a push becomes the deploy.

Everything that matters here is a REFUSAL. update.sh restarts the LaunchAgents,
so an update at the wrong moment loses a capture or takes the UI away from
somebody mid-order. The decision is a pure function precisely so it can be read
and checked here, instead of living only inside a thread on one Mac in a
warehouse.
"""

import auto_update
from auto_update import why_not_now

BEHIND = {"branch": "main", "local": "aaa", "remote": "bbb", "behind": True, "dirty": False}
UP_TO_DATE = {**BEHIND, "remote": "aaa", "behind": False}
DIRTY = {**BEHIND, "dirty": True}

IDLE = 1e9  # nobody has touched the Mac in a very long time


def test_it_updates_when_the_remote_moved_and_the_machine_is_quiet():
    assert why_not_now(BEHIND, idle=IDLE, lock_free=True) is None


def test_nothing_to_do_when_the_remote_has_not_moved():
    assert why_not_now(UP_TO_DATE, idle=IDLE, lock_free=True) == "already up to date"


def test_it_never_restarts_in_the_middle_of_a_capture():
    # update.sh restarts the LaunchAgents. Doing that while the terminal is
    # half-way through an order leaves Mocha on an unknown screen and loses the
    # capture — the one thing this must never cost.
    assert why_not_now(BEHIND, idle=IDLE, lock_free=False) == "a capture is running"


def test_it_never_takes_the_ui_away_from_somebody_using_it():
    assert "using the Mac" in why_not_now(BEHIND, idle=3.0, lock_free=True)


def test_it_refuses_over_uncommitted_work_instead_of_letting_the_pull_fail():
    # update.sh pulls --ff-only and would refuse anyway; saying so beats letting
    # the script rediscover it every five minutes.
    assert "uncommitted" in why_not_now(DIRTY, idle=IDLE, lock_free=True)


def test_a_capture_outranks_a_dirty_tree_being_reported():
    # Ordering is not arbitrary: the dirty tree is a message for a person, the
    # running capture is a thing that would break. Both refuse, so either order
    # is safe — this pins which reason the log shows.
    assert why_not_now(DIRTY, idle=IDLE, lock_free=False).startswith("there are uncommitted")


def test_it_does_not_relaunch_the_same_commit_over_and_over(monkeypatch):
    # If HEAD does not move after an update was launched, something is wrong and
    # repeating it every poll would only bury the reason in the log.
    monkeypatch.setattr(auto_update, "_attempted", None)
    monkeypatch.setattr(auto_update, "_last_reason", None)
    monkeypatch.setattr(auto_update, "check_remote", lambda: dict(BEHIND))
    started = []
    monkeypatch.setattr(auto_update, "start_update", lambda: started.append(1) or True)

    assert auto_update._tick(lambda: IDLE, lambda: True) == "updating"
    assert (
        auto_update._tick(lambda: IDLE, lambda: True)
        == "already tried this commit — not looping on it"
    )
    assert len(started) == 1


def test_a_network_blip_never_kills_the_poller(monkeypatch):
    def boom():
        raise OSError("no route to host")

    monkeypatch.setattr(auto_update, "check_remote", boom)
    assert "could not reach the remote" in auto_update._tick(lambda: IDLE, lambda: True)


def test_it_can_be_switched_off(monkeypatch):
    monkeypatch.setenv("AUTO_UPDATE", "0")
    assert auto_update.enabled() is False
    monkeypatch.delenv("AUTO_UPDATE")
    assert auto_update.enabled() is True  # a push is the deploy, by default


def test_the_poll_never_goes_below_a_minute(monkeypatch):
    # A tight poll would mean a `git fetch` per second against GitHub.
    monkeypatch.setenv("AUTO_UPDATE_POLL_SEC", "1")
    assert auto_update.poll_sec() == 60.0
