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


def test_untracked_files_are_not_local_work(monkeypatch):
    """The bug this updater wrote for itself.

    `update.sh` does `mkdir -p logs` on its way in and `logs/` was not ignored,
    so the FIRST successful auto-update left the tree permanently "dirty" under
    a plain `--porcelain` — and every poll after it refused. Bay 2 sat 18 hours
    on 812012d repeating "there are uncommitted changes here" to nobody.

    `git pull --ff-only` does not refuse over untracked files it is not going to
    overwrite. What this gate is for is a tracked file somebody edited on the
    Mac, and that is what `--untracked-files=no` reports.
    """
    calls = []

    def fake_git(*args, cwd=None):
        calls.append(args)
        if args[0] == "ls-remote":
            return "bbb\trefs/heads/main"
        if args[0] == "rev-parse":
            return "main" if "--abbrev-ref" in args else "aaa"
        if args[0] == "status":
            # An untracked logs/ dir is invisible to -uno, which is the point.
            return "" if "--untracked-files=no" in args else "?? logs/"
        return ""

    monkeypatch.setattr(auto_update, "_git", fake_git)
    state = auto_update.check_remote()

    assert state["dirty"] is False
    assert why_not_now(state, idle=IDLE, lock_free=True) is None
    assert ("status", "--porcelain", "--untracked-files=no") in calls


def test_a_tracked_edit_on_the_mac_still_stops_the_update(monkeypatch):
    def fake_git(*args, cwd=None):
        if args[0] == "ls-remote":
            return "bbb\trefs/heads/main"
        if args[0] == "rev-parse":
            return "main" if "--abbrev-ref" in args else "aaa"
        if args[0] == "status":
            return " M watcher.py"
        return ""

    monkeypatch.setattr(auto_update, "_git", fake_git)
    assert auto_update.check_remote()["dirty"] is True


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


# ── the two changes of 2026-09-08 must not block each other ──────────────────


def test_a_blocked_update_asks_the_catalogue_work_to_stand_down(monkeypatch):
    # The scanner stopped sleeping — bursts of up to 300s holding capture_lock —
    # and the updater refuses to restart during a capture. Without this flag the
    # lock is free about 5 seconds in every 305, and a poll every 300s would
    # take hours to land on one.
    monkeypatch.setattr(auto_update, "_attempted", None)
    monkeypatch.setattr(auto_update, "_last_reason", None)
    monkeypatch.setattr(auto_update, "check_remote", lambda: dict(BEHIND))
    auto_update.update_pending.clear()

    assert auto_update._tick(lambda: IDLE, lambda: False) == "a capture is running"
    assert auto_update.update_pending.is_set()


def test_it_stops_asking_once_there_is_nothing_to_wait_for(monkeypatch):
    monkeypatch.setattr(auto_update, "check_remote", lambda: dict(UP_TO_DATE))
    auto_update.update_pending.set()
    auto_update._tick(lambda: IDLE, lambda: True)
    assert not auto_update.update_pending.is_set()


def test_uncommitted_work_does_not_hold_the_catalogue_hostage(monkeypatch):
    # A dirty tree is a message for a person, not a deploy that is about to land.
    # Asking the scanner to stand down for it would stop the catalogue work all
    # day for something no amount of waiting fixes.
    monkeypatch.setattr(auto_update, "check_remote", lambda: dict(DIRTY))
    auto_update.update_pending.set()
    auto_update._tick(lambda: IDLE, lambda: True)
    assert not auto_update.update_pending.is_set()


def test_the_flag_clears_once_the_update_is_launched(monkeypatch):
    monkeypatch.setattr(auto_update, "_attempted", None)
    monkeypatch.setattr(auto_update, "_last_reason", None)
    monkeypatch.setattr(auto_update, "check_remote", lambda: dict(BEHIND))
    monkeypatch.setattr(auto_update, "start_update", lambda: True)
    auto_update.update_pending.set()
    assert auto_update._tick(lambda: IDLE, lambda: True) == "updating"
    assert not auto_update.update_pending.is_set()


def test_a_ready_update_looks_again_in_seconds_not_minutes(monkeypatch):
    # The window it is waiting for is seconds wide.
    monkeypatch.setenv("AUTO_UPDATE_RETRY_SEC", "20")
    assert auto_update.retry_sec() < auto_update.poll_sec()


def test_it_never_restarts_with_a_send_in_flight():
    # The send does not take capture_lock (it never drives the terminal), so
    # without this an update could kill process_order_text between the
    # picking_lists insert and the row being marked sent.
    assert why_not_now(BEHIND, idle=IDLE, lock_free=True, door_busy=True) == "a send is in flight"
    assert why_not_now(BEHIND, idle=IDLE, lock_free=True, door_busy=False) is None


def test_the_updater_asks_who_was_at_the_keyboard_not_the_raw_clock(monkeypatch):
    """The gate means "don't take the UI away from a person", and the raw
    HIDIdleTime counts the watchdog's own typing as a person.

    11 sep 2026: the catalogue burst types constantly, so the clock never
    reached sixty seconds, the updater kept saying "somebody is using the Mac",
    and `update_pending` stayed set — which is exactly what the catalogue stands
    down for. The two waited for each other and a deploy landed only by luck.
    """
    import auto_scanner
    import auto_update

    passed = []
    monkeypatch.setattr(auto_update, "enabled", lambda: True)
    monkeypatch.setattr(auto_update, "_thread", None)
    monkeypatch.setattr(
        auto_update.threading,
        "Thread",
        lambda **kw: type(
            "T",
            (),
            {
                "start": lambda self: passed.append(kw["args"][0]),
                "is_alive": lambda self: False,
            },
        )(),
    )
    auto_update.start_auto_update()
    assert passed == [auto_scanner.operator_idle_seconds]
    assert passed[0] is not auto_scanner.system_idle_seconds
