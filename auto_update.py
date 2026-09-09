"""
auto_update.py — Bay 2 pulls its own updates, so a push is the deploy.

Rafael, 2026-09-08: "hay manera de hacer que en cada push se actualice solo el
watchdog?". There is, and for a Mac in a warehouse the shape is the same one the
rest of this system already has: **Bay 2 asks**, GitHub is never asked to call.
Rafael would have preferred GitHub to push, and it is the better shape in the
abstract. It is the worse one HERE, and measurably: every way of being told
needs Bay 2 either reachable from the internet or holding a permanent outbound
connection — a `cloudflared` daemon, or a GitHub Actions self-hosted runner,
which is itself a long-poll wearing a service. Each of those is MORE running on
the machine he is worried about slowing down, not less.

What this costs instead, measured on Bay 2's own repo: **0.25 s per poll**, of
which nearly all is network wait rather than CPU, 288 times a day — under a
tenth of a percent of the machine, and less than two AS400 captures. The thread
sleeps the rest of the time.

So: a thread checks whether `origin/<branch>` moved and, when it is SAFE, runs
exactly the same `scripts/update.sh` the ⟳ button runs. Nothing new happens on
update day; the only new thing is who decides to press it.

"When it is safe" is the whole of this file:

  - **Never during a capture.** update.sh restarts the LaunchAgents, and doing
    that while the terminal is half-way through an order would leave Mocha on an
    unknown screen and lose the capture. It takes `capture_lock` without
    blocking and skips the round if anything holds it.
  - **Never under the operator's hands.** A restart takes the UI away from
    whoever is looking at it.
  - **Never over local work.** update.sh pulls `--ff-only`, so a dirty tree or a
    diverged history stops it. That is reported ONCE rather than every five
    minutes for ever.
  - **Never in a loop.** If HEAD does not move after an update was launched,
    something is wrong and repeating it every poll would only bury the reason.
"""

from __future__ import annotations  # PEP 563: "str | None" on Python 3.9 (Bay 2 Mac)

import logging
import os
import subprocess
import threading
from pathlib import Path

log = logging.getLogger("pickd-auto-update")

REPO = Path(__file__).resolve().parent


def enabled() -> bool:
    """On by default: Rafael asked for a push to be the deploy. Off is one line
    in .env for the day somebody needs the machine to stop moving."""
    return os.getenv("AUTO_UPDATE", "1") in ("1", "true", "True", "yes")


def poll_sec() -> float:
    return max(60.0, float(os.getenv("AUTO_UPDATE_POLL_SEC", "300")))


def retry_sec() -> float:
    """How soon to look again when an update is READY but the moment is not.

    The full interval would be wrong here: the catalogue work has just been
    asked to stand down and will free the terminal within seconds, and waiting
    five more minutes to notice would waste the window we just made.
    """
    return max(5.0, float(os.getenv("AUTO_UPDATE_RETRY_SEC", "20")))


def idle_needed() -> float:
    """A restart takes the UI away from whoever is looking at it. Same threshold
    as the scanner's own gate — this is the same courtesy."""
    return float(os.getenv("AUTO_UPDATE_IDLE_SEC", "60"))


def _git(*args, cwd=None) -> str:
    out = subprocess.run(
        ["git", *args],
        cwd=str(cwd or REPO),
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    )
    return out.stdout.strip()


def branch() -> str:
    return os.getenv("AUTO_UPDATE_BRANCH") or _git("rev-parse", "--abbrev-ref", "HEAD")


def check_remote() -> dict:
    """What git says: are we behind, and is the tree clean enough to move?

    Uses `ls-remote`, not `fetch`, and the reason is not speed — both are about
    a quarter of a second, nearly all of it network. `ls-remote` asks for one ref
    and **writes nothing**: no objects downloaded, no FETCH_HEAD, no refs
    touched. A poller that runs 288 times a day on a machine somebody else is
    working on should leave no trace on disk at all.

    Fetching here was also redundant: `update.sh` does its own `git pull`, so all
    this needs to know is whether the remote SHA differs from ours.

    Returns {"branch", "local", "remote", "behind", "dirty"}. Raises whatever git
    raises — the caller decides how loud that should be.
    """
    b = branch()
    line = _git("ls-remote", "origin", f"refs/heads/{b}")
    remote = line.split()[0] if line else ""
    local = _git("rev-parse", "HEAD")
    dirty = bool(_git("status", "--porcelain"))
    if not remote:
        raise RuntimeError(f"origin has no branch {b}")
    return {
        "branch": b,
        "local": local,
        "remote": remote,
        "behind": local != remote,
        "dirty": dirty,
    }


def why_not_now(state: dict, *, idle: float, lock_free: bool) -> str | None:
    """The reason this poll should not update, or None to go ahead. Pure.

    Split out because it is the whole safety argument, and an argument that
    lives in a thread that only runs on one Mac in a warehouse is an argument
    nobody can check.
    """
    if not state["behind"]:
        return "already up to date"
    if state["dirty"]:
        # update.sh pulls --ff-only and would fail anyway; saying so beats
        # letting the script discover it every five minutes.
        return "there are uncommitted changes here — update.sh would refuse to pull"
    if not lock_free:
        return "a capture is running"
    if idle < idle_needed():
        return f"somebody is using the Mac (idle {idle:.0f}s)"
    return None


def start_update() -> bool:
    """Run scripts/update.sh detached, exactly as the ⟳ button does.

    Detached and in a new session because the script restarts this process's own
    LaunchAgent: it has to outlive us.
    """
    script = REPO / "scripts" / "update.sh"
    if not script.exists():
        log.error("auto-update: scripts/update.sh is missing — cannot update")
        return False
    logs = REPO / "logs"
    logs.mkdir(exist_ok=True)
    try:
        log_file = open(logs / "update.log", "a")  # noqa: SIM115 — the child keeps this fd
        subprocess.Popen(
            ["/bin/bash", str(script)],
            cwd=str(REPO),
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        return True
    except Exception as e:  # noqa: BLE001
        log.error("auto-update: could not start update.sh (%s)", e)
        return False


# Raised when an update is waiting for the terminal to go quiet. The catalogue
# work watches this and stands down: it is the lowest-priority thing in the
# system, so it yields to an update exactly as it already yields to the operator
# and to the orders.
#
# Without it the two changes of 2026-09-08 deadlock each other in slow motion:
# the scanner stopped sleeping (bursts of up to 300s holding capture_lock) and
# the updater refuses to restart during a capture, so the lock is free about 5
# seconds in every 305 — 1.6% of the time — and a poll every 300s would take
# hours to land on one.
update_pending = threading.Event()

_stop = threading.Event()
_thread: threading.Thread | None = None
# The commit we last launched an update FOR. If the remote is still that commit
# next time round, the update did not take and repeating it would only bury the
# reason in the log.
_attempted: str | None = None
_last_reason: str | None = None


def _tick(idle_fn, lock_free_fn) -> str | None:
    """One poll. Returns what it did or why it didn't, for the log and the tests."""
    global _attempted, _last_reason
    try:
        state = check_remote()
    except Exception as e:  # noqa: BLE001 — a network blip must not kill the thread
        return f"could not reach the remote ({e})"

    reason = why_not_now(state, idle=idle_fn(), lock_free=lock_free_fn())

    # Ask the catalogue work to stand down while we wait, and stop asking the
    # moment there is nothing to wait for.
    if state["behind"] and not state["dirty"]:
        update_pending.set()
    else:
        update_pending.clear()

    if reason:
        # Say a NEW reason once. The same one every five minutes all day is how
        # a real problem gets lost among the routine ones.
        if reason != _last_reason and reason != "already up to date":
            log.info("auto-update: not now — %s", reason)
        _last_reason = reason
        return reason

    if _attempted == state["remote"]:
        return "already tried this commit — not looping on it"

    _last_reason = None
    _attempted = state["remote"]
    update_pending.clear()
    log.warning(
        "auto-update: origin/%s moved (%s → %s) — updating now",
        state["branch"],
        state["local"][:8],
        state["remote"][:8],
    )
    return "updating" if start_update() else "update.sh would not start"


def _loop(idle_fn, lock_free_fn) -> None:
    # A restart is the normal end of this thread's life, so it says nothing on
    # the way out.
    delay = poll_sec()
    while not _stop.wait(delay):
        delay = poll_sec()
        try:
            _tick(idle_fn, lock_free_fn)
            # An update that is ready and merely blocked deserves a fast second
            # look: the window it is waiting for is seconds wide, not minutes.
            if update_pending.is_set():
                delay = retry_sec()
        except Exception:
            log.exception("auto-update: poll crashed — the daemon keeps going")


def start_auto_update() -> None:
    """Start the poller (idempotent). Gated by AUTO_UPDATE."""
    global _thread
    if not enabled():
        log.info("auto-update disabled (AUTO_UPDATE is off)")
        return
    if _thread and _thread.is_alive():
        return

    from auto_scanner import capture_lock, system_idle_seconds

    def lock_free() -> bool:
        if not capture_lock.acquire(blocking=False):
            return False
        capture_lock.release()
        return True

    _stop.clear()
    _thread = threading.Thread(
        target=_loop,
        args=(system_idle_seconds, lock_free),
        daemon=True,
        name="auto-update",
    )
    _thread.start()
    log.info("auto-update: watching origin every %.0fs — a push is the deploy", poll_sec())


def stop_auto_update() -> None:
    _stop.set()
