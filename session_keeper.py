"""
session_keeper.py — When someone else takes the AS400 user, Bay 2 takes it back.

Rafael, 6 nov 2026: «si abro la misma cuenta de as400 en otra computadora le
cierra la sesión a bay 2, quisiera que volviera a intentar iniciar sesión
después de 3 minutos, quitándome la sesión al que le quitó inicialmente para
poder seguir sacando órdenes».

Until now the watcher only noticed a lost session on its next read, which with
nothing to do can be twenty minutes away, and then logged in at once. So:

  - a thread looks at the screen every `AS400_SESSION_CHECK_SEC` (it takes
    `capture_lock` without waiting, and stays away while a person is at the Mac);
  - a Sign On or disconnected screen, or Mocha closed, starts the grace (`AS400_RETAKE_WAIT_SEC`,
    3 min): nobody in the watcher logs in during it — not this thread, not the
    sweep, not a request from PickD (`retake_allowed`, checked where the scanner
    would restart) — so the person who took the user gets their three minutes;
  - when it runs out, the session is taken back the way `AS400_RETAKE_MODE` says:

      same — type the login on the Sign On that is already there; a
             disconnected window falls back to `new`;
      new  — Cmd+N, close the dead window, log into the fresh one (the path a
             dead session already took).

The heartbeat says all of it in `last_gap_reason` («session: lost, taking it back
in 120s», «session: taken back (same)»), so it can be followed from PickD's base.
`AS400_SESSION_KEEPER=0` switches it off.
"""

from __future__ import annotations

import logging
import os
import threading
import time

log = logging.getLogger("pickd-session-keeper")

# Mocha itself is not running: no window to read, nothing to type into.
STATE_CLOSED = "closed"
# Mocha running with no session window (6 nov 2026): keys beep, reads copy nothing.
STATE_NO_WINDOW = "no_window"

_lock = threading.Lock()
_state = {"lost_since": None}
_thread: threading.Thread | None = None


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except ValueError:
        return default


def enabled() -> bool:
    return os.getenv("AS400_SESSION_KEEPER", "1") not in ("0", "false", "no")


def retake_wait_sec() -> float:
    return max(0.0, _env_float("AS400_RETAKE_WAIT_SEC", 20.0))


def check_every_sec() -> float:
    return max(2.0, _env_float("AS400_SESSION_CHECK_SEC", 10.0))


def retake_mode() -> str:
    mode = os.getenv("AS400_RETAKE_MODE", "same").strip().lower()
    return mode if mode in ("same", "new") else "same"


def lost(now: float | None = None) -> float:
    """Mark the session lost (first notice wins). Returns seconds since."""
    now = time.monotonic() if now is None else now
    with _lock:
        if _state["lost_since"] is None:
            _state["lost_since"] = now
        return now - _state["lost_since"]


def back() -> None:
    with _lock:
        _state["lost_since"] = None


def grace_left(now: float | None = None) -> float:
    now = time.monotonic() if now is None else now
    with _lock:
        since = _state["lost_since"]
    if since is None:
        return 0.0
    return max(0.0, retake_wait_sec() - (now - since))


def blocks_restart(screen: str | None, now: float | None = None) -> bool:
    """Where the scanner is about to log back in after a dead session: True while
    the grace runs, if that dead screen is a Sign On or a disconnected window —
    someone else took the user. Any other dead screen restarts as before."""
    if not screen:
        return False
    import as400_capture as cap

    if cap.classify_screen(screen) not in (cap.STATE_LOGIN, cap.STATE_DISCONNECTED):
        return False
    return not retake_allowed(now)


def retake_allowed(now: float | None = None) -> bool:
    """The first call starts the grace; until it ends the answer is no. Always
    yes when switched off."""
    if not enabled():
        return True
    lost(now)
    left = grace_left(now)
    if left > 0:
        log.info("session: lost — leaving it to whoever took it for %.0fs more", left)
        return False
    return True


def check_once(driver, *, note_gap, note_as400, wake, now=None) -> str:
    """One look at the screen; logs back in when the grace is over.

    Returns what happened, for the log and the tests: ready, unknown, lost,
    retaken, retake_failed, unreadable.
    """
    import as400_capture as cap

    # Mocha closed is a lost session too (Rafael closed it on Bay 2 to test this,
    # 6 nov 2026): a read then fails with an AppleScript error, not a
    # CaptureError, and nothing ever opened it again. Taking it back is the same
    # bootstrap that opens it and logs in.
    # Only when the read failed: the process name check was never seen on
    # Bay 2, and a wrong name must not turn every look into a false «closed».
    try:
        screen = driver.copy_screen()
        state = cap.classify_screen(screen)
    except Exception as e:  # noqa: BLE001 — a read that fails is not a verdict
        is_running = getattr(driver, "_is_running", None)
        windows = getattr(driver, "window_count", None)
        if callable(is_running) and not is_running():
            screen, state = "", STATE_CLOSED
        elif callable(windows) and windows() == 0:
            screen, state = "", STATE_NO_WINDOW
        else:
            log.info("session: could not read the screen (%s)", e)
            note_gap(f"session: could not read the screen ({type(e).__name__}: {e})"[:300])
            return "unreadable"
    if state == cap.STATE_UNKNOWN:
        # Not ours to judge: the scanner's own recovery handles a strange screen.
        # Said in the heartbeat, though: a silent keeper is a blind one.
        shown = " ".join((screen or "").split())[:120]
        note_gap(f"session: unknown screen «{shown}»")
        return "unknown"
    if state not in (cap.STATE_LOGIN, cap.STATE_DISCONNECTED, STATE_CLOSED, STATE_NO_WINDOW):
        if _state["lost_since"] is not None:
            back()
            note_gap("session: back")
        return "ready"

    since = lost(now)
    left = grace_left(now)
    note_as400(False, parked=state)
    if left > 0:
        note_gap(f"session: lost ({state}), taking it back in {left:.0f}s")
        return "lost"

    mode = retake_mode()
    note_gap(f"session: taking it back ({mode}) after {since:.0f}s")
    log.warning("session: lost for %.0fs — taking it back (%s)", since, mode)
    try:
        if state in (STATE_CLOSED, STATE_NO_WINDOW):
            cap.bootstrap_session(driver)  # opens Mocha / a window, then logs in
        elif mode == "same":
            try:
                cap.bootstrap_session(driver)
            except cap.AS400Disconnected:
                log.info("session: the window is disconnected — opening a new one")
                cap.restart_as400_session(driver, dead_screen=screen)
        else:
            cap.restart_as400_session(driver, dead_screen=screen)
    except Exception as e:  # noqa: BLE001 — try again after another grace
        back()
        lost(now)
        note_gap(f"session: taking it back failed ({type(e).__name__}: {e})"[:300])
        log.warning("session: taking it back failed: %s", e)
        return "retake_failed"
    back()
    note_as400(True)
    note_gap(f"session: taken back ({mode})")
    wake()
    return "retaken"


def _loop(stop: threading.Event, capture_lock, operator_present, make_driver, hooks) -> None:
    hooks["mark_automated"]()
    driver = None
    while not stop.wait(check_every_sec()):
        if operator_present():
            continue
        if not capture_lock.acquire(blocking=False):
            continue
        try:
            driver = driver or make_driver()
            check_once(
                driver,
                note_gap=hooks["note_gap"],
                note_as400=hooks["note_as400"],
                wake=hooks["wake"],
            )
        except Exception:  # noqa: BLE001 — the keeper must never die
            log.exception("session keeper step crashed")
        finally:
            capture_lock.release()


def start(stop: threading.Event, capture_lock, operator_present, make_driver, hooks) -> None:
    global _thread
    if not enabled():
        log.info("session keeper off (AS400_SESSION_KEEPER=0)")
        return
    if _thread and _thread.is_alive():
        return
    _thread = threading.Thread(
        target=_loop,
        args=(stop, capture_lock, operator_present, make_driver, hooks),
        daemon=True,
        name="session-keeper",
    )
    _thread.start()
    log.info(
        "session keeper: every %.0fs, takes the user back after %.0fs (%s)",
        check_every_sec(),
        retake_wait_sec(),
        retake_mode(),
    )
