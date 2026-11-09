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
  - when it runs out: focus Mocha, Cmd+N, verify the screen and log in
    (`restart_as400_session`; a closed Mocha is opened first). The new session
    invalidates the old one (Rafael), so a second window is never a problem here.

A screen the watcher does not know (9 nov 2026: «System operator canceled job.
Press Enter») used to be only noted, and Bay 2 sat on it all morning. Rafael:
«incluir la recuperación cerrando el mocha automáticamente, esperar unos segundos
y volver a abrir». So when an unknown screen lasts `AS400_UNKNOWN_RESTART_SEC`
(120 s), and nobody is at the Mac, Mocha is closed, the keeper waits
`AS400_RELAUNCH_PAUSE_SEC` (5 s) and opens it and logs in — the same bootstrap as a
closed Mocha. A failure counts in the restart backoff of `as400_capture` (4, 8,
16… min); `AS400_UNKNOWN_RELAUNCH=0` switches only this part off.

The heartbeat says all of it in `last_gap_reason` («session: lost, taking it back
in 120s», «session: taken back»), so it can be followed from PickD's base.
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
_state = {"lost_since": None, "unknown_since": None}
_thread: threading.Thread | None = None


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except ValueError:
        return default


def enabled() -> bool:
    return os.getenv("AS400_SESSION_KEEPER", "1") not in ("0", "false", "no")


def relaunch_on_unknown() -> bool:
    return os.getenv("AS400_UNKNOWN_RELAUNCH", "1") not in ("0", "false", "no")


def unknown_restart_sec() -> float:
    return max(0.0, _env_float("AS400_UNKNOWN_RESTART_SEC", 120.0))


def relaunch_pause_sec() -> float:
    return max(0.0, _env_float("AS400_RELAUNCH_PAUSE_SEC", 5.0))


def unknown_for(now: float | None = None) -> float:
    """Mark an unknown screen seen (first notice wins). Returns seconds since."""
    now = time.monotonic() if now is None else now
    with _lock:
        if _state["unknown_since"] is None:
            _state["unknown_since"] = now
        return now - _state["unknown_since"]


def known() -> None:
    with _lock:
        _state["unknown_since"] = None


def retake_wait_sec() -> float:
    return max(0.0, _env_float("AS400_RETAKE_WAIT_SEC", 20.0))


def check_every_sec() -> float:
    return max(2.0, _env_float("AS400_SESSION_CHECK_SEC", 10.0))


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


def _relaunch(driver, *, since, note_gap, note_as400, wake, sleep, now) -> str:
    """Close Mocha, wait, open it and log in. Guarded by the operator and the
    restart backoff; a failure feeds that backoff, a success resets it."""
    import as400_capture as cap

    clock = time.time() if now is None else now
    if cap.is_restart_backing_off(clock):
        left = cap.restart_cooldown_remaining(clock)
        note_gap(f"session: unknown screen for {since:.0f}s, backing off {left:.0f}s")
        return "unknown"
    if not cap.operator_allows_restart():
        note_gap(f"session: unknown screen for {since:.0f}s, waiting for the Mac to be free")
        return "unknown"
    note_gap(f"session: unknown screen for {since:.0f}s, closing Mocha")
    log.warning("session: unknown screen for %.0fs — closing Mocha and opening it again", since)
    try:
        if not driver.quit():
            raise RuntimeError("Mocha would not close")
        sleep(relaunch_pause_sec())
        cap.bootstrap_session(driver)
    except Exception as e:  # noqa: BLE001 — the backoff decides when to try again
        cap.record_restart_failure(clock)
        known()
        note_gap(f"session: reopening Mocha failed ({type(e).__name__}: {e})"[:300])
        log.warning("session: reopening Mocha failed: %s", e)
        return "relaunch_failed"
    cap.record_restart_success()
    known()
    back()
    note_as400(True)
    note_gap("session: Mocha reopened")
    wake()
    return "relaunched"


def check_once(driver, *, note_gap, note_as400, wake, now=None, sleep=time.sleep) -> str:
    """One look at the screen; logs back in when the grace is over.

    Returns what happened, for the log and the tests: ready, unknown, lost,
    retaken, retake_failed, unreadable, relaunched, relaunch_failed.
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
        # The scanner's own recovery gets the first `unknown_restart_sec`; past
        # that, close Mocha and open it again (Rafael, 9 nov 2026).
        shown = " ".join((screen or "").split())[:120]
        since = unknown_for(now)
        if relaunch_on_unknown() and since >= unknown_restart_sec():
            return _relaunch(
                driver,
                since=since,
                note_gap=note_gap,
                note_as400=note_as400,
                wake=wake,
                sleep=sleep,
                now=now,
            )
        note_gap(f"session: unknown screen «{shown}»")
        return "unknown"
    known()
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

    # Rafael, 6 nov 2026: «enfocar Mocha, después el comando de nueva ventana y
    # después verifica … la sesión nueva invalida la anterior». That is
    # `restart_as400_session`: Cmd+N (which focuses first), tidy the dead window
    # only if it really is dead, then the verified login. A closed Mocha has
    # nothing to focus, so it is opened first by the bootstrap.
    note_gap(f"session: taking it back after {since:.0f}s")
    log.warning("session: lost (%s) for %.0fs — taking it back", state, since)
    try:
        if state == STATE_CLOSED:
            cap.bootstrap_session(driver)
        else:
            cap.restart_as400_session(driver, dead_screen=screen or None)
    except Exception as e:  # noqa: BLE001 — try again after another grace
        back()
        lost(now)
        note_gap(f"session: taking it back failed ({type(e).__name__}: {e})"[:300])
        log.warning("session: taking it back failed: %s", e)
        return "retake_failed"
    back()
    note_as400(True)
    note_gap("session: taken back")
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
        "session keeper: every %.0fs, takes the user back after %.0fs",
        check_every_sec(),
        retake_wait_sec(),
    )
