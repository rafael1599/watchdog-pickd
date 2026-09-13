"""
auto_scanner.py — Continuously capture new AS400 orders into the local scanned cache.

Design (confirmed with the operator, 2026-06-10):
  - Runs inside the capture-UI process (app.py), the single owner of Mocha, on a
    background thread. ONE order per step (not a catch-up burst): each captured order
    is stored immediately so it shows in the UI right away.
  - Pace per step result:
      * captured      → wait FOUND_NEXT_DELAY_SEC (5s), then try the next number.
      * not_found     → the number isn't an order yet. Instead of sleeping twenty
                        minutes, spend that time on the AS400 catalogue and ask
                        again (Rafael, 2026-09-08: "en ningún momento quiero al
                        watchdog lazy"). Only when there is nothing to work on —
                        empty queue, feature off — does it fall back to
                        NOT_FOUND_WAIT_SEC, because otherwise it would ask the
                        AS400 for the same missing order every few seconds.
      * incomplete    → capture stalled (no END OF ORDER, e.g. the operator grabbed
                        the keyboard) → wait INCOMPLETE_RETRY_SEC (5m) and retry the
                        SAME number (the cursor only advances on success).
      * unavailable   → AS400 not connected / not logged in → (re)bootstrap, retry.
  - Pause entirely while the operator is using the computer: if the system has had
    mouse/keyboard input recently (idle < IDLE_THRESHOLD_SEC) we don't scan, so we
    never fight the human for the keyboard. A manual capture (which holds capture_lock)
    also pauses us.
  - Cache only: captures go to scanned_store; sending happens from the UI.

Only the threaded runner touches macOS/Mocha; `run_scan_step` is pure (injectable
capture/preview fns) and unit-tested with a fake driver.
"""

# PEP 563: defer annotations so "X | None" hints work on Python 3.9 (Bay 2 Mac).
from __future__ import annotations

import logging
import os
import re
import subprocess
import threading
import time
from datetime import datetime, timezone

import scanned_store
from as400_capture import (
    STATE_CUSTOMER_DISPLAY,
    STATE_STOCK_INQUIRY,
    AS400Disconnected,
    AS400ManualLoginRequired,
    CaptureError,
    MochaDriver,
    OrderNotFound,
    OrderVoidSkip,
    bootstrap_session,
    capture_order,
    classify_screen,
    seconds_since_self_input,
)

log = logging.getLogger("pickd-auto-scanner")

# Wait after startup before the first capture, so the UI loads first.
SCAN_INITIAL_DELAY_SEC = float(os.getenv("SCAN_INITIAL_DELAY_SEC", "20"))
# Between two consecutive found orders.
FOUND_NEXT_DELAY_SEC = float(os.getenv("SCAN_FOUND_DELAY_SEC", "5"))
# After the next number doesn't exist yet (not registered in AS400).
NOT_FOUND_WAIT_SEC = float(os.getenv("SCAN_NOT_FOUND_WAIT_SEC", "1200"))  # 20 min
# After a partial/stalled capture — retry the same number.
INCOMPLETE_RETRY_SEC = float(os.getenv("SCAN_INCOMPLETE_RETRY_SEC", "300"))  # 5 min
# After AS400 is unavailable (disconnected / needs login).
UNAVAILABLE_WAIT_SEC = float(os.getenv("SCAN_UNAVAILABLE_WAIT_SEC", "300"))  # 5 min
# Operator is "using the computer" if there was input within this many seconds.
IDLE_THRESHOLD_SEC = float(os.getenv("SCAN_IDLE_THRESHOLD_SEC", "60"))

# ── El día que no hay nadie (Rafael, 12 sep 2026) ────────────────────────────
#
# «No hay nadie hoy, quita ese tipo de paradas con un contador para que se
# vuelva a activar en 24 horas».
#
# Mientras dura, el catálogo NO se aparta por creer que alguien está en el
# teclado. Todo lo demás sigue mandando: el botón de «get orders now» —que es
# una persona pidiendo algo, no una suposición sobre ella—, un deploy
# pendiente, y la negativa a teclear en una pantalla desconocida.
#
# Con FECHA y no con interruptor, por el mismo motivo que el modo catálogo: un
# permiso excepcional que depende de que alguien lo apague sigue puesto el
# martes, y este en concreto le quita el teclado a quien llegue el lunes.
IGNORE_OPERATOR_UNTIL_DEFAULT = "2026-09-14T03:00:00Z"  # 24 h desde que se pidió


def ignoring_operator() -> bool:
    raw = os.getenv("SCAN_IGNORE_OPERATOR_UNTIL", IGNORE_OPERATOR_UNTIL_DEFAULT).strip()
    try:
        until = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        # Una fecha ilegible no puede significar «ignora al operario para
        # siempre». El valor que no se entiende cae del lado seguro.
        log.warning("SCAN_IGNORE_OPERATOR_UNTIL no es una fecha (%r) — modo normal", raw)
        return False
    return datetime.now(timezone.utc) < until


# How often to re-check while paused (operator active / manual capture running).
IDLE_POLL_SEC = float(os.getenv("SCAN_IDLE_POLL_SEC", "15"))
# Most already-cached numbers a single scan step will skip past (without driving
# AS400) before capturing anyway — guards a pathologically large run of cached
# orders from spinning one step forever.
MAX_SKIP_CACHED_PER_STEP = int(os.getenv("SCAN_MAX_SKIP_CACHED", "500"))
# How long the AS400 may be unreachable before the log stops being polite about
# it. Ten minutes is longer than any normal hiccup and far shorter than the 36
# that went unnoticed on 2026-09-08.
UNAVAILABLE_LOUD_SEC = float(os.getenv("SCAN_UNAVAILABLE_LOUD_SEC", "600"))
# A screen the operator went to on purpose is not a stuck terminal, and the
# difference matters: bootstrap_session unsticks with F6·F6·F7, which yanks
# somebody out of what they were reading, and then retries five seconds later.
# From the operator's seat that is the watcher stealing the terminal in a loop
# (Rafael, 11 sep 2026: "cuando tomo control al watcher no le importa"). These
# are screens a person navigates to; UNKNOWN, LOGIN and MESSAGE are not.
OPERATOR_SCREENS = (STATE_CUSTOMER_DISPLAY, STATE_STOCK_INQUIRY)
# How long the terminal is left alone once it looks like somebody is using it,
# before assuming they walked away and forgot.
OPERATOR_HOLD_SEC = float(os.getenv("SCAN_OPERATOR_HOLD_SEC", "600"))
# Catalogue work long enough to count as "the wait already happened". Below this
# there was nothing to do — an empty queue, the feature off, the operator on the
# keyboard — and asking the AS400 for the same missing order every few seconds
# would be worse than sleeping.
MIN_WORK_TO_SKIP_WAIT_SEC = float(os.getenv("SCAN_MIN_WORK_TO_SKIP_WAIT_SEC", "20"))

# Serializes all AS400/Mocha access between the auto-scanner and manual captures.
capture_lock = threading.Lock()
# A manual capture sets this; the loop also pauses on general user activity.
manual_waiting = threading.Event()

_stop = threading.Event()
_thread: threading.Thread | None = None

# Manual "get orders now": wakes the loop from any pacing wait AND bypasses the
# operator-activity gate for one pass (the operator just clicked the button, so
# the computer is obviously in use). Consumed right before the scan step runs.
_kick = threading.Event()

# ── AS400 health beacon ──────────────────────────────────────────────────────
# The UI status dot used to turn green ONLY after a manual Connect/Check click —
# it stayed gray while the auto-scanner was happily capturing orders. Every AS400
# interaction (scanner step, manual capture, connect) now records whether the
# host responded; the UI polls this instead of requiring a manual check.
# A signal older than the max age (default 30 min — the scanner can legitimately
# sit 20 min between not-found retries) degrades to "unknown" (gray).
AS400_HEALTH_MAX_AGE_SEC = float(os.getenv("AS400_HEALTH_MAX_AGE_SEC", "1800"))
_as400_health = {"at": 0.0, "ok": None, "parked": None}


def note_as400(ok: bool, parked: str | None = None) -> None:
    """Record the outcome of the latest AS400 interaction (thread-safe enough:
    two atomic dict writes; readers tolerate either ordering).

    `parked` is the screen the terminal was left on when it went wrong, and it
    is the difference between "the AS400 is unreachable" and "we left it on a
    stock screen and cannot get off it" — two problems with different answers.
    """
    import time

    _as400_health["ok"] = ok
    _as400_health["at"] = time.time()
    if not ok:
        _as400_health["parked"] = parked
    else:
        _as400_health["parked"] = None


def as400_health() -> dict:
    """{"state": "ok"|"err"|"unknown", "age_sec": float|None} for the UI dot."""
    import time

    if _as400_health["ok"] is None:
        return {"state": "unknown", "age_sec": None, "parked": None}
    age = time.time() - _as400_health["at"]
    if age > AS400_HEALTH_MAX_AGE_SEC:
        return {"state": "unknown", "age_sec": age, "parked": _as400_health["parked"]}
    return {
        "state": "ok" if _as400_health["ok"] else "err",
        "age_sec": age,
        "parked": _as400_health["parked"],
    }


def system_idle_seconds() -> float:
    """Seconds since the last mouse/keyboard input (macOS HIDIdleTime).

    Uses `ioreg` (no special permissions). Returns a very large number if it can't
    be determined (non-macOS / parse failure) so scanning isn't blocked there.
    """
    try:
        out = subprocess.run(
            ["ioreg", "-c", "IOHIDSystem"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        ).stdout
        m = re.search(r'"HIDIdleTime"\s*=\s*(\d+)', out)
        if m:
            return int(m.group(1)) / 1_000_000_000  # nanoseconds → seconds
    except Exception as e:
        log.debug("could not read HIDIdleTime: %s", e)
    return 1e9


# ── staying awake for the catalogue ──────────────────────────────────────────
#
# The queue only runs while the Mac is awake, and there is nobody there at the
# weekend to keep it so (Rafael, 11 sep 2026: "ya no estoy allá, tenemos que
# hacer que se active solo").
#
# `caffeinate -s` asserts sleep prevention **only on AC power** — on battery the
# Mac still sleeps, which is the whole reason the lid gets closed at night. And
# `-w <our pid>` ties the assertion to this process: it dies when we do, so an
# update restart or a crash cannot leave the machine pinned awake for ever.
#
# It is held only while there is catalogue work, and dropped the moment the
# queue empties. A machine kept awake for nothing is a machine somebody has to
# remember to fix.
_awake_proc = None


def hold_awake() -> None:
    """Keep the Mac awake on AC while the queue has work. Idempotent."""
    global _awake_proc
    if os.getenv("SCAN_HOLD_AWAKE", "1") not in ("1", "true", "True", "yes"):
        return
    if _awake_proc is not None and _awake_proc.poll() is None:
        return
    try:
        _awake_proc = subprocess.Popen(
            ["caffeinate", "-s", "-w", str(os.getpid())],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        log.info("auto-scan: holding the Mac awake (on AC) while the catalogue has work")
    except Exception as e:  # noqa: BLE001 — never worth taking the scanner down
        log.warning("auto-scan: could not hold the Mac awake (%s)", e)


def let_sleep() -> None:
    """Release it. Called when the queue is empty or the step is switched off."""
    global _awake_proc
    if _awake_proc is not None and _awake_proc.poll() is None:
        try:
            _awake_proc.terminate()
            log.info("auto-scan: nothing left in the queue — the Mac may sleep again")
        except Exception:  # noqa: BLE001
            pass
    _awake_proc = None


# The last HID event we are confident came from a PERSON. Everything the
# watchdog types is ours, so the operator's idle has to be measured from here
# and not from the raw clock — which our own keystrokes reset several times a
# minute (11 sep 2026).
_last_operator_input: float | None = None


def operator_idle_seconds() -> float:
    """Seconds since the OPERATOR last touched the Mac. Ours do not count.

    `system_idle_seconds` reads macOS's HIDIdleTime: time since the LAST input
    event of any kind. The driver types with `System Events keystroke`, and
    those are real events, so every key and every screen read resets that clock.
    Reading it directly, the watchdog saw its own typing as somebody at the
    keyboard and stood down for itself — for eight hours on 11 sep, at three in
    the morning, with nobody in the building.

    Comparing the two clocks identifies a foreign event exactly: HIDIdleTime is
    time since the last one, so if it is meaningfully SMALLER than the time
    since our own last keystroke, something newer happened and it was not us.
    What it CANNOT do is answer "how long since a person" on its own — right
    after we type, both clocks read zero. So the foreign events are what gets
    remembered, and the answer is measured from the most recent of those.

    Before we have driven anything, the current HID event is somebody else's by
    default: one cautious minute after a restart, then the gate opens.
    """
    global _last_operator_input
    now = time.monotonic()
    idle = system_idle_seconds()
    since_self = seconds_since_self_input()

    # A second of slack: the event lands a moment after the osascript asks.
    if _last_operator_input is None or idle < since_self - 1.0:
        _last_operator_input = now - idle

    return now - _last_operator_input


def _meta_from_preview(preview: dict) -> dict:
    """The light summary we keep alongside the raw capture (for display/lookup)."""
    return {
        "order_number": preview.get("order_number"),
        "customer": preview.get("customer"),
        "ship_to": preview.get("ship_to"),
        "item_count": preview.get("item_count"),
        "total_units": preview.get("total_units"),
        "subtotal": preview.get("subtotal"),
        "parsed_total": preview.get("parsed_total"),
        "total_mismatch": preview.get("total_mismatch", False),
    }


def run_scan_step(
    driver,
    *,
    start: int = scanned_store.SCAN_START,
    capture_fn=capture_order,
    preview_fn=None,
) -> dict:
    """Capture the single next unscanned order. Returns {"action", "number"}.

    action ∈ {captured, empty_skipped, not_found, incomplete, unavailable}. A
    successful capture stores to the cache (advancing the cursor); a VOID/empty
    order only advances the cursor (nothing cached); the rest leave the cursor so
    the same number is retried.
    """
    if preview_fn is None:
        from pipeline import preview_order as preview_fn  # local import: avoids DB deps at import

    n = scanned_store.next_scan_number(start)
    # Don't re-pull an order we already have. The scan position can legitimately
    # land on a number that's already cached — most often when the operator
    # manually captured the very next order (a manual capture deliberately doesn't
    # advance the cursor, so the scanner still backfills the gap below an order
    # grabbed a few ahead). Re-driving AS400 for an order already in the list is
    # wasted work and steals the operator's keyboard, so advance past any cached
    # numbers and land on the first one we don't have yet.
    skipped = 0
    while scanned_store.get(n) is not None and skipped < MAX_SKIP_CACHED_PER_STEP:
        scanned_store.skip(n)  # advance the cursor past the already-cached number
        nxt = scanned_store.next_scan_number(start)
        if nxt == n:  # cursor couldn't advance (e.g. n below SCAN_START) — don't spin
            break
        n = nxt
        skipped += 1
    try:
        text = capture_fn(str(n), driver)
    except OrderVoidSkip:
        # VOID order dead-ended on the message screen; capture pressed F6 to recover.
        # Advance past it (like an empty order) instead of retrying forever.
        scanned_store.skip(n)
        log.info("auto-scan: #%s routed to AS400 message screen (VOID) — skipped past it", n)
        return {"action": "empty_skipped", "number": str(n)}
    except OrderNotFound:
        return {"action": "not_found", "number": str(n)}
    except (AS400Disconnected, AS400ManualLoginRequired):
        return {"action": "unavailable", "number": str(n)}
    except CaptureError:
        # Partial / stalled capture (no END OF ORDER) — retry the same number later.
        return {"action": "incomplete", "number": str(n)}

    preview = preview_fn(text)
    meta = _meta_from_preview(preview)
    # VOID/empty order: a COMPLETE screen (END OF ORDER reached) with a real order
    # number but ZERO items — e.g. an order voided in AS400 ('Account Number: VOID').
    # It will never gain items, so retrying is useless: advance the cursor past it
    # (caching nothing) and move on to the next number. Without this the scanner
    # treated it as not_found and retried the SAME number forever.
    # ...but ONLY when the header agrees that the order is empty. Pressing ENTER
    # past the last items page redraws the SAME screen with no lines and the
    # END OF ORDER row still showing (real order 880996, Rafael 2026-09-01) — a
    # screen indistinguishable from a VOID one by "number + zero items + last
    # page". The header's Sub-Total tells them apart: a VOID order has none, a
    # real order still shows its total, so the parse mismatches. Skipping on that
    # would advance the cursor past a REAL order, forever, in silence. The loop
    # stops at the first END OF ORDER so it shouldn't land there — but a half-
    # painted items page reads the same way, and that gets likelier the faster
    # the capture reads (see docs/capture-speed-plan.md, F4).
    if (
        meta.get("order_number")
        and not (meta.get("item_count") or 0)
        and preview.get("is_last_page")
        and not meta.get("total_mismatch")
    ):
        scanned_store.skip(n)
        log.info("auto-scan: #%s is VOID/empty (no items) — skipped past it", n)
        return {"action": "empty_skipped", "number": str(n)}
    # Never cache junk: a real capture must parse to an order with items. The
    # 'Invalid Order Number, REENTER' screen (and any other non-order text) parses
    # to no number / no items — caching it floods the UI with empty 'Order #—'
    # cards. Treat it as not_found so the same number is retried later.
    if not meta.get("order_number") or not (meta.get("item_count") or 0):
        return {"action": "not_found", "number": str(n)}
    scanned_store.put(n, text, meta, source="auto_scan")
    return {"action": "captured", "number": str(n)}


def _wait_for(action: str) -> float:
    return {
        "captured": FOUND_NEXT_DELAY_SEC,
        # A skipped VOID/empty order is progress — move to the next number quickly.
        "empty_skipped": FOUND_NEXT_DELAY_SEC,
        "not_found": NOT_FOUND_WAIT_SEC,
        "incomplete": INCOMPLETE_RETRY_SEC,
        "unavailable": UNAVAILABLE_WAIT_SEC,
    }.get(action, NOT_FOUND_WAIT_SEC)


def _interruptible_wait(seconds: float) -> None:
    """Sleep up to `seconds`, returning early on shutdown or a manual kick."""
    deadline = time.monotonic() + seconds
    while not _stop.is_set():
        remaining = deadline - time.monotonic()
        if remaining <= 0 or _kick.is_set():
            return
        _kick.wait(min(0.5, remaining))


# What the catalogue step last decided, and when. The door's heartbeat carries
# it to Supabase so the answer to "why is it doing nothing" does not require
# typing on Bay 2 — which is itself the operator coming back, and the operator
# coming back is one of the reasons it stops (Rafael, 10 sep 2026).
_gap_state: dict = {"reason": None, "at": None, "read": 0}


def gap_state() -> dict:
    """The last decision of the catalogue step. Pure read, for the heartbeat."""
    return dict(_gap_state)


def _note_gap(reason: str, read: int = 0) -> None:
    _gap_state["reason"] = reason
    _gap_state["at"] = datetime.now(timezone.utc).isoformat()
    _gap_state["read"] += read


def _run_sku_gap() -> float:
    """Spend the gap on the AS400 catalogue, if that is switched on.

    The gap is TWENTY MINUTES long and one lookup uses seconds of it. With a
    queue of 745 bikes, one per gap is 28 business days — which is why Rafael's
    original ask (2026-09-02: "que dedique unos 5 minutos") is back, and why the
    one-per-gap rule of 2026-06-10 does not carry over here unchanged.

    What that rule was actually protecting is the operator's keyboard, and this
    protects it BETTER than one-per-gap did: the idle gate used to be checked
    once, before the gap, so a burst could have run straight through the moment
    somebody sat down. Now it is re-checked before EVERY lookup, and the step
    that is already running always returns the terminal to the order search
    before we stop. Two limits, whichever comes first: a wall-clock budget and a
    count, both read from .env at call time.

    The orders are untouched by any of this — the search for the next order has
    already run, and this only fills the sleep that followed it.

    Wrapped whole: a side errand may not take the scanner down with it.
    """
    started = time.monotonic()
    done = 0
    on_search = False  # the first lookup of a gap navigates the verified way
    try:
        import auto_update
        import sku_enrichment

        if not sku_enrichment.enabled():
            _note_gap("switched off (SKU_ENRICH)")
            let_sleep()
            return 0.0

        deadline = started + sku_enrichment.gap_budget_sec()
        for _ in range(sku_enrichment.max_per_gap()):
            if time.monotonic() >= deadline:
                _note_gap("budget spent")
                log.info("auto-scan: SKU budget spent after %d lookup(s)", done)
                return time.monotonic() - started
            # A manual "get orders now" siempre gana: eso es una persona
            # PIDIENDO algo, no una suposición sobre ella, y por eso no caduca.
            if _kick.is_set():
                _note_gap("orders requested")
                log.info("auto-scan: orders requested — SKU queue yields after %d", done)
                return time.monotonic() - started
            # El teclado del operario gana también, comprobado antes de CADA
            # consulta… salvo mientras dure el permiso de «hoy no hay nadie».
            if not ignoring_operator() and operator_idle_seconds() < IDLE_THRESHOLD_SEC:
                _note_gap("the operator is back")
                log.info("auto-scan: the operator is back — SKU queue yields after %d", done)
                return time.monotonic() - started
            # A pending update wins as well. This burst holds capture_lock for up
            # to five minutes and the updater refuses to restart during a
            # capture, so without standing down the two would block each other
            # for hours: the lock would be free about five seconds in every three
            # hundred. Catalogue work is the lowest-priority thing here — it
            # yields to the operator, to the orders, and to a deploy.
            if auto_update.update_pending.is_set():
                _note_gap("an update is waiting")
                log.info("auto-scan: an update is waiting — SKU queue yields after %d", done)
                return time.monotonic() - started
            hold_awake()
            row = sku_enrichment.next_sku()
            if not row:
                _note_gap("the queue is empty")
                let_sleep()
                log.info("auto-scan: the SKU queue is empty — nothing to look up")
                return time.monotonic() - started
            # Optimistic from the second lookup on, exactly like the manual run
            # (`run_until_disturbed`): Cmd7 lands on the search form with the
            # fields blank, so the next SKU is typed where we stand — no read, no
            # menu, no option 2. This loop was still going back to the MENU
            # between lookups, which is ~27 s per SKU against ~7 s measured on
            # the manual path (11 sep 2026, 03:49–03:51 on Bay 2). The automatic
            # burst is the one with a whole weekend, and it was the slow one.
            #
            # The rule lives in two loops and must stay the same in both: stay
            # optimistic while the lookups come back clean, and the moment one
            # does not we no longer know what is on the screen — walk the
            # verified way home and verify the next one.
            res = sku_enrichment.run_sku_step(
                _driver_for_sku_step(), row, home="search", on_search_screen=on_search
            )
            done += 1
            # "registered" is the discovery queue's own success: a SKU AS400
            # knows and the catalogue didn't, now a row at UNKNOWN.
            good = res.get("action") in ("read", "written", "registered")
            # The reason carries the STEP's own answer, not just "working", and
            # that is the whole point of this field. On 11 sep the heartbeat said
            # `working` for hours while the counter sat at 17: a step was running
            # and failing before it read anything, and the only place that said
            # which of the four ways it failed was a log file on a Mac in Bay 2
            # with nobody in front of it. A number that does not move is not a
            # diagnosis.
            _note_gap(
                "working" if good else f"{res.get('action')}: {res.get('why') or res.get('sku')}",
                read=1 if good else 0,
            )
            # `is False` y no `not ...`: None significa «no se intentó el viaje
            # porque el paso salió mal, y el camino verificado de aquí abajo ES
            # el viaje». Tratarlo como fallo terminaría el hueco en cada
            # tropiezo, que es justo lo que esto viene a evitar.
            if res.get("returned") is False:
                # The terminal isn't back on the order search. Stop touching it;
                # the next cycle's bootstrap is what recovers.
                log.warning("auto-scan: SKU step didn't get home — pausing the SKU queue")
                _note_gap(f"didn't get home after {res.get('sku')}")
                return time.monotonic() - started
            if res["action"] in ("unavailable", "error"):
                return time.monotonic() - started
            # Un SKU que AS400 no tiene deja el terminal en el formulario de
            # búsqueda —su propia respuesta es esa pantalla— así que la
            # siguiente consulta sigue el camino rápido. Caminar al menú y
            # volver costaba 29 s por número muerto, y son un tercio.
            if good or res.get("on_search"):
                on_search = True
            else:
                # An `unknown` or a `mismatch` leaves us unsure of the screen.
                # One Cmd7 is not enough to trust it: walk the verified way home
                # before the next lookup, and verify that one.
                #
                # Al MENÚ, no a la búsqueda de órdenes. Lo siguiente es otro SKU
                # y `capture_stock_inquiry` entra desde el menú con un `2`;
                # caminar hasta Order Inquiry obligaba a teclear 3 para entrar y
                # F7 para salir otra vez — sale del menú para volver a entrar, y
                # cada paso de esos es una lectura de pantalla por AppleScript,
                # que es donde de verdad se va el tiempo (Rafael, 12 sep 2026:
                # «por qué retrasa tanto»). El viaje completo a casa sigue
                # siendo uno por hueco, en el `finally`.
                on_search = False
                try:
                    from as400_capture import return_to_menu

                    return_to_menu(_driver_for_sku_step())
                except Exception as e:  # noqa: BLE001
                    log.warning("auto-scan: could not recover after a %s (%s)", res["action"], e)
                    # The exception text carries the screens it saw; dropping it
                    # left the heartbeat saying the same sentence for three
                    # different jams.
                    _note_gap(f"lost the terminal after a {res['action']}: {e}")
                    return time.monotonic() - started
        log.info("auto-scan: SKU count cap reached after %d lookup(s)", done)
    except Exception:
        log.exception("auto-scan: SKU step crashed — the orders keep going")
    finally:
        # The full trip home, ONCE per gap. Each lookup only comes back to the
        # SEARCH form (home="search"), because the next one is typed there — but
        # the gap ends by handing the terminal back to the orders, and that is
        # the screen the scanner expects to find.
        if done:
            try:
                from as400_capture import return_to_order_search

                return_to_order_search(_driver_for_sku_step())
            except Exception as e:  # noqa: BLE001
                log.warning("auto-scan: the terminal didn't get home after the gap (%s)", e)
    return time.monotonic() - started


_sku_driver = None


def _driver_for_sku_step():
    """The same kind of driver the captures use. Kept module-level so the step
    doesn't pay for a new one every gap."""
    global _sku_driver
    if _sku_driver is None:
        _sku_driver = MochaDriver()
    return _sku_driver


def _loop() -> None:
    log.info(
        "auto-scanner started (from #%s) — first capture in %.0fs",
        scanned_store.next_scan_number(),
        SCAN_INITIAL_DELAY_SEC,
    )
    _interruptible_wait(SCAN_INITIAL_DELAY_SEC)
    driver = None
    paused_since = None
    # How long the AS400 has been unreachable, and whether we've said so loudly.
    _unavailable_since = None
    _unavailable_shouted = False
    _operator_since = None
    while not _stop.is_set():
        # Pause while the operator is actively using the computer, or while a manual
        # capture holds the lock — never fight the human for the keyboard. A manual
        # kick skips the activity gate: the operator asked for this pass explicitly.
        if operator_idle_seconds() < IDLE_THRESHOLD_SEC and not _kick.is_set():
            if paused_since is None:
                paused_since = time.monotonic()
            _interruptible_wait(IDLE_POLL_SEC)
            continue
        if paused_since is not None:
            # Measurement (plan F0): how much of the day the scanner spends yielding
            # the keyboard is the other half of "why did that order take so long to
            # show up". One line per pause, not one per poll.
            log.info(
                "auto-scan: resumed after %.0fs paused (the operator was on the keyboard)",
                time.monotonic() - paused_since,
            )
            paused_since = None
        if not capture_lock.acquire(blocking=False):
            # Keep a pending kick armed while the manual capture finishes, but
            # poll faster so the kicked pass starts right after it.
            _stop.wait(0.5 if _kick.is_set() else IDLE_POLL_SEC)
            continue
        _kick.clear()  # this pass consumes the manual trigger

        wait = NOT_FOUND_WAIT_SEC
        try:
            if driver is None:
                driver = MochaDriver()
            res = run_scan_step(driver)
            action = res["action"]
            wait = _wait_for(action)
            # Health beacon: every non-unavailable step means AS400 answered
            # (a not_found is still a response — "invalid order number").
            note_as400(action != "unavailable")
            if action != "unavailable":
                _unavailable_since = None
                _unavailable_shouted = False
                _operator_since = None
            if action == "unavailable":
                # Is somebody using it, or is it stuck? Unsticking a person's
                # screen is how the watcher ends up taking the terminal back
                # every few seconds; waiting out a real jam costs nothing but
                # time. Ask the screen before reaching for F6·F6·F7.
                parked = None
                try:
                    parked = classify_screen(driver.copy_screen())
                except Exception:  # noqa: BLE001 — can't read it, treat as stuck
                    parked = None
                note_as400(False, parked)
                # …pero sólo si hay alguien. El cronómetro contaba desde que
                # NOSOTROS vimos la pantalla, y la ráfaga del catálogo termina
                # justamente en una pantalla de stock — así que el watchdog se
                # apartaba diez minutos por su propio trabajo. Medido el 12 sep:
                # 33 SKUs en siete minutos (283/hora) y después 26 minutos de
                # nada, una y otra vez.
                #
                # La pregunta no es «cuánto lleva ahí esa pantalla» sino «¿tocó
                # alguien el teclado?», y esa ya tiene respuesta:
                # `operator_idle_seconds` recuerda el último evento que NO fue
                # nuestro. Es la cuarta casa de la misma lección.
                idle_now = operator_idle_seconds()
                if (
                    parked in OPERATOR_SCREENS
                    and idle_now < OPERATOR_HOLD_SEC
                    and not ignoring_operator()
                ):
                    if _operator_since is None:
                        _operator_since = time.monotonic()
                        log.info(
                            "auto-scan: somebody is on %s (idle %.0fs) — standing down",
                            parked,
                            idle_now,
                        )
                    _kick.clear()
                    _interruptible_wait(IDLE_POLL_SEC)
                    continue
                if _operator_since is not None:
                    log.info("auto-scan: taking the terminal back (idle %.0fs)", idle_now)
                _operator_since = None
                # Try to (re)connect; if it works, retry promptly next iteration.
                try:
                    bootstrap_session(driver)
                    wait = FOUND_NEXT_DELAY_SEC
                    note_as400(True)
                    _unavailable_since = None
                except Exception as e:
                    # A stuck terminal used to log at INFO — the same level as
                    # "that order doesn't exist yet" — every five minutes, for
                    # as long as it took somebody to notice. On 2026-09-08 that
                    # was 36 minutes of a working day, and on a Friday evening
                    # it would have been the weekend. After UNAVAILABLE_LOUD_SEC
                    # it says so at ERROR, once, naming what a person has to do.
                    if _unavailable_since is None:
                        _unavailable_since = time.monotonic()
                        log.info("auto-scan: AS400 not ready (%s)", e)
                    else:
                        stuck = time.monotonic() - _unavailable_since
                        if stuck >= UNAVAILABLE_LOUD_SEC and not _unavailable_shouted:
                            _unavailable_shouted = True
                            log.error(
                                "auto-scan: the AS400 has been unreachable for %.0f min and "
                                "nothing has been captured in that time. A person has to open "
                                "the session in Mocha. (%s)",
                                stuck / 60,
                                e,
                            )
                        else:
                            log.info("auto-scan: AS400 not ready for %.0f min (%s)", stuck / 60, e)
            elif action == "not_found":
                # No order yet. Instead of sleeping twenty minutes, spend that
                # time on the catalogue and then ASK AGAIN (Rafael, 2026-09-08:
                # "en ningún momento quiero al watchdog lazy, aprovechemos el
                # acceso al AS400").
                #
                # The work REPLACES the wait rather than fitting inside it: the
                # scanner used to work five minutes and then sleep fifteen more
                # for nothing. Now the loop is work → check for orders → if none,
                # work again; and when an order does turn up it captures orders
                # at the usual pace until they run out, then comes back here.
                #
                # A side effect worth having: orders are found four times sooner,
                # because the terminal is asked every budget instead of every
                # twenty minutes.
                spent = _run_sku_gap()
                if spent >= MIN_WORK_TO_SKIP_WAIT_SEC:
                    wait = FOUND_NEXT_DELAY_SEC  # the waiting already happened, usefully
                    log.info(
                        "auto-scan: not_found on #%s — spent %.0fs on the catalogue, "
                        "asking again now",
                        res["number"],
                        spent,
                    )
                else:
                    # Nothing to work on (queue empty, or the feature is off):
                    # the old pacing is still the right one, or we would ask the
                    # AS400 for the same missing order every few seconds.
                    log.info("auto-scan: %s on #%s (waiting %.0fs)", action, res["number"], wait)
            elif action == "captured":
                log.info("auto-scan: cached order #%s", res["number"])
            else:
                log.info("auto-scan: %s on #%s (waiting %.0fs)", action, res["number"], wait)
        except Exception:
            log.exception("auto-scan step crashed")
            wait = UNAVAILABLE_WAIT_SEC
        finally:
            capture_lock.release()

        _interruptible_wait(wait)


def trigger_scan_now() -> bool:
    """Wake the scanner for one immediate pass (operator's "get orders now").

    Returns False when the scanner thread isn't running (AUTO_SCAN off or not
    started) so the UI can say why nothing will happen.
    """
    if not (_thread and _thread.is_alive()):
        return False
    _kick.set()
    return True


def start_auto_scanner() -> None:
    """Start the background auto-scanner thread (idempotent). Gated by AUTO_SCAN."""
    global _thread
    if os.getenv("AUTO_SCAN", "1") not in ("1", "true", "True", "yes"):
        log.info("auto-scanner disabled (AUTO_SCAN is off)")
        return
    if _thread and _thread.is_alive():
        return
    _stop.clear()
    _thread = threading.Thread(target=_loop, daemon=True, name="auto-scanner")
    _thread.start()


def stop_auto_scanner() -> None:
    _stop.set()
