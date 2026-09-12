"""
sku_enrichment.py — Fill in the bikes' catalogue data from AS400, in the gaps
between orders.

Design (docs/sku-catalog-enrichment.md, Rafael 2026-09-02/08):
  - ONE SKU per gap, never a burst. The scanner exists for the orders; this is
    what it does while there isn't one. Same `capture_lock` and the same 60 s
    operator-idle gate as a capture: it never fights the human for the keyboard.
  - Orders and SKUs interleave — the SKU step runs INSIDE the not-found wait,
    it does not replace the search for the next order.
  - The return trip to the order-search screen is part of the step, not a
    tidy-up, and it runs whether the lookup worked or blew up.
  - Bikes before customers: this queue is finite (docs/customer-enrichment.md
    starts when it reaches zero).

PHASE F2 IS READ-ONLY. `run_sku_step` drives the terminal, parses the screen and
LOGS what it would have written. It does not touch the database. `plan_write` is
the pure function that decides what a write WOULD be, so F3 turns this on by
applying that plan instead of logging it — and it is unit-tested now, before it
can do any damage with a service_role connection that has no RLS underneath it.
"""

from __future__ import annotations  # PEP 563: "dict | None" hints on Python 3.9 (Bay 2 Mac)

import json
import logging
import os
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from as400_capture import (
    AS400Disconnected,
    AS400ManualLoginRequired,
    CaptureError,
    StockScreenMismatch,
    StockSkuNotFound,
    capture_stock_inquiry,
    return_to_menu,
    return_to_order_search,
    return_to_search,
)
from parser import parse_stock_inquiry

log = logging.getLogger("pickd-sku-enrichment")


class MissingColumn(RuntimeError):
    """The catalogue column this phase writes doesn't exist on this database."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


# Off by default. The Bay 2 deploy that carries this also carries the change from
# one F6·F6·F7 to three, and the first thing a deploy does should not be to start
# driving the terminal on its own. Flip it in .env and restart — no deploy.
def enabled() -> bool:
    return os.getenv("SKU_ENRICH", "0") in ("1", "true", "True", "yes")


def writes_enabled() -> bool:
    """F3's switch. While this is off the step logs its plan and writes nothing."""
    return os.getenv("SKU_ENRICH_WRITE", "0") in ("1", "true", "True", "yes")


# ── El fin de semana del catálogo (Rafael, 12 sep 2026) ──────────────────────
#
# «Dejémoslo en solo escanear info hoy hasta que se acabe la lista y después
# recién escanear órdenes, y el domingo por la noche lo regresamos a la
# normalidad».
#
# Va con FECHA DE CADUCIDAD en lugar de un interruptor, y eso es lo importante:
# un modo excepcional que depende de que alguien se acuerde de apagarlo es un
# modo que sigue encendido el martes. Pasada la fecha, todo vuelve solo — sin
# desplegar, sin que nadie toque el Mac, sin acordarse.
#
# Lo que NO cambia mientras está activo: sigue cediendo el teclado al operario,
# al botón de «get orders now» y a un deploy pendiente. Eso no es pacing, es
# respeto por quien tiene mejor derecho, y no tiene fecha.
EXCLUSIVE_UNTIL_DEFAULT = "2026-09-14T02:00:00Z"  # domingo 13, 22:00 en NY


def exclusive_until() -> datetime:
    raw = os.getenv("SKU_ENRICH_EXCLUSIVE_UNTIL", EXCLUSIVE_UNTIL_DEFAULT).strip()
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        # El epoch, no "ahora": `catalogue_first` compara `now() < esto` y
        # evalúa el lado izquierdo primero, así que devolver "ahora" daba un
        # instante POSTERIOR y una fecha ilegible habría significado «catálogo
        # para siempre». Un valor que no se entiende tiene que caer del lado
        # seguro, y el seguro es el modo normal.
        log.warning("SKU_ENRICH_EXCLUSIVE_UNTIL no es una fecha (%r) — modo normal", raw)
        return datetime.fromtimestamp(0, timezone.utc)


def catalogue_first() -> bool:
    """Mientras dure: el catálogo se queda el hueco entero."""
    return datetime.now(timezone.utc) < exclusive_until()


def gap_budget_sec() -> float:
    """How long one gap may spend on catalogue lookups.

    Rafael asked for "unos 5 minutos" on 2026-09-02 and I talked him into one SKU
    per gap, because the queue was seven. It is 745 now (§20.2), and at one per
    gap that is 28 business days of a terminal that sits idle twenty minutes at a
    time. This is his original number, earned back by the queue's size.

    A WALL-CLOCK budget rather than a count, because what matters is how long the
    keyboard is held: a slow lookup should end the burst earlier, not later.
    """
    default = "3600" if catalogue_first() else "300"
    return max(0.0, float(os.getenv("SKU_ENRICH_GAP_BUDGET_SEC", default)))


def max_per_gap() -> int:
    """Hard cap on lookups per gap; the budget and this one both apply, whichever
    is reached first.

    Belt to the budget's braces: if a lookup ever returned instantly — a screen
    that needs no driving, a bug — the clock alone would spin. Set it to 1 to get
    the old one-per-gap cadence back with a .env edit instead of a deploy.
    """
    default = "1000" if catalogue_first() else "40"
    return max(1, int(os.getenv("SKU_ENRICH_MAX_PER_GAP", default)))


# Only an AS400 stock number can be looked up: two digits, four digits, and 0-3
# finish letters. The SHAPE is the filter (R6), so the 19 FedEx/USPS tracking
# numbers and the 15 serials are excluded by what they are rather than by a
# hand-kept list that would go stale. A tracking number given a model would land
# in FedEx's Dimensions table as though it were a carton.
STOCK_SKU_RE = re.compile(r"^\d{2}-\d{4}[A-Z]{0,3}$")


def is_lookupable(sku: str) -> bool:
    return bool(STOCK_SKU_RE.fullmatch((sku or "").strip().upper()))


# ── SKUs AS400 doesn't know (R7) ─────────────────────────────────────────────
# Local working state, next to .scanned_orders.json: a SKU the ERP has no record
# for must not come back in the next gap, or the queue jams on the first one.

_lock = threading.Lock()


def _unknown_path() -> Path:
    return Path(
        os.getenv(
            "SKU_UNKNOWN_PATH",
            str(Path(__file__).resolve().parent / ".sku_unknown.json"),
        )
    )


def load_unknown() -> dict:
    try:
        p = _unknown_path()
        if p.exists():
            data = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
    except Exception as e:
        log.warning("Could not read the unknown-SKU list: %s", e)
    return {}


def _remember(sku: str, entry: dict, data: dict | None = None) -> None:
    """Write one entry into the local set-aside list, atomically."""

    def _write(d):
        d[sku] = {
            **entry,
            "at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        }
        p = _unknown_path()
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, p)

    if data is not None:  # the caller already holds the lock
        _write(data)
        return
    with _lock:
        _write(load_unknown())


def mark_unknown(sku: str, reason: str = "not_in_as400") -> None:
    """AS400 has no such stock number. A verdict, so it carries no `until`."""
    _remember(sku, {"reason": reason})


# How long a SKU that keeps failing steps aside. Doubling, from half an hour.
DEFER_BASE_SEC = float(os.getenv("SKU_ENRICH_DEFER_SEC", "1800"))
DEFER_MAX_SEC = float(os.getenv("SKU_ENRICH_DEFER_MAX_SEC", "86400"))


def defer_sku(sku: str, reason: str) -> float:
    """Step this SKU aside for a while — it failed for a reason that is NOT
    "AS400 doesn't have it", so marking it unknown would be a lie.

    The hole this closes, found live on 12 sep 2026: `03-4605OR` kept landing on
    the STOCK INQUIRY **NOTES** form (§2.12b — same title, no fields), the guard
    correctly refused to read it, the gap ended, and the next gap asked
    `next_sku()`, which returned **the same SKU** — nothing about a mismatch
    changes the row or the unknown list. The whole catalogue sat behind one SKU:
    `skus_read_total` stayed at 0 across every gap of that build.

    A queue whose head cannot advance does not advance. So a failure that is not
    the SKU's own answer buys a cooldown instead of a verdict: half an hour,
    doubling per attempt, capped at a day. A one-off costs nothing; a chronic one
    stops blocking the other 1,800.
    """
    with _lock:
        data = load_unknown()
        tries = int((data.get(sku) or {}).get("tries") or 0) + 1
        wait = min(DEFER_BASE_SEC * (2 ** (tries - 1)), DEFER_MAX_SEC)
        _remember(sku, {"reason": reason, "tries": tries, "until": time.time() + wait}, data=data)
    log.info("SKU %s: %s — set aside for %.0f min (try %d)", sku, reason, wait / 60, tries)
    return wait


def is_set_aside(entry) -> bool:
    """Whether an entry keeps its SKU out of the queue.

    No `until` is a verdict: AS400 said it has no such number. An `until` in the
    past has served its cooldown and the SKU comes back.
    """
    if entry is None:  # nunca ha fallado: no está apartado
        return False
    if not isinstance(entry, dict):
        return True
    until = entry.get("until")
    if until is None:
        return True
    try:
        return float(until) > time.time()
    except (TypeError, ValueError):
        return True


# ── the queue (R5) ───────────────────────────────────────────────────────────


def select_sku_queue(rows, unknown=None) -> list:
    """Order the catalogue rows into the queue, most valuable first. Pure.

    Every bike that hasn't been read yet is a candidate (Rafael, 2026-09-08) —
    not only the ones with no model. Reading a bike whose model is dirty is what
    turns cleaning those 227 rows from "somebody decides whether JUV CAPRI 2.4
    carries a size" into "the manufacturer's own name says", and it costs
    nothing: `as400_description` is empty on all 836, so nothing is overwritten.

    The order, most valuable first:
      1. no model at all, on the floor — a picker can be holding it right now.
      2. no model, no stock.
      3. a model nobody split (`size` is empty), on the floor — the 263.
      4. everything else, stock first.

    Rows AS400 has already refused (`unknown`), rows whose SKU is not a stock
    number (R6), and rows ALREADY READ never enter. That last one is not an
    optimisation: `model` stays as it is until Pickd splits the description, so
    without it the same SKU would come back every gap for ever (Q7).
    """
    unknown = unknown or {}

    def rank(r):
        has_model = bool((r.get("model") or "").strip())
        has_size = bool((r.get("size") or "").strip())
        in_stock = (r.get("qty") or 0) > 0
        if not has_model:
            band = 0 if in_stock else 1
        elif not has_size:
            band = 2 if in_stock else 3
        else:
            band = 4 if in_stock else 5
        return band

    candidates = [
        r
        for r in rows
        if (r.get("sku") or "").strip().upper()
        and not is_set_aside(unknown.get((r.get("sku") or "").strip().upper()))
        and is_lookupable(r.get("sku") or "")
        # Already read: AS400 has nothing left to tell us about this SKU — the
        # weight came back on the same screen — and its model does not change
        # until Pickd splits, so anything short of skipping loops for ever (Q7).
        and not (r.get("as400_description") or "").strip()
    ]
    # Stable: rows inside a band keep the order the caller sent them in, which is
    # the demand order the RPC already decided. Two answers to one question is
    # how the ranking and the queue drift apart.
    return sorted(candidates, key=rank)


# ── what a write WOULD be (§6) ───────────────────────────────────────────────


def plan_write(row: dict, parsed: dict) -> dict:
    """The columns this SKU would get, given the screen. Pure, and the whole of
    the write rule lives here so F3 has nothing left to decide.

    Rafael, 2026-09-02: straight into `sku_metadata`, and ONLY onto an empty gap
    — the same pact as `customers.phone` and the sealing of `as400_account`.
    Filling a hole is safe; overwriting what a person typed is not.

      as400_description  the catalogue name, RAW and unsplit, whenever the
                         column is empty — which is EVERY bike, not just the
                         ones with no model (Rafael, 2026-09-08). Reading a bike
                         whose model is dirty is what turns cleaning those 227
                         rows from a judgement call into a mechanical one, and
                         it overwrites nothing: the column is the empty gap.
                         The watchdog does not split it:
                         `parseBikeName` lives in Pickd's TypeScript and is not
                         mirrored here (Rafael, 2026-09-02 and 2026-09-08 —
                         "pickd la parte"). It writes what it read; Pickd parses
                         it where the parser is. That also keeps this side clear
                         of R12: a raw name is not the FedEx grouping key, so it
                         owes no export simulation. `model` and `size` do, and
                         that is exactly why they are not written from here.
      as400_read_at      when it was read. Its presence is what stops the queue
                         asking again (Q7) — the model stays empty until Pickd
                         splits, so without this the same SKU would come back
                         every gap, forever.
    **The weight is not written, and F4 is withdrawn** (11 sep 2026). R4 says
    this path must never claim a weighing, and the database makes that
    impossible: `set_dimensions_verified` sets `weight_verified = true` on ANY
    update that changes `weight_lbs` (Pickd, 1 sep 2026 — "el que mide lo dice,
    no el valor que cambió"), and it is monotonic, so it cannot be undone
    afterwards. Writing AS400's 36 would therefore file it as a scale reading
    for a bike Pickd weighed at 33.6. The number is not lost: it rides in
    `as400_snapshot.weight_lbs`, where it says whose it is. Promoting it to
    Pickd's shipping weight is a decision with a name and a person, and the
    evidence to make it is already being collected.

    Returns {} when there is nothing safe to write.
    """
    plan: dict = {}

    if not (row.get("as400_description") or "").strip():
        description = (parsed.get("description") or "").strip()
        if description:
            plan["as400_description"] = description
            plan["as400_read_at"] = _now()
            # The WHOLE screen, not just the name. Four of the five fields we
            # parse were being thrown away — AS400's own on-hand per warehouse,
            # its weight, its B/P classification, the model year — and those are
            # exactly what answers "where do Pickd and AS400 disagree" (Rafael,
            # 2026-09-08). A jsonb blob because which field matters is the
            # question, not the answer; the one that earns a column gets it
            # later, with the evidence already collected.
            plan["as400_snapshot"] = {
                "description": description,
                "kind": parsed.get("kind"),
                "model_year": parsed.get("model_year"),
                "weight_lbs": parsed.get("weight_lbs"),
                "on_hand": parsed.get("on_hand"),
            }

    return plan


def apply_write(sku: str, plan: dict, client=None) -> dict:
    """Write the plan. Only reached when SKU_ENRICH_WRITE is on (F3).

    An `update` by primary key, never an upsert: an upsert on a SKU that somehow
    isn't in the catalogue would CREATE a metadata row with no inventory behind
    it, which is the orphan shape Pickd spent a migration cleaning up.
    """
    if not plan:
        return {"written": 0}
    if client is None:
        from supabase_client import get_client

        client = get_client()
    res = client.table("sku_metadata").update(plan).eq("sku", sku).execute()
    rows = res.data or []
    n = len(rows)
    if n != 1:
        # Zero means the row moved or the SKU is spelled differently; more than
        # one should be impossible (`sku` is the key). Either way, say so.
        log.warning("SKU %s: update touched %d rows, expected 1", sku, n)
        return {"written": n}

    # Did the write actually LAND? PostgREST silently DROPS a column that does
    # not exist — no error, and the row still comes back — so a watcher deployed
    # onto a database without `as400_description` would log a happy success,
    # write nothing, and hand the same SKU back to the queue every gap for ever.
    # The returned representation is the proof: a column that exists comes back.
    # `migrations.py` creates it during update.sh, but only when SUPABASE_DB_URL
    # is set on that machine, and it skips cleanly when it isn't — which is
    # exactly how this failure arrives without anybody noticing.
    missing = [k for k in plan if k not in rows[0]]
    if missing:
        raise MissingColumn(
            f"sku_metadata has no column(s) {missing} — the write was dropped in silence. "
            "Run migrations.py (or check SUPABASE_DB_URL) on this machine."
        )
    return {"written": n}


def register_from_as400(sku: str, parsed: dict, client=None) -> dict:
    """Give a SKU AS400 knows and PickD doesn't a row in the catalogue.

    Rafael, 11 sep 2026: «hay que preguntar incluso por sku que aun no existen
    en pickd y registrarlos en pickd con ubicación unknown, para que el usuario
    cuando lo encuentre solo mueva su ubicación a la real».

    An RPC, not an insert, and that is the point: the rules of what a catalogue
    row may claim —qty 0, `UNKNOWN`, `weight_verified` untouched— live in one
    place, next to the triggers that would otherwise contradict them, and PickD
    owns them. From here it is one call with what the screen said.

    `is_bike` comes from AS400's own `B-Bike/P-Part`, which is the authoritative
    answer to a question PickD guesses from a two-digit prefix: registered
    blind, a part in department 03 would be born a bike, with 45 lb and a bike
    carton that ends up in the FedEx export.
    """
    if client is None:
        from supabase_client import get_client

        client = get_client()

    description = (parsed.get("description") or "").strip()
    if not description:
        # Without a name there is nothing to register: the RPC would reject it,
        # and rightly — a row called nothing helps nobody find a box.
        return {"action": "skipped", "sku": sku, "why": "the screen carried no description"}

    kind = parsed.get("kind")
    res = client.rpc(
        "register_sku_from_as400",
        {
            "p_sku": sku,
            "p_item_name": description,
            "p_is_bike": (kind == "B") if kind in ("B", "P") else None,
            "p_location": "UNKNOWN",
            "p_warehouse": "LUDLOW",
            "p_as400_description": description,
            "p_as400_snapshot": {
                "description": description,
                "kind": kind,
                "model_year": parsed.get("model_year"),
                "weight_lbs": parsed.get("weight_lbs"),
                "on_hand": parsed.get("on_hand"),
            },
            "p_internal_note": None,
        },
    ).execute()
    out = res.data if isinstance(res.data, dict) else {}
    return {"action": out.get("action") or "registered", "sku": sku, "rpc": out}


# ── the step ─────────────────────────────────────────────────────────────────


def run_sku_step(
    driver,
    row: dict,
    *,
    capture_fn=capture_stock_inquiry,
    parse_fn=parse_stock_inquiry,
    return_fn=None,
    home: str = "order_search",
    on_search_screen: bool = False,
) -> dict:
    """Look one SKU up on AS400 and report what it found. ONE SKU, no burst.

    Returns {"action", "sku", ...}. action ∈ {read, unknown, mismatch,
    unavailable, error}. Read-only while SKU_ENRICH_WRITE is off, which is F2:
    the plan is logged, never applied.

    The return trip runs in a `finally`, because a step that leaves the terminal
    on a stock screen costs the scanner its next order — and that is the whole
    budget this feature is spending.
    """
    # `home="menu"` between two lookups: the menu is a valid starting point for
    # the next capture, so stopping there skips typing 3 to enter the order
    # search and F7 to leave it again. The full trip home runs once, when the
    # run ends — the caller owns that.
    if return_fn is None:
        return_fn = {
            "search": return_to_search,
            "menu": return_to_menu,
        }.get(home, return_to_order_search)
    sku = (row.get("sku") or "").strip().upper()
    started = time.monotonic()

    try:
        result = _look_up(
            driver, row, sku, started, capture_fn, parse_fn, on_search_screen=on_search_screen
        )
    finally:
        # Part of the step, not a cleanup, and it runs whether the lookup worked
        # or blew up: a step that leaves the terminal on a stock screen costs the
        # scanner its next order, which is the whole budget this is spending.
        try:
            return_fn(driver)
            returned = True
        except Exception as e:
            log.warning("SKU %s: could not get back to %s (%s)", sku, home, e)
            returned = False

    result["returned"] = returned
    return result


def _look_up(driver, row, sku, started, capture_fn, parse_fn, *, on_search_screen=False) -> dict:
    """The lookup itself. Split out so the return trip above owns the `finally`
    and every path reports through one dict."""
    try:
        screen = (
            capture_fn(sku, driver, on_search_screen=True)
            if on_search_screen
            else capture_fn(sku, driver)
        )
        parsed = parse_fn(screen)

        # The identity guard. If the screen is not showing the SKU we asked for,
        # we are on somebody else's record: nothing is read from it and nothing
        # is planned. It is the only defence against a mistyped lookup.
        if parsed.get("sku") != sku:
            log.warning(
                "SKU %s: the screen shows %s — not ours, nothing written", sku, parsed.get("sku")
            )
            defer_sku(sku, "the screen showed somebody else")
            return {
                "action": "mismatch",
                "sku": sku,
                "screen_sku": parsed.get("sku"),
                "why": f"the screen showed {parsed.get('sku')}",
            }

        log.info(
            "SKU %s in %.2fs — description=%r weight=%s kind=%s on_hand=%s",
            sku,
            time.monotonic() - started,
            parsed.get("description"),
            parsed.get("weight_lbs"),
            parsed.get("kind"),
            parsed.get("on_hand"),
        )
        # AS400 knows whether it is a bike; Pickd guesses it from a prefix. Log
        # the disagreements and change nothing — enough of them and it earns a
        # PRD of its own (Q6). For a SKU with no row yet there is no guess to
        # disagree with: AS400's answer goes straight in.
        kind = parsed.get("kind")
        description = (parsed.get("description") or "").strip()
        if kind and row.get("is_bike") is not None and (kind == "B") != bool(row.get("is_bike")):
            log.warning(
                "SKU %s: AS400 says %s, Pickd has is_bike=%s", sku, kind, row.get("is_bike")
            )
        # A SKU with no catalogue row at all is registered, not updated: there
        # is no row to fill in. Same switch as every other write.
        if row.get("unregistered"):
            if writes_enabled():
                out = register_from_as400(sku, parsed)
                log.info("SKU %s: %s at UNKNOWN — %r", sku, out["action"], description)
                return {"action": out["action"], "sku": sku, "parsed": parsed}
            log.info("SKU %s WOULD register at UNKNOWN: %r (%s)", sku, description, kind)
            return {"action": "read", "sku": sku, "parsed": parsed, "plan": {}}

        plan = plan_write(row, parsed)
        if plan and writes_enabled():
            written = apply_write(sku, plan)
            log.info("SKU %s wrote %s (%d row)", sku, plan, written["written"])
            return {"action": "written", "sku": sku, "parsed": parsed, "plan": plan}
        if plan:
            log.info("SKU %s WOULD write: %s", sku, plan)
        else:
            log.info("SKU %s: nothing to fill — every gap is already taken", sku)
        return {"action": "read", "sku": sku, "parsed": parsed, "plan": plan}

    except StockSkuNotFound as e:
        # AS400 has no record. Mark it so the queue doesn't jam on it (R7).
        mark_unknown(sku)
        log.info("SKU %s: %s — marked, won't be asked again", sku, e)
        return {"action": "unknown", "sku": sku, "why": str(e)}
    except StockScreenMismatch as e:
        # ANY mismatch steps the SKU aside, and the asymmetry is the whole
        # argument. I first deferred only the verified path's failures, on the
        # grounds that an optimistic miss is our fault and not the SKU's. True,
        # and irrelevant: setting a GOOD sku aside for half an hour costs
        # nothing — there are 1,800 others — while NOT setting a bad one aside
        # costs every gap it is handed to. Measured on 12 sep, with the seven
        # SKUs of §2.12c cycling back to the head of the queue: 77 reads an hour
        # became 36, then 2 in fifteen minutes.
        defer_sku(sku, str(e))
        log.warning("SKU %s: %s", sku, e)
        return {"action": "mismatch", "sku": sku, "why": str(e)}
    except (AS400Disconnected, AS400ManualLoginRequired) as e:
        log.info("SKU %s: AS400 not available (%s)", sku, e)
        return {"action": "unavailable", "sku": sku, "why": str(e)}
    except MissingColumn as e:
        # Not this SKU's problem and not something a retry fixes: every write
        # would vanish the same way. Stop the queue, loudly.
        log.error("SKU %s: %s", sku, e)
        return {"action": "unavailable", "sku": sku, "why": str(e)}
    except CaptureError as e:
        log.warning("SKU %s: lookup failed (%s)", sku, e)
        return {"action": "error", "sku": sku, "why": str(e)}


# ── the queue, against the database ──────────────────────────────────────────

# The queue is short on purpose: the model-less bikes first, and only when those
# run out the ones nobody has weighed. Pulling the whole catalogue every gap
# would be a lot of rows to decide one lookup.
QUEUE_FETCH_LIMIT = int(os.getenv("SKU_ENRICH_FETCH_LIMIT", "200"))

_META_COLS = "sku, model, size, color, weight_lbs, weight_verified, is_bike, as400_description"


def fetch_candidates(client=None) -> list:
    """Rows that could use a lookup, with their floor stock attached.

    Three queues in order, one at a time: the SKUs AS400 named on a paper and
    the catalogue does not have, then the bikes nobody has read, then the parts.

    Two reads, not a join: PostgREST cannot join `inventory` onto `sku_metadata`
    here, and the set is small enough that asking twice is cheaper than teaching
    it to. Returns rows shaped for `select_sku_queue`.
    """
    if client is None:
        from supabase_client import get_client

        client = get_client()

    # The SKUs AS400 printed on a paper and the catalogue does not have, FIRST
    # (Rafael, 11 sep 2026). Three reasons it jumps the bike queue: it is tiny
    # and finite (55 at the time of writing, ~6 minutes of terminal), every one
    # of them is a line that says UNREG in Double Check *today*, and the alta
    # cures those orders without anybody opening them
    # (`zz_touch_open_orders_for_sku`). The other two queues are 1,800 SKUs of
    # catalogue tidying: they can wait six minutes.
    #
    # `looks_like` is the view's own filter: a variant sibling would split what
    # idea-154 merged, a Scratch & Dent number is one bike sold once, and a
    # billing line is not a box on a shelf. All three stay visible in the view;
    # only `merchandise` gets registered.
    rows = [
        {"sku": r["sku"], "last_name": r.get("last_name"), "unregistered": True}
        for r in (
            client.table("v_as400_skus_unregistered")
            .select("sku, last_name, last_seen, looks_like")
            .eq("looks_like", "merchandise")
            .order("last_seen", desc=True)
            .limit(QUEUE_FETCH_LIMIT)
            .execute()
        ).data
        or []
        if is_lookupable(r.get("sku") or "")
    ]

    # Then the bikes, and strictly: parts only come up once no bike is left to
    # ask about. Same cadence the catalogue work already has with the customers
    # (docs/customer-enrichment.md) — one finite queue at a time, in the order
    # somebody would work them.
    if not rows:
        rows = [
            r
            for r in (
                client.table("sku_metadata")
                .select(_META_COLS)
                .eq("is_bike", True)
                .is_("as400_description", "null")  # every bike, once (Q7)
                .limit(QUEUE_FETCH_LIMIT)
                .execute()
            ).data
            or []
            if is_lookupable(r.get("sku") or "")
        ]

    if not rows:
        # The parts. They were out of scope while this was about the bike
        # CATALOGUE — a part has no model, size or colour to split. They are
        # very much in scope now that the same screen answers "what does AS400
        # think is on the shelf": parts are 1,351 of the 1,923 SKUs with stock
        # and **95% of the units** (Rafael, 11 sep 2026 — "sea de sku, clientes,
        # ordenes"). 440 of them have a shape AS400 cannot look up at all (UPCs,
        # serials) and `is_lookupable` drops those without a list to maintain.
        rows = [
            r
            for r in (
                client.table("sku_metadata")
                .select(_META_COLS)
                .neq("is_bike", True)
                .is_("as400_description", "null")
                .limit(QUEUE_FETCH_LIMIT)
                .execute()
            ).data
            or []
            if is_lookupable(r.get("sku") or "")
        ]

    skus = [r["sku"] for r in rows]
    if not skus:
        return []

    qty: dict = {}
    inv = (
        client.table("inventory")
        .select("sku, quantity")
        .in_("sku", skus)
        .gt("quantity", 0)
        .execute()
    ).data or []
    for i in inv:
        qty[i["sku"]] = qty.get(i["sku"], 0) + (i.get("quantity") or 0)

    for r in rows:
        r["qty"] = qty.get(r["sku"], 0)
    return rows


def next_sku(client=None) -> dict | None:
    """The one SKU this gap should look up, or None when the queue is empty."""
    queue = select_sku_queue(fetch_candidates(client), load_unknown())
    return queue[0] if queue else None


# The manual run lives on its own thread: an unbounded loop cannot sit inside a
# Flask request, and this one is meant to last until somebody touches the Mac.
_run_thread: threading.Thread | None = None
_run_stop = threading.Event()
_run_last: dict = {}


def catalogue_run_active() -> bool:
    return bool(_run_thread and _run_thread.is_alive())


def stop_catalogue_run() -> None:
    _run_stop.set()


def start_catalogue_run(open_driver, lock, note_as400=None) -> bool:
    """Start the operator's run in the background. False if one is already up.

    The lock is taken by the THREAD, not by the caller, and released in its
    `finally` — a run that lasts until the operator returns cannot borrow the
    request's lifetime for either.
    """
    global _run_thread
    if catalogue_run_active():
        return False
    if not lock.acquire(blocking=False):
        return False
    _run_stop.clear()

    def _body():
        # The manual run reports to the heartbeat too. Without this the only
        # thing visible from outside Bay 2 was the gap loop, so "is a run going
        # right now?" could only be answered by walking to the Mac — which is
        # the trap this whole beacon exists to avoid.
        def _note(out):
            try:
                import auto_scanner

                auto_scanner._note_gap("working (manual run)", read=1)
            except Exception:  # noqa: BLE001
                pass

        try:
            driver = open_driver()
            result = run_until_disturbed(driver, stop_fn=_run_stop.is_set, note=_note)
            _run_last.clear()
            _run_last.update(result)
            log.info("catalogue run finished: %s", result)
            try:
                import auto_scanner

                auto_scanner._note_gap(f"manual run ended: {result.get('stopped')}")
            except Exception:  # noqa: BLE001
                pass
            if note_as400:
                note_as400(True)
        except Exception as e:  # noqa: BLE001 — a side errand may not take the app down
            _run_last.clear()
            _run_last.update({"stopped": f"error: {e}"})
            log.exception("catalogue run failed")
        finally:
            lock.release()

    _run_thread = threading.Thread(target=_body, daemon=True, name="catalogue-run")
    _run_thread.start()
    return True


def catalogue_run_state() -> dict:
    return {"active": catalogue_run_active(), "last": dict(_run_last)}


def grace_sec() -> float:
    """How long after the click the operator's hands are ignored.

    Without it the run is unusable in the way it is meant to be used: you press
    the button and you are, by definition, AT the Mac — so the very next check
    reads your hand on the mouse and stops. You would never see it work, which
    is the same trap as having to type on Bay 2 to find out why Bay 2 is idle.
    Two minutes is enough to stand up and leave.
    """
    return max(0.0, float(os.getenv("SKU_ENRICH_GRACE_SEC", "120")))


def run_until_disturbed(
    driver,
    *,
    idle_fn=None,
    kick_fn=None,
    update_pending_fn=None,
    stop_fn=None,
    note=None,
    step_fn=None,
    grace=None,
    home_fn=None,
) -> dict:
    """Read SKUs back to back until something asks for the terminal. Pure-ish.

    Rafael, 10 sep 2026: "quiero que cuando lo active manualmente no se pare
    hasta que yo mueva algo… si yo quiero órdenes regreso y presiono get orders
    now". So the operator's own hands are the brake, not a count and not a
    budget.

    The idle gate cannot be the usual "idle < 60": the operator just clicked a
    button, so idle is ZERO at the start and the run would stop before its first
    lookup. What matters is whether anybody touched the Mac after the grace
    window ran out — and if nobody has, `operator_idle_seconds()` grows at least
    as fast as our own clock. Anything smaller is a hand. See `grace_sec`.

    It has to be `operator_idle_seconds` and not the raw HID reading: the driver
    types with real input events, so a run that measured raw idle would read its
    own keystrokes as the operator returning and stop after the grace — which is
    exactly what happened on 11 sep 2026, seven SKUs in, with nobody there.

    It stops for three other things, and each is somebody with a better claim:
      - "get orders now": they asked for orders, not for the catalogue.
      - a pending deploy: this run holds `capture_lock` and the updater refuses
        to restart during a capture, so without yielding it would keep Bay 2 on
        an old build for as long as the run lasts.
      - the AS400 going away: hammering a terminal that is not there is how a
        session gets stuck.
    """
    from auto_scanner import _kick, operator_idle_seconds

    idle_fn = idle_fn or operator_idle_seconds
    kick_fn = kick_fn or _kick.is_set
    stop_fn = stop_fn or (lambda: False)
    step_fn = step_fn or run_sku_step
    home_fn = home_fn or return_to_order_search
    if update_pending_fn is None:
        import auto_update

        update_pending_fn = auto_update.update_pending.is_set

    started = time.monotonic()
    grace = grace_sec() if grace is None else grace
    on_search = False  # the first lookup navigates the verified way
    out = {"read": 0, "unknown": 0, "failed": 0, "stopped": None}

    while True:
        if stop_fn():
            out["stopped"] = "asked to stop"
            break
        # Touched since the grace ran out? When nobody is there, idle grows at
        # least as fast as our own clock, so anything smaller than the time
        # since (start + grace) is a hand. Movement DURING the grace is the
        # operator walking away and does not count.
        elapsed = time.monotonic() - started
        if elapsed > grace and idle_fn() < elapsed - grace:
            out["stopped"] = "the operator is back"
            break
        if kick_fn():
            out["stopped"] = "orders requested"
            break
        if update_pending_fn():
            out["stopped"] = "an update is waiting"
            break

        row = next_sku()
        if not row:
            out["stopped"] = "the queue is empty"
            break

        # Optimistic from the second lookup on: the last one ended with Cmd7,
        # which lands on the search form with the fields blank, so the next SKU
        # is typed where we stand — no read, no menu, no option 2. The first one
        # of a run still navigates the verified way, because we do not know
        # where the terminal was left.
        res = step_fn(driver, row, home="search", on_search_screen=on_search)
        action = res.get("action")
        if action in ("read", "written"):
            out["read"] += 1
        elif action == "unknown":
            out["unknown"] += 1
        else:
            out["failed"] += 1
        if action == "unavailable":
            out["stopped"] = "the AS400 is not available"
            break

        # Stay optimistic while it keeps working. The moment one comes back
        # anything other than a clean read, we do not know what is on the
        # screen any more: walk the verified way home and verify the next one.
        if action in ("read", "written"):
            on_search = True
        else:
            on_search = False
            try:
                home_fn(driver)
            except Exception as e:  # noqa: BLE001
                log.warning("catalogue run: could not recover after %s (%s)", action, e)
                out["stopped"] = f"lost the terminal after a {action}"
                break

        if note:
            note(out)

    # One full trip home for the whole run, not one per lookup. A terminal left
    # on a stock screen costs the scanner its next order, so this is not
    # optional — it is just not needed thirty times in a row.
    try:
        home_fn(driver)
    except Exception as e:  # noqa: BLE001
        log.warning("catalogue run: could not get back to the order search (%s)", e)
        out["stopped"] = f"{out['stopped']} (did not get home: {e})"

    return out
