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
    return_to_order_search,
)
from parser import parse_stock_inquiry

log = logging.getLogger("pickd-sku-enrichment")


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


def max_per_gap() -> int:
    """SKUs per gap. The decision is one; the lever exists in case the gap turns
    out cheaper than measured, and it is read at call time so Bay 2 can turn it
    without a deploy."""
    return max(1, int(os.getenv("SKU_ENRICH_MAX_PER_GAP", "1")))


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


def mark_unknown(sku: str, reason: str = "not_in_as400") -> None:
    with _lock:
        data = load_unknown()
        data[sku] = {
            "reason": reason,
            "at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        }
        p = _unknown_path()
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, p)


# ── the queue (R5) ───────────────────────────────────────────────────────────


def select_sku_queue(rows, unknown=None) -> list:
    """Order the catalogue rows into the queue, most valuable first. Pure.

    1. bikes ON THE FLOOR with no model — the ones a picker can be holding.
    2. the rest with no model.
    3. bikes whose weight nobody has ever put on a scale, most ordered first
       (that ordering is the caller's — `get_bike_demand_ranking` already sorts
       by demand, and re-sorting here would be a second answer to one question).

    Rows AS400 has already refused (`unknown`), rows whose SKU is not a stock
    number (R6), and rows ALREADY READ never enter. That last one is not an
    optimisation: `model` stays empty until Pickd splits the description, so
    without it the same SKU would come back every gap for ever (Q7).
    """
    unknown = unknown or {}
    floor, rest, weights = [], [], []
    for r in rows:
        sku = (r.get("sku") or "").strip().upper()
        if not sku or sku in unknown or not is_lookupable(sku):
            continue
        if (r.get("as400_description") or "").strip():
            # Already read. AS400 has nothing left to tell us about this SKU —
            # including its weight, which came back on the same screen — and the
            # model stays empty until Pickd splits the description, so anything
            # short of skipping the whole row loops for ever (Q7).
            continue
        has_model = bool((r.get("model") or "").strip())
        in_stock = (r.get("qty") or 0) > 0
        if not has_model:
            (floor if in_stock else rest).append(r)
        elif not r.get("weight_verified"):
            weights.append(r)
    return floor + rest + weights


# ── what a write WOULD be (§6) ───────────────────────────────────────────────


def plan_write(row: dict, parsed: dict) -> dict:
    """The columns this SKU would get, given the screen. Pure, and the whole of
    the write rule lives here so F3 has nothing left to decide.

    Rafael, 2026-09-02: straight into `sku_metadata`, and ONLY onto an empty gap
    — the same pact as `customers.phone` and the sealing of `as400_account`.
    Filling a hole is safe; overwriting what a person typed is not.

      as400_description  the catalogue name, RAW and unsplit, and only when
                         `model` is empty. The watchdog does not split it:
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
      weight_lbs         only when `weight_verified` is false. The gap for the
                         weight is NOT a NULL: the trigger writes 45 into every
                         bike, so that 45 is a placeholder, not anybody's data.
                         A scale reading is never touched.
      weight_verified    never set to true by this path (R4). AS400 says 36 where
                         Pickd's scale says 33.6 for the same bike: it is better
                         than a generic 45 and it is not a weighing.

    Returns {} when there is nothing safe to write.
    """
    plan: dict = {}

    if not (row.get("weight_verified")) and parsed.get("weight_lbs") is not None:
        weight = parsed["weight_lbs"]
        if weight > 0 and weight != row.get("weight_lbs"):
            plan["weight_lbs"] = weight

    if not (row.get("model") or "").strip() and not (row.get("as400_description") or "").strip():
        description = (parsed.get("description") or "").strip()
        if description:
            plan["as400_description"] = description
            plan["as400_read_at"] = _now()

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
    n = len(res.data or [])
    if n != 1:
        # Zero means the row moved or the SKU is spelled differently; more than
        # one should be impossible (`sku` is the key). Either way, say so.
        log.warning("SKU %s: update touched %d rows, expected 1", sku, n)
    return {"written": n}


# ── the step ─────────────────────────────────────────────────────────────────


def run_sku_step(
    driver,
    row: dict,
    *,
    capture_fn=capture_stock_inquiry,
    parse_fn=parse_stock_inquiry,
    return_fn=return_to_order_search,
) -> dict:
    """Look one SKU up on AS400 and report what it found. ONE SKU, no burst.

    Returns {"action", "sku", ...}. action ∈ {read, unknown, mismatch,
    unavailable, error}. Read-only while SKU_ENRICH_WRITE is off, which is F2:
    the plan is logged, never applied.

    The return trip runs in a `finally`, because a step that leaves the terminal
    on a stock screen costs the scanner its next order — and that is the whole
    budget this feature is spending.
    """
    sku = (row.get("sku") or "").strip().upper()
    started = time.monotonic()

    try:
        result = _look_up(driver, row, sku, started, capture_fn, parse_fn)
    finally:
        # Part of the step, not a cleanup, and it runs whether the lookup worked
        # or blew up: a step that leaves the terminal on a stock screen costs the
        # scanner its next order, which is the whole budget this is spending.
        try:
            return_fn(driver)
            returned = True
        except Exception as e:
            log.warning("SKU %s: could not get back to the order search (%s)", sku, e)
            returned = False

    result["returned"] = returned
    return result


def _look_up(driver, row, sku, started, capture_fn, parse_fn) -> dict:
    """The lookup itself. Split out so the return trip above owns the `finally`
    and every path reports through one dict."""
    try:
        screen = capture_fn(sku, driver)
        parsed = parse_fn(screen)

        # The identity guard. If the screen is not showing the SKU we asked for,
        # we are on somebody else's record: nothing is read from it and nothing
        # is planned. It is the only defence against a mistyped lookup.
        if parsed.get("sku") != sku:
            log.warning(
                "SKU %s: the screen shows %s — not ours, nothing written", sku, parsed.get("sku")
            )
            return {"action": "mismatch", "sku": sku, "screen_sku": parsed.get("sku")}

        plan = plan_write(row, parsed)

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
        # PRD of its own (Q6).
        kind = parsed.get("kind")
        if kind and row.get("is_bike") is not None and (kind == "B") != bool(row.get("is_bike")):
            log.warning(
                "SKU %s: AS400 says %s, Pickd has is_bike=%s", sku, kind, row.get("is_bike")
            )
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
        return {"action": "unknown", "sku": sku}
    except StockScreenMismatch as e:
        log.warning("SKU %s: %s", sku, e)
        return {"action": "mismatch", "sku": sku}
    except (AS400Disconnected, AS400ManualLoginRequired) as e:
        log.info("SKU %s: AS400 not available (%s)", sku, e)
        return {"action": "unavailable", "sku": sku}
    except CaptureError as e:
        log.warning("SKU %s: lookup failed (%s)", sku, e)
        return {"action": "error", "sku": sku}


# ── the queue, against the database ──────────────────────────────────────────

# The queue is short on purpose: the model-less bikes first, and only when those
# run out the ones nobody has weighed. Pulling the whole catalogue every gap
# would be a lot of rows to decide one lookup.
QUEUE_FETCH_LIMIT = int(os.getenv("SKU_ENRICH_FETCH_LIMIT", "200"))

_META_COLS = "sku, model, size, color, weight_lbs, weight_verified, is_bike, as400_description"


def fetch_candidates(client=None) -> list:
    """Catalogue rows that could use a lookup, with their floor stock attached.

    Two reads, not a join: PostgREST cannot join `inventory` onto `sku_metadata`
    here, and the set is small enough that asking twice is cheaper than teaching
    it to. Returns rows shaped for `select_sku_queue`.
    """
    if client is None:
        from supabase_client import get_client

        client = get_client()

    rows = (
        client.table("sku_metadata")
        .select(_META_COLS)
        .eq("is_bike", True)
        .or_("model.is.null,model.eq.")
        .is_("as400_description", "null")  # already read → never again (Q7)
        .limit(QUEUE_FETCH_LIMIT)
        .execute()
    ).data or []

    if not rows and os.getenv("SKU_ENRICH_WEIGHT", "0") in ("1", "true", "True", "yes"):
        # F4. Only once the names are done, and behind its own switch so the two
        # phases can be separated from .env without a trip to Bay 2.
        rows = (
            client.table("sku_metadata")
            .select(_META_COLS)
            .eq("is_bike", True)
            .eq("weight_verified", False)
            .limit(QUEUE_FETCH_LIMIT)
            .execute()
        ).data or []

    skus = [r["sku"] for r in rows if is_lookupable(r.get("sku") or "")]
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
