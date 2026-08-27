"""
maintenance.py — one-off actions the operator runs from the UI (⋯ → Maintenance)
instead of a terminal.

Every action has a title, a one-sentence explanation of what it does, a dry-run
("Preview") and an "Apply" mode, and returns the same shape: counts, one line per
order it looked at, and how long it took. The UI renders ACTIONS generically, so
the next action (merging duplicate customers, re-capturing an order…) is a new
entry here and nothing else.

Only one action runs at a time (`_busy`): the actions are idempotent, but two
passes interleaving their reads and writes could tag the same slot twice.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Callable

import scanned_store
from parser import (
    parse_account_number,
    parse_order_number,
    parse_shipping_address_struct,
    split_account_number,
)
from supabase_client import _normalized_address, get_client, split_order_numbers

log = logging.getLogger(__name__)

MAX_LINES = 400  # per-order lines returned to the UI; the rest is counted, not lost

LIST_COLS = "id, order_number, customer_id, as400_account_number, ship_to_address_id"
ADDR_COLS = "id, street, city, state, zip_code, as400_ship_to"


# --- backfill: AS400 account + ship-to onto orders captured before the watcher kept them


def _find_list(client, order_number: str):
    """The picking_lists row for this number: exact, or a member of a combined
    '880106 / 880107' (same rule as supabase_client.find_existing_order)."""
    lists = client.table("picking_lists")
    r = lists.select(LIST_COLS).eq("order_number", order_number).limit(1).execute()
    if r.data:
        return r.data[0]
    r = lists.select(LIST_COLS).like("order_number", f"%{order_number}%").limit(10).execute()
    for row in r.data or []:
        if order_number in split_order_numbers(row.get("order_number")):
            return row
    return None


def _load_customer(client, customer_id: str) -> dict:
    """One read per customer: account, channel flag, and all its addresses."""
    c = client.table("customers").select("as400_account, ship_to_varies")
    c = c.eq("id", customer_id).limit(1).execute()
    a = client.table("customer_addresses").select(ADDR_COLS).eq("customer_id", customer_id)
    row = (c.data or [{}])[0]
    return {
        "account": row.get("as400_account"),
        "varies": bool(row.get("ship_to_varies")),
        "addresses": a.execute().data or [],
    }


def _slot_taken_elsewhere(client, rid: str, address_id: str) -> bool:
    """Another address already carries this Recipient ID (one dealer, duplicate
    customer rows). Tagging this one would trip the partial unique index."""
    r = client.table("customer_addresses").select("id").eq("fedex_recipient_id", rid)
    r = r.limit(1).execute()
    return bool(r.data) and r.data[0]["id"] != address_id


def backfill_account_numbers(apply: bool) -> dict:
    """Seal customers.as400_account, tag customer_addresses.as400_ship_to (the DB
    trigger then mints fedex_recipient_id) and link picking_lists.ship_to_address_id
    for every order in the local scan cache. Reads .scanned_orders.json — the AS400
    is never driven. Every write fills a NULL and leaves anything set alone, so a
    second pass is a no-op. Dry-run when `apply` is False."""
    client = get_client()
    cache = scanned_store.load()
    n = dict(cache=len(cache), found=0, headers=0, accounts=0, addresses=0, links=0, skipped=0)
    n["errors"] = 0
    lines: list[str] = []
    customers: dict = {}  # customer_id → _load_customer(), updated in memory as we go
    claimed: set = set()  # Recipient IDs tagged during this pass (dry-run has no DB echo)

    for key in sorted(cache):
        text = (cache[key] or {}).get("raw_text") or ""
        order_number = parse_order_number(text) or str(key)
        raw = parse_account_number(text)
        account, ship_to = split_account_number(raw)
        if not account:  # VOID screen, no header, or an all-zero account
            n["skipped"] += 1
            continue
        try:
            pl = _find_list(client, order_number)
            if not pl:
                n["skipped"] += 1
                continue
            n["found"] += 1
            done, list_updates = [], {}

            # 1. the raw header on the order
            if not pl.get("as400_account_number"):
                list_updates["as400_account_number"] = raw
                n["headers"] += 1
                done.append(f"header {raw!r}")

            # 2. the account on the customer (fill only NULL)
            cid = pl.get("customer_id")
            cust = customers.get(cid) if cid else None
            if cid and cust is None:
                cust = customers[cid] = _load_customer(client, cid)
            if cust and not cust["account"]:
                if apply:
                    q = client.table("customers").update({"as400_account": account})
                    q.eq("id", cid).is_("as400_account", "null").execute()
                cust["account"] = account
                n["accounts"] += 1
                done.append(f"account {account}")

            # 3. the suffix on the ship-to address, and the order → address link
            ship = parse_shipping_address_struct(text) or {}
            addr = None
            if cust and ship.get("street"):
                want = _normalized_address(ship)
                addr = next((a for a in cust["addresses"] if _normalized_address(a) == want), None)
            if addr:
                if ship_to and not addr.get("as400_ship_to") and not cust["varies"]:
                    rid = cust["account"] + ship_to  # what the DB trigger will derive
                    if cust["account"] != account:
                        done.append(f"account mismatch: row has {cust['account']}, not tagged")
                    elif rid in claimed or _slot_taken_elsewhere(client, rid, addr["id"]):
                        done.append(f"slot {rid} already on another address, not tagged")
                    else:
                        if apply:
                            q = client.table("customer_addresses").update(
                                {"as400_ship_to": ship_to}
                            )
                            q.eq("id", addr["id"]).execute()
                        addr["as400_ship_to"] = ship_to
                        claimed.add(rid)
                        n["addresses"] += 1
                        done.append(f"ship-to {ship_to}")
                if not pl.get("ship_to_address_id"):
                    list_updates["ship_to_address_id"] = addr["id"]
                    n["links"] += 1
                    done.append("link")

            if list_updates and apply:
                client.table("picking_lists").update(list_updates).eq("id", pl["id"]).execute()
            lines.append(f"{order_number}: {', '.join(done) or 'nothing to do'}")
        except Exception as e:  # noqa: BLE001 — one bad order must not stop the pass
            n["errors"] += 1
            lines.append(f"{order_number}: ERROR {e}")
            log.warning("backfill %s: %s", order_number, e)

    return {"counts": n, "lines": lines[:MAX_LINES], "truncated": max(0, len(lines) - MAX_LINES)}


# --- registry ------------------------------------------------------------------

ACTIONS: dict[str, dict] = {
    "backfill_account_numbers": {
        "title": "Backfill AS400 accounts",
        "what": (
            "Fills the AS400 account and ship-to on the orders already captured, so their "
            "FedEx Recipient ID shows in PickD. Reads the local scan cache — the AS400 is "
            "never touched. Safe to repeat: it only fills blanks."
        ),
        "run": backfill_account_numbers,
    },
}

# Labels for the counts, in display order. The UI knows nothing about the action.
COUNT_LABELS = {
    "cache": "orders in cache",
    "found": "found in PickD",
    "headers": "headers written",
    "accounts": "accounts sealed",
    "addresses": "addresses tagged",
    "links": "links set",
    "skipped": "skipped (no account / VOID / not in PickD)",
    "errors": "errors",
}

_busy = threading.Lock()


class Busy(RuntimeError):
    """Another maintenance action is still running."""


def list_actions() -> list[dict]:
    return [{"id": k, "title": v["title"], "what": v["what"]} for k, v in ACTIONS.items()]


def run_action(action_id: str, apply: bool) -> dict:
    """Run one action (KeyError if unknown, Busy if another is running). The result
    carries the action id, the mode and the elapsed seconds so the UI can say
    exactly what just happened."""
    run: Callable[[bool], dict] = ACTIONS[action_id]["run"]
    if not _busy.acquire(blocking=False):
        raise Busy("another maintenance action is still running")
    try:
        t0 = time.monotonic()
        log.info("maintenance %s (%s) started", action_id, "apply" if apply else "dry run")
        result = run(apply)
        result.update(
            action=action_id,
            apply=apply,
            seconds=round(time.monotonic() - t0, 1),
            labels=COUNT_LABELS,
        )
        log.info(
            "maintenance %s finished in %ss: %s", action_id, result["seconds"], result["counts"]
        )
        return result
    finally:
        _busy.release()
