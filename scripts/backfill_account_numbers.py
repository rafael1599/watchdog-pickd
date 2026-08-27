"""
backfill_account_numbers.py — Seal the AS400 account and ship-to onto the orders
captured before the watcher kept them.

Run ONCE on the watcher machine (the Bay 2 MacBook) right after updating to the
version that writes customers.as400_account, customer_addresses.as400_ship_to and
picking_lists.as400_account_number / ship_to_address_id. That machine holds
.scanned_orders.json, whose entries keep the full raw_text of each capture, so the
header is re-parsed from disk — the AS400 is never driven.

Dry-run by default (prints what it would do); --apply writes. Safe to re-run: every
write fills a NULL and leaves anything already set alone, so a second pass is a
no-op. Orders older than the scan cache are not in it (those get their link from
the FedEx address-book import, Pickd idea-153 phase 2).

    ./venv/bin/python3 scripts/backfill_account_numbers.py          # dry run
    ./venv/bin/python3 scripts/backfill_account_numbers.py --apply
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import scanned_store  # noqa: E402
from parser import (  # noqa: E402
    parse_account_number,
    parse_order_number,
    parse_shipping_address_struct,
    split_account_number,
)
from supabase_client import _normalized_address, get_client, split_order_numbers  # noqa: E402

LIST_COLS = "id, order_number, customer_id, as400_account_number, ship_to_address_id"
ADDR_COLS = "id, street, city, state, zip_code, as400_ship_to"


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


def backfill(apply: bool) -> dict:
    client = get_client()
    cache = scanned_store.load()
    n = dict(cache=len(cache), found=0, headers=0, accounts=0, addresses=0, links=0, skipped=0)
    n["errors"] = 0
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
            print(f"  {order_number}: {', '.join(done) or 'nothing to do'}")
        except Exception as e:  # noqa: BLE001 — one bad order must not stop the pass
            n["errors"] += 1
            print(f"  {order_number}: ERROR {e}")
    return n


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--apply", action="store_true", help="write to Pickd (default: dry run)")
    apply = ap.parse_args().apply
    print(f"{'APPLY' if apply else 'DRY RUN'} — scan cache: {scanned_store._path()}")
    n = backfill(apply)
    would = "" if apply else " (would be)"
    print(
        f"\norders in cache: {n['cache']}   found in Pickd: {n['found']}"
        f"   skipped (no account / VOID / not in Pickd): {n['skipped']}   errors: {n['errors']}\n"
        f"headers written{would}: {n['headers']}   accounts sealed{would}: {n['accounts']}"
        f"   addresses tagged{would}: {n['addresses']}   links set{would}: {n['links']}"
    )
    return 1 if n["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
