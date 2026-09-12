"""
door.py — The captures the watchdog holds, published to Pickd; the requests
Pickd makes, executed here.

Rafael, 2026-09-08: "el watcher se encarga de la parte operativa y pickd de la
visual". Until now the only way to know what the scanner had captured was to
walk to Bay 2 and press Send on this app's own UI. This module turns that
around:

  - every entry in the scan cache is PUBLISHED to `as400_captures` — parsed
    summary, the items with each one's is_bike, the raw screen text — so the
    Live Board can draw the order (FedEx/regular, customer, pallets, bikes,
    parts) before anyone taps anything;
  - a tap on "Traer" in Pickd only flips the row to `requested`; this module
    POLLS for that and sends through `process_order_text`, the exact path the
    Send button always used. Nothing travels from Pickd to Bay 2 — it cannot:
    this Mac is behind the warehouse NAT.

Two things this deliberately does NOT do, both found by review before a line
was written:

  - It does not publish from the capture path. If the watcher auto-deployed
    before the migration, PostgREST would 404 on every put, the scanner's loop
    would treat that as UNAVAILABLE and crawl at one order per five minutes.
    Publishing is a reconciler on its own thread: idempotent, best effort, its
    own backoff — and, for free, the backfill of whatever the cache already
    holds when the table appears.
  - It does not take capture_lock. Sending is database work and never drives
    the terminal. What it DOES take is `busy`, which auto_update checks so an
    update can't restart the process between a picking_lists insert and the
    row being marked sent.

The junk rules live here now — ebay, no customer, lost page, stale — because
until today they only ran when somebody opened the Bay 2 UI (`GET /api/orders`)
and, once the door replaces that UI, nothing would run them at all.
"""

from __future__ import annotations  # PEP 563: "dict | None" hints on Python 3.9 (Bay 2 Mac)

import hashlib
import json
import logging
import os
import subprocess
import threading
import time
from datetime import datetime, timezone

import scanned_store
from parser import canonical_sku, normalize_sku

log = logging.getLogger("pickd-door")


# ── switches, read at call time (a .env edit and a restart, never a deploy) ──


def enabled() -> bool:
    return os.getenv("AS400_DOOR", "1") in ("1", "true", "True", "yes")


def poll_sec() -> float:
    """How often requests are picked up. Rafael: today's orders should feel
    instant; a tap → the order on the board in about this long."""
    return max(3.0, float(os.getenv("AS400_DOOR_POLL_SEC", "10")))


def reconcile_sec() -> float:
    """How often the cache is re-published. Nothing here is urgent — the scan
    loop adds one order at a time — so a minute keeps the RPC traffic small."""
    return max(15.0, float(os.getenv("AS400_DOOR_RECONCILE_SEC", "60")))


def auto_send() -> bool:
    """The lever for the day the numbers say the manual gate is theatre: with
    it on, clean `pending` rows are requested by the reconciler itself. Off,
    because Rafael chose the gate — the human choice at Bay 2 IS today's junk
    filter — and this is measured before it is flipped."""
    return os.getenv("AS400_AUTO_SEND", "0") in ("1", "true", "True", "yes")


def hold_stale_days() -> float:
    """A capture nobody brought in after this long is probably an order that
    will not be picked. Real orders complete in a median of 1.1 h (p90 25 h);
    three days survives a weekend."""
    return float(os.getenv("AS400_HOLD_STALE_DAYS", "3"))


def archive_days() -> float:
    """Today's AUTO_ARCHIVE_DAYS, applied here instead of by a UI poll."""
    return float(os.getenv("AS400_ARCHIVE_DAYS", os.getenv("AUTO_ARCHIVE_DAYS", "8")))


# Bill-to customers whose orders are never picked here (parts-only channels).
# Shared with app.py's auto-archive so both sides agree on what junk is.
JUNK_CUSTOMERS = tuple(
    c.strip()
    for c in os.getenv("AUTO_ARCHIVE_CUSTOMERS", "EBAY PART SALES").split(",")
    if c.strip()
)


def is_junk_customer(customer) -> bool:
    norm = " ".join(str(customer or "").upper().split())
    return any(c in norm for c in JUNK_CUSTOMERS)


# ── shared with the Bay 2 UI: one send at a time per order ───────────────────
# Keyed by ORDER NUMBER, not by the UI's card id, because the door has no card
# id. Without one shared key, a tap in Pickd and a click on Bay 2 at the same
# moment run two process_order_text calls that both miss in find_existing_order
# and both create a row — there is no UNIQUE on picking_lists.order_number.
sending: set[str] = set()
sending_lock = threading.Lock()

# A send is in flight. auto_update refuses to restart the process while set.
busy = threading.Event()


def claim(order_number: str) -> bool:
    with sending_lock:
        if order_number in sending:
            return False
        sending.add(order_number)
        return True


def release(order_number: str) -> None:
    with sending_lock:
        sending.discard(order_number)


# ── the junk rules (pure) ────────────────────────────────────────────────────


def classify(entry: dict, now: datetime) -> tuple[str, str | None] | None:
    """What state a cached capture belongs in. None = never publish it.

    Order matters: a lost page is held even if the customer is fine; a junk
    customer is junk even if the capture is fresh. Stale is judged last so a
    held row keeps its more specific reason.
    """
    if not entry.get("order_number") or not (entry.get("item_count") or 0):
        return None  # the 'Invalid Order Number' screen an old scanner cached
    if is_junk_customer(entry.get("customer")):
        return ("junk", "ebay")
    if not (entry.get("customer") or "").strip() or entry.get("customer") == "Unknown":
        return ("held", "no_customer")
    if entry.get("total_mismatch"):
        return ("held", "total_mismatch")

    age_days = _age_days(entry.get("scanned_at"), now)
    if age_days is not None:
        if age_days >= archive_days():
            return ("archived", "stale")
        if age_days >= hold_stale_days():
            return ("held", "stale")
    return ("pending", None)


def _age_days(scanned_at, now: datetime):
    if not scanned_at:
        return None
    try:
        ts = datetime.fromisoformat(str(scanned_at).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return (now - ts).total_seconds() / 86400.0


# ── what gets published ──────────────────────────────────────────────────────


def build_payload(entry: dict, preview: dict, bike_skus: set) -> dict:
    """The row Pickd will draw the card from.

    Items carry the CANONICAL spelling (`03-3684BR`) and an embedded
    `sku_metadata.is_bike`, because that is what the board's own classifier
    reads: `autoClassifyShippingType` prefers the embedded flag and needs
    `pickingQty`, and its SKU lookup is an exact-string match that the parser's
    normalized `033684BR` would miss — every order would read as parts, FDX.
    """
    items = []
    for it in preview.get("items") or []:
        qty = int(it.get("qty") or 0)
        if qty <= 0:
            continue
        norm = normalize_sku(it.get("sku") or "")
        items.append(
            {
                "sku": canonical_sku(it.get("raw_sku") or it.get("sku") or ""),
                "pickingQty": qty,
                "description": it.get("description"),
                "unit_price": it.get("unit_price"),
                "sku_metadata": {"is_bike": norm in bike_skus},
            }
        )
    shipping = preview.get("shipping_address") or {}
    return {
        "source": entry.get("source"),
        "captured_at": entry.get("scanned_at"),
        "customer": preview.get("customer"),
        "ship_to": preview.get("ship_to")
        or (shipping.get("name") if isinstance(shipping, dict) else None),
        "as400_account_number": preview.get("as400_account_number")
        or entry.get("as400_account_number"),
        "order_date": preview.get("order_date"),
        "item_count": preview.get("item_count"),
        "total_units": preview.get("total_units"),
        "subtotal": preview.get("subtotal"),
        "total_mismatch": bool(preview.get("total_mismatch")),
        "items": items,
        "raw_text": entry.get("raw_text"),
    }


def _signature(status: str, reason, payload: dict) -> str:
    """So an unchanged capture is not re-published every minute."""
    body = json.dumps([status, reason, payload], sort_keys=True, default=str)
    return hashlib.sha1(body.encode("utf-8")).hexdigest()


# ── the reconciler ───────────────────────────────────────────────────────────

_published: dict[str, str] = {}  # order_number → last signature sent


def reconcile(client, *, now=None, preview_fn=None, bike_skus=None) -> dict:
    """Publish every cached capture whose state or content changed. Idempotent.

    The first run after the table appears publishes everything the cache holds
    — that is the backfill, and the stale/archive rules mean a cache full of
    old captures does not flood the door: anything past the archive age lands
    as `archived`, and 3–8 days as `held:stale`.
    """
    from pipeline import preview_order

    now = now or datetime.now(timezone.utc)
    preview_fn = preview_fn or preview_order
    if bike_skus is None:
        from supabase_client import get_bike_skus

        bike_skus = get_bike_skus()

    counts = {"published": 0, "unchanged": 0, "skipped": 0, "failed": 0}
    for number, entry in scanned_store.load().items():
        decision = classify(entry, now)
        if decision is None:
            counts["skipped"] += 1
            continue
        status, reason = decision
        try:
            preview = preview_fn(entry.get("raw_text") or "")
        except Exception as e:  # noqa: BLE001 — one bad capture must not stop the rest
            log.warning("door: could not parse cached #%s (%s)", number, e)
            counts["failed"] += 1
            continue
        payload = build_payload(entry, preview, bike_skus)
        sig = _signature(status, reason, payload)
        if _published.get(str(number)) == sig:
            counts["unchanged"] += 1
            continue
        try:
            client.rpc(
                "publish_as400_capture",
                {
                    "p_order_number": str(number),
                    "p_status": status,
                    "p_hold_reason": reason,
                    "p_payload": payload,
                },
            ).execute()
            _published[str(number)] = sig
            counts["published"] += 1
        except Exception as e:  # noqa: BLE001
            counts["failed"] += 1
            log.warning("door: publish #%s failed (%s)", number, e)
            if counts["failed"] >= 3:
                # The table isn't there, or the network is. Say it once and
                # let the next tick try again; do not walk the whole cache
                # into the same error.
                log.error(
                    "door: publishing keeps failing — is the as400_captures migration applied?"
                )
                break
    return counts


# ── the requests ─────────────────────────────────────────────────────────────

SENT_STATUSES = ("created", "appended", "reopened", "duplicate")


def outcome_of(result: dict) -> tuple[str, str | None]:
    """Map a process_order_text result onto the door's states. Pure.

    `duplicate` is `sent`: the order is already on the board, which is the
    whole point. `waiting_locked` is held with that reason so a person can
    unmark waiting in Pickd and tap again. Anything else is held with the
    pipeline's own message.
    """
    status = (result or {}).get("status")
    if status in SENT_STATUSES:
        return ("sent", None)
    if status == "waiting_locked":
        return ("held", "waiting_locked")
    return ("held", status or "error")


def send_one(client, row: dict, *, send_fn=None, find_fn=None, now=None) -> dict:
    """Execute one request. Returns what it did, for the log and the tests."""
    from pipeline import process_order_text
    from supabase_client import find_existing_order

    send_fn = send_fn or process_order_text
    find_fn = find_fn or find_existing_order
    number = str(row["order_number"])
    now_iso = (now or datetime.now(timezone.utc)).isoformat()

    if not claim(number):
        return {"order_number": number, "action": "already sending"}
    busy.set()
    try:
        # requested → sending, and only if it is still requested: a cancel from
        # the phone between the poll and here wins.
        took = (
            client.table("as400_captures")
            .update({"status": "sending", "updated_at": now_iso})
            .eq("order_number", number)
            .eq("status", "requested")
            .execute()
        ).data
        if not took:
            return {"order_number": number, "action": "no longer requested"}

        try:
            result = send_fn(row.get("raw_text") or "", source_name=f"pickd_door:{number}")
        except Exception as e:  # noqa: BLE001 — the row must say why, not the thread
            log.exception("door: send #%s crashed", number)
            client.table("as400_captures").update(
                {
                    "status": "held",
                    "hold_reason": "error",
                    "last_error": str(e)[:500],
                    "updated_at": now_iso,
                }
            ).eq("order_number", number).execute()
            return {"order_number": number, "action": "held", "reason": "error"}

        status, reason = outcome_of(result)
        if status == "sent":
            pl = result.get("picking_list") or {}
            list_id = pl.get("id")
            if not list_id:
                try:
                    found = find_fn(number)
                    list_id = (found or {}).get("id")
                except Exception:  # noqa: BLE001
                    list_id = None
            client.table("as400_captures").update(
                {
                    "status": "sent",
                    "sent_at": now_iso,
                    "picking_list_id": list_id,
                    "result": {k: v for k, v in result.items() if k != "picking_list"},
                    "last_error": None,
                    "updated_at": now_iso,
                }
            ).eq("order_number", number).execute()
            scanned_store.delete(number)
            log.info("door: sent #%s → %s", number, result.get("status"))
            return {"order_number": number, "action": "sent", "result": result.get("status")}

        client.table("as400_captures").update(
            {
                "status": "held",
                "hold_reason": reason,
                "last_error": (result.get("message") or "")[:500] or None,
                "result": {k: v for k, v in result.items() if k != "picking_list"},
                "updated_at": now_iso,
            }
        ).eq("order_number", number).execute()
        log.info("door: #%s held (%s)", number, reason)
        return {"order_number": number, "action": "held", "reason": reason}
    finally:
        busy.clear()
        release(number)


def poll_requests(client) -> int:
    rows = (
        client.table("as400_captures")
        .select("order_number, raw_text, requested_at")
        .eq("status", "requested")
        .order("requested_at")
        .limit(20)
        .execute()
    ).data or []
    for row in rows:
        send_one(client, row)
    return len(rows)


def recover_stuck(client, *, now=None, find_fn=None, stale_after_sec: float = 120.0) -> int:
    """A `sending` older than two minutes was interrupted — a restart, a crash.

    Retrying is safe because the pipeline finds an order by number and answers
    `duplicate`, so: if the picking list exists the request is done, otherwise
    it goes back to `requested` and the next poll takes it.
    """
    from supabase_client import find_existing_order

    find_fn = find_fn or find_existing_order
    now = now or datetime.now(timezone.utc)
    cutoff = datetime.fromtimestamp(now.timestamp() - stale_after_sec, tz=timezone.utc).isoformat()
    rows = (
        client.table("as400_captures")
        .select("order_number")
        .eq("status", "sending")
        .lt("updated_at", cutoff)
        .execute()
    ).data or []
    for row in rows:
        number = str(row["order_number"])
        try:
            found = find_fn(number)
        except Exception:  # noqa: BLE001
            found = None
        if found:
            client.table("as400_captures").update(
                {
                    "status": "sent",
                    "picking_list_id": found.get("id"),
                    "sent_at": now.isoformat(),
                    "updated_at": now.isoformat(),
                }
            ).eq("order_number", number).execute()
            scanned_store.delete(number)
            log.warning("door: #%s was mid-send at restart — it had landed, marked sent", number)
        else:
            client.table("as400_captures").update(
                {"status": "requested", "updated_at": now.isoformat()}
            ).eq("order_number", number).execute()
            log.warning("door: #%s was mid-send at restart — back to requested", number)
    return len(rows)


def auto_request(client) -> int:
    """AS400_AUTO_SEND: the reconciler asks for clean pending rows itself."""
    rows = (
        client.table("as400_captures")
        .select("order_number")
        .eq("status", "pending")
        .eq("total_mismatch", False)
        .execute()
    ).data or []
    now_iso = datetime.now(timezone.utc).isoformat()
    for row in rows:
        client.table("as400_captures").update(
            {"status": "requested", "requested_at": now_iso, "updated_at": now_iso}
        ).eq("order_number", row["order_number"]).eq("status", "pending").execute()
    return len(rows)


# ── the pulse ────────────────────────────────────────────────────────────────


def _version() -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            check=True,
            cwd=os.path.dirname(os.path.abspath(__file__)),
        ).stdout.strip()
    except Exception:  # noqa: BLE001
        return "unknown"


def heartbeat(client, version: str) -> None:
    """Say we are alive, and say what we are doing.

    The second half is not decoration. Why the catalogue step is idle lives in
    Bay 2's log, and reading that log means typing on Bay 2 — which IS the
    operator coming back, which is one of the reasons the step stands down. The
    diagnosis switched off the thing it was measuring (Rafael, 10 sep 2026). It
    rides the beat that was already going out; no new write, no new schedule.
    """
    row = {
        "id": 1,
        "seen_at": datetime.now(timezone.utc).isoformat(),
        "version": version,
    }
    try:
        import auto_scanner

        gap = auto_scanner.gap_state()
        row["last_gap_reason"] = gap.get("reason")
        row["last_gap_at"] = gap.get("at")
        row["skus_read_total"] = gap.get("read")
        # Whether the terminal answers at all, and where it got stuck if not.
        # This lived only in the process that paints Bay 2's own dot, so from
        # anywhere else a jammed scanner looked exactly like a quiet weekend
        # (12 sep 2026, 00:20 NY).
        h = auto_scanner.as400_health()
        row["as400_state"] = (
            h["state"] if h["state"] != "err" or not h.get("parked") else f"err: {h['parked']}"
        )
    except Exception:  # noqa: BLE001 — the beat matters more than the detail
        pass
    client.table("as400_watcher_heartbeat").upsert(row).execute()


# ── the thread ───────────────────────────────────────────────────────────────

_stop = threading.Event()
_thread: threading.Thread | None = None


def _loop() -> None:
    from supabase_client import get_client

    version = _version()
    client = None
    last_reconcile = 0.0
    failures = 0
    recovered_once = False
    log.info(
        "door: open — polling requests every %.0fs, reconciling every %.0fs",
        poll_sec(),
        reconcile_sec(),
    )

    while not _stop.is_set():
        try:
            if client is None:
                client = get_client()
            heartbeat(client, version)
            if not recovered_once:
                recover_stuck(client)
                recovered_once = True
            poll_requests(client)
            if time.monotonic() - last_reconcile >= reconcile_sec():
                counts = reconcile(client)
                if counts["published"]:
                    log.info("door: reconciled — %s", counts)
                if auto_send():
                    n = auto_request(client)
                    if n:
                        log.info("door: AS400_AUTO_SEND requested %d pending capture(s)", n)
                last_reconcile = time.monotonic()
            failures = 0
            delay = poll_sec()
        except Exception as e:  # noqa: BLE001 — the loop outlives any single failure
            failures += 1
            # Back off, and say it at ERROR only once it stops looking like a blip.
            delay = min(300.0, poll_sec() * (2 ** min(failures, 5)))
            (log.error if failures >= 3 else log.info)(
                "door: tick failed (%s) — next try in %.0fs", e, delay
            )
        _stop.wait(delay)


def start_door() -> None:
    """Start the door thread (idempotent). Gated by AS400_DOOR."""
    global _thread
    if not enabled():
        log.info("door: disabled (AS400_DOOR is off)")
        return
    if _thread and _thread.is_alive():
        return
    _stop.clear()
    _thread = threading.Thread(target=_loop, daemon=True, name="door")
    _thread.start()


def stop_door() -> None:
    _stop.set()
