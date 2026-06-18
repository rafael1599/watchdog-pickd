"""
app.py — Local web UI to capture AS400 orders and send them to PickD one by one.

Run on the Mac that has Mocha TN5250 open and logged in:

    pip install -r requirements.txt
    python3 app.py
    # open http://127.0.0.1:5000

Flow:
    1. Type an order number → "Capturar" drives Mocha (AS400) and reads the screens.
    2. The captured order shows a preview: order number, customer, total item count.
    3. Review it, then "Enviar a PickD" pushes that single order into Supabase.

The capture layer only works on macOS. Sending requires the Supabase env vars (.env).
"""

from __future__ import annotations  # PEP 563: keep "dict | None" annotations working on Python 3.9

import json
import logging
import os
import subprocess
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()  # must run before importing modules that read env at import time

from flask import Flask, abort, jsonify, render_template_string, request  # noqa: E402

import auto_scanner  # noqa: E402
import scanned_store  # noqa: E402
from as400_capture import (  # noqa: E402
    AS400Disconnected,
    AS400ManualLoginRequired,
    CaptureError,
    MochaDriver,
    OrderVoidSkip,
    activate_app,
    bootstrap_session,
    capture_order,
    classify_screen,
    frontmost_app_name,
)
from auto_scanner import capture_lock, manual_waiting, start_auto_scanner  # noqa: E402
from pipeline import (  # noqa: E402
    estimate_pallets,
    meaningful_note,
    preview_order,
    process_order_text,
    resolve_order_items,
)
from supabase_client import (  # noqa: E402
    find_orders_in_pickd,
    get_bike_skus,
    get_verification_board,
    get_verification_count,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")


def _build_version() -> str:
    """Git short SHA of the running code — shown in the UI footer and served at
    /api/version so update.sh can verify the restart actually picked up the new
    build (an answering server is NOT proof: a stale process or a LaunchAgent
    pointing at another clone serves the old UI with a 200)."""
    try:
        out = subprocess.run(
            ["git", "-C", str(Path(__file__).resolve().parent), "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=3,
            check=True,
        )
        return out.stdout.strip() or "unknown"
    except Exception:  # noqa: BLE001
        return "unknown"


BUILD_VERSION = _build_version()

app = Flask(__name__)

# Only loopback hosts are valid. Rejecting other Host headers blocks DNS-rebinding
# attacks; rejecting cross-site Origins blocks a malicious page (open in the same
# Mac's browser) from driving the app via CSRF. The server is bound to 127.0.0.1,
# so it is not reachable from the local network at all.
PORT = 5000
_ALLOWED_HOSTS = {f"127.0.0.1:{PORT}", f"localhost:{PORT}"}
_ALLOWED_ORIGINS = {f"http://127.0.0.1:{PORT}", f"http://localhost:{PORT}"}


@app.before_request
def _guard_localhost_only():
    if request.headers.get("Host") not in _ALLOWED_HOSTS:
        abort(403)
    origin = request.headers.get("Origin")
    if origin and origin not in _ALLOWED_ORIGINS:
        abort(403)


# In-memory queue of captured orders for this session.
_orders: dict[int, dict] = {}
_next_id = 1
_lock = threading.Lock()
# Order ids with a send currently in flight (server-side double-click guard).
_sending: set[int] = set()

# Archived orders the operator chose NOT to send. Unlike the pending queue these
# are persisted to a local JSON file so they survive an app restart. Keyed by a
# stable archive id (aid). The file is gitignored — it is local working state.
ARCHIVE_PATH = Path(__file__).resolve().parent / ".archived_orders.json"
_archive: dict[str, dict] = {}

# Unsent candidate orders older than this many days are auto-archived (they leave
# the active list but stay recoverable in the archive). Sent orders are excluded.
AUTO_ARCHIVE_DAYS = int(os.getenv("AUTO_ARCHIVE_DAYS", "8"))

# Orders billed to these customers are parts-only and never picked in the
# warehouse: they are archived on arrival instead of parking in the pending
# queue (still recoverable via Restore). Matched as a case/whitespace-
# insensitive substring of the parsed Bill-to customer.
AUTO_ARCHIVE_CUSTOMERS = ("EBAY PART SALES",)

# Verification-board read cache. The UI polls /api/verification on every load(),
# so we throttle the Supabase read behind a short TTL to avoid hammering it.
VERIFICATION_TTL_SEC = int(os.getenv("VERIFICATION_TTL_SEC", "30"))
_verification_cache: dict = {"ts": 0.0, "data": None}
_verification_lock = threading.Lock()


def _refresh_verification() -> dict:
    """Return {count, board}, refreshing from Supabase at most once per TTL.

    On error (e.g. Supabase env not set) returns a cached value if present, else
    a zeroed snapshot — the UI counter should never crash the page.
    """
    with _verification_lock:
        now = time.monotonic()
        cached = _verification_cache["data"]
        if cached is not None and (now - _verification_cache["ts"]) < VERIFICATION_TTL_SEC:
            return cached
        try:
            data = {"count": get_verification_count(), "board": get_verification_board()}
        except Exception as e:  # noqa: BLE001
            logging.warning("Verification read failed: %s", e)
            return cached if cached is not None else {"count": 0, "board": {}}
        _verification_cache["ts"] = now
        _verification_cache["data"] = data
        return data


def _load_archive() -> None:
    """Load archived orders from disk into memory (best-effort)."""
    global _archive
    try:
        if ARCHIVE_PATH.exists():
            data = json.loads(ARCHIVE_PATH.read_text(encoding="utf-8"))
            _archive = {e["aid"]: e for e in data if e.get("aid")}
    except Exception as e:
        logging.warning("Could not load archive: %s", e)
        _archive = {}


def _persist_archive() -> None:
    """Write the archive to disk (best-effort; never raises to the request)."""
    try:
        ARCHIVE_PATH.write_text(
            json.dumps(list(_archive.values()), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as e:
        logging.warning("Could not save archive: %s", e)


def _compare_items(archived_items: list, new_items: list) -> dict:
    """Diff an archived order's items against a fresh capture's, by SKU→qty.

    Used when re-capturing an order whose number is already archived: tells the
    operator whether the new scan is identical or what changed, so they can decide
    whether to pull the archived copy back out or keep the fresh one.
    """

    def by_sku(items):
        m = {}
        for it in items:
            sku = it.get("sku") or it.get("raw_sku") or "?"
            qty = int(it.get("qty") or it.get("pickingQty") or 0)
            m[sku] = m.get(sku, 0) + qty
        return m

    old, new = by_sku(archived_items), by_sku(new_items)
    added = [s for s in new if s not in old]
    removed = [s for s in old if s not in new]
    changed = [s for s in old if s in new and old[s] != new[s]]
    identical = not (added or removed or changed)

    parts = []
    if added:
        parts.append(f"{len(added)} SKU nuevo(s)")
    if removed:
        parts.append(f"{len(removed)} faltante(s)")
    if changed:
        parts.append(f"{len(changed)} cantidad(es) distinta(s)")
    return {"identical": identical, "summary": "idéntica" if identical else ", ".join(parts)}


def _find_archived_by_number(order_number) -> dict | None:
    if not order_number:
        return None
    return next((a for a in _archive.values() if a.get("order_number") == order_number), None)


def _is_auto_archive_customer(customer) -> bool:
    """True when the Bill-to customer marks a parts-only order (e.g. eBay)."""
    norm = " ".join(str(customer or "").upper().split())
    return any(c in norm for c in AUTO_ARCHIVE_CUSTOMERS)


def _archive_entry(entry: dict) -> str:
    """Move a pending order dict into the persisted local archive. Returns its aid.

    Caller must hold _lock. Strips the in-memory-only fields (id/sent/result) and
    stamps an archived_at timestamp, mirroring the manual /archive endpoint.
    """
    aid = uuid.uuid4().hex
    arch = {k: v for k, v in entry.items() if k not in ("id", "sent", "result")}
    arch["aid"] = aid
    arch["archived_at"] = datetime.now(timezone.utc).isoformat()
    _archive[aid] = arch
    return aid


def _auto_archive_stale() -> int:
    """Auto-archive UNSENT candidate orders older than AUTO_ARCHIVE_DAYS.

    Sent orders are never touched (they already collapse into their own section).
    Returns the number of orders archived. Caller must hold _lock.
    """
    if AUTO_ARCHIVE_DAYS <= 0:
        return 0
    cutoff = datetime.now(timezone.utc) - timedelta(days=AUTO_ARCHIVE_DAYS)
    stale_ids = []
    for oid, entry in _orders.items():
        if entry.get("sent"):
            continue
        scanned_at = entry.get("scanned_at")
        if not scanned_at:
            continue
        try:
            ts = datetime.fromisoformat(scanned_at)
        except (ValueError, TypeError):
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if ts < cutoff:
            stale_ids.append(oid)

    for oid in stale_ids:
        _archive_entry(_orders.pop(oid))
    if stale_ids:
        _persist_archive()
    return len(stale_ids)


def _add_order(raw_text: str, auto_archive: bool = True) -> dict:
    global _next_id
    preview = preview_order(raw_text)
    # Pallet estimate (same rule PickD applies): needs the bike catalog, which is
    # one cached Supabase query per hour. Fail-open: without DB access the card
    # simply falls back to showing units only.
    try:
        pallets_est = estimate_pallets(preview["items"], get_bike_skus())
    except Exception as e:  # noqa: BLE001
        logging.debug("Pallet estimate unavailable: %s", e)
        pallets_est = None
    with _lock:
        oid = _next_id
        _next_id += 1
        # If this order number is already archived, attach a non-blocking warning
        # plus a content comparison so the operator can decide what to do.
        match = _find_archived_by_number(preview["order_number"])
        archived_match = None
        if match:
            cmp = _compare_items(match.get("items", []), preview["items"])
            archived_match = {
                "aid": match["aid"],
                "archived_at": match.get("archived_at"),
                "identical": cmp["identical"],
                "summary": cmp["summary"],
            }
        entry = {
            "id": oid,
            "order_number": preview["order_number"],
            "customer": preview["customer"],
            "item_count": preview["item_count"],
            "total_units": preview["total_units"],
            "pallets_est": pallets_est,
            "subtotal": preview.get("subtotal"),
            "parsed_total": preview.get("parsed_total"),
            "total_mismatch": preview.get("total_mismatch", False),
            "ship_via": preview.get("ship_via"),
            "shipping_type": preview.get("shipping_type"),
            "order_date": preview.get("order_date"),
            "shipping_address": preview.get("shipping_address"),
            "order_comments": preview.get("order_comments"),
            # Red note on the card: only meaningful comments (freight boilerplate
            # filtered). The FULL order_comments still goes to PickD on send.
            "order_note_display": meaningful_note(preview.get("order_comments")),
            "items": preview["items"],
            "archived_match": archived_match,
            "raw_text": raw_text,
            "scanned_at": datetime.now(timezone.utc).isoformat(),
            "sent": False,
            "result": None,
        }
        if auto_archive and _is_auto_archive_customer(entry["customer"]):
            # Parts-only customer (e.g. EBAY PART SALES): straight to the
            # archive — never a pending card. Recoverable via Restore, which
            # re-adds with auto_archive=False so it isn't swallowed again.
            entry["auto_archived"] = True
            _archive_entry(entry)
            _persist_archive()
        else:
            _orders[oid] = entry
    if entry.get("auto_archived"):
        # Purge any scanned-cache copy, or the auto-scan sweep would archive a
        # fresh duplicate on every refresh.
        if entry.get("order_number"):
            try:
                scanned_store.delete(entry["order_number"])
            except Exception as e:  # noqa: BLE001
                logging.warning(
                    "Could not purge cache for auto-archived #%s: %s",
                    entry["order_number"],
                    e,
                )
        logging.info("Auto-archived order #%s (%s)", entry["order_number"], entry["customer"])
    return entry


def _public(entry: dict) -> dict:
    """Strip raw_text from the payload sent to the browser."""
    return {k: v for k, v in entry.items() if k != "raw_text"}


@app.get("/")
def index():
    return render_template_string(INDEX_HTML, version=BUILD_VERSION)


@app.get("/api/version")
def version():
    """Build identity for humans and for update.sh's post-restart verification."""
    return jsonify({"version": BUILD_VERSION})


def _sync_scanned_into_orders() -> None:
    """Materialize auto-scanned cache entries as full, sendable orders.

    The auto-scanner stores captures in scanned_store; we surface each as a normal
    order card (no separate 'load to review' step). Deduped by order_number AND by
    cache key, so a manual capture never shows twice. Sent/archived/removed orders
    are deleted from the cache, so they don't re-appear here.

    Junk guard: a cache entry whose raw_text doesn't parse to a real order (no
    order number or zero items — e.g. an 'Invalid Order Number, REENTER' screen
    cached by an older scanner) is PURGED from the cache instead of surfaced.
    Without this, its parsed order_number is None, the dedup-by-number never
    matches, and every refresh re-adds another empty 'Order #—' card.
    """
    cache = scanned_store.load()
    with _lock:
        existing = {o["order_number"] for o in _orders.values() if o.get("order_number")}
        materialized = {o.get("cache_key") for o in _orders.values() if o.get("cache_key")}
    for cache_key, e in cache.items():
        if cache_key in existing or cache_key in materialized:
            continue
        raw = e.get("raw_text")
        if not raw:
            scanned_store.delete(cache_key)
            continue
        entry = _add_order(raw)
        if entry.get("auto_archived"):
            # Parts-only customer: archived on arrival; the cache copy was
            # already purged by _add_order, so it won't re-materialize.
            continue
        if not entry.get("order_number") or not (entry.get("item_count") or 0):
            # Junk capture: drop it from both the session list and the cache.
            with _lock:
                _orders.pop(entry["id"], None)
            scanned_store.delete(cache_key)
            logging.warning("Purged junk scanned entry %s (no order number/items)", cache_key)
            continue
        with _lock:
            entry["from_cache"] = True
            entry["cache_key"] = cache_key
            entry["scanned_at"] = e.get("scanned_at")
            entry["source"] = e.get("source", "auto_scan")
            entry["in_pickd"] = bool(e.get("in_pickd"))
        existing.add(entry["order_number"])
        materialized.add(cache_key)


# Throttle for the batched "does it already exist in PickD?" check, so the UI's
# periodic refresh doesn't query Supabase every 8s. One tiny query per TTL.
PICKD_CHECK_TTL_SEC = float(os.getenv("PICKD_CHECK_TTL_SEC", "60"))
_pickd_check = {"at": 0.0, "found": set()}


def _refresh_pickd_status() -> None:
    """Mark candidates that already exist in PickD (batched + throttled).

    Orders reach PickD by other paths too (PDF drop, another machine), so a
    candidate sitting in this list may already be there — noise the operator
    asked to filter out. find_orders_in_pickd applies the same membership rule
    as the send pipeline (exact or inside a combined "A / B" number). Matches
    are flagged in_pickd (persisted in the scanned cache, so the flag survives
    a restart) and the UI moves them to a collapsed section. Nothing is deleted.
    """
    with _lock:
        pending = {
            o["order_number"]: o
            for o in _orders.values()
            if o.get("order_number") and not o["sent"] and not o.get("in_pickd")
        }
    if not pending:
        return
    now = time.time()
    if now - _pickd_check["at"] < PICKD_CHECK_TTL_SEC:
        found = _pickd_check["found"]
    else:
        try:
            found = find_orders_in_pickd(list(pending))
        except Exception as e:
            logging.warning("PickD existence check failed: %s", e)
            return
        _pickd_check["at"] = now
        _pickd_check["found"] = found
    for num, entry in pending.items():
        if num in found:
            with _lock:
                entry["in_pickd"] = True
            scanned_store.update_meta(num, in_pickd=True)


@app.get("/api/orders")
def list_orders():
    _sync_scanned_into_orders()
    _refresh_pickd_status()
    with _lock:
        _auto_archive_stale()  # sweep stale unsent candidates into the archive
        # Newest first (by id, which increments as orders are added).
        ordered = sorted(_orders.values(), key=lambda o: o["id"], reverse=True)
        return jsonify([_public(o) for o in ordered])


@app.get("/api/search")
def search_orders():
    """Search the scan cache for orders the daemon already has — no AS400 round-trip.

    Backs the UI's live filter so cache-only orders (captured by the auto-scanner but
    not yet materialized into the visible list) are still findable while typing. The
    already-visible orders are filtered client-side from the data the page already
    holds; this endpoint only adds the not-yet-materialized tail."""
    q = request.args.get("q", "")
    return jsonify(scanned_store.search(q))


# Trailing digits the operator types by hand; the prefill supplies every leading
# digit before them (a tail of 2 means a prefill like "8802" for order 880267).
PREFILL_TAIL_DIGITS = 2


@app.get("/api/order-prefix")
def order_prefix():
    """Leading digits of the latest order, for the search box's capture prefill.

    The operator types only the last two digits to capture the next order; the UI
    pre-fills everything before them. Derived from the highest auto-scanned number
    (manual one-off captures of old orders are excluded so they don't drag it back),
    so it follows the live sequence and rolls to the next hundred on its own when the
    latest order crosses a boundary (…99 → …00)."""
    latest = scanned_store.latest_number()
    s = str(latest)
    prefix = s[:-PREFILL_TAIL_DIGITS] if len(s) > PREFILL_TAIL_DIGITS else ""
    return jsonify({"latest": latest, "prefix": prefix})


@app.post("/api/connect")
def connect():
    """Launch the AS400 emulator and log in — verifying state, not assuming it."""
    try:
        state = bootstrap_session(MochaDriver())
    except AS400Disconnected as e:
        auto_scanner.note_as400(False)
        return jsonify({"ok": False, "state": "disconnected", "error": str(e)}), 503
    except AS400ManualLoginRequired as e:
        auto_scanner.note_as400(False)
        return jsonify({"ok": False, "state": "needs_login", "error": str(e)}), 409
    except Exception as e:
        return jsonify({"ok": False, "error": f"Error connecting to AS400: {e}"}), 500
    auto_scanner.note_as400(True)
    return jsonify(
        {"ok": True, "state": state, "message": "AS400 ready on the order-search screen."}
    )


@app.get("/api/as400")
def as400_status():
    """AS400 health for the UI dot — fed by the scanner and manual captures, so
    the dot turns green from real activity instead of requiring a manual check."""
    return jsonify(auto_scanner.as400_health())


@app.post("/api/status")
def status():
    """Read the current Mocha screen and classify it, without driving anything."""
    try:
        text = MochaDriver().copy_screen()
    except CaptureError as e:
        return jsonify({"error": str(e)}), 422
    except Exception as e:
        return jsonify({"error": f"Error reading screen: {e}"}), 500
    return jsonify({"state": classify_screen(text)})


def _restore_focus(prev_app) -> None:
    """Hand macOS focus back to the app the operator was in (their browser).

    Best-effort only: a focus failure must never break the capture response.
    Every step logs at a visible level so a non-working restore is diagnosable
    from the app log (look for "focus:" lines).
    """
    if not prev_app:
        logging.warning("focus: no snapshot of the previous app — restore skipped")
        return
    try:
        # Let Mocha settle for an instant so the activation isn't swallowed
        # by its last keystroke/clipboard operation.
        time.sleep(0.2)
        activate_app(prev_app)
        logging.info("focus: returned to %s after the capture", prev_app)
    except Exception as e:  # noqa: BLE001
        logging.warning("focus: could not return to %s: %s", prev_app, e)


@app.post("/api/capture")
def capture():
    data = request.get_json(force=True) or {}
    order_number = str(data.get("order_number", "")).strip()
    if not order_number:
        return jsonify({"error": "Missing order number."}), 400

    # If the auto-scanner already captured this order, reuse the cached AS400 text
    # instead of driving Mocha again (faster, and works even if the session is busy).
    cached = scanned_store.get(order_number)
    if cached:
        entry = _add_order(cached["raw_text"])
        if not entry.get("auto_archived"):
            with _lock:
                entry["from_cache"] = True
        return jsonify({**_public(entry), "from_cache": True})

    # Not cached → drive Mocha. Take priority over the auto-scanner: announce we're
    # waiting (so an in-flight scan cycle yields), then hold the shared capture lock.
    # Mocha steals macOS focus during the capture — remember who has it now
    # (the operator's browser) so the focus comes back when the capture finishes.
    prev_app = frontmost_app_name()
    manual_waiting.set()
    try:
        with capture_lock:
            manual_waiting.clear()
            raw_text = capture_order(order_number, MochaDriver())
        auto_scanner.note_as400(True)
    except AS400Disconnected as e:
        auto_scanner.note_as400(False)
        return jsonify({"error": str(e), "state": "disconnected"}), 503
    except AS400ManualLoginRequired as e:
        auto_scanner.note_as400(False)
        return jsonify({"error": str(e), "state": "needs_login"}), 409
    except OrderVoidSkip:
        # VOID order dead-ended on the message screen (capture already pressed F6).
        auto_scanner.note_as400(True)
        return jsonify(
            {"error": f"Order #{order_number} is VOID (AS400 message screen) — nothing to capture."}
        ), 422
    except CaptureError as e:
        # A stalled capture still means the host answered.
        auto_scanner.note_as400(True)
        return jsonify({"error": str(e)}), 422
    except Exception as e:  # AppleScript / clipboard / environment failures
        return jsonify({"error": f"Error capturing from AS400: {e}"}), 500
    finally:
        manual_waiting.clear()
        _restore_focus(prev_app)

    # Record the manual capture in the scanned cache too, so a later PDF/recapture
    # of the same number reuses it instead of re-driving Mocha. Only cache REAL
    # orders: junk text (e.g. an error screen) would resurface as empty cards.
    try:
        preview = preview_order(raw_text)
        meta = auto_scanner._meta_from_preview(preview)
        # VOID/empty order (complete screen, zero items): tell the operator clearly
        # instead of adding an empty card that can never be sent.
        if (
            meta.get("order_number")
            and not (meta.get("item_count") or 0)
            and preview.get("is_last_page")
        ):
            return jsonify(
                {"error": f"Order #{order_number} is VOID/empty (no items) — nothing to capture."}
            ), 422
        if meta.get("order_number") and (meta.get("item_count") or 0):
            scanned_store.put(order_number, raw_text, meta, source="manual_capture")
    except Exception as e:
        logging.warning("Could not cache manual capture #%s: %s", order_number, e)

    entry = _add_order(raw_text)
    return jsonify(_public(entry))


@app.post("/api/orders/<int:oid>/send")
def send(oid: int):
    with _lock:
        entry = _orders.get(oid)
        if not entry:
            return jsonify({"error": "Order not found."}), 404
        if entry["sent"]:
            return jsonify({"error": "This order was already sent."}), 409
        # Concurrency guard: the UI locks the button, this locks the server —
        # two parallel sends of the same card would race process_order_text.
        if oid in _sending:
            return jsonify({"error": "Send already in progress for this order."}), 409
        _sending.add(oid)

    try:
        try:
            result = process_order_text(entry["raw_text"], source_name="as400_app")
        except Exception as e:
            return jsonify({"error": f"Error sending to PickD: {e}"}), 500

        with _lock:
            entry["result"] = result
            entry["sent"] = result["status"] in (
                "created",
                "appended",
                "reopened",
                "combined",
                "duplicate",
            )
        if result["status"] == "waiting_locked":
            # Target order is parked WAITING FOR INVENTORY — nothing was written.
            # 409 keeps the card pending and surfaces the message in red.
            return jsonify({**_public(entry), "error": result["message"]}), 409
        # Once sent, drop it from the scanned cache so it doesn't re-appear as a fresh
        # sendable order after an app restart (the entry stays in-session under "Sent").
        if entry["sent"] and entry.get("order_number"):
            scanned_store.delete(entry["order_number"])
        return jsonify({**_public(entry), "result": result})
    finally:
        with _lock:
            _sending.discard(oid)


@app.get("/api/orders/<int:oid>/detail")
def order_detail(oid: int):
    """Read-only pick-location detail for a captured order (like PickD's
    double-check view), resolved fresh from inventory without reserving anything."""
    with _lock:
        entry = _orders.get(oid)
    if not entry:
        return jsonify({"error": "Order not found."}), 404
    try:
        items = resolve_order_items(entry["raw_text"])
    except Exception as e:
        return jsonify({"error": f"Error resolving detail: {e}"}), 500
    return jsonify(
        {
            "order_number": entry["order_number"],
            "customer": entry["customer"],
            "total_units": entry["total_units"],
            "items": items,
        }
    )


@app.delete("/api/orders/<int:oid>")
def remove(oid: int):
    with _lock:
        entry = _orders.pop(oid, None)
    # Also drop it from the scanned cache, or _sync would just re-add it next load.
    if entry and entry.get("order_number"):
        scanned_store.delete(entry["order_number"])
    return jsonify({"ok": True})


@app.post("/api/orders/<int:oid>/archive")
def archive(oid: int):
    """Move a pending order into the persisted local archive (do not send it)."""
    with _lock:
        entry = _orders.pop(oid, None)
        if not entry:
            return jsonify({"error": "Order not found."}), 404
        _archive_entry(entry)
        _persist_archive()
    # Remove from the scanned cache so it doesn't re-sync as a sendable order.
    if entry.get("order_number"):
        scanned_store.delete(entry["order_number"])
    return jsonify({"ok": True})


@app.get("/api/verification")
def verification():
    """Read-only mirror of PickD's verification queue: {count, board}.

    Throttled behind VERIFICATION_TTL_SEC so the UI polling doesn't hammer Supabase.
    """
    return jsonify(_refresh_verification())


@app.get("/api/archived")
def list_archived():
    with _lock:
        return jsonify([_public(e) for e in _archive.values()])


@app.post("/api/archived/<aid>/restore")
def restore_archived(aid: str):
    """Pull an archived order back into the pending queue."""
    with _lock:
        arch = _archive.pop(aid, None)
        if not arch:
            return jsonify({"error": "Archived order not found."}), 404
        _persist_archive()
    # auto_archive=False: an explicit Restore must win over the parts-only
    # customer rule, or eBay orders could never be pulled back.
    entry = _add_order(arch["raw_text"], auto_archive=False)
    return jsonify(_public(entry))


@app.post("/api/scan-now")
def scan_now():
    """Operator's "get orders now": wake the auto-scanner for an immediate pass.

    The kick also bypasses the operator-activity gate for that pass — the
    operator just clicked the button, so waiting for idle makes no sense.
    """
    if auto_scanner.trigger_scan_now():
        return jsonify(
            {
                "ok": True,
                "message": "Scanner kicked — capturing the next order now. "
                "It shows up in the list in a few seconds.",
            }
        )
    return jsonify({"error": "Auto-scanner is not running (AUTO_SCAN is off)."}), 409


@app.post("/api/update")
def update():
    """Pull the latest code, refresh deps and restart the LaunchAgents.

    Runs scripts/update.sh detached in a NEW SESSION: the script restarts the
    app's own LaunchAgent (which kills this process), so it must outlive us. The
    launcher then reopens the UI automatically. We return immediately — the work
    continues in the background and finishes a few seconds later.
    """
    repo = Path(__file__).resolve().parent
    script = repo / "scripts" / "update.sh"
    if not script.exists():
        return jsonify({"error": "update.sh not found."}), 500

    logs = repo / "logs"
    logs.mkdir(exist_ok=True)
    try:
        log_file = open(logs / "update.log", "a")  # noqa: SIM115 — child keeps this fd
        subprocess.Popen(
            ["/bin/bash", str(script)],
            cwd=str(repo),
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except Exception as e:
        return jsonify({"error": f"Could not start update: {e}"}), 500

    return jsonify(
        {
            "ok": True,
            "message": "Updating in the background (~30s)… the app will restart "
            "and reopen automatically.",
        }
    )


INDEX_HTML = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>AS400 → PickD</title>
  <style>
    :root { color-scheme: light dark; }
    body { font-family: -apple-system, system-ui, sans-serif; max-width: 720px;
           margin: 2rem auto; padding: 0 1rem; }
    h1 { font-size: 1.3rem; }
    .row { display: flex; gap: .5rem; margin-bottom: 1.2rem; }
    /* Sticky topbar: title + status + capture stay visible while the order
       list scrolls. Background + shadow so cards slide underneath cleanly. */
    /* Canvas = the UA's color-scheme-aware page background, so the sticky bar
       matches the body in BOTH light and dark mode (a fixed #fff turned the
       header into white-on-white on dark-mode Macs). */
    #topbar { position: sticky; top: 0; z-index: 40; background: Canvas;
              padding-top: .6rem; margin-top: -.6rem;
              box-shadow: 0 8px 14px -12px rgba(0,0,0,.35); }
    input { flex: 1; padding: .6rem .8rem; font-size: 1.1rem; border: 1px solid #999;
            border-radius: 8px; }
    button { padding: .6rem 1rem; font-size: 1rem; border: 0; border-radius: 8px;
             cursor: pointer; background: #2563eb; color: #fff; }
    button.secondary { background: #6b7280; }
    button.send { background: #16a34a; }
    button.send:disabled { opacity: .65; cursor: default; }
    .spin { display: inline-block; animation: spin .8s linear infinite; }
    @keyframes spin { to { transform: rotate(360deg); } }
    button:disabled { opacity: .5; cursor: not-allowed; }
    .card { border: 1px solid #d1d5db; border-radius: 12px; padding: 1rem;
            margin-bottom: .8rem; }
    .meta { display: flex; gap: 1.2rem; flex-wrap: wrap; margin: .3rem 0 .7rem; }
    .meta b { font-size: 1.1rem; }
    .muted { color: #6b7280; font-size: .85rem; }
    .ok { color: #16a34a; } .warn { color: #d97706; } .err { color: #dc2626; }
    .mismatch { background: rgba(217,119,6,.12); border: 1px solid rgba(217,119,6,.5);
                color: #b45309; border-radius: 8px; padding: .5rem .7rem; margin: .2rem 0 .7rem;
                font-size: .85rem; font-weight: 600; }
    .archived-note { background: rgba(37,99,235,.1); border: 1px solid rgba(37,99,235,.4);
                color: #1d4ed8; border-radius: 8px; padding: .5rem .7rem; margin: .2rem 0 .7rem;
                font-size: .83rem; }
    .linkbtn { background: none; color: #1d4ed8; text-decoration: underline; padding: 0 .2rem;
               font-size: .83rem; font-weight: 700; }
    .actions { display: flex; gap: .5rem; align-items: center; }
    #msg { min-height: 1.2rem; margin-bottom: .8rem; }
    /* Minimal card: the whole card is tappable to reveal the items. */
    .card.tappable { cursor: pointer; transition: border-color .15s; }
    .card.tappable:hover { border-color: #9ca3af; }
    .chead { display: flex; align-items: baseline; gap: .4rem .8rem; flex-wrap: wrap; }
    .onum { font-size: 1.45rem; font-weight: 900; letter-spacing: .02em; }
    /* The customer name WRAPS instead of ellipsizing away — inside the half-width
       lanes the old nowrap+ellipsis crushed it to nothing. */
    .ocust { font-size: .95rem; font-weight: 600; color: #374151; flex: 1 1 12ch;
             min-width: 0; line-height: 1.25; overflow-wrap: anywhere; }
    .ostats { font-size: .8rem; font-weight: 700; color: #6b7280; white-space: nowrap; }
    /* Inside a lane there's half the width: the customer name takes its OWN full
       line (always visible), with number/badges above and stats below; number and
       stats are compacted. This keeps two lanes usable even at half-screen Safari. */
    .lane .onum { font-size: 1.15rem; }
    .lane .ostats { font-size: .72rem; }
    .lane .ocust { flex: 1 1 100%; order: 10; font-size: .9rem; }
    .lane .lane, .lane .card { min-width: 0; }
    .chev { color: #9ca3af; font-size: .85rem; }
    .badge.amber { font-size: .65rem; font-weight: 800; letter-spacing: .06em;
                   padding: .1rem .4rem; border-radius: 6px;
                   background: rgba(217,119,6,.15); color: #b45309;
                   border: 1px solid rgba(217,119,6,.4); }
    /* Per-card ⋯ menu */
    .more { position: relative; margin-left: auto; }
    .more > button { background: none; border: 1px solid #d1d5db; color: #6b7280;
                     border-radius: 8px; padding: .45rem .7rem; font-weight: 900; }
    .menu { position: absolute; right: 0; top: 110%; background: #fff; border: 1px solid #d1d5db;
            border-radius: 10px; box-shadow: 0 8px 24px rgba(0,0,0,.12); z-index: 30;
            min-width: 130px; overflow: hidden; }
    .menu button { display: block; width: 100%; text-align: left; background: none;
                   color: #111827; border: 0; border-radius: 0; padding: .55rem .8rem;
                   font-size: .85rem; }
    .menu button:hover { background: #f3f4f6; }
    .menu button.danger { color: #dc2626; }
    @media (prefers-color-scheme: dark) {
      .ocust { color: #d1d5db; }
      .menu { background: #1f2937; border-color: #374151; }
      .menu button { color: #e5e7eb; }
      .menu button:hover { background: #374151; }
    }
    /* Top status chip */
    .statuschip { display: inline-flex; align-items: center; gap: .45rem; background: none;
                  border: 1px solid #d1d5db; color: inherit; border-radius: 999px;
                  padding: .5rem .9rem; font-weight: 700; }
    .dot { width: 10px; height: 10px; border-radius: 50%; background: #9ca3af; }
    .dot.ok { background: #16a34a; } .dot.err { background: #dc2626; } .dot.warn { background: #d97706; }
    .shipvia { font-size: .8rem; font-weight: 700; letter-spacing: .04em;
               padding: .05rem .5rem; border-radius: 999px; align-self: center;
               background: rgba(37,99,235,.12); color: #2563eb;
               border: 1px solid rgba(37,99,235,.35); }
    /* FedEx orders get a purple accent (mirrors PickD's verification palette). */
    .card.fedex { border-left: 5px solid #a855f7; background: rgba(168,85,247,.06); }
    /* Two lanes: FedEx (left, purple) and Truck (right, emerald) — same palette
       as PickD's Verification Board (FDX purple / TRK emerald). */
    .lanes { display: grid; grid-template-columns: 1fr 1fr; gap: .8rem; align-items: start; }
    /* The two lanes MUST survive Safari at half-screen (the operator's 50/50
       setup) — cards reflow inside them instead of the lanes collapsing. Only a
       truly tiny window (a phone) stacks to one column. */
    @media (max-width: 560px) { .lanes { grid-template-columns: 1fr; } }
    .lane { border-radius: 12px; padding: .6rem; }
    .lane-fedex { background: rgba(168,85,247,.08); border: 1px solid rgba(168,85,247,.25); }
    .lane-truck { background: rgba(16,185,129,.07); border: 1px solid rgba(16,185,129,.22); }
    .lane-title { font-size: .75rem; font-weight: 800; letter-spacing: .08em;
                  text-transform: uppercase; margin: 0 0 .5rem .2rem; }
    .lane-fedex .lane-title { color: #a855f7; }
    .lane-truck .lane-title { color: #10b981; }
    .lane-truck .card { border-left: 5px solid rgba(16,185,129,.6); }
    .lane-empty { font-size: .85rem; margin: .2rem; }
    .fdx-badge { font-size: .8rem; font-weight: 800; letter-spacing: .04em;
                 padding: .05rem .5rem; border-radius: 999px; align-self: center;
                 background: rgba(168,85,247,.12); color: #a855f7;
                 border: 1px solid rgba(168,85,247,.45); }
    /* Order Comments are operationally important → prominent red note on the card. */
    .order-note { background: rgba(220,38,38,.1); border: 1px solid rgba(220,38,38,.5);
                  color: #dc2626; border-radius: 8px; padding: .5rem .7rem;
                  margin: .2rem 0 .7rem; font-size: .9rem; font-weight: 700; }
    .orderdate { font-size: .78rem; color: #6b7280; margin: .1rem 0 .5rem; }
    /* Prominent green success banner shown after an order is sent. */
    #toast { position: fixed; top: 1rem; left: 50%; transform: translateX(-50%);
             background: #16a34a; color: #fff; font-weight: 700; font-size: 1rem;
             padding: .8rem 1.4rem; border-radius: 10px; box-shadow: 0 6px 24px rgba(0,0,0,.25);
             z-index: 1000; max-width: 90vw; text-align: center; display: none; }
    /* Verification board (read-only mirror) badge + modal. */
    .vbadge { display: inline-block; min-width: 1.4rem; text-align: center;
              background: #dc2626; color: #fff; font-weight: 800; font-size: .8rem;
              border-radius: 999px; padding: .05rem .45rem; margin-left: .35rem; }
    #vboard-overlay { position: fixed; inset: 0; background: rgba(0,0,0,.5);
                      display: none; z-index: 900; }
    #vboard { position: absolute; top: 5%; left: 50%; transform: translateX(-50%);
              width: min(640px, 92vw); max-height: 85vh; overflow: auto;
              background: #fff; color: #111; border-radius: 12px; padding: 1rem; }
    @media (prefers-color-scheme: dark) { #vboard { background: #17171c; color: #e5e7eb; } }
    .vgroup { margin-bottom: .8rem; }
    .vgroup h3 { font-size: .85rem; text-transform: uppercase; letter-spacing: .05em;
                 color: #6b7280; margin: .4rem 0 .3rem; }
    .vrow { display: flex; gap: .6rem; align-items: center; padding: .35rem .5rem;
            border: 1px solid #d1d5db; border-radius: 8px; margin-bottom: .3rem;
            font-size: .85rem; }
    .vrow.fedex { border-left: 4px solid #a855f7; }
    /* Read-only detail panel — dark, double-check inspired. */
    .detail { background: #0f0f12; color: #e5e7eb; border-radius: 12px;
              padding: .6rem; margin: .2rem 0 .8rem; }
    .detail .dhead { display: flex; gap: 1rem; flex-wrap: wrap; font-size: .8rem;
                     color: #9ca3af; padding: .2rem .4rem .6rem; }
    .detail .dhead b { color: #e5e7eb; }
    .ditem { display: flex; align-items: center; gap: .7rem; background: #17171c;
             border: 1px solid #26262e; border-radius: 12px; padding: .55rem .7rem;
             margin-bottom: .45rem; }
    .ditem.prob  { background: rgba(239,68,68,.07); border-color: rgba(239,68,68,.35); }
    .ditem.lowst { background: rgba(232,160,74,.07); border-color: rgba(232,160,74,.35); }
    .ditem .qty { text-align: center; min-width: 42px; border-right: 1px solid #26262e;
                  padding-right: .6rem; }
    .ditem .qty .lbl { font-size: .55rem; letter-spacing: .15em; color: #9ca3af; }
    .ditem .qty b { display: block; font-size: 1.4rem; font-weight: 800; line-height: 1; }
    .ditem .qty.alert b { color: #e8a04a; }
    .ditem .mid { flex: 1; min-width: 0; }
    .ditem .sku { font-size: 1.05rem; font-weight: 800; white-space: nowrap;
                  overflow: hidden; text-overflow: ellipsis; }
    .ditem .sku.bad { color: #f87171; }
    .ditem .name { font-size: .72rem; color: #9ca3af; text-transform: uppercase;
                   white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .ditem .badge { font-size: .58rem; font-weight: 800; letter-spacing: .08em;
                    padding: .08rem .3rem; border-radius: 5px; margin-left: .4rem;
                    vertical-align: middle; }
    .badge.unreg { background: rgba(239,68,68,.2); color: #fca5a5; }
    .badge.low   { background: rgba(232,160,74,.2); color: #f0c089; }
    .dist { display: inline-flex; gap: .25rem; margin-top: .3rem; flex-wrap: wrap; }
    .dist .tile { min-width: 30px; text-align: center; border-radius: 5px;
                  padding: .1rem .25rem; background: #e8a04a; color: #3a1f06;
                  border: 1px solid #5c2e0a; }
    .dist .tile b { display: block; font-size: .9rem; font-weight: 800; line-height: 1; }
    .dist .tile .t { font-size: .5rem; letter-spacing: .06em; }
    .ditem .loc { text-align: right; min-width: 64px; }
    .ditem .loc .lbl { font-size: .55rem; letter-spacing: .15em; color: #9ca3af; }
    .ditem .loc b { display: block; font-family: ui-monospace, monospace; font-weight: 800;
                    font-size: 1.4rem; color: #e8a04a; line-height: 1.05; }
    .ditem .loc .none { color: #6b7280; font-size: 1rem; }
    .ditem .loc .sub { display: inline-block; font-size: .6rem; font-weight: 700;
                       color: #e8a04a; background: rgba(232,160,74,.15);
                       border: 1px solid rgba(232,160,74,.4); border-radius: 5px;
                       padding: 0 .3rem; margin-top: .2rem; }
  </style>
</head>
<body>
  <div id="toast"></div>
  <div id="topbar">
    <h1>📦 AS400 → PickD</h1>
    <div class="row">
      <button id="conn" class="statuschip" onclick="doConnect()" title="Reconnect AS400">
        <span class="dot" id="dot"></span> AS400
      </button>
      <button id="vbtn" class="statuschip" onclick="openBoard()" title="Verification Board (live mirror)">
        Verification<span id="vbadge" class="vbadge" style="display:none;">0</span>
      </button>
      <div class="more" id="topmore">
        <button onclick="toggleMenu(event, 'topmenu')" title="More">⋯</button>
        <div class="menu" id="topmenu" style="display:none;">
          <button onclick="doScanNow()">▶ Get orders now</button>
          <button onclick="doStatus()">Check AS400</button>
          <button onclick="doUpdate()">⟳ Update app</button>
        </div>
      </div>
    </div>
    <div class="row">
      <input id="num" placeholder="Search orders or type # to capture (e.g. 880005)" autofocus
             oninput="applyFilter()"
             onkeydown="if(event.key==='Enter') onSearchEnter()">
      <button id="cap" onclick="doCapture()" title="Fetch this order number from AS400">Capture</button>
    </div>
  </div>
  <div id="vboard-overlay" onclick="if(event.target===this) closeBoard()">
    <div id="vboard">
      <div style="display:flex; justify-content:space-between; align-items:center;">
        <h2 style="font-size:1.1rem; margin:.2rem 0;">Verification Board <span class="muted">(live mirror)</span></h2>
        <button class="secondary" onclick="closeBoard()">Close</button>
      </div>
      <div id="vboard-body"><p class="muted">Loading…</p></div>
    </div>
  </div>
  <div id="msg" class="muted"></div>
  <div id="list"></div>

<script>
const msg = (t, cls='muted') => { const m=document.getElementById('msg'); m.className=cls; m.textContent=t; };

// Live-filter state. The page already holds every order it has (active, already-in-
// PickD, sent and archived), so filtering is instant and local — no AS400 round-trip.
// _cacheHits adds the tail of orders that live only in the scan cache (just scanned,
// not yet materialized), fetched via /api/search.
let _allOrders = [];
let _allArchived = [];
let _filter = '';
let _cacheHits = [];
let _searchTimer = null;
// The digits we last auto-filled into the box, so we can tell an untouched prefill
// from something the operator typed (and never clobber the latter).
let _prefill = '';

// Single source of truth for the search box + filter, so programmatic clears
// (e.g. after a capture) keep the box and the rendered list in sync.
function setSearch(v) {
  _filter = v || '';
  const box = document.getElementById('num');
  if (box) box.value = _filter;
}

// Soft-prefill the search box with the latest order's leading digits (all but the
// last two), so the operator types only the final two to capture the next order.
// Fills only when the box is empty or still holds the previous prefill — never over
// the operator's own input. Follows the live sequence: when the latest order crosses
// into a new hundred the prefill rolls on its own.
function applyPrefill(prefix) {
  const box = document.getElementById('num');
  if (!box || !prefix) return;
  const cur = box.value;
  if ((cur === '' || cur === _prefill) && cur !== prefix) {
    box.value = prefix;
    try { box.setSelectionRange(prefix.length, prefix.length); } catch (e) { /* not focused yet */ }
  }
  _prefill = prefix;
  // Keep the effective filter in sync: a bare prefill is a typing head-start, not a
  // search, so it must not filter the list down to its own band.
  _filter = (box.value === _prefill) ? '' : box.value;
}

// Does an order card match the query? Looks at number, customer, shipping type and
// each item's SKU/description — the fields the operator would search by.
function orderMatches(o, q) {
  if (!q) return true;
  const parts = [o.order_number, o.customer, o.shipping_type];
  for (const it of (o.items || [])) parts.push(it.sku, it.raw_sku, it.description, it.item_name);
  return parts.filter(Boolean).join(' ').toLowerCase().includes(q);
}

function hitMatches(h, q) {
  if (!q) return true;
  return [h.order_number, h.customer].filter(Boolean).join(' ').toLowerCase().includes(q);
}

// Filter as the operator types (instant, local). Then debounce a reach into the scan
// cache so orders scanned in the last few seconds (not yet in the live list) surface
// too — still without driving AS400.
function applyFilter() {
  const v = document.getElementById('num').value;
  // A bare, untouched prefill is a typing head-start, not a search — treat it as
  // empty so the full list stays visible until the operator types the tail.
  _filter = (_prefill && v === _prefill) ? '' : v;
  render(_allOrders, _allArchived);
  if (_searchTimer) clearTimeout(_searchTimer);
  const q = _filter.trim();
  if (!q) { _cacheHits = []; return; }
  _searchTimer = setTimeout(() => searchCache(q), 200);
}

async function searchCache(q) {
  try {
    const r = await fetch('/api/search?q=' + encodeURIComponent(q));
    if (!r.ok) return;
    const hits = await r.json();
    // Only surface cache hits that aren't already on screen (deduped by number).
    const known = new Set([..._allOrders, ..._allArchived].map(o => o.order_number).filter(Boolean));
    _cacheHits = hits.filter(h => h.order_number && !known.has(h.order_number));
    render(_allOrders, _allArchived);
  } catch (e) { /* network hiccup — the local filter still works */ }
}

// Enter only falls back to AS400 when the order isn't already in hand. If it matches
// something we already have, Enter just keeps the filter — it never re-scans.
function onSearchEnter() {
  const q = document.getElementById('num').value.trim();
  if (!q || q === _prefill) return;  // empty, or a bare prefill with no tail typed yet
  const ql = q.toLowerCase();
  const hasLocal = [..._allOrders, ..._allArchived].some(o => orderMatches(o, ql))
                || _cacheHits.some(h => hitMatches(h, ql));
  if (hasLocal) return;  // already on screen — don't touch AS400
  doCapture();           // genuinely new number → explicit AS400 fetch
}

function captureNumber(num) {
  setSearch(num);
  doCapture();
}

let _toastTimer = null;
// Prominent green success banner (used after a successful send).
function toast(t) {
  const el = document.getElementById('toast');
  el.textContent = t;
  el.style.display = 'block';
  if (_toastTimer) clearTimeout(_toastTimer);
  _toastTimer = setTimeout(() => { el.style.display = 'none'; }, 5000);
}

async function load() {
  const [ro, ra, rp] = await Promise.all([
    fetch('/api/orders'), fetch('/api/archived'), fetch('/api/order-prefix')]);
  _allOrders = await ro.json();
  _allArchived = await ra.json();
  let prefix = '';
  try { if (rp.ok) prefix = (await rp.json()).prefix || ''; } catch (e) { /* prefill is best-effort */ }
  applyPrefill(prefix);              // soft head-start in the search box (before render)
  render(_allOrders, _allArchived);  // render() applies the current (effective) filter
  refreshVerification();  // keep the red counter live on each load
  refreshAs400Dot();      // dot goes green from real scanner/capture activity
}

// The dot used to stay gray until a MANUAL Connect/Check click, even while the
// auto-scanner was capturing orders fine. Now it reflects the server-side health
// beacon (fed by every AS400 interaction). 'unknown' (no signal / stale) keeps
// whatever the last manual action set — default gray.
async function refreshAs400Dot() {
  try {
    const r = await fetch('/api/as400');
    if (!r.ok) return;
    const d = await r.json();
    if (d.state === 'ok') setDot('ok');
    else if (d.state === 'err') setDot('err');
  } catch(e) { /* network blip — leave the dot as-is */ }
}

async function refreshVerification() {
  try {
    const r = await fetch('/api/verification');
    if (!r.ok) return;
    const data = await r.json();
    const badge = document.getElementById('vbadge');
    const n = data.count ?? 0;
    badge.textContent = n;
    badge.style.display = n > 0 ? 'inline-block' : 'none';
    window._vboard = data.board || {};
    // If the board modal is open, re-render it with the fresh snapshot.
    if (document.getElementById('vboard-overlay').style.display === 'block') renderBoard();
  } catch(e) { /* counter is best-effort; never break the page */ }
}

const VSTATUS_LABELS = {
  active: 'Active', ready_to_double_check: 'Ready to double-check',
  double_checking: 'Double-checking', needs_correction: 'Needs correction',
  reopened: 'Reopened',
};

function renderBoard() {
  const board = window._vboard || {};
  const body = document.getElementById('vboard-body');
  const groups = Object.keys(board).filter(s => (board[s] || []).length);
  if (!groups.length) { body.innerHTML = '<p class="muted">Nothing in verification right now.</p>'; return; }
  body.innerHTML = groups.map(s => {
    const rows = board[s].map(o =>
      `<div class="vrow${o.shipping_type === 'fedex' ? ' fedex' : ''}">
        <span><b>#${o.order_number ?? '—'}</b></span>
        <span>${o.customer ?? 'Unknown'}</span>
        <span class="muted" style="margin-left:auto;">${o.items} items${o.shipping_type ? ' · ' + o.shipping_type : ''}</span>
      </div>`).join('');
    return `<div class="vgroup"><h3>${VSTATUS_LABELS[s] || s} (${board[s].length})</h3>${rows}</div>`;
  }).join('');
}

function openBoard() {
  document.getElementById('vboard-overlay').style.display = 'block';
  renderBoard();
  refreshVerification();
}
function closeBoard() { document.getElementById('vboard-overlay').style.display = 'none'; }

// "2 pallets · 20 units" — pallets estimated with PickD's own rule (parts-only = 1;
// bikes = ceil(units/12)). Falls back to the item count only if the estimate is
// unavailable (e.g. Supabase unreachable when the order was captured).
function palletStats(o) {
  const units = `${o.total_units ?? '—'} units`;
  if (o.pallets_est != null) {
    return `${o.pallets_est} ${o.pallets_est === 1 ? 'pallet' : 'pallets'} · ${units}`;
  }
  return `${o.item_count} items · ${units}`;
}

function card(o) {
  const res = o.result;
  let status = '';
  if (res) {
    const cls = res.status === 'duplicate' ? 'warn' : (res.needs_correction ? 'warn' : 'ok');
    status = `<div class="${cls}">→ ${res.status}${res.needs_correction ? ' (needs_correction)' : ''}: ${res.message||''}</div>`;
  }
  // Context that lives INSIDE the detail panel (the compact card stays clean):
  // ship-to, carrier and the total-mismatch explanation.
  // FedEx orders get a purple accent + FDX badge (regular orders: no special paint).
  const isFedex = o.shipping_type === 'fedex';
  const fdx = isFedex ? `<span class="fdx-badge">FDX</span>` : '';
  const odate = o.order_date ? `<div class="orderdate">Order date: ${o.order_date}</div>` : '';
  // Meaningful Order Comments → prominent red note in the main view (freight
  // boilerplate like a bare 'FREE FREIGHT' is filtered server-side; the full
  // comment is still sent to PickD). Fallback covers cached pre-filter entries.
  const noteText = o.order_note_display !== undefined ? o.order_note_display : o.order_comments;
  const note = noteText ? `<div class="order-note">⚠ ${noteText}</div>` : '';
  const money = n => '$' + Number(n).toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2});
  let dinfo = '';
  if (o.total_mismatch) dinfo += `<div class="mismatch">⚠ El total no cuadra: parseado ${money(o.parsed_total)} vs orden ${money(o.subtotal)} — pueden faltar items.</div>`;
  if (o.shipping_address) dinfo += `<div class="muted">📍 ${o.shipping_address}</div>`;
  // order_comments is surfaced as a prominent red note on the card itself (not here).
  if (o.ship_via) dinfo += `<div class="muted">🚚 ${o.ship_via}</div>`;
  // An archived copy of this number exists — rare, actionable, so it stays visible.
  const am = o.archived_match;
  const archNote = am
    ? `<div class="archived-note">📦 Ya hay una versión <b>archivada</b> de #${o.order_number}. `
      + (am.identical ? '✓ Idéntica.' : `⚠ Difiere: ${am.summary}.`)
      + ` <button class="linkbtn" onclick="event.stopPropagation(); doRestore('${am.aid}')">Sacar del archivo</button></div>`
    : '';
  const mm = o.total_mismatch ? '<span class="badge amber">⚠ TOTAL</span>' : '';
  return `<div class="card tappable${isFedex ? ' fedex' : ''}" onclick="toggleDetail(${o.id})" title="Tap to see items">
      <div class="chead">
        <span class="onum">#${o.order_number ?? '—'}</span>
        <span class="ocust">${o.customer ?? ''}</span>
        ${fdx}${mm}
        <span class="ostats">${palletStats(o)}</span>
        <span class="chev" id="chev-${o.id}">▾</span>
      </div>
      ${odate}
      ${note}
      <div class="detail" id="detail-${o.id}" style="display:none;">
        <div class="dinfo">${dinfo}</div>
        <div id="ditems-${o.id}"></div>
      </div>
      ${archNote}
      ${status}
      <div class="actions" onclick="event.stopPropagation()">
        <button class="send" id="send-${o.id}" ${o.sent?'disabled':''} onclick="doSend(${o.id})">${o.sent?'Sent ✓':'Send to PickD'}</button>
        <div class="more">
          <button onclick="toggleMenu(event, 'menu-${o.id}')" title="More actions">⋯</button>
          <div class="menu" id="menu-${o.id}" style="display:none;">
            ${o.sent?'':`<button onclick="doArchive(${o.id})">Archive</button>`}
            <button class="danger" onclick="doRemove(${o.id}, '${o.order_number ?? ''}')">Remove</button>
          </div>
        </div>
      </div>
    </div>`;
}

function fmtDate(s) {
  if (!s) return '';
  const d = new Date(s);
  return isNaN(d) ? s : d.toLocaleString();
}

function archCard(a) {
  return `<div class="card">
      <div class="chead">
        <span class="onum">#${a.order_number ?? '—'}</span>
        <span class="ocust">${a.customer ?? ''}</span>
        <span class="ostats">${palletStats(a)}</span>
      </div>
      <div class="muted">📦 Archived ${fmtDate(a.archived_at)}</div>
      <div class="actions">
        <button class="secondary" onclick="doRestore('${a.aid}')">Restore</button>
      </div>
    </div>`;
}

function distFigures(distribution) {
  if (!Array.isArray(distribution) || !distribution.length) return '';
  const tiles = distribution.map(d => {
    const type = (d.type || 'OTHER').toUpperCase();
    const n = d.count ?? d.units_each ?? '';
    return `<span class="tile" title="${type}${d.units_each?(' · '+d.units_each+' each'):''}"><b>${n}</b><span class="t">${type.slice(0,4)}</span></span>`;
  }).join('');
  return `<div class="dist">${tiles}</div>`;
}

function locCell(it) {
  const loc = (it.location || '').trim();
  if (!loc) return `<div class="loc"><span class="lbl">LOC</span><span class="none">—</span></div>`;
  const isRow = /row/i.test(loc);
  const label = isRow ? 'ROW' : 'LOC';
  const value = isRow ? loc.replace(/row/i, '').trim() : loc.toUpperCase();
  const sub = it.sublocation ? `<span class="sub">${it.sublocation}</span>` : '';
  return `<div class="loc"><span class="lbl">${label}</span><b>${value}</b>${sub}</div>`;
}

function ditem(it) {
  const qty = it.pickingQty ?? it.qty ?? 0;
  const prob = it.sku_not_found ? ' prob' : (it.insufficient_stock ? ' lowst' : '');
  const skuBad = it.sku_not_found ? ' bad' : '';
  let badges = '';
  if (it.sku_not_found) badges += '<span class="badge unreg">UNREG</span>';
  if (it.insufficient_stock) badges += '<span class="badge low">LOW STOCK</span>';
  const name = (it.item_name || it.description || '').toString();
  return `<div class="ditem${prob}">
      <div class="qty${qty!=1?' alert':''}"><span class="lbl">QTY</span><b>${qty}</b></div>
      <div class="mid">
        <div class="sku${skuBad}">${it.sku ?? it.raw_sku ?? '—'}${badges}</div>
        <div class="name">${name}</div>
        ${distFigures(it.distribution)}
      </div>
      ${locCell(it)}
    </div>`;
}

async function toggleDetail(id) {
  const box = document.getElementById('detail-'+id);
  const itemsBox = document.getElementById('ditems-'+id);
  const chev = document.getElementById('chev-'+id);
  if (!box || !itemsBox) return;
  if (box.style.display !== 'none') { box.style.display='none'; if(chev) chev.textContent='▾'; return; }
  box.style.display = 'block';
  if (chev) chev.textContent = '▴';
  if (itemsBox.dataset.loaded) return;        // already fetched this session
  itemsBox.innerHTML = '<div class="dhead">Resolving pick locations…</div>';
  try {
    const r = await fetch(`/api/orders/${id}/detail`);
    const data = await r.json();
    if (!r.ok) { itemsBox.innerHTML = `<div class="dhead err">${data.error || 'Error loading detail.'}</div>`; return; }
    const items = data.items || [];
    const probs = items.filter(i => i.sku_not_found || i.insufficient_stock).length;
    const head = `<div class="dhead">
        <span>Lines <b>${items.length}</b></span>
        <span>Units <b>${data.total_units ?? '—'}</b></span>
        ${probs ? `<span class="err">⚠ ${probs} need attention</span>` : '<span class="ok">✓ all resolved</span>'}
      </div>`;
    itemsBox.innerHTML = head + (items.length ? items.map(ditem).join('') : '<div class="dhead">No items.</div>');
    itemsBox.dataset.loaded = '1';
  } catch(e) {
    itemsBox.innerHTML = `<div class="dhead err">Network error: ${e}</div>`;
  }
}

function toggleMenu(ev, id) {
  ev.stopPropagation();
  const m = document.getElementById(id);
  if (!m) return;
  const open = m.style.display !== 'none';
  closeMenus();
  m.style.display = open ? 'none' : 'block';
}
function closeMenus() {
  document.querySelectorAll('.menu').forEach(m => { m.style.display = 'none'; });
}
document.addEventListener('click', closeMenus);

// Compact row for an order that lives only in the scan cache (not yet materialized).
function cacheHitRow(h) {
  const n = h.order_number || '—';
  const meta = [h.customer, (h.item_count != null ? h.item_count + ' items' : null)]
    .filter(Boolean).join(' · ');
  return `<div class="card" style="display:flex; justify-content:space-between; align-items:center; gap:.6rem;">
    <span>#${n}${meta ? ` <span class="muted">· ${meta}</span>` : ''}</span>
    <button onclick="captureNumber('${n}')">Capture</button>
  </div>`;
}

function render(orders, archived) {
  const list = document.getElementById('list');
  const q = _filter.trim().toLowerCase();
  const searching = q.length > 0;

  // Live filter across EVERY state the page already holds — instant and local,
  // never a round-trip to AS400.
  if (searching) {
    orders = orders.filter(o => orderMatches(o, q));
    archived = (archived || []).filter(o => orderMatches(o, q));
  }

  // Candidates = not sent AND not already living in PickD. Orders detected in the
  // DB (sent by PDF / another path) are noise here — they collapse below.
  const active = orders.filter(o => !o.sent && !o.in_pickd);
  const inPickd = orders.filter(o => !o.sent && o.in_pickd);
  const sent = orders.filter(o => o.sent);
  const arch = archived || [];
  const cacheHits = searching ? _cacheHits.filter(h => hitMatches(h, q)) : [];
  // While searching, expand the normally-collapsed sections so matches in Sent /
  // Already-in-PickD / Archived are visible without an extra click.
  const openAttr = searching ? ' open' : '';

  let html = '';
  if (!active.length) {
    if (searching) {
      const term = _filter.trim();
      const elsewhere = inPickd.length + sent.length + arch.length + cacheHits.length;
      html += elsewhere
        ? `<p class="muted">No active matches for "${term}" — see the sections below.</p>`
        : `<p class="muted">No order matches "${term}". Press Enter or Capture to fetch it from AS400.</p>`;
    } else {
      html += '<p class="muted">No orders yet. The scanner adds them automatically, or capture one above.</p>';
    }
  } else {
    // Two lanes (like the Verification Board): FedEx on the LEFT, trucks on the
    // RIGHT, each with its background tint. Cards stay full/sendable as before.
    const fedex = active.filter(o => o.shipping_type === 'fedex');
    const trucks = active.filter(o => o.shipping_type !== 'fedex');
    html += `<div class="lanes">
      <div class="lane lane-fedex">
        <div class="lane-title">FedEx (${fedex.length})</div>
        ${fedex.map(card).join('') || '<p class="muted lane-empty">No FedEx orders.</p>'}
      </div>
      <div class="lane lane-truck">
        <div class="lane-title">Truck (${trucks.length})</div>
        ${trucks.map(card).join('') || '<p class="muted lane-empty">No truck orders.</p>'}
      </div>
    </div>`;
  }
  // Already in PickD (arrived via PDF or elsewhere) — kept locally, out of the way.
  // Cards keep their Send button: re-sending appends any missing SKUs (delta).
  if (inPickd.length) {
    html += `<details${openAttr} style="margin-top:1rem;">
      <summary class="muted" style="cursor:pointer;">✓ Already in PickD (${inPickd.length})</summary>
      <div style="margin-top:.6rem;">${inPickd.map(card).join('')}</div>
    </details>`;
  }
  // Sent orders are hidden in a collapsed section so they don't clutter the list.
  if (sent.length) {
    html += `<details${openAttr} style="margin-top:1rem;">
      <summary class="muted" style="cursor:pointer;">✓ Sent to PickD (${sent.length})</summary>
      <div style="margin-top:.6rem;">${sent.map(card).join('')}</div>
    </details>`;
  }
  // Archived orders persist locally across restarts; collapsed by default.
  if (arch.length) {
    html += `<details${openAttr} style="margin-top:1rem;">
      <summary class="muted" style="cursor:pointer;">📦 Archived (${arch.length})</summary>
      <div style="margin-top:.6rem;">${arch.map(archCard).join('')}</div>
    </details>`;
  }
  // Cache-only matches: scanned by the auto-scanner but not yet materialized into
  // the live list. Capture reuses the cached AS400 text (no re-scan).
  if (cacheHits.length) {
    html += `<details open style="margin-top:1rem;">
      <summary class="muted" style="cursor:pointer;">🔎 In scan cache (${cacheHits.length})</summary>
      <div style="margin-top:.6rem;">${cacheHits.map(cacheHitRow).join('')}</div>
    </details>`;
  }
  list.innerHTML = html;
}

function setDot(cls) {
  const d = document.getElementById('dot');
  if (d) d.className = 'dot' + (cls ? ' ' + cls : '');
}

async function doConnect() {
  const btn = document.getElementById('conn'); btn.disabled = true;
  msg('Launching AS400 and logging in… do not touch the keyboard (~10s).', 'warn');
  try {
    const r = await fetch('/api/connect', {method:'POST'});
    const data = await r.json();
    setDot(r.ok ? 'ok' : 'err');
    msg(r.ok ? (data.message || 'Connected.') : (data.error||'Error connecting.'), r.ok?'ok':'err');
  } catch(e) { setDot('err'); msg('Network error: '+e, 'err'); }
  finally { btn.disabled = false; document.getElementById('num').focus(); }
}

const STATE_LABELS = {
  disconnected: ['❌ AS400 disconnected (host down). Log in manually in Mocha.', 'err'],
  login:        ['🔑 On the login screen. Press Connect AS400 to log in.', 'warn'],
  menu:         ['📋 On the SALESN menu. Press Connect AS400 to go to Order Inquiry.', 'warn'],
  message:      ['💬 "Press Enter to continue" screen. Press Connect AS400 to continue.', 'warn'],
  order_search: ['✅ Ready: order-search screen.', 'ok'],
  order_inquiry:['✅ Viewing an order. Ready to capture.', 'ok'],
  unknown:      ['⚠️ Unrecognized screen. Log in manually to the order-search screen.', 'warn'],
};

async function doScanNow() {
  msg('Kicking the scanner — capturing the next order…', 'warn');
  try {
    const r = await fetch('/api/scan-now', {method:'POST'});
    const data = await r.json();
    if (r.ok) msg(data.message || 'Scanner kicked.', 'ok');
    else msg(data.error || 'Could not kick the scanner.', 'err');
  } catch(e) { msg('Network error: '+e, 'err'); }
}

async function doStatus() {
  msg('Checking AS400 status…', 'warn');
  try {
    const r = await fetch('/api/status', {method:'POST'});
    const data = await r.json();
    if (!r.ok) { setDot('err'); msg(data.error || 'Error reading the screen.', 'err'); }
    else { const [label, cls] = STATE_LABELS[data.state] || [`State: ${data.state}`, 'muted'];
           setDot(cls === 'ok' ? 'ok' : cls === 'err' ? 'err' : 'warn');
           msg(label, cls); }
  } catch(e) { setDot('err'); msg('Network error: '+e, 'err'); }
}

async function doCapture() {
  const num = document.getElementById('num').value.trim();
  if (!num) { msg('Enter an order number.', 'err'); return; }
  if (_prefill && num === _prefill) { msg('Type the last two digits to capture.', 'warn'); return; }
  const btn = document.getElementById('cap'); btn.disabled = true;
  msg('Capturing from AS400… do not touch the keyboard.', 'warn');
  try {
    const r = await fetch('/api/capture', {method:'POST', headers:{'Content-Type':'application/json'},
                                            body: JSON.stringify({order_number: num})});
    const data = await r.json();
    if (!r.ok) { msg(data.error || 'Error capturing.', 'err'); }
    else if (data.auto_archived) {
           msg(`Order #${data.order_number ?? '—'} auto-archived (${data.customer ?? 'parts-only customer'}) — see Archived below.`, 'warn');
           setSearch(''); }
    else { const fc = data.from_cache ? ' · ya escaneada por el auto-scan' : '';
           msg(`Captured order #${data.order_number ?? '—'} (${palletStats(data)})${fc}.`, 'ok');
           setSearch(''); }
  } catch(e) { msg('Network error: '+e, 'err'); }
  finally { btn.disabled = false; await load(); document.getElementById('num').focus(); }
}

async function doSend(id) {
  // Lock the button immediately: a send takes a few seconds and double-clicks
  // were tempting ("did my click land?"). The spinner answers that question.
  const btn = document.getElementById(`send-${id}`);
  if (btn && btn.disabled) return;
  const orig = btn ? btn.innerHTML : '';
  if (btn) { btn.disabled = true; btn.innerHTML = '<span class="spin">⏳</span> Sending…'; }
  msg('Sending to PickD…', 'warn');
  try {
    const r = await fetch(`/api/orders/${id}/send`, {method:'POST'});
    const data = await r.json();
    const m = data.result?.message;
    if (r.ok) {
      msg(m || 'Sent.', 'ok');
      const res = data.result || {};
      // Prominent green banner so a successful send is impossible to miss.
      toast(`✓ Order #${res.order_number ?? '—'} sent to PickD (${res.status || 'sent'}, ${res.item_count ?? 0} items)`);
    } else {
      msg(data.error || 'Error sending.', 'err');
    }
  } catch(e) {
    msg('Network error: '+e, 'err');
  } finally {
    await load();  // re-renders the card (Sent ✓, or the button restored)
    if (btn && document.body.contains(btn)) { btn.disabled = false; btn.innerHTML = orig; }
  }
}

async function doRemove(id, num) {
  if (!confirm(`Remove order ${num ? '#' + num : ''}? It won't come back automatically.`)) return;
  await fetch(`/api/orders/${id}`, {method:'DELETE'});
  await load();
}

async function doArchive(id) {
  const r = await fetch(`/api/orders/${id}/archive`, {method:'POST'});
  msg(r.ok ? 'Order archived (saved locally).' : 'Could not archive.', r.ok ? 'ok' : 'err');
  await load();
}

async function doRestore(aid) {
  const r = await fetch(`/api/archived/${aid}/restore`, {method:'POST'});
  msg(r.ok ? 'Restored to pending.' : 'Could not restore.', r.ok ? 'ok' : 'err');
  await load();
}

async function doUpdate() {
  if (!confirm('Update the app to the latest version?\\nIt will pull the new code, install libraries and restart — the window reopens automatically.')) return;
  msg('Updating… pulling code and installing libraries. The app will restart and reopen shortly.', 'warn');
  try {
    const r = await fetch('/api/update', {method:'POST'});
    const data = await r.json();
    msg(r.ok ? (data.message || 'Updating…') : (data.error || 'Error updating.'), r.ok ? 'ok' : 'err');
  } catch(e) {
    // The app may have already restarted mid-request — that's expected.
    msg('Update in progress — the app is restarting and will reopen automatically.', 'warn');
  }
}

load();
// Live refresh so auto-scanned orders appear without a manual reload — but never
// while the operator is interacting (an open detail or menu would get wiped by
// the re-render).
function uiBusy() {
  const open = el => el.style.display !== 'none';
  return [...document.querySelectorAll('.detail')].some(open)
      || [...document.querySelectorAll('.menu')].some(open);
}
setInterval(() => { if (!uiBusy()) load(); }, 8000);
</script>
<div class="muted" style="text-align:center; font-size:.7rem; margin:1.2rem 0 .6rem;">build {{ version }}</div>
</body>
</html>
"""


if __name__ == "__main__":
    _load_archive()
    start_auto_scanner()
    # threaded=True so UI requests are served promptly even while the auto-scanner's
    # background thread is busy driving Mocha (otherwise the page hangs blank until
    # the scan cycle finishes).
    app.run(host="127.0.0.1", port=PORT, debug=False, threaded=True)
