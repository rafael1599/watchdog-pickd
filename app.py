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
import subprocess
import threading
import uuid
from datetime import datetime, timezone
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
    bootstrap_session,
    capture_order,
    classify_screen,
)
from auto_scanner import capture_lock, manual_waiting, start_auto_scanner  # noqa: E402
from pipeline import preview_order, process_order_text, resolve_order_items  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")

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

# Archived orders the operator chose NOT to send. Unlike the pending queue these
# are persisted to a local JSON file so they survive an app restart. Keyed by a
# stable archive id (aid). The file is gitignored — it is local working state.
ARCHIVE_PATH = Path(__file__).resolve().parent / ".archived_orders.json"
_archive: dict[str, dict] = {}


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


def _add_order(raw_text: str) -> dict:
    global _next_id
    preview = preview_order(raw_text)
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
            "subtotal": preview.get("subtotal"),
            "parsed_total": preview.get("parsed_total"),
            "total_mismatch": preview.get("total_mismatch", False),
            "ship_via": preview.get("ship_via"),
            "shipping_address": preview.get("shipping_address"),
            "order_comments": preview.get("order_comments"),
            "items": preview["items"],
            "archived_match": archived_match,
            "raw_text": raw_text,
            "sent": False,
            "result": None,
        }
        _orders[oid] = entry
    return entry


def _public(entry: dict) -> dict:
    """Strip raw_text from the payload sent to the browser."""
    return {k: v for k, v in entry.items() if k != "raw_text"}


@app.get("/")
def index():
    return render_template_string(INDEX_HTML)


@app.get("/api/orders")
def list_orders():
    with _lock:
        return jsonify([_public(o) for o in _orders.values()])


@app.post("/api/connect")
def connect():
    """Launch the AS400 emulator and log in — verifying state, not assuming it."""
    try:
        state = bootstrap_session(MochaDriver())
    except AS400Disconnected as e:
        return jsonify({"ok": False, "state": "disconnected", "error": str(e)}), 503
    except AS400ManualLoginRequired as e:
        return jsonify({"ok": False, "state": "needs_login", "error": str(e)}), 409
    except Exception as e:
        return jsonify({"ok": False, "error": f"Error connecting to AS400: {e}"}), 500
    return jsonify(
        {"ok": True, "state": state, "message": "AS400 ready on the order-search screen."}
    )


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


@app.post("/api/peek")
def peek():
    """Return what Mocha currently shows on screen (for debugging login/focus/copy)."""
    try:
        text = MochaDriver().copy_screen()
    except CaptureError as e:
        return jsonify({"error": str(e)}), 422
    except Exception as e:
        return jsonify({"error": f"Error reading screen: {e}"}), 500
    return jsonify({"screen": text})


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
        with _lock:
            entry["from_cache"] = True
        return jsonify({**_public(entry), "from_cache": True})

    # Not cached → drive Mocha. Take priority over the auto-scanner: announce we're
    # waiting (so an in-flight scan cycle yields), then hold the shared capture lock.
    manual_waiting.set()
    try:
        with capture_lock:
            manual_waiting.clear()
            raw_text = capture_order(order_number, MochaDriver())
    except AS400Disconnected as e:
        return jsonify({"error": str(e), "state": "disconnected"}), 503
    except AS400ManualLoginRequired as e:
        return jsonify({"error": str(e), "state": "needs_login"}), 409
    except CaptureError as e:
        return jsonify({"error": str(e)}), 422
    except Exception as e:  # AppleScript / clipboard / environment failures
        return jsonify({"error": f"Error capturing from AS400: {e}"}), 500
    finally:
        manual_waiting.clear()

    # Record the manual capture in the scanned cache too, so a later PDF/recapture
    # of the same number reuses it instead of re-driving Mocha.
    try:
        scanned_store.put(
            order_number,
            raw_text,
            auto_scanner._meta_from_preview(preview_order(raw_text)),
            source="manual_capture",
        )
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
    return jsonify({**_public(entry), "result": result})


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
        _orders.pop(oid, None)
    return jsonify({"ok": True})


@app.post("/api/orders/<int:oid>/archive")
def archive(oid: int):
    """Move a pending order into the persisted local archive (do not send it)."""
    with _lock:
        entry = _orders.pop(oid, None)
        if not entry:
            return jsonify({"error": "Order not found."}), 404
        aid = uuid.uuid4().hex
        arch = {k: v for k, v in entry.items() if k not in ("id", "sent", "result")}
        arch["aid"] = aid
        arch["archived_at"] = datetime.now(timezone.utc).isoformat()
        _archive[aid] = arch
        _persist_archive()
    return jsonify({"ok": True})


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
    entry = _add_order(arch["raw_text"])
    return jsonify(_public(entry))


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
    input { flex: 1; padding: .6rem .8rem; font-size: 1.1rem; border: 1px solid #999;
            border-radius: 8px; }
    button { padding: .6rem 1rem; font-size: 1rem; border: 0; border-radius: 8px;
             cursor: pointer; background: #2563eb; color: #fff; }
    button.secondary { background: #6b7280; }
    button.send { background: #16a34a; }
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
    .actions { display: flex; gap: .5rem; }
    #msg { min-height: 1.2rem; margin-bottom: .8rem; }
    .meta.clickable { cursor: pointer; }
    .meta .chev { color: #9ca3af; font-size: .9rem; }
    .shipvia { font-size: .8rem; font-weight: 700; letter-spacing: .04em;
               padding: .05rem .5rem; border-radius: 999px; align-self: center;
               background: rgba(37,99,235,.12); color: #2563eb;
               border: 1px solid rgba(37,99,235,.35); }
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
  <h1>📦 AS400 → PickD</h1>
  <div class="row">
    <button id="conn" class="secondary" onclick="doConnect()">Connect AS400</button>
    <button id="stat" class="secondary" onclick="doStatus()">Check AS400</button>
    <button id="peek" class="secondary" onclick="doPeek()">Peek screen</button>
    <button id="upd" class="secondary" style="margin-left:auto;" onclick="doUpdate()">⟳ Update</button>
  </div>
  <pre id="screen" style="display:none; background:#111; color:#0f0; padding:.8rem;
       border-radius:8px; overflow:auto; font-size:.75rem; line-height:1.15;"></pre>
  <div class="row">
    <input id="num" placeholder="Order number (e.g. 880005)" autofocus
           onkeydown="if(event.key==='Enter') doCapture()">
    <button id="cap" onclick="doCapture()">Capture</button>
  </div>
  <div id="msg" class="muted"></div>
  <div id="list"></div>

<script>
const msg = (t, cls='muted') => { const m=document.getElementById('msg'); m.className=cls; m.textContent=t; };

async function load() {
  const [ro, ra] = await Promise.all([fetch('/api/orders'), fetch('/api/archived')]);
  render(await ro.json(), await ra.json());
}

function card(o) {
  const res = o.result;
  let status = '';
  if (res) {
    const cls = res.status === 'duplicate' ? 'warn' : (res.needs_correction ? 'warn' : 'ok');
    status = `<div class="${cls}">→ ${res.status}${res.needs_correction ? ' (needs_correction)' : ''}: ${res.message||''}</div>`;
  }
  const ship = o.shipping_address ? `<div class="muted">📍 Ship to: ${o.shipping_address}</div>` : '';
  const notes = o.order_comments ? `<div class="muted">📝 Notes: ${o.order_comments}</div>` : '';
  const via = o.ship_via ? `<span class="shipvia">🚚 ${o.ship_via}</span>` : '';
  // Reconciliation: parsed line total vs the order Sub-Total. A mismatch means a
  // line was likely dropped/misparsed — warn loudly but still allow sending.
  const money = n => '$' + Number(n).toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2});
  const mismatch = o.total_mismatch
    ? `<div class="mismatch">⚠ El total no cuadra: parseado ${money(o.parsed_total)} vs orden ${money(o.subtotal)} — pueden faltar items. Revisa el detalle antes de enviar.</div>`
    : '';
  // This order number already has an archived copy — warn and offer to pull it out,
  // showing whether the fresh scan is identical or what changed.
  const am = o.archived_match;
  const archNote = am
    ? `<div class="archived-note">📦 Ya hay una versión <b>archivada</b> de #${o.order_number} (${fmtDate(am.archived_at)}). `
      + (am.identical
          ? '✓ El nuevo escaneo es idéntico.'
          : `⚠ Difiere del nuevo escaneo: ${am.summary}.`)
      + ` <button class="linkbtn" onclick="doRestore('${am.aid}')">Sacar del archivo</button></div>`
    : '';
  return `<div class="card">
      <div class="meta clickable" onclick="toggleDetail(${o.id})" title="Show pick detail">
        <span>Order <b>#${o.order_number ?? '—'}</b></span>
        <span>Customer <b>${o.customer}</b></span>
        <span>Items <b>${o.item_count}</b></span>
        <span>Total units <b>${o.total_units ?? '—'}</b></span>
        ${via}
        <span class="chev" id="chev-${o.id}">▾ detail</span>
      </div>
      <div class="detail" id="detail-${o.id}" style="display:none;"></div>
      ${mismatch}
      ${archNote}
      ${ship}${notes}
      ${status}
      <div class="actions">
        <button class="send" ${o.sent?'disabled':''} onclick="doSend(${o.id})">${o.sent?'Sent ✓':'Send to PickD'}</button>
        ${o.sent?'':`<button class="secondary" onclick="doArchive(${o.id})">Archive</button>`}
        <button class="secondary" onclick="doRemove(${o.id})">Remove</button>
      </div>
    </div>`;
}

function fmtDate(s) {
  if (!s) return '';
  const d = new Date(s);
  return isNaN(d) ? s : d.toLocaleString();
}

function archCard(a) {
  const via = a.ship_via ? `<span class="shipvia">🚚 ${a.ship_via}</span>` : '';
  const ship = a.shipping_address ? `<div class="muted">📍 Ship to: ${a.shipping_address}</div>` : '';
  return `<div class="card">
      <div class="meta">
        <span>Order <b>#${a.order_number ?? '—'}</b></span>
        <span>Customer <b>${a.customer}</b></span>
        <span>Items <b>${a.item_count}</b></span>
        <span>Total units <b>${a.total_units ?? '—'}</b></span>
        ${via}
      </div>
      <div class="muted">📦 Archivada ${fmtDate(a.archived_at)}</div>
      ${ship}
      <div class="actions">
        <button class="secondary" onclick="doRestore('${a.aid}')">Restaurar a pendientes</button>
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
  const chev = document.getElementById('chev-'+id);
  if (!box) return;
  if (box.style.display !== 'none') { box.style.display='none'; if(chev) chev.textContent='▾ detail'; return; }
  box.style.display = 'block';
  if (chev) chev.textContent = '▴ detail';
  if (box.dataset.loaded) return;             // already fetched this session
  box.innerHTML = '<div class="dhead">Resolving pick locations…</div>';
  try {
    const r = await fetch(`/api/orders/${id}/detail`);
    const data = await r.json();
    if (!r.ok) { box.innerHTML = `<div class="dhead err">${data.error || 'Error loading detail.'}</div>`; return; }
    const items = data.items || [];
    const probs = items.filter(i => i.sku_not_found || i.insufficient_stock).length;
    const head = `<div class="dhead">
        <span>Order <b>#${data.order_number ?? '—'}</b></span>
        <span>Units <b>${data.total_units ?? '—'}</b></span>
        <span>Lines <b>${items.length}</b></span>
        ${probs ? `<span class="err">⚠ ${probs} need attention</span>` : '<span class="ok">✓ all resolved</span>'}
      </div>`;
    box.innerHTML = head + (items.length ? items.map(ditem).join('') : '<div class="dhead">No items.</div>');
    box.dataset.loaded = '1';
  } catch(e) {
    box.innerHTML = `<div class="dhead err">Network error: ${e}</div>`;
  }
}

function render(orders, archived) {
  const list = document.getElementById('list');
  const active = orders.filter(o => !o.sent);
  const sent = orders.filter(o => o.sent);

  let html = '';
  if (!active.length) {
    html += '<p class="muted">No pending orders. Capture one above.</p>';
  } else {
    html += active.map(card).join('');
  }
  // Sent orders are hidden in a collapsed section so they don't clutter the list.
  if (sent.length) {
    html += `<details style="margin-top:1rem;">
      <summary class="muted" style="cursor:pointer;">✓ Sent to PickD (${sent.length})</summary>
      <div style="margin-top:.6rem;">${sent.map(card).join('')}</div>
    </details>`;
  }
  // Archived orders persist locally across restarts; collapsed by default.
  if (archived && archived.length) {
    html += `<details style="margin-top:1rem;">
      <summary class="muted" style="cursor:pointer;">📦 Archived (${archived.length})</summary>
      <div style="margin-top:.6rem;">${archived.map(archCard).join('')}</div>
    </details>`;
  }
  list.innerHTML = html;
}

async function doConnect() {
  const btn = document.getElementById('conn'); btn.disabled = true;
  msg('Launching AS400 and logging in… do not touch the keyboard (~10s).', 'warn');
  try {
    const r = await fetch('/api/connect', {method:'POST'});
    const data = await r.json();
    msg(r.ok ? (data.message || 'Connected.') : (data.error||'Error connecting.'), r.ok?'ok':'err');
  } catch(e) { msg('Network error: '+e, 'err'); }
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

async function doStatus() {
  const btn = document.getElementById('stat'); btn.disabled = true;
  msg('Checking AS400 status…', 'warn');
  try {
    const r = await fetch('/api/status', {method:'POST'});
    const data = await r.json();
    if (!r.ok) { msg(data.error || 'Error reading the screen.', 'err'); }
    else { const [label, cls] = STATE_LABELS[data.state] || [`State: ${data.state}`, 'muted'];
           msg(label, cls); }
  } catch(e) { msg('Network error: '+e, 'err'); }
  finally { btn.disabled = false; }
}

async function doPeek() {
  const btn = document.getElementById('peek'); btn.disabled = true;
  msg('Reading current Mocha screen…', 'warn');
  try {
    const r = await fetch('/api/peek', {method:'POST'});
    const data = await r.json();
    const pre = document.getElementById('screen');
    if (!r.ok) { msg(data.error || 'Error reading screen.', 'err'); pre.style.display='none'; }
    else { pre.textContent = data.screen || '(empty)'; pre.style.display='block';
           msg('Screen captured below (' + (data.screen||'').length + ' chars).', 'ok'); }
  } catch(e) { msg('Network error: '+e, 'err'); }
  finally { btn.disabled = false; }
}

async function doCapture() {
  const num = document.getElementById('num').value.trim();
  if (!num) { msg('Enter an order number.', 'err'); return; }
  const btn = document.getElementById('cap'); btn.disabled = true;
  msg('Capturing from AS400… do not touch the keyboard.', 'warn');
  try {
    const r = await fetch('/api/capture', {method:'POST', headers:{'Content-Type':'application/json'},
                                            body: JSON.stringify({order_number: num})});
    const data = await r.json();
    if (!r.ok) { msg(data.error || 'Error capturing.', 'err'); }
    else { const fc = data.from_cache ? ' · ya escaneada por el auto-scan' : '';
           msg(`Captured order #${data.order_number ?? '—'} (${data.item_count} items, ${data.total_units} units)${fc}.`, 'ok');
           document.getElementById('num').value=''; }
  } catch(e) { msg('Network error: '+e, 'err'); }
  finally { btn.disabled = false; await load(); document.getElementById('num').focus(); }
}

async function doSend(id) {
  msg('Sending to PickD…', 'warn');
  const r = await fetch(`/api/orders/${id}/send`, {method:'POST'});
  const data = await r.json();
  msg(r.ok ? (data.result?.message || 'Sent.') : (data.error||'Error sending.'), r.ok?'ok':'err');
  await load();
}

async function doRemove(id) {
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
  const btn = document.getElementById('upd'); btn.disabled = true;
  msg('Updating… pulling code and installing libraries. The app will restart and reopen shortly.', 'warn');
  try {
    const r = await fetch('/api/update', {method:'POST'});
    const data = await r.json();
    msg(r.ok ? (data.message || 'Updating…') : (data.error || 'Error updating.'), r.ok ? 'ok' : 'err');
    if (!r.ok) btn.disabled = false;
  } catch(e) {
    // The app may have already restarted mid-request — that's expected.
    msg('Update in progress — the app is restarting and will reopen automatically.', 'warn');
  }
}

load();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    _load_archive()
    start_auto_scanner()
    app.run(host="127.0.0.1", port=PORT, debug=False)
