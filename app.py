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

import logging
import subprocess
import threading
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()  # must run before importing modules that read env at import time

from flask import Flask, abort, jsonify, render_template_string, request  # noqa: E402

from as400_capture import (  # noqa: E402
    CaptureError,
    MochaDriver,
    bootstrap_session,
    capture_order,
)
from pipeline import preview_order, process_order_text  # noqa: E402

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


def _add_order(raw_text: str) -> dict:
    global _next_id
    preview = preview_order(raw_text)
    with _lock:
        oid = _next_id
        _next_id += 1
        entry = {
            "id": oid,
            "order_number": preview["order_number"],
            "customer": preview["customer"],
            "item_count": preview["item_count"],
            "shipping_address": preview.get("shipping_address"),
            "order_comments": preview.get("order_comments"),
            "items": preview["items"],
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
    """Launch the AS400 emulator and run the login macro, all from the UI."""
    try:
        bootstrap_session(MochaDriver())
    except Exception as e:
        return jsonify({"error": f"Error connecting to AS400: {e}"}), 500
    return jsonify({"ok": True, "message": "AS400 connected and logged in."})


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

    try:
        raw_text = capture_order(order_number, MochaDriver())
    except CaptureError as e:
        return jsonify({"error": str(e)}), 422
    except Exception as e:  # AppleScript / clipboard / environment failures
        return jsonify({"error": f"Error capturing from AS400: {e}"}), 500

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


@app.delete("/api/orders/<int:oid>")
def remove(oid: int):
    with _lock:
        _orders.pop(oid, None)
    return jsonify({"ok": True})


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
    .actions { display: flex; gap: .5rem; }
    #msg { min-height: 1.2rem; margin-bottom: .8rem; }
  </style>
</head>
<body>
  <h1>📦 AS400 → PickD</h1>
  <div class="row">
    <button id="conn" class="secondary" onclick="doConnect()">Connect AS400</button>
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
  const r = await fetch('/api/orders');
  render(await r.json());
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
  return `<div class="card">
      <div class="meta">
        <span>Order <b>#${o.order_number ?? '—'}</b></span>
        <span>Customer <b>${o.customer}</b></span>
        <span>Items <b>${o.item_count}</b></span>
      </div>
      ${ship}${notes}
      ${status}
      <div class="actions">
        <button class="send" ${o.sent?'disabled':''} onclick="doSend(${o.id})">${o.sent?'Sent ✓':'Send to PickD'}</button>
        <button class="secondary" onclick="doRemove(${o.id})">Remove</button>
      </div>
    </div>`;
}

function render(orders) {
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
    else { msg(`Captured order #${data.order_number ?? '—'} (${data.item_count} items).`, 'ok');
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
    app.run(host="127.0.0.1", port=PORT, debug=False)
