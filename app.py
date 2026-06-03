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
import threading

from flask import Flask, jsonify, render_template_string, request

from as400_capture import (
    CaptureError,
    MochaDriver,
    bootstrap_session,
    capture_order,
)
from pipeline import preview_order, process_order_text

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")

app = Flask(__name__)

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
        return jsonify({"error": f"Error conectando al AS400: {e}"}), 500
    return jsonify({"ok": True, "message": "AS400 conectado y logueado."})


@app.post("/api/capture")
def capture():
    data = request.get_json(force=True) or {}
    order_number = str(data.get("order_number", "")).strip()
    if not order_number:
        return jsonify({"error": "Falta el número de orden."}), 400

    try:
        raw_text = capture_order(order_number, MochaDriver())
    except CaptureError as e:
        return jsonify({"error": str(e)}), 422
    except Exception as e:  # AppleScript / clipboard / environment failures
        return jsonify({"error": f"Error capturando del AS400: {e}"}), 500

    entry = _add_order(raw_text)
    return jsonify(_public(entry))


@app.post("/api/orders/<int:oid>/send")
def send(oid: int):
    with _lock:
        entry = _orders.get(oid)
    if not entry:
        return jsonify({"error": "Orden no encontrada."}), 404
    if entry["sent"]:
        return jsonify({"error": "Esta orden ya fue enviada."}), 409

    try:
        result = process_order_text(entry["raw_text"], source_name="as400_app")
    except Exception as e:
        return jsonify({"error": f"Error enviando a PickD: {e}"}), 500

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


INDEX_HTML = """
<!doctype html>
<html lang="es">
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
    <button id="conn" class="secondary" onclick="doConnect()">Conectar AS400</button>
  </div>
  <div class="row">
    <input id="num" placeholder="Número de orden (ej. 880005)" autofocus
           onkeydown="if(event.key==='Enter') doCapture()">
    <button id="cap" onclick="doCapture()">Capturar</button>
  </div>
  <div id="msg" class="muted"></div>
  <div id="list"></div>

<script>
const msg = (t, cls='muted') => { const m=document.getElementById('msg'); m.className=cls; m.textContent=t; };

async function load() {
  const r = await fetch('/api/orders');
  render(await r.json());
}

function render(orders) {
  const list = document.getElementById('list');
  if (!orders.length) { list.innerHTML = '<p class="muted">Sin órdenes capturadas todavía.</p>'; return; }
  list.innerHTML = orders.map(o => {
    const res = o.result;
    let status = '';
    if (res) {
      const cls = res.status === 'duplicate' ? 'warn' : (res.needs_correction ? 'warn' : 'ok');
      status = `<div class="${cls}">→ ${res.status}${res.needs_correction ? ' (needs_correction)' : ''}: ${res.message||''}</div>`;
    }
    return `<div class="card">
      <div class="meta">
        <span>Orden <b>#${o.order_number ?? '—'}</b></span>
        <span>Cliente <b>${o.customer}</b></span>
        <span>Ítems <b>${o.item_count}</b></span>
      </div>
      ${status}
      <div class="actions">
        <button class="send" ${o.sent?'disabled':''} onclick="doSend(${o.id})">${o.sent?'Enviada ✓':'Enviar a PickD'}</button>
        <button class="secondary" onclick="doRemove(${o.id})">Quitar</button>
      </div>
    </div>`;
  }).join('');
}

async function doConnect() {
  const btn = document.getElementById('conn'); btn.disabled = true;
  msg('Lanzando AS400 y haciendo login… no toques el teclado (~10s).', 'warn');
  try {
    const r = await fetch('/api/connect', {method:'POST'});
    const data = await r.json();
    msg(r.ok ? (data.message || 'Conectado.') : (data.error||'Error al conectar.'), r.ok?'ok':'err');
  } catch(e) { msg('Error de red: '+e, 'err'); }
  finally { btn.disabled = false; document.getElementById('num').focus(); }
}

async function doCapture() {
  const num = document.getElementById('num').value.trim();
  if (!num) { msg('Escribe un número de orden.', 'err'); return; }
  const btn = document.getElementById('cap'); btn.disabled = true;
  msg('Capturando del AS400… no toques el teclado.', 'warn');
  try {
    const r = await fetch('/api/capture', {method:'POST', headers:{'Content-Type':'application/json'},
                                            body: JSON.stringify({order_number: num})});
    const data = await r.json();
    if (!r.ok) { msg(data.error || 'Error al capturar.', 'err'); }
    else { msg(`Capturada orden #${data.order_number ?? '—'} (${data.item_count} ítems).`, 'ok');
           document.getElementById('num').value=''; }
  } catch(e) { msg('Error de red: '+e, 'err'); }
  finally { btn.disabled = false; await load(); document.getElementById('num').focus(); }
}

async function doSend(id) {
  msg('Enviando a PickD…', 'warn');
  const r = await fetch(`/api/orders/${id}/send`, {method:'POST'});
  const data = await r.json();
  msg(r.ok ? (data.result?.message || 'Enviada.') : (data.error||'Error al enviar.'), r.ok?'ok':'err');
  await load();
}

async function doRemove(id) {
  await fetch(`/api/orders/${id}`, {method:'DELETE'});
  await load();
}

load();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False)
