"""Una orden pedida desde la búsqueda de PickD (5 nov 2026).

PR 2 de docs/prds/ask-as400-from-search.md (pickd): la puerta lee las filas
`queued` de as400_order_requests, el escáner las teclea antes que el barrido y
la puerta escribe lo que pasó. Lo que más importa, y lo primero que se prueba:
atender una petición NUNCA mueve el cursor del barrido. Un error de dedo (892090)
saltaría unas diez mil órdenes.
"""

import threading
import time
from datetime import datetime, timedelta, timezone

import pytest

import as400_capture
import auto_scanner
import door
import operator_sentinel
import scanned_store
from as400_capture import (
    AS400Disconnected,
    AS400SessionClosed,
    CaptureError,
    OrderNotFound,
    OrderVoidSkip,
)

NOW = datetime(2026, 11, 5, 17, 0, tzinfo=timezone.utc)

EMPTY_SEARCH = (
    "                            O R D E R   I N Q U I R Y\n\n"
    " Order Number:                              Account Number:\n\n"
    " Alpha Search:                                     Invoice:\n"
)


@pytest.fixture(autouse=True)
def _state(tmp_path, monkeypatch):
    monkeypatch.setenv("SCANNED_STORE_PATH", str(tmp_path / "scanned.json"))
    monkeypatch.setenv("SCAN_CURSOR_PATH", str(tmp_path / "scan_cursor"))
    monkeypatch.setenv("SCAN_PARKED_PATH", str(tmp_path / "parked.json"))
    as400_capture._reset_restart_backoff_for_tests()
    operator_sentinel._reset_for_tests()
    monkeypatch.setattr(auto_scanner, "_last_operator_input", None)
    monkeypatch.setattr(as400_capture, "_system_idle_seconds", lambda: 120.0)
    _clear_requests()
    yield
    _clear_requests()
    operator_sentinel._reset_for_tests()
    as400_capture._reset_restart_backoff_for_tests()


def _clear_requests():
    auto_scanner._req_queue.clear()
    auto_scanner._req_events.clear()
    auto_scanner._req_current = None
    auto_scanner._request.clear()
    auto_scanner._kick.clear()
    auto_scanner.request_events_ready.clear()


def _preview(text):
    parts = text.split()
    if parts and parts[0] == "VOID":
        return {"order_number": parts[1], "item_count": 0, "is_last_page": True}
    return {"order_number": parts[1], "customer": "C", "item_count": 1, "total_units": 1}


class Driver:
    def new_window(self):
        pass


def _seed_sweep(cursor=882082):
    """El barrido va por la 882082: la 882081 es la última que capturó."""
    scanned_store.put("882081", "ORDER 882081", {"order_number": "882081", "item_count": 1})
    assert scanned_store.next_scan_number(880112) == cursor


def _cursor_file():
    import os
    from pathlib import Path

    return Path(os.environ["SCAN_CURSOR_PATH"]).read_text()


def _serve(number, cap, rid=7, attempts=0):
    return auto_scanner.serve_request(
        {"id": rid, "order_number": number, "attempts": attempts},
        Driver(),
        capture_fn=cap,
        preview_fn=_preview,
    )


def _events():
    return [dict(e) for e in auto_scanner._req_events]


# ── serve_request: los desenlaces, y el cursor quieto ────────────────────────


def test_an_order_asked_for_is_captured_without_moving_the_cursor():
    _seed_sweep()
    before = _cursor_file()

    r = _serve("881500", lambda n, d: f"ORDER {n}")

    assert r == {"action": "captured", "number": "881500", "request": "captured"}
    assert scanned_store.get("881500")["source"] == "pickd_request"
    assert _cursor_file() == before
    assert scanned_store.next_scan_number(880112) == 882082
    assert [e["kind"] for e in _events()] == ["started", "result"]
    assert _events()[1]["outcome"] == "captured"
    assert auto_scanner.requests_held() == {7}  # el resultado aún no está escrito


def test_892090_never_moves_the_cursor_found_or_not():
    """El error de dedo de §4.3: ni si el AS400 dice que no existe, ni si
    existiera. Ésta es la prueba que pidió Rafael."""
    _seed_sweep()
    before = _cursor_file()

    def not_there(n, d):
        raise OrderNotFound("Invalid Order Number")

    r = _serve("892090", not_there)
    assert r["request"] == "not_in_as400"
    assert _cursor_file() == before
    assert scanned_store.next_scan_number(880112) == 882082

    # Y si abriera: entra al caché, pero el barrido sigue en la 882082 y la
    # «última capturada» (la de la prueba de b108f36) sigue siendo la 882081.
    r = _serve("892090", lambda n, d: f"ORDER {n}", rid=8)
    assert r["request"] == "captured"
    assert _cursor_file() == before
    assert scanned_store.next_scan_number(880112) == 882082
    assert scanned_store.latest_number(880112) == 882081
    assert scanned_store.parked() == {}


def test_882082_does_not_open_and_is_not_parked_nor_restarted():
    _seed_sweep()
    before = _cursor_file()
    asked = []

    def cap(n, d):
        asked.append(n)
        if n == "882082":
            raise AS400SessionClosed("empty", screen=EMPTY_SEARCH)
        return f"ORDER {n}"

    r = _serve("882082", cap)

    assert r == {"action": "request_closed", "number": "882082", "request": "wont_open"}
    assert asked == ["882082", "882081"]  # la misma prueba que b108f36
    assert scanned_store.parked() == {}  # sin park(): movería el cursor
    assert _cursor_file() == before
    assert _events()[1]["detail"] == "#882081 opens"


def test_an_empty_search_on_a_dead_session_goes_back_to_the_queue():
    _seed_sweep()

    def cap(n, d):
        raise AS400SessionClosed("empty", screen=EMPTY_SEARCH)

    r = _serve("882082", cap)
    # `unavailable`: el bucle sigue el camino de reinicio de siempre.
    assert r["action"] == "unavailable"
    assert r["request"] == "retry"
    assert scanned_store.parked() == {}


def test_a_void_order_is_closed_as_void():
    _seed_sweep()
    before = _cursor_file()

    def void(n, d):
        raise OrderVoidSkip("message screen")

    assert _serve("881600", void)["request"] == "void"
    # Y por la cabecera: completa, sin líneas, total que cuadra.
    assert _serve("881601", lambda n, d: f"VOID {n}", rid=8)["request"] == "void"
    assert _cursor_file() == before  # sin skip()


@pytest.mark.parametrize(
    "err, action",
    [
        (AS400Disconnected("down"), "unavailable"),
        (CaptureError("no END OF ORDER"), "incomplete"),
    ],
)
def test_an_interrupted_capture_goes_back_to_the_queue(err, action):
    def cap(n, d):
        raise err

    r = _serve("881700", cap, attempts=1)
    assert r["action"] == action
    assert r["request"] == "retry"
    assert _events()[1]["attempts"] == 2


def test_the_queue_mirrors_the_database_and_skips_what_is_in_hand():
    rows = [{"id": 1, "order_number": "881001"}, {"id": 2, "order_number": "881002"}]
    assert auto_scanner.set_request_queue(rows) == 2
    assert auto_scanner._request.is_set()
    first = auto_scanner._take_request()
    assert first["id"] == 1
    # La puerta vuelve a leer las dos (la 1 aún dice queued en la BD): la que se
    # teclea no vuelve a la cola.
    assert auto_scanner.set_request_queue(rows) == 1
    # La 2 caducó en la BD: desaparece de aquí también.
    assert auto_scanner.set_request_queue([]) == 0
    assert not auto_scanner._request.is_set()


# ── el bucle: antes que el barrido, nunca con alguien en Bay 2 ───────────────


def _loop_harness(monkeypatch, idle):
    served, swept = [], []
    monkeypatch.setattr(auto_scanner, "SCAN_INITIAL_DELAY_SEC", 0.0)
    monkeypatch.setattr(auto_scanner, "IDLE_POLL_SEC", 0.02)
    monkeypatch.setattr(auto_scanner, "FOUND_NEXT_DELAY_SEC", 0.02)
    monkeypatch.setattr(auto_scanner, "as400_capture_mark_automated", lambda: None)
    monkeypatch.setattr(auto_scanner, "MochaDriver", Driver)
    monkeypatch.setattr(auto_scanner, "operator_idle_seconds", lambda: idle["v"])

    def serve(req, driver, **kw):
        served.append(req["order_number"])
        swept.append("request")
        auto_scanner._request_done()
        return {"action": "request_closed", "number": req["order_number"]}

    def sweep(driver, **kw):
        swept.append("sweep")
        return {"action": "captured", "number": "882082"}

    monkeypatch.setattr(auto_scanner, "serve_request", serve)
    monkeypatch.setattr(auto_scanner, "run_scan_step", sweep)
    return served, swept


def _run_loop_for(seconds):
    auto_scanner._stop.clear()
    t = threading.Thread(target=auto_scanner._loop, daemon=True)
    t.start()
    time.sleep(seconds)
    auto_scanner._stop.set()
    t.join(2)
    auto_scanner._stop.clear()


def test_with_somebody_on_bay_2_a_request_waits(monkeypatch):
    idle = {"v": 3.0}  # el ratón se movió hace 3 s
    served, swept = _loop_harness(monkeypatch, idle)
    auto_scanner.set_request_queue([{"id": 1, "order_number": "881500"}])

    _run_loop_for(0.3)

    assert served == [] and swept == []
    assert auto_scanner._request.is_set()  # sigue en cola


def test_a_request_goes_before_the_sweep(monkeypatch):
    idle = {"v": 120.0}
    served, swept = _loop_harness(monkeypatch, idle)
    auto_scanner.set_request_queue([{"id": 1, "order_number": "881500"}])

    _run_loop_for(0.3)

    assert served == ["881500"]
    assert swept[0] == "request" and swept[1:] and set(swept[1:]) == {"sweep"}


def test_the_operator_gate_wait_is_not_woken_by_a_request():
    auto_scanner._request.set()
    t0 = time.monotonic()
    auto_scanner._interruptible_wait(0.3, wake_on_request=False)
    assert time.monotonic() - t0 >= 0.25
    t0 = time.monotonic()
    auto_scanner._interruptible_wait(5.0)  # la de un not_found sí despierta
    assert time.monotonic() - t0 < 0.5


def test_a_catalogue_burst_yields_before_its_next_lookup(monkeypatch):
    from test_sku_enrichment import _gap_harness

    monkeypatch.setenv("SKU_ENRICH_MAX_PER_GAP", "10")
    seen = _gap_harness(monkeypatch)
    import sku_enrichment

    real_step = sku_enrichment.run_sku_step

    def step(driver, row, **kw):
        out = real_step(driver, row, **kw)
        auto_scanner._request.set()  # PickD pide una orden durante la consulta
        return out

    monkeypatch.setattr(sku_enrichment, "run_sku_step", step)
    monkeypatch.setattr("as400_capture.return_to_order_search", lambda d: None)

    auto_scanner._run_sku_gap()

    assert len(seen) == 1
    assert auto_scanner.gap_state()["reason"] == "an order was asked for in PickD"


def test_the_manual_catalogue_run_yields_too(monkeypatch):
    import sku_enrichment

    monkeypatch.setattr(sku_enrichment, "next_sku", lambda *a, **k: {"sku": "03-3492BL"})
    auto_scanner._request.set()
    out = sku_enrichment.run_until_disturbed(
        None,
        home_fn=lambda _d: None,
        idle_fn=lambda: 1e9,
        update_pending_fn=lambda: False,
        step_fn=lambda d, row, **kw: {"action": "read", "returned": True},
    )
    assert out["stopped"] == "orders requested"


# ── la puerta: lo que se escribe ─────────────────────────────────────────────


class _Query:
    def __init__(self, db, table):
        self.db, self.table, self.filters = db, table, []
        self.op, self.values = "select", None

    def select(self, _cols):
        self.op = "select"
        return self

    def update(self, values):
        self.op, self.values = "update", values
        return self

    def upsert(self, values):
        self.op, self.values = "upsert", values
        return self

    def eq(self, col, val):
        self.filters.append(lambda r: r.get(col) == val)
        return self

    def in_(self, col, vals):
        self.filters.append(lambda r: r.get(col) in vals)
        return self

    def gte(self, col, val):
        self.filters.append(lambda r: str(r.get(col)) >= val)
        return self

    def lt(self, col, val):
        self.filters.append(lambda r: str(r.get(col)) < val)
        return self

    def order(self, col):
        self.order_by = col
        return self

    def limit(self, _n):
        return self

    def execute(self):
        class R:
            pass

        res = R()
        if self.op == "upsert":
            self.db.upserts.append((self.table, dict(self.values)))
            if self.db.upsert_fails:
                self.db.upsert_fails -= 1
                raise RuntimeError("column operator_seen_at does not exist")
            res.data = [self.values]
            return res
        rows = [r for r in self.db.tables.get(self.table, []) if all(f(r) for f in self.filters)]
        if self.op == "update":
            for r in rows:
                r.update(self.values)
        res.data = [dict(r) for r in rows]
        return res


class FakeDB:
    def __init__(self, rows=(), rpc_answer="send"):
        self.tables = {"as400_order_requests": [dict(r) for r in rows]}
        self.rpcs = []
        self.rpc_answer = rpc_answer
        self.upserts = []
        self.upsert_fails = 0

    def table(self, name):
        return _Query(self, name)

    def rpc(self, name, params):
        self.rpcs.append((name, params))
        answer = self.rpc_answer

        class R:
            def execute(self_inner):
                class D:
                    data = answer

                return D()

        return R()

    def row(self, rid):
        return next(r for r in self.tables["as400_order_requests"] if r["id"] == rid)


def _req(rid=7, status="queued", **over):
    base = {
        "id": rid,
        "order_number": "881500",
        "status": status,
        "attempts": 0,
        "started_at": None,
        "requested_at": (NOW - timedelta(minutes=1)).isoformat(),
        "updated_at": (NOW - timedelta(minutes=1)).isoformat(),
    }
    base.update(over)
    return base


def _started(rid=7, attempts=1):
    return {"kind": "started", "id": rid, "order_number": "881500", "attempts": attempts}


def _result(outcome, rid=7, attempts=1, detail=None):
    return {
        "kind": "result",
        "id": rid,
        "order_number": "881500",
        "attempts": attempts,
        "outcome": outcome,
        "detail": detail,
    }


def test_an_order_not_yet_captured_goes_queued_opening_sending():
    db = FakeDB([_req()], rpc_answer="send")
    sent = []
    published = []

    def publish(client, number):
        published.append(number)
        return True

    kw = {"now": NOW, "publish_fn": publish, "send_fn": lambda c: sent.append(1)}
    assert door.resolve_order_request(db, _started(), **kw) == "opening"
    assert db.row(7)["status"] == "opening"
    assert db.row(7)["started_at"] == NOW.isoformat()
    assert db.row(7)["attempts"] == 1

    assert door.resolve_order_request(db, _result("captured"), **kw) == "sending"
    assert published == ["881500"]  # publicada antes de preguntarle a SQL
    assert db.rpcs == [("as400_request_captured", {"p_request_id": 7})]
    assert sent == [1]  # la puerta la envía en el acto, sin esperar al tick


def test_881874_reread_is_shipped_and_nothing_is_sent():
    # La factura la decide SQL (as400_request_captured); aquí sólo no se envía.
    db = FakeDB([_req(status="opening")], rpc_answer="shipped")
    sent = []
    became = door.resolve_order_request(
        db,
        _result("captured"),
        now=NOW,
        publish_fn=lambda c, n: True,
        send_fn=lambda c: sent.append(1),
    )
    assert became == "shipped"
    assert sent == []


@pytest.mark.parametrize("outcome", ["not_in_as400", "void", "wont_open"])
def test_final_outcomes_close_the_request(outcome):
    db = FakeDB([_req(status="opening")])
    assert door.resolve_order_request(db, _result(outcome, detail="d"), now=NOW) == outcome
    row = db.row(7)
    assert row["status"] == outcome
    assert row["resolved_at"] == NOW.isoformat()
    assert row["detail"] == "d"


def test_a_closed_request_is_never_reopened():
    # Caducó (o la cerró el trigger) mientras Bay 2 la tecleaba.
    db = FakeDB([_req(status="expired")])
    assert door.resolve_order_request(db, _started(), now=NOW) == "not queued"
    door.resolve_order_request(db, _result("not_in_as400"), now=NOW)
    assert db.row(7)["status"] == "expired"


def test_an_interruption_requeues_and_the_third_fails():
    db = FakeDB([_req(status="opening", attempts=1)])
    assert door.resolve_order_request(db, _result("retry", attempts=1), now=NOW) == "queued"
    db.row(7)["status"] = "opening"
    assert door.resolve_order_request(db, _result("retry", attempts=3), now=NOW) == "failed"
    assert db.row(7)["resolved_at"] == NOW.isoformat()


def test_poll_hands_over_only_the_live_queue():
    db = FakeDB(
        [
            _req(1, requested_at=(NOW - timedelta(minutes=5)).isoformat()),
            _req(2, requested_at=(NOW - timedelta(minutes=61)).isoformat()),  # el board: expired
            _req(3, status="opening"),
        ]
    )
    handed = []
    assert door.poll_order_requests(db, now=NOW, enqueue_fn=handed.extend) == 1
    assert [r["id"] for r in handed] == [1]


def test_a_restart_mid_opening_goes_back_to_the_queue_after_3_min():
    old = (NOW - timedelta(minutes=4)).isoformat()
    db = FakeDB(
        [
            _req(1, status="opening", attempts=1, updated_at=old),
            _req(2, status="opening", attempts=3, updated_at=old),
            _req(3, status="opening", attempts=1, updated_at=old),  # éste lo tecleamos ahora
            _req(
                4, status="opening", attempts=1, updated_at=(NOW - timedelta(minutes=1)).isoformat()
            ),
        ]
    )
    assert door.recover_stuck_requests(db, now=NOW, held={3}) == 2
    assert db.row(1)["status"] == "queued"
    assert db.row(2)["status"] == "failed"
    assert db.row(3)["status"] == "opening"
    assert db.row(4)["status"] == "opening"


def test_events_leave_the_list_only_once_written():
    auto_scanner._post_request_event(_started())
    auto_scanner._post_request_event(_result("not_in_as400"))

    class Down(FakeDB):
        def table(self, name):
            raise ConnectionError("network")

    with pytest.raises(ConnectionError):
        door.drain_order_request_events(Down(), now=NOW)
    assert len(auto_scanner._req_events) == 2

    db = FakeDB([_req()])
    assert door.drain_order_request_events(db, now=NOW) == 2
    assert auto_scanner._req_events == []
    assert db.row(7)["status"] == "not_in_as400"


def test_the_heartbeat_says_when_a_person_last_touched_bay_2(monkeypatch):
    monkeypatch.setattr(auto_scanner, "_last_operator_input", time.monotonic() - 30)
    db = FakeDB()
    door.heartbeat(db, "abc1234")
    _, row = db.upserts[-1]
    seen = datetime.fromisoformat(row["operator_seen_at"])
    assert abs((datetime.now(timezone.utc) - seen).total_seconds() - 30) < 2


def test_the_heartbeat_survives_a_database_without_the_column(monkeypatch):
    monkeypatch.setattr(auto_scanner, "_last_operator_input", time.monotonic() - 30)
    db = FakeDB()
    db.upsert_fails = 1
    door.heartbeat(db, "abc1234")
    assert "operator_seen_at" in db.upserts[0][1]
    assert "operator_seen_at" not in db.upserts[1][1]


def test_off_macos_nobody_is_ever_seen(monkeypatch):
    # HIDIdleTime ilegible = mil millones de segundos: no es una fecha.
    monkeypatch.setattr(auto_scanner, "_last_operator_input", time.monotonic() - 1e9)
    assert auto_scanner.operator_seen_at() is None
