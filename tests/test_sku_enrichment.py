"""
Tests for the AS400 catalogue step (F2 — read-only).

Everything here runs against fake drivers and canned screens, the same way the
capture loop is tested: no macOS, no Mocha, no database. `plan_write` is the one
that matters most — it is the function F3 will point at a service_role
connection with no RLS underneath, so its rules are pinned before it can write.
"""

import json

import pytest

import sku_enrichment  # noqa: E402
from as400_capture import (
    AS400ManualLoginRequired,
    StockScreenMismatch,
    StockSkuNotFound,
    capture_stock_inquiry,
    return_to_order_search,
)
from sku_enrichment import is_lookupable, plan_write, run_sku_step, select_sku_queue

ORDER_SEARCH = " Order Number:                    Account Number:\n Alpha Search:"
MENU = "SALESN Options\n 03. Order Inquiry\n Ready for option number or command"

# The real screen (Rafael, 2026-09-02) — same text as the capture fixture.
STOCK_DETAIL = """                            S T O C K   I N Q U I R Y

  Stock Number: 03 3933 BK      B-Bike/P-Part: B    Model Year: 2025
  Description:  CODA S2 L16 2026 GLOSS BLACK

     Inventory  NJ       FL       CA                 Price  Quantity
  On Hand       56        0        0       Each    380.95         49
  Unit Meas:   EA
  Duty %                          Weight:    36
                     Cmd10                                          Cmd7
                      NOTES                                          EXIT"""

# Cmd10 NOTES: same title, same stock number, none of the fields.
STOCK_NOTES = """                            S T O C K   I N Q U I R Y
                                                                   (Cmd7-Exit)
  Stock Number: 03 3933 BK      CODA S2 L16 2026 GLOSS BLACK"""


class FakeDriver:
    """Replays screens and records what was typed and pressed, in order."""

    def __init__(self, screens):
        self._screens = list(screens)
        self.actions = []  # ("text"|"key", value)

    def copy_screen(self):
        return self._screens.pop(0) if len(self._screens) > 1 else self._screens[0]

    def type_text(self, text):
        self.actions.append(("text", str(text)))

    def key(self, name):
        self.actions.append(("key", name))


# ── R6: the shape of the SKU is the filter ───────────────────────────────────


@pytest.mark.parametrize(
    "sku,ok",
    [
        ("03-3933BK", True),
        ("01-0169", True),  # no colour suffix — 126 bikes look like this
        ("03-3768BLD", True),  # the three-letter finish
        ("792270149760", False),  # FedEx tracking number
        ("9400130109355378930996", False),  # USPS
        ("Y22A016211", False),  # serial
        ("S/D06-4482BL", False),
        ("", False),
    ],
)
def test_only_as400_stock_numbers_are_looked_up(sku, ok):
    # A tracking number handed a model would land in FedEx's Dimensions table as
    # though it were a carton. The shape decides, so no hand-kept list of
    # exceptions can go stale behind our backs.
    assert is_lookupable(sku) is ok


def test_a_sku_that_is_not_a_stock_number_never_reaches_the_terminal():
    driver = FakeDriver([ORDER_SEARCH])
    with pytest.raises(StockSkuNotFound):
        capture_stock_inquiry("792270149760", driver)
    assert driver.actions == []  # not one keystroke spent on it


# ── R5: the queue ────────────────────────────────────────────────────────────


def test_the_queue_puts_the_bikes_on_the_floor_first():
    # Every unread bike is a candidate now (Rafael, 2026-09-08) — reading the
    # ones whose model is dirty is what makes cleaning them mechanical. The
    # order is what changes, not the membership.
    rows = [
        {"sku": "03-9999BK", "model": "CODA S2", "size": "17", "qty": 5},  # already fine
        {"sku": "03-4159BL", "model": "KROMO L 2025 MIDNIGHT", "size": None, "qty": 33},
        {"sku": "03-4894BL", "model": None, "qty": 0},
        {"sku": "03-3492BL", "model": None, "qty": 1},  # ROW 32, a picker can hold it
        {"sku": "03-8888BK", "model": "TAXI 24 COSMO BLUE", "size": None, "qty": 0},
    ]
    assert [r["sku"] for r in select_sku_queue(rows)] == [
        "03-3492BL",  # no model, on the floor
        "03-4894BL",  # no model, no stock
        "03-4159BL",  # model nobody split, on the floor — one of the 263
        "03-8888BK",  # model nobody split, no stock
        "03-9999BK",  # already split; still read once, but last
    ]


def test_the_queue_keeps_the_order_it_was_handed_inside_a_band():
    # The RPC already sorted by demand. Re-sorting here would be a second answer
    # to one question, which is how a ranking and a queue drift apart.
    rows = [
        {"sku": "03-1111BK", "model": None, "qty": 2},
        {"sku": "03-2222BK", "model": None, "qty": 9},
    ]
    assert [r["sku"] for r in select_sku_queue(rows)] == ["03-1111BK", "03-2222BK"]


def test_a_sku_as400_already_refused_does_not_come_back():
    rows = [{"sku": "03-3492BL", "model": None, "qty": 1}]
    assert select_sku_queue(rows, unknown={"03-3492BL": {"reason": "not_in_as400"}}) == []


def test_a_sku_that_keeps_failing_steps_aside_instead_of_blocking_the_queue(tmp_path, monkeypatch):
    """12 sep 2026, en vivo: `03-4605OR` aterrizaba una y otra vez en el
    formulario NOTES del Stock Inquiry (§2.12b — mismo título, sin campos). La
    guarda se negaba a leerlo (bien), el hueco terminaba, y el siguiente hueco
    le pedía a `next_sku()` **el mismo SKU**: un mismatch no cambia la fila ni la
    lista de desconocidos. Las 1.800 fichas del catálogo detrás de una.

    Un veredicto sería mentira —AS400 sí lo tiene—, así que compra un enfriamiento.
    """
    import sku_enrichment

    monkeypatch.setenv("SKU_UNKNOWN_PATH", str(tmp_path / "u.json"))
    rows = [{"sku": "03-4605OR", "model": None}, {"sku": "03-3933BK", "model": None}]

    # Antes de fallar, es la cabeza de la cola.
    assert select_sku_queue(rows, sku_enrichment.load_unknown())[0]["sku"] == "03-4605OR"

    sku_enrichment.defer_sku("03-4605OR", "landed on the NOTES form")
    cola = select_sku_queue(rows, sku_enrichment.load_unknown())
    assert [r["sku"] for r in cola] == ["03-3933BK"]  # la cola avanza

    # Y vuelve sola cuando el enfriamiento se cumple — no es un veredicto.
    data = sku_enrichment.load_unknown()
    data["03-4605OR"]["until"] = 0
    (tmp_path / "u.json").write_text(json.dumps(data), encoding="utf-8")
    assert len(select_sku_queue(rows, sku_enrichment.load_unknown())) == 2


def test_a_verdict_and_a_cooldown_are_not_the_same_thing(tmp_path, monkeypatch):
    import sku_enrichment

    monkeypatch.setenv("SKU_UNKNOWN_PATH", str(tmp_path / "u.json"))
    sku_enrichment.mark_unknown("01-0001")  # AS400 no lo tiene: para siempre
    sku_enrichment.defer_sku("01-0002", "mismatch")  # nosotros fallamos: un rato
    u = sku_enrichment.load_unknown()
    assert "until" not in u["01-0001"] and sku_enrichment.is_set_aside(u["01-0001"])
    assert u["01-0002"]["until"] > 0 and u["01-0002"]["tries"] == 1
    # y el segundo intento espera el doble
    sku_enrichment.defer_sku("01-0002", "mismatch")
    assert sku_enrichment.load_unknown()["01-0002"]["tries"] == 2


def test_an_sku_that_never_failed_is_not_set_aside():
    import sku_enrichment

    assert sku_enrichment.is_set_aside(None) is False


# ── §6 / R4: what a write would be ───────────────────────────────────────────


def test_the_weight_is_never_written_because_writing_it_would_seal_it():
    # R4 says this path may improve a placeholder and may never claim somebody
    # weighed it, and the database makes the two the same act: PickD's
    # `set_dimensions_verified` sets `weight_verified = true` on ANY update that
    # changes `weight_lbs`, and it is monotonic. So AS400's 36 would be filed as
    # a scale reading for the bike PickD weighed at 33.6. It stays in the
    # snapshot, where it says whose number it is.
    row = {"sku": "03-3933BK", "model": "CODA S2", "weight_lbs": 45, "weight_verified": False}
    assert plan_write(row, {"weight_lbs": 36.0}) == {}
    plan = plan_write(row, {"description": "CODA S2 L16 2026 GLOSS BLACK", "weight_lbs": 36.0})
    assert "weight_lbs" not in plan
    assert plan["as400_snapshot"]["weight_lbs"] == 36.0


def test_a_scale_reading_is_never_touched():
    # AS400 says 36 for the bike Pickd weighed at 33.6. The scale wins, always.
    row = {"sku": "03-3933BK", "model": "CODA S2", "weight_lbs": 33.6, "weight_verified": True}
    assert plan_write(row, {"weight_lbs": 36.0}) == {}


def test_the_plan_never_marks_a_weight_as_verified():
    # R4: this path can improve a placeholder, it cannot claim somebody weighed it.
    row = {"sku": "03-3933BK", "model": "CODA S2", "weight_lbs": 45, "weight_verified": False}
    assert "weight_verified" not in plan_write(row, {"weight_lbs": 36.0})


def test_an_unchanged_weight_is_not_a_write():
    row = {"sku": "03-3933BK", "model": "CODA S2", "weight_lbs": 36.0, "weight_verified": False}
    assert plan_write(row, {"weight_lbs": 36.0}) == {}


def test_the_name_is_recorded_even_when_a_model_is_already_there():
    # This is the widening of 2026-09-08 and the whole point of it: the 227 rows
    # with a dirty model are exactly the ones that need the manufacturer's name,
    # and writing it overwrites nothing — `as400_description` is the empty gap.
    parsed = {"description": "CODA S2 L16 2026 GLOSS BLACK", "weight_lbs": None}
    gap = {"sku": "03-3933BK", "model": None, "weight_verified": True}
    dirty = {"sku": "03-3933BK", "model": "CODA S2 L16", "weight_verified": True}
    assert plan_write(gap, parsed)["as400_description"] == "CODA S2 L16 2026 GLOSS BLACK"
    assert plan_write(dirty, parsed)["as400_description"] == "CODA S2 L16 2026 GLOSS BLACK"
    # and it still never touches the grouping key itself
    for never in ("model", "size", "color"):
        assert never not in plan_write(dirty, parsed)


def test_the_watchdog_writes_the_name_raw_and_never_splits_it():
    # Rafael, 2026-09-08: "pickd la parte". parseBikeName lives in Pickd's
    # TypeScript and is not mirrored here, so this side writes what it READ.
    # It also keeps the watchdog clear of R12: a raw name is not the FedEx
    # grouping key, so it owes no export simulation — `model` does, which is
    # exactly why `model` is not written from here.
    plan = plan_write(
        {"sku": "03-3933BK", "model": None, "weight_verified": True},
        {"description": "CODA S2 L16 2026 GLOSS BLACK", "weight_lbs": None},
    )
    assert plan["as400_description"] == "CODA S2 L16 2026 GLOSS BLACK"
    assert plan["as400_read_at"]
    for never in ("model", "size", "color"):
        assert never not in plan


def test_a_sku_already_read_is_not_planned_again():
    # Q7. The model stays empty until Pickd splits the description, so without
    # this the same SKU would come back every gap for ever.
    row = {
        "sku": "03-3933BK",
        "model": None,
        "weight_verified": True,
        "as400_description": "CODA S2 L16 2026 GLOSS BLACK",
    }
    assert plan_write(row, {"description": "CODA S2 L16 2026 GLOSS BLACK"}) == {}


def test_a_sku_already_read_is_not_queued_again():
    rows = [
        {"sku": "03-3492BL", "model": None, "qty": 1, "as400_description": "TRAIL X A1 13"},
        {"sku": "03-4473BK", "model": None, "qty": 1},
    ]
    assert [r["sku"] for r in select_sku_queue(rows)] == ["03-4473BK"]


# ── the route ────────────────────────────────────────────────────────────────


def test_the_lookup_types_exactly_what_the_operator_types():
    # Rafael, 2026-09-02 and 2026-09-08: 2 → ENTER → the digits WITHOUT the dash
    # → TAB → the colour code → TAB → X, and NOTHING after the X — it submits by
    # itself. Option 2, never 3: 3 is the order screen.
    driver = FakeDriver([ORDER_SEARCH, MENU, STOCK_DETAIL, STOCK_DETAIL])
    assert capture_stock_inquiry("03-3933BK", driver, page_wait=0, step_wait=0) == STOCK_DETAIL
    assert driver.actions == [
        ("key", "f7"),  # order search → menu
        ("text", "2"),
        ("key", "enter"),
        ("text", "033933"),
        ("key", "tab"),
        ("text", "BK"),
        ("key", "tab"),
        ("text", "X"),
    ]
    assert ("text", "3") not in driver.actions  # 3 is for orders, never for stock


def test_a_menu_that_did_not_open_stock_inquiry_never_gets_a_sku_typed_into_it():
    # And it must NOT read as "AS400 doesn't have this SKU". That answer marks
    # the SKU and drops it from the queue for ever (R7), so a navigation failure
    # would quietly poison the queue one good SKU at a time.
    driver = FakeDriver([ORDER_SEARCH, MENU, MENU])  # option 2 didn't take
    with pytest.raises(StockScreenMismatch):
        capture_stock_inquiry("03-3933BK", driver, page_wait=0, step_wait=0)
    assert ("text", "033933") not in driver.actions  # nothing typed into it


def test_a_sku_with_no_colour_suffix_tabs_straight_past_the_field():
    # Rafael, 2026-09-02: "se presiona tab directo sin llenar el color, si el sku
    # no lo tiene". The 126 bikes shaped like this are in the queue, not excluded.
    screen = STOCK_DETAIL.replace("Stock Number: 03 3933 BK", "Stock Number: 01 0169   ")
    driver = FakeDriver([ORDER_SEARCH, MENU, screen, screen])
    capture_stock_inquiry("01-0169", driver, page_wait=0, step_wait=0)
    typed = [a for a in driver.actions if a[0] == "text"]
    assert typed == [("text", "2"), ("text", "010169"), ("text", "X")]
    assert driver.actions.count(("key", "tab")) == 2  # both tabs pressed, one field left blank


def test_the_notes_screen_is_refused_instead_of_read():
    # R10. Same title, no fields: reading here would report the weight as ABSENT
    # rather than as "I am not where I think I am".
    driver = FakeDriver([ORDER_SEARCH, MENU, STOCK_NOTES, STOCK_NOTES])
    with pytest.raises(StockScreenMismatch):
        capture_stock_inquiry("03-3933BK", driver, page_wait=0, step_wait=0)


def test_a_lookup_that_lands_nowhere_is_an_unknown_sku():
    # The stock program DID open, so this is the SKU's own answer, not navigation.
    driver = FakeDriver([ORDER_SEARCH, MENU, STOCK_DETAIL, "Invalid stock number"])
    with pytest.raises(StockSkuNotFound):
        capture_stock_inquiry("03-3933BK", driver, page_wait=0, step_wait=0)


# ── R3: the way home ─────────────────────────────────────────────────────────


def test_the_way_home_walks_the_menu_and_proves_it_arrived():
    driver = FakeDriver([STOCK_DETAIL, MENU, ORDER_SEARCH])
    return_to_order_search(driver, step_wait=0)
    assert driver.actions == [
        ("key", "f6"),
        ("key", "f6"),
        ("key", "f7"),  # unknown-ish screen → the operator's way out
        ("text", "3"),
        ("key", "enter"),  # menu → order search
    ]


def test_the_way_home_gives_up_after_three_tries(monkeypatch):
    # Rafael, 2026-09-02: three attempts, then stop. It must not hammer keys at a
    # terminal nobody is watching.
    monkeypatch.setenv("AS400_UNSTICK_TRIES", "3")
    driver = FakeDriver(["SOME OTHER PROGRAM"])
    with pytest.raises(AS400ManualLoginRequired):
        return_to_order_search(driver, step_wait=0)
    assert driver.actions == [("key", "f6"), ("key", "f6"), ("key", "f7")] * 3


# ── the step ─────────────────────────────────────────────────────────────────


def test_the_step_reads_logs_and_writes_nothing():
    row = {"sku": "03-3933BK", "model": None, "weight_lbs": 45, "weight_verified": False}
    res = run_sku_step(
        object(),
        row,
        capture_fn=lambda sku, driver: STOCK_DETAIL,
        return_fn=lambda driver: None,
    )
    assert res["action"] == "read"
    assert res["parsed"]["description"] == "CODA S2 L16 2026 GLOSS BLACK"
    assert res["parsed"]["weight_lbs"] == 36.0
    # A PLAN — F2 logs it, nothing applies it. No weight: that is F4's switch.
    assert res["plan"]["as400_description"] == "CODA S2 L16 2026 GLOSS BLACK"
    assert "weight_lbs" not in res["plan"]
    assert res["returned"] is True


def test_the_step_writes_nothing_when_the_screen_shows_another_sku():
    # The only defence against a mistyped lookup: the screen has to be showing
    # the record we asked for, or the step reports and stops.
    row = {"sku": "03-4473BK", "model": None, "weight_lbs": 45, "weight_verified": False}
    res = run_sku_step(
        object(),
        row,
        capture_fn=lambda sku, driver: STOCK_DETAIL,  # shows 03-3933BK
        return_fn=lambda driver: None,
    )
    assert res["action"] == "mismatch"
    assert res["screen_sku"] == "03-3933BK"
    assert "plan" not in res


def test_the_terminal_comes_home_even_when_the_lookup_blows_up():
    # The return trip is part of the step, not a tidy-up: a step that leaves the
    # terminal on a stock screen costs the scanner its next order.
    came_home = []

    def boom(sku, driver):
        raise StockScreenMismatch("landed on NOTES")

    res = run_sku_step(
        object(), {"sku": "03-3933BK"}, capture_fn=boom, return_fn=lambda d: came_home.append(True)
    )
    assert res["action"] == "mismatch"
    assert came_home == [True]
    assert res["returned"] is True


def test_a_failed_return_is_reported_not_swallowed():
    def cannot_return(driver):
        raise AS400ManualLoginRequired("stuck")

    res = run_sku_step(
        object(),
        {"sku": "03-3933BK", "model": None, "weight_verified": True},
        capture_fn=lambda sku, driver: STOCK_DETAIL,
        return_fn=cannot_return,
    )
    assert res["returned"] is False


def test_an_unknown_sku_is_marked_so_the_queue_does_not_jam(tmp_path, monkeypatch):
    # R7. Without this the queue spins forever on the first SKU AS400 lacks.
    monkeypatch.setenv("SKU_UNKNOWN_PATH", str(tmp_path / "unknown.json"))

    def missing(sku, driver):
        raise StockSkuNotFound("no record")

    res = run_sku_step(object(), {"sku": "03-3492BL"}, capture_fn=missing, return_fn=lambda d: None)
    assert res["action"] == "unknown"

    from sku_enrichment import load_unknown

    assert "03-3492BL" in load_unknown()
    assert select_sku_queue([{"sku": "03-3492BL", "model": None, "qty": 1}], load_unknown()) == []


# ── the wiring into the scanner ──────────────────────────────────────────────


def test_the_gap_does_nothing_while_the_switch_is_off(monkeypatch):
    # The Bay 2 deploy that carries this also carries the change from one
    # F6·F6·F7 to three. A deploy must not start driving the terminal by itself:
    # the switch is flipped in .env, deliberately, after the deploy is in.
    import auto_scanner
    import sku_enrichment

    monkeypatch.delenv("SKU_ENRICH", raising=False)
    called = []
    monkeypatch.setattr(sku_enrichment, "next_sku", lambda *a, **k: called.append(1))
    auto_scanner._run_sku_gap()
    assert called == []


def _gap_harness(monkeypatch, *, idle=1e9, results=None, hops=None):
    """Wire _run_sku_gap up to fakes and return the list of SKUs it looked up.

    `system_idle_seconds` is always faked: the real one shells out to ioreg and
    would make these tests depend on whether somebody is touching this Mac.

    `operator_idle_seconds` remembers the last event it judged to be a PERSON's,
    and that memory is module state — so it has to be cleared here, or a gap
    test inherits whoever the previous one pretended was at the keyboard.

    Our own input stamp is cleared to **None = never typed**, not to 0.0. It was
    0.0, and that is what made this file's burst test pass in the suite and fail
    on its own: `time.monotonic()` counts from process start here, so 0.0 means
    "the watchdog typed the moment the process began". Run alone, that was a few
    milliseconds ago, every faked operator keystroke looked older than our own
    typing, and the gate that should have fired never did. Run late in the
    suite, the same 0.0 was thirteen seconds ago and the test passed. The
    fixture was lying, not the gate.
    """
    import as400_capture
    import auto_scanner
    import sku_enrichment

    monkeypatch.setattr(auto_scanner, "_last_operator_input", None)
    monkeypatch.setattr(as400_capture, "_last_self_input", None)
    monkeypatch.setenv("SKU_ENRICH", "1")
    monkeypatch.setattr(auto_scanner, "_driver_for_sku_step", lambda: object())
    idles = iter(idle) if isinstance(idle, list) else None
    monkeypatch.setattr(
        auto_scanner, "system_idle_seconds", (lambda: next(idles)) if idles else (lambda: idle)
    )
    monkeypatch.setattr(sku_enrichment, "next_sku", lambda *a, **k: {"sku": "03-3492BL"})
    seen = []
    outcomes = iter(results or [])

    def step(driver, row, **kw):
        seen.append(row["sku"])
        if hops is not None:
            hops.append(kw.get("on_search_screen"))
        return next(outcomes, {"action": "read", "sku": row["sku"], "returned": True})

    monkeypatch.setattr(sku_enrichment, "run_sku_step", step)
    return seen


def test_the_gap_fills_itself_instead_of_sleeping_through(monkeypatch):
    # The gap is twenty minutes and one lookup costs seconds. With 745 bikes to
    # read, one per gap is 28 business days — this is Rafael's original "unos 5
    # minutos" (2026-09-02), which the queue's size earned back.
    monkeypatch.setenv("SKU_ENRICH_MAX_PER_GAP", "6")
    seen = _gap_harness(monkeypatch)
    import auto_scanner

    auto_scanner._run_sku_gap()
    assert len(seen) == 6


def test_one_per_gap_comes_back_with_a_single_env_line(monkeypatch):
    # The old cadence is a .env edit away, not a deploy.
    monkeypatch.setenv("SKU_ENRICH_MAX_PER_GAP", "1")
    seen = _gap_harness(monkeypatch)
    import auto_scanner

    auto_scanner._run_sku_gap()
    assert len(seen) == 1


def test_the_burst_stops_the_moment_the_operator_touches_the_keyboard(monkeypatch):
    # THE point of this change. The idle gate used to be checked once, before
    # the gap, so a burst could have run straight through somebody sitting down.
    # Now it is re-checked before every single lookup: idle, idle, then busy.
    monkeypatch.setenv("SKU_ENRICH_MAX_PER_GAP", "10")
    seen = _gap_harness(monkeypatch, idle=[1e9, 1e9, 3.0])
    import auto_scanner

    auto_scanner._run_sku_gap()
    assert len(seen) == 2  # the third never started


def test_the_gap_types_the_next_sku_where_it_stands(monkeypatch):
    """The automatic burst was the slow one, and it is the one with a weekend.

    Measured on Bay 2 on 11 sep 2026: ~27 s per SKU going back to the MENU
    between lookups, against ~7 s on the manual run, which types the next SKU on
    the search form Cmd7 already left us on. Same rule as
    `run_until_disturbed`: verified first, optimistic while they come back
    clean, verified again after anything else.
    """
    import auto_scanner

    monkeypatch.setenv("SKU_ENRICH_MAX_PER_GAP", "4")
    hops = []
    _gap_harness(
        monkeypatch,
        hops=hops,
        results=[
            {"action": "read", "sku": "A", "returned": True},
            {"action": "read", "sku": "B", "returned": True},
            {"action": "unknown", "sku": "C", "returned": True},
            {"action": "read", "sku": "D", "returned": True},
        ],
    )
    recovered = []
    import as400_capture

    monkeypatch.setattr(as400_capture, "return_to_order_search", lambda d: recovered.append(1))
    auto_scanner._run_sku_gap()
    # 1st verified, 2nd and 3rd optimistic, 4th verified again after the unknown.
    assert hops == [False, True, True, False]
    assert len(recovered) == 2  # once to recover from the unknown, once at the end


def test_the_heartbeat_says_which_way_the_step_failed(monkeypatch):
    # 11 sep 2026: the heartbeat said `working` for hours while the read counter
    # sat at 17. A step was running and dying before it read anything, and the
    # only place that said which of the four ways was a log file on a Mac in Bay
    # 2 with nobody in front of it. A number that does not move is not a
    # diagnosis.
    import auto_scanner

    monkeypatch.setenv("SKU_ENRICH_MAX_PER_GAP", "5")
    _gap_harness(
        monkeypatch,
        results=[
            {
                "action": "unavailable",
                "sku": "03-3492BL",
                "why": "The AS400 isn't connected",
                "returned": True,
            }
        ],
    )
    auto_scanner._run_sku_gap()
    reason = auto_scanner.gap_state()["reason"]
    assert "unavailable" in reason and "isn't connected" in reason


def test_a_step_that_read_says_only_working(monkeypatch):
    import auto_scanner

    monkeypatch.setenv("SKU_ENRICH_MAX_PER_GAP", "1")
    _gap_harness(monkeypatch)
    auto_scanner._run_sku_gap()
    assert auto_scanner.gap_state()["reason"] == "working"


def test_a_manual_get_orders_now_wins_over_catalogue_work(monkeypatch):
    # They asked for orders, not for catalogue work.
    import auto_scanner

    monkeypatch.setenv("SKU_ENRICH_MAX_PER_GAP", "10")
    seen = _gap_harness(monkeypatch)
    auto_scanner._kick.set()
    try:
        auto_scanner._run_sku_gap()
    finally:
        auto_scanner._kick.clear()
    assert seen == []


def test_the_budget_ends_the_burst_even_with_the_count_left(monkeypatch):
    monkeypatch.setenv("SKU_ENRICH_MAX_PER_GAP", "100")
    monkeypatch.setenv("SKU_ENRICH_GAP_BUDGET_SEC", "0")
    seen = _gap_harness(monkeypatch)
    import auto_scanner

    auto_scanner._run_sku_gap()
    assert seen == []


def test_the_queue_pauses_when_the_terminal_did_not_come_home(monkeypatch):
    # A step that left the terminal on a stock screen must not be followed by
    # another one: the next cycle's bootstrap is what recovers it.
    monkeypatch.setenv("SKU_ENRICH_MAX_PER_GAP", "5")
    seen = _gap_harness(
        monkeypatch, results=[{"action": "read", "sku": "03-3492BL", "returned": False}]
    )
    import auto_scanner

    auto_scanner._run_sku_gap()
    assert len(seen) == 1


def test_a_crash_in_the_sku_step_never_stops_the_orders(monkeypatch):
    # This is a side errand. The scanner exists for the orders.
    import auto_scanner
    import sku_enrichment

    monkeypatch.setenv("SKU_ENRICH", "1")
    monkeypatch.setattr(auto_scanner, "_driver_for_sku_step", lambda: object())

    def boom(*a, **k):
        raise RuntimeError("the database is on fire")

    monkeypatch.setattr(sku_enrichment, "next_sku", boom)
    auto_scanner._run_sku_gap()  # must not raise


# ── F3: the write itself ─────────────────────────────────────────────────────


class FakeTable:
    """The two calls apply_write makes, and nothing else."""

    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def table(self, name):
        self.calls.append(("table", name))
        return self

    def update(self, values):
        self.calls.append(("update", values))
        return self

    def eq(self, col, val):
        self.calls.append(("eq", col, val))
        return self

    def execute(self):
        return type("R", (), {"data": self.rows})()


def test_the_write_is_an_update_by_sku_never_an_upsert():
    # An upsert on a SKU that somehow isn't in the catalogue would CREATE a
    # metadata row with no inventory behind it — the orphan shape Pickd spent a
    # migration cleaning up.
    from sku_enrichment import apply_write

    # The returned row carries the column, which is how we know the write landed
    # rather than being dropped by PostgREST.
    client = FakeTable([{"sku": "03-3933BK", "as400_description": "CODA S2"}])
    assert apply_write("03-3933BK", {"as400_description": "CODA S2"}, client) == {"written": 1}
    assert client.calls == [
        ("table", "sku_metadata"),
        ("update", {"as400_description": "CODA S2"}),
        ("eq", "sku", "03-3933BK"),
    ]


def test_an_empty_plan_never_reaches_the_database():
    from sku_enrichment import apply_write

    client = FakeTable([])
    assert apply_write("03-3933BK", {}, client) == {"written": 0}
    assert client.calls == []


def test_the_step_writes_only_once_f3_is_switched_on(monkeypatch):
    import sku_enrichment

    row = {"sku": "03-3933BK", "model": None, "weight_lbs": 45, "weight_verified": False}
    written = []
    monkeypatch.setattr(
        sku_enrichment,
        "apply_write",
        lambda sku, plan, client=None: (written.append((sku, plan)), {"written": 1})[1],
    )

    monkeypatch.delenv("SKU_ENRICH_WRITE", raising=False)
    res = run_sku_step(
        object(), dict(row), capture_fn=lambda s, d: STOCK_DETAIL, return_fn=lambda d: None
    )
    assert res["action"] == "read" and written == []  # F2: the plan is logged, not applied

    monkeypatch.setenv("SKU_ENRICH_WRITE", "1")
    res = run_sku_step(
        object(), dict(row), capture_fn=lambda s, d: STOCK_DETAIL, return_fn=lambda d: None
    )
    assert res["action"] == "written"
    assert written[0][0] == "03-3933BK"
    assert written[0][1]["as400_description"] == "CODA S2 L16 2026 GLOSS BLACK"
    assert "weight_lbs" not in written[0][1]  # never: writing it would seal the flag
    assert "weight_verified" not in written[0][1]


def test_a_sku_with_no_catalogue_row_is_registered_not_updated(monkeypatch):
    # The discovery queue. There is no row to fill in, so the step calls the
    # alta instead of the update — behind the same F3 switch as every write.
    import sku_enrichment

    row = {"sku": "03-3933BK", "unregistered": True}
    calls = []
    monkeypatch.setattr(
        sku_enrichment,
        "register_from_as400",
        lambda sku, parsed, client=None: (
            calls.append((sku, parsed)),
            {"action": "registered", "sku": sku},
        )[1],
    )
    monkeypatch.setattr(
        sku_enrichment,
        "apply_write",
        lambda *a, **k: pytest.fail("an unregistered SKU must never take the update path"),
    )

    monkeypatch.delenv("SKU_ENRICH_WRITE", raising=False)
    res = run_sku_step(
        object(), dict(row), capture_fn=lambda s, d: STOCK_DETAIL, return_fn=lambda d: None
    )
    assert res["action"] == "read" and calls == []  # F2 logs the alta, never does it

    monkeypatch.setenv("SKU_ENRICH_WRITE", "1")
    res = run_sku_step(
        object(), dict(row), capture_fn=lambda s, d: STOCK_DETAIL, return_fn=lambda d: None
    )
    assert res["action"] == "registered"
    assert calls[0][0] == "03-3933BK"
    assert calls[0][1]["description"] == "CODA S2 L16 2026 GLOSS BLACK"


def test_the_alta_sends_as400s_own_bike_or_part_answer(monkeypatch):
    # AS400's `B-Bike/P-Part` is the authoritative answer to what PickD guesses
    # from a two-digit prefix. Registered blind, a part in department 03 would
    # be born a bike — 45 lb and a bike carton, which is what the FedEx export
    # reads.
    import sku_enrichment

    sent = {}

    class _Rpc:
        def execute(self):
            return type("R", (), {"data": {"action": "registered"}})()

    class _Client:
        def rpc(self, name, args):
            sent.update({"name": name, **args})
            return _Rpc()

    out = sku_enrichment.register_from_as400(
        "03-9999ZZ",
        {"description": "FRAME RENEGADE S1 61 2026 CHAR", "kind": "P", "on_hand": {"NJ": 3}},
        client=_Client(),
    )
    assert out["action"] == "registered"
    assert sent["name"] == "register_sku_from_as400"
    assert sent["p_is_bike"] is False  # not the prefix's answer
    assert sent["p_location"] == "UNKNOWN"
    assert sent["p_as400_snapshot"]["on_hand"] == {"NJ": 3}


def test_the_alta_refuses_a_screen_with_no_name():
    # A row called nothing helps nobody find a box, and the RPC would reject it.
    import sku_enrichment

    out = sku_enrichment.register_from_as400("03-9999ZZ", {"description": "  "}, client=object())
    assert out["action"] == "skipped"


def test_a_write_that_vanished_into_a_missing_column_is_not_a_success():
    # PostgREST DROPS an unknown column without erroring and still returns the
    # row, so a watcher deployed onto a database that never got the migration
    # would log success, write nothing, and hand the same SKU back every gap for
    # ever. The returned representation is the proof the column exists.
    from sku_enrichment import MissingColumn, apply_write

    client = FakeTable([{"sku": "03-3933BK"}])  # no as400_description in the row
    with pytest.raises(MissingColumn):
        apply_write("03-3933BK", {"as400_description": "CODA S2"}, client)


def test_a_write_that_landed_is_a_success():
    from sku_enrichment import apply_write

    client = FakeTable([{"sku": "03-3933BK", "as400_description": "CODA S2"}])
    assert apply_write("03-3933BK", {"as400_description": "CODA S2"}, client) == {"written": 1}


# ── the compare button ───────────────────────────────────────────────────────


def test_the_whole_screen_is_kept_not_just_the_name():
    # Four of the five fields were being parsed and thrown away — AS400's own
    # on-hand, its weight, its B/P classification, the model year — and those
    # are what answer "where do Pickd and AS400 disagree".
    plan = plan_write(
        {"sku": "03-3933BK", "model": None, "weight_verified": True},
        {
            "description": "CODA S2 L16 2026 GLOSS BLACK",
            "kind": "B",
            "model_year": "2025",
            "weight_lbs": 36.0,
            "on_hand": {"NJ": 56, "FL": 0, "CA": 0},
        },
    )
    snap = plan["as400_snapshot"]
    assert snap["on_hand"] == {"NJ": 56, "FL": 0, "CA": 0}
    assert snap["weight_lbs"] == 36.0
    assert snap["kind"] == "B"
    assert snap["model_year"] == "2025"


def test_the_gap_reports_how_long_it_actually_worked(monkeypatch):
    # The loop needs the number to decide whether the wait already happened.
    import auto_scanner

    monkeypatch.setenv("SKU_ENRICH_MAX_PER_GAP", "2")
    _gap_harness(monkeypatch)
    assert auto_scanner._run_sku_gap() >= 0.0


def test_nothing_to_work_on_reports_no_time_spent(monkeypatch):
    # An empty queue must NOT look like "the wait already happened", or the
    # scanner would ask AS400 for the same missing order every few seconds.
    import auto_scanner
    import sku_enrichment

    monkeypatch.setenv("SKU_ENRICH", "1")
    monkeypatch.setattr(auto_scanner, "system_idle_seconds", lambda: 1e9)
    monkeypatch.setattr(auto_scanner, "_driver_for_sku_step", lambda: object())
    monkeypatch.setattr(sku_enrichment, "next_sku", lambda *a, **k: None)
    assert auto_scanner._run_sku_gap() < auto_scanner.MIN_WORK_TO_SKIP_WAIT_SEC


def test_the_switch_being_off_reports_no_time_spent(monkeypatch):
    import auto_scanner

    monkeypatch.delenv("SKU_ENRICH", raising=False)
    assert auto_scanner._run_sku_gap() == 0.0


def test_the_catalogue_stands_down_for_a_pending_update(monkeypatch):
    # Catalogue work is the lowest-priority thing here: it yields to the
    # operator, to the orders, and to a deploy waiting for the terminal.
    import auto_scanner
    import auto_update

    monkeypatch.setenv("SKU_ENRICH_MAX_PER_GAP", "10")
    seen = _gap_harness(monkeypatch)
    auto_update.update_pending.set()
    try:
        auto_scanner._run_sku_gap()
    finally:
        auto_update.update_pending.clear()
    assert seen == []


# ── the operator's open-ended run ────────────────────────────────────────────
#
# Rafael, 10 sep 2026: "quiero que cuando lo active manualmente no se pare hasta
# que yo mueva algo… si yo quiero órdenes regreso y presiono get orders now".


def _step_ok(_driver, row, **_kw):
    return {"action": "read", "sku": row.get("sku")}


def _queue(monkeypatch, n):
    rows = [{"sku": f"03-000{i}BK"} for i in range(n)]
    monkeypatch.setattr(sku_enrichment, "next_sku", lambda: rows.pop(0) if rows else None)


def test_it_keeps_going_while_nobody_touches_the_mac(monkeypatch):
    _queue(monkeypatch, 5)
    out = sku_enrichment.run_until_disturbed(
        None,
        home_fn=lambda _d: None,
        idle_fn=lambda: 1e9,
        kick_fn=lambda: False,
        update_pending_fn=lambda: False,
        step_fn=_step_ok,
    )
    assert out["read"] == 5
    assert out["stopped"] == "the queue is empty"


def test_zero_idle_at_the_start_does_not_stop_it_before_the_first_lookup(monkeypatch):
    # The operator just clicked the button, so idle is 0. Reading that as "the
    # operator is here" is what used to make it do one and stop.
    _queue(monkeypatch, 3)
    out = sku_enrichment.run_until_disturbed(
        None,
        home_fn=lambda _d: None,
        idle_fn=lambda: 1e9,
        kick_fn=lambda: False,
        update_pending_fn=lambda: False,
        step_fn=_step_ok,
    )
    assert out["read"] == 3


def test_a_touch_after_it_started_stops_it(monkeypatch):
    # Idle that no longer keeps up with our own elapsed time means somebody
    # touched the machine after we began.
    _queue(monkeypatch, 10)
    calls = {"n": 0}

    def idle():
        calls["n"] += 1
        return 1e9 if calls["n"] <= 2 else 0.0

    out = sku_enrichment.run_until_disturbed(
        None,
        home_fn=lambda _d: None,
        idle_fn=idle,
        kick_fn=lambda: False,
        update_pending_fn=lambda: False,
        step_fn=_step_ok,
        grace=0,
    )
    assert out["read"] == 2
    assert out["stopped"] == "the operator is back"


def test_get_orders_now_takes_the_terminal_back(monkeypatch):
    _queue(monkeypatch, 10)
    kicked = {"v": False}

    def step(driver, row, **_kw):
        kicked["v"] = True  # the operator presses it while a lookup is running
        return _step_ok(driver, row)

    out = sku_enrichment.run_until_disturbed(
        None,
        home_fn=lambda _d: None,
        idle_fn=lambda: 1e9,
        kick_fn=lambda: kicked["v"],
        update_pending_fn=lambda: False,
        step_fn=step,
    )
    assert out["read"] == 1
    assert out["stopped"] == "orders requested"


def test_it_yields_to_a_pending_deploy(monkeypatch):
    # The run holds capture_lock and the updater refuses to restart during a
    # capture — without this it would pin Bay 2 to an old build for hours.
    _queue(monkeypatch, 10)
    out = sku_enrichment.run_until_disturbed(
        None,
        home_fn=lambda _d: None,
        idle_fn=lambda: 1e9,
        kick_fn=lambda: False,
        update_pending_fn=lambda: True,
        step_fn=_step_ok,
    )
    assert out["read"] == 0
    assert out["stopped"] == "an update is waiting"


def test_it_stops_when_the_as400_goes_away(monkeypatch):
    _queue(monkeypatch, 10)
    out = sku_enrichment.run_until_disturbed(
        None,
        home_fn=lambda _d: None,
        idle_fn=lambda: 1e9,
        kick_fn=lambda: False,
        update_pending_fn=lambda: False,
        step_fn=lambda d, r, **_kw: {"action": "unavailable"},
    )
    assert out["stopped"] == "the AS400 is not available"


def test_the_grace_window_lets_the_operator_walk_away(monkeypatch):
    # Pressing the button IS being at the Mac: idle is zero at that moment. With
    # no grace the very next check reads the hand that just clicked and stops,
    # so the run could never be watched starting — the same trap as having to
    # type on Bay 2 to learn why Bay 2 is idle.
    _queue(monkeypatch, 4)
    out = sku_enrichment.run_until_disturbed(
        None,
        home_fn=lambda _d: None,
        idle_fn=lambda: 0.0,  # a hand on the mouse the whole time
        kick_fn=lambda: False,
        update_pending_fn=lambda: False,
        step_fn=_step_ok,
        grace=3600,  # …but we are still inside the grace
    )
    assert out["read"] == 4
    assert out["stopped"] == "the queue is empty"


def test_without_the_grace_a_hand_on_the_mouse_stops_it_at_once(monkeypatch):
    _queue(monkeypatch, 4)
    out = sku_enrichment.run_until_disturbed(
        None,
        home_fn=lambda _d: None,
        idle_fn=lambda: 0.0,
        kick_fn=lambda: False,
        update_pending_fn=lambda: False,
        step_fn=_step_ok,
        grace=0,
    )
    assert out["read"] == 0
    assert out["stopped"] == "the operator is back"


# ── the short hop between lookups ────────────────────────────────────────────
#
# Rafael, 11 sep 2026: "no quiero volver a ver que se sale de la busqueda de sku
# a proposito en vez de seguir con el siguiente en la lista".


def test_between_lookups_it_stops_at_the_menu_not_the_order_search():
    # Going home between two SKUs means typing 3 to enter the order search and
    # F7 to leave it again — out of the menu to walk straight back in.
    used = {}

    def capture(sku, driver):
        return "STOCK INQUIRY"

    def parse(screen):
        return {"description": "X", "on_hand": {"NJ": 1}}

    for home, expected in (("menu", "menu"), ("order_search", "order_search")):
        used.clear()
        run_sku_step(
            object(),
            {"sku": "03-3933BK"},
            capture_fn=capture,
            parse_fn=parse,
            return_fn=lambda _d, h=home: used.setdefault("went", h),
            home=home,
        )
        assert used["went"] == expected


def test_the_run_goes_all_the_way_home_once_at_the_end(monkeypatch):
    _queue(monkeypatch, 3)
    hops = []
    home = []
    out = sku_enrichment.run_until_disturbed(
        None,
        idle_fn=lambda: 1e9,
        kick_fn=lambda: False,
        update_pending_fn=lambda: False,
        step_fn=lambda d, r, home="order_search", **kw: (
            hops.append((home, kw.get("on_search_screen"))) or {"action": "read"}
        ),
        home_fn=lambda _d: home.append("order_search"),
    )
    assert out["read"] == 3
    # Never out to the menu and back between lookups: Cmd7 to the search form,
    # and from the second one on it does not even look before typing.
    assert hops == [("search", False), ("search", True), ("search", True)]
    assert home == ["order_search"]  # the long way home, exactly once, at the end


def test_one_bad_lookup_drops_it_back_to_the_verified_way(monkeypatch):
    # A mismatch means we do not know what is on the screen any more, so the
    # next SKU must not be typed blind.
    _queue(monkeypatch, 3)
    hops = []
    recovered = []
    actions = iter(["read", "mismatch", "read"])
    sku_enrichment.run_until_disturbed(
        None,
        idle_fn=lambda: 1e9,
        kick_fn=lambda: False,
        update_pending_fn=lambda: False,
        step_fn=lambda d, r, home="order_search", **kw: (
            hops.append(kw.get("on_search_screen")) or {"action": next(actions)}
        ),
        home_fn=lambda _d: recovered.append(1),
    )
    # 1st verified, 2nd optimistic (the 1st read fine), 3rd verified again.
    assert hops == [False, True, False]
    assert len(recovered) == 2  # once to recover, once at the end


# ── the weekend queue: bikes, then parts ─────────────────────────────────────
#
# Rafael, 11 sep 2026: "tienes todo el fin de semana para aprovechar al as400,
# solo el watcher lo está usando". Parts were out of scope while this was about
# the bike CATALOGUE — a part has no model, size or colour to split — and very
# much in scope now that the same screen answers what AS400 thinks is on the
# shelf: parts are 95% of the units in LUDLOW.


class _FakeTable:
    def __init__(self, which, store, calls):
        self.which, self.store, self.calls, self.f = which, store, calls, {}

    def select(self, *_a, **_k):
        return self

    def eq(self, k, v):
        self.f[k] = ("eq", v)
        return self

    def neq(self, k, v):
        self.f[k] = ("neq", v)
        return self

    def is_(self, *_a):
        return self

    def gt(self, *_a):
        return self

    def in_(self, *_a):
        return self

    def order(self, *_a, **_k):
        return self

    def limit(self, *_a):
        return self

    def execute(self):
        if self.which in ("unregistered", "inv"):
            key = self.which
        else:
            key = "bikes" if self.f.get("is_bike") == ("eq", True) else "parts"
        self.calls.append(key)
        return type("R", (), {"data": self.store[key]})()


class _FakeClient:
    def __init__(self, bikes, parts, unregistered=None):
        self.store = {
            "bikes": bikes,
            "parts": parts,
            "unregistered": unregistered or [],
            "inv": [],
        }
        self.calls = []

    def table(self, name):
        if name == "inventory":
            return _FakeTable("inv", {"inv": []}, [])
        if name == "v_as400_skus_unregistered":
            return _FakeTable("unregistered", self.store, self.calls)
        return _FakeTable("meta", self.store, self.calls)


def test_what_the_catalogue_does_not_have_at_all_goes_first():
    # Rafael, 11 sep 2026. 55 SKUs, ~6 minutes of terminal, and every one is a
    # line that says UNREG in Double Check today — the other two queues are
    # 1,800 SKUs of tidying.
    c = _FakeClient(
        bikes=[{"sku": "03-4039BR"}],
        parts=[{"sku": "98-6860"}],
        unregistered=[{"sku": "86-0027", "last_name": "CHAINGUIDE EVO UPPER"}],
    )
    rows = sku_enrichment.fetch_candidates(c)
    assert [r["sku"] for r in rows] == ["86-0027"]
    assert rows[0]["unregistered"] is True
    assert "bikes" not in c.calls and "parts" not in c.calls  # never even asked


def test_a_bike_still_outranks_every_part():
    c = _FakeClient(bikes=[{"sku": "03-4039BR"}], parts=[{"sku": "98-6860"}])
    rows = sku_enrichment.fetch_candidates(c)
    assert [r["sku"] for r in rows] == ["03-4039BR"]
    assert "parts" not in c.calls  # never even asked


def test_the_parts_come_up_once_the_bikes_run_out():
    c = _FakeClient(bikes=[], parts=[{"sku": "98-6860"}, {"sku": "12-8342BL"}])
    rows = sku_enrichment.fetch_candidates(c)
    assert {r["sku"] for r in rows} == {"98-6860", "12-8342BL"}


def test_a_shape_the_as400_cannot_look_up_never_enters():
    # UPCs and serials: 440 of them. The shape is the filter, so no hand-kept
    # list of exceptions can go stale — and it applies to the discovery queue
    # too, where nothing has been vetted by a catalogue row.
    c = _FakeClient(
        bikes=[],
        parts=[{"sku": "792584991050"}, {"sku": "32-0419"}],
        unregistered=[{"sku": "Y22B010415"}],
    )
    rows = sku_enrichment.fetch_candidates(c)
    assert [r["sku"] for r in rows] == ["32-0419"]
