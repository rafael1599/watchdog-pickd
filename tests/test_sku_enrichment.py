"""
Tests for the AS400 catalogue step (F2 — read-only).

Everything here runs against fake drivers and canned screens, the same way the
capture loop is tested: no macOS, no Mocha, no database. `plan_write` is the one
that matters most — it is the function F3 will point at a service_role
connection with no RLS underneath, so its rules are pinned before it can write.
"""

import pytest

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


# ── §6 / R4: what a write would be ───────────────────────────────────────────


def test_the_weight_gap_is_weight_verified_not_a_null():
    # weight_lbs is NEVER null — the trigger writes 45 into every bike — so the
    # hole to fill is "nobody weighed this", not "the column is empty".
    row = {"sku": "03-3933BK", "model": "CODA S2", "weight_lbs": 45, "weight_verified": False}
    assert plan_write(row, {"weight_lbs": 36.0}) == {"weight_lbs": 36.0}


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


def test_the_weight_is_its_own_phase(monkeypatch):
    # Now that every gap lands on a bike, the name phase and the weight phase
    # would otherwise ship as one. §10 promised .env could separate them.
    row = {"sku": "03-3933BK", "model": "CODA S2", "weight_lbs": 45, "weight_verified": False}
    parsed = {"description": "CODA S2 L16 2026 GLOSS BLACK", "weight_lbs": 36.0}
    assert "weight_lbs" not in plan_write(row, parsed, with_weight=False)
    assert plan_write(row, parsed, with_weight=True)["weight_lbs"] == 36.0


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
    # Rafael, 2026-09-02: 2 → ENTER → the digits WITHOUT the dash → TAB → the
    # colour code → TAB → X.
    driver = FakeDriver([ORDER_SEARCH, MENU, STOCK_DETAIL])
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
        ("key", "enter"),
    ]


def test_a_sku_with_no_colour_suffix_tabs_straight_past_the_field():
    # Rafael, 2026-09-02: "se presiona tab directo sin llenar el color, si el sku
    # no lo tiene". The 126 bikes shaped like this are in the queue, not excluded.
    screen = STOCK_DETAIL.replace("Stock Number: 03 3933 BK", "Stock Number: 01 0169   ")
    driver = FakeDriver([ORDER_SEARCH, MENU, screen])
    capture_stock_inquiry("01-0169", driver, page_wait=0, step_wait=0)
    typed = [a for a in driver.actions if a[0] == "text"]
    assert typed == [("text", "2"), ("text", "010169"), ("text", "X")]
    assert driver.actions.count(("key", "tab")) == 2  # both tabs pressed, one field left blank


def test_the_notes_screen_is_refused_instead_of_read():
    # R10. Same title, no fields: reading here would report the weight as ABSENT
    # rather than as "I am not where I think I am".
    driver = FakeDriver([ORDER_SEARCH, MENU, STOCK_NOTES])
    with pytest.raises(StockScreenMismatch):
        capture_stock_inquiry("03-3933BK", driver, page_wait=0, step_wait=0)


def test_a_lookup_that_lands_nowhere_is_an_unknown_sku():
    driver = FakeDriver([ORDER_SEARCH, MENU, "Invalid stock number"])
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


def _gap_harness(monkeypatch, *, idle=1e9, results=None):
    """Wire _run_sku_gap up to fakes and return the list of SKUs it looked up.

    `system_idle_seconds` is always faked: the real one shells out to ioreg and
    would make these tests depend on whether somebody is touching this Mac.
    """
    import auto_scanner
    import sku_enrichment

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
    assert "weight_lbs" not in written[0][1]  # F4 is off

    monkeypatch.setenv("SKU_ENRICH_WEIGHT", "1")
    run_sku_step(
        object(), dict(row), capture_fn=lambda s, d: STOCK_DETAIL, return_fn=lambda d: None
    )
    assert written[1][1]["weight_lbs"] == 36.0
    assert "weight_verified" not in written[1][1]


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
