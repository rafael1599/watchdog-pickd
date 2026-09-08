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
    rows = [
        {"sku": "03-4159BL", "model": "KROMO", "qty": 33, "weight_verified": False},
        {"sku": "03-4894BL", "model": None, "qty": 0},
        {"sku": "03-3492BL", "model": None, "qty": 1},  # ROW 32, a picker can hold it
        {"sku": "03-9999BK", "model": "X", "qty": 5, "weight_verified": True},
    ]
    assert [r["sku"] for r in select_sku_queue(rows)] == [
        "03-3492BL",  # no model, in stock
        "03-4894BL",  # no model, no stock
        "03-4159BL",  # has a model, weight never on a scale
    ]  # the verified weight isn't in the queue at all


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


def test_the_name_is_only_planned_when_the_model_is_empty():
    parsed = {"description": "CODA S2 L16 2026 GLOSS BLACK", "weight_lbs": None}
    gap = {"sku": "03-3933BK", "model": None, "weight_verified": True}
    taken = {"sku": "03-3933BK", "model": "CODA S2", "weight_verified": True}
    assert plan_write(gap, parsed)["_description"] == "CODA S2 L16 2026 GLOSS BLACK"
    assert plan_write(taken, parsed) == {}


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
    assert res["plan"]["weight_lbs"] == 36.0  # a PLAN — F2 logs it, nothing applies it
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


def test_the_gap_spends_exactly_one_sku(monkeypatch):
    # ONE per gap, never a burst — the decision of 2026-06-10, unchanged.
    import auto_scanner
    import sku_enrichment

    monkeypatch.setenv("SKU_ENRICH", "1")
    monkeypatch.setattr(auto_scanner, "_driver_for_sku_step", lambda: object())
    monkeypatch.setattr(sku_enrichment, "next_sku", lambda *a, **k: {"sku": "03-3492BL"})
    steps = []

    def one_step(driver, row, **kw):
        steps.append(row["sku"])
        return {"action": "read", "sku": row["sku"], "returned": True}

    monkeypatch.setattr(sku_enrichment, "run_sku_step", one_step)
    auto_scanner._run_sku_gap()
    assert steps == ["03-3492BL"]


def test_the_queue_pauses_when_the_terminal_did_not_come_home(monkeypatch):
    # A step that left the terminal on a stock screen must not be followed by
    # another one: the next cycle's bootstrap is what recovers it.
    import auto_scanner
    import sku_enrichment

    monkeypatch.setenv("SKU_ENRICH", "1")
    monkeypatch.setenv("SKU_ENRICH_MAX_PER_GAP", "5")
    monkeypatch.setattr(auto_scanner, "_driver_for_sku_step", lambda: object())
    monkeypatch.setattr(sku_enrichment, "next_sku", lambda *a, **k: {"sku": "03-3492BL"})
    steps = []

    def lost(driver, row, **kw):
        steps.append(row["sku"])
        return {"action": "read", "sku": row["sku"], "returned": False}

    monkeypatch.setattr(sku_enrichment, "run_sku_step", lost)
    auto_scanner._run_sku_gap()
    assert len(steps) == 1


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
