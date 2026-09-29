"""The dealer's phone and e-mail from CUSTOMER DISPLAY, and the expedition that
maps where the CONTACT lives (customer_enrichment.py, Rafael 29 sep 2026)."""

import pytest

import customer_enrichment as ce
from as400_capture import (
    CustomerScreenMismatch,
    capture_customer_display,
    customer_account_fields,
    enter_menu_option,
)
from parser import format_phone, parse_customer_display
from tests.test_as400_capture import (
    CUSTOMER_DISPLAY_SCREEN,
    MENU_SCREEN,
    ORDER_SEARCH_SCREEN,
    FakeDriver,
)

CUSTOMER_ENTRY_SCREEN = """                    C U S T O M E R   I N Q U I R Y
  Account Number: _______  __"""


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("CUSTOMER_SEEN_PATH", str(tmp_path / "seen.json"))
    monkeypatch.setenv("AS400_PAGE_WAIT", "0")
    for name in (
        "CUSTOMER_ENRICH",
        "CUSTOMER_ENRICH_WRITE",
        "CUSTOMER_EXPLORE",
        "CUSTOMER_ACCOUNT_PAD",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(ce.time, "sleep", lambda s: None)
    import as400_capture

    monkeypatch.setattr(as400_capture.time, "sleep", lambda s: None)
    ce._flag_cache.update(at=0.0, row=None)


# ── the screen ───────────────────────────────────────────────────────────────


def test_parses_the_real_customer_display():
    p = parse_customer_display(CUSTOMER_DISPLAY_SCREEN)
    assert p["account"] == "9981"
    assert p["suffix"] == "00"
    assert p["name"] == "SHREWSBURY BICYCLES INC."
    assert p["phone"] == "(732) 741-2799"
    assert p["phone_raw"] == "732 7412799"
    assert p["email"] == "info@shrewsburybicycles.com"
    assert p["fax"] is None  # the line holds only «Salesman ID», not a fax number
    assert p["salesman"] == "179 LAMBERT/PARSONS"
    assert p["bike_buyer"] == "ACT# 2385  ROUT# 0353"
    assert p["parts_buyer"] is None and p["other_buyer"] is None


@pytest.mark.parametrize(
    "raw, out",
    [
        ("732 7412799", "(732) 741-2799"),
        ("(201) 891-5500", "(201) 891-5500"),
        ("1-201-891-5500", "(201) 891-5500"),
        ("891-5500", "891-5500"),  # not ten digits: kept as it came
        ("", None),
        (None, None),
    ],
)
def test_phone_is_spelled_like_the_pack_slip(raw, out):
    assert format_phone(raw) == out


def test_account_fields_type_what_the_record_says(monkeypatch):
    assert customer_account_fields("6034") == ("6034", "00")
    assert customer_account_fields("0006034", "0") == ("6034", "00")
    assert customer_account_fields("EBAY") is None
    assert customer_account_fields("12345678") is None
    monkeypatch.setenv("CUSTOMER_ACCOUNT_PAD", "1")
    assert customer_account_fields("6034") == ("0006034", "00")


# ── the route ────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("option", ["7", "9", "09", "24"])
def test_never_opens_an_option_that_writes(option):
    with pytest.raises(ValueError):
        enter_menu_option(FakeDriver([MENU_SCREEN]), option, page_wait=0)


def test_capture_walks_menu_1_account_tab_suffix_enter():
    # precheck (order search) → return_to_menu reads the search, F6·F6·F7, reads
    # the menu → option 1 → entry form → account → the display.
    driver = FakeDriver(
        [
            ORDER_SEARCH_SCREEN,
            ORDER_SEARCH_SCREEN,
            MENU_SCREEN,
            CUSTOMER_ENTRY_SCREEN,
            CUSTOMER_DISPLAY_SCREEN,
        ]
    )
    entry, screen = capture_customer_display("9981", "00", driver, page_wait=0, step_wait=0)
    assert entry == CUSTOMER_ENTRY_SCREEN and screen == CUSTOMER_DISPLAY_SCREEN
    assert driver.actions[-6:] == [
        ("text", "1"),
        ("key", "enter"),
        ("text", "9981"),
        ("key", "tab"),
        ("text", "00"),
        ("key", "enter"),
    ]


def test_capture_stops_when_option_1_did_not_open_customer_inquiry():
    driver = FakeDriver([MENU_SCREEN, MENU_SCREEN, "SOMETHING ELSE ENTIRELY"])
    with pytest.raises(CustomerScreenMismatch):
        capture_customer_display("9981", "00", driver, page_wait=0, step_wait=0)
    assert ("text", "9981") not in driver.actions  # nothing typed into a stranger


def test_capture_stops_when_the_account_does_not_open_the_display():
    driver = FakeDriver([MENU_SCREEN, MENU_SCREEN, CUSTOMER_ENTRY_SCREEN, CUSTOMER_ENTRY_SCREEN])
    with pytest.raises(CustomerScreenMismatch):
        capture_customer_display("9981", "00", driver, page_wait=0, step_wait=0)


# ── what gets written ────────────────────────────────────────────────────────


def test_plan_fills_only_empty_columns_of_the_right_account():
    parsed = parse_customer_display(CUSTOMER_DISPLAY_SCREEN)
    row = {"id": "c1", "as400_account": "9981", "phone": None, "email": None}
    assert ce.plan_write(row, parsed) == {
        "phone": "(732) 741-2799",
        "email": "info@shrewsburybicycles.com",
    }
    typed = {**row, "phone": "(732) 555-0000"}
    assert ce.plan_write(typed, parsed) == {"email": "info@shrewsburybicycles.com"}
    other = {**row, "as400_account": "6034"}
    assert ce.plan_write(other, parsed) == {}


def test_queue_puts_todays_customers_first_then_the_busiest():
    customers = [
        {"id": "a", "name": "A", "as400_account": "1"},
        {"id": "b", "name": "B", "as400_account": "2"},
        {"id": "c", "name": "C", "as400_account": "3"},
        {"id": "d", "name": "D", "as400_account": "4", "ship_to_varies": True},
        {"id": "e", "name": "E", "as400_account": None},
        {"id": "f", "name": "F", "as400_account": "6", "phone": "x", "email": "y"},
        {"id": "g", "name": "G", "as400_account": "7"},
    ]
    orders = [
        {"customer_id": "b", "created_at": "2026-09-01T10:00:00Z"},
        {"customer_id": "b", "created_at": "2026-09-02T10:00:00Z"},
        {"customer_id": "c", "created_at": "2026-09-29T10:00:00Z"},
    ]
    ranked = ce.rank_customers(customers, orders, seen={"7-00": {}}, today="2026-09-29")
    assert [c["id"] for c in ranked] == ["c", "b", "a"]


class _FakeTable:
    def __init__(self, store, name):
        self.store, self.name, self.op = store, name, None

    def insert(self, row):
        self.store.setdefault(self.name, []).append(row)
        return self

    def update(self, row):
        self.op = row
        return self

    def select(self, *_):
        return self

    def eq(self, *_):
        return self

    def is_(self, *_):
        return self

    def limit(self, *_):
        return self

    def execute(self):
        if self.op is not None:
            self.store.setdefault("updates", []).append(self.op)
            return type("R", (), {"data": [self.op]})()
        rows = self.store.get(self.name, [])
        return type("R", (), {"data": rows})()


class _FakeClient:
    def __init__(self, flags=None):
        self.store = {"app_flags": flags or []}

    def table(self, name):
        return _FakeTable(self.store, name)


def test_read_only_step_keeps_the_screen_and_writes_nothing():
    client = _FakeClient([{"enabled": True, "config": {"write": False}}])
    row = {"id": "c1", "as400_account": "9981", "phone": None, "email": None}
    res = ce.run_customer_step(
        None,
        row,
        capture_fn=lambda a, s, d: (CUSTOMER_ENTRY_SCREEN, CUSTOMER_DISPLAY_SCREEN),
        client=client,
    )
    assert res["action"] == "read"
    assert res["plan"]["phone"] == "(732) 741-2799"
    assert "updates" not in client.store
    kept = [r["classified"] for r in client.store["as400_screens"]]
    assert kept == ["customer_entry", "customer_display"]
    assert "9981-00" in ce.load_seen()


def test_write_phase_fills_the_columns():
    client = _FakeClient([{"enabled": True, "config": {"write": True}}])
    row = {"id": "c1", "as400_account": "9981", "phone": None, "email": None}
    res = ce.run_customer_step(
        None,
        row,
        capture_fn=lambda a, s, d: (CUSTOMER_ENTRY_SCREEN, CUSTOMER_DISPLAY_SCREEN),
        client=client,
    )
    assert res["action"] == "written"
    assert {"phone": "(732) 741-2799"} in client.store["updates"]


def test_somebody_elses_record_is_a_mismatch():
    client = _FakeClient([{"enabled": True, "config": {"write": True}}])
    row = {"id": "c1", "as400_account": "6034", "phone": None, "email": None}
    res = ce.run_customer_step(
        None,
        row,
        capture_fn=lambda a, s, d: (CUSTOMER_ENTRY_SCREEN, CUSTOMER_DISPLAY_SCREEN),
        client=client,
    )
    assert res["action"] == "mismatch"
    assert "updates" not in client.store


# ── the switch ───────────────────────────────────────────────────────────────


def test_off_without_a_flag_and_the_env_wins(monkeypatch):
    assert ce.enabled(_FakeClient([])) is False
    ce._flag_cache.update(at=0.0, row=None)
    assert ce.enabled(_FakeClient([{"enabled": True, "config": {}}])) is True
    monkeypatch.setenv("CUSTOMER_ENRICH", "0")
    assert ce.enabled(_FakeClient([{"enabled": True, "config": {}}])) is False


# ── the expedition ───────────────────────────────────────────────────────────


def test_expedition_one_key_one_read_then_home():
    kept, homes = [], []
    driver = FakeDriver(["after f4", "after f5"])

    ce.run_expedition(
        driver,
        account="6034",
        keys=("f4", "f5"),
        menu_options=(),
        capture_fn=lambda a, s, d: ("entry", "display"),
        home_fn=lambda d: homes.append(1),
        page_wait=0,
        keep=lambda label, classified, after, raw: kept.append((classified, raw)) or True,
    )
    assert kept == [
        ("explore:customer_display", "display"),
        ("explore:customer:f4", "after f4"),
        ("explore:customer:f5", "after f5"),
    ]
    assert driver.keys == ["f4", "f5"]
    assert len(homes) == 2  # home after every key


def test_expedition_aborts_when_it_cannot_get_home():
    driver = FakeDriver(["after f4", "after f5"])

    def no_home(_):
        raise RuntimeError("lost")

    ce.run_expedition(
        driver,
        account="6034",
        keys=("f4", "f5"),
        menu_options=("6",),
        capture_fn=lambda a, s, d: ("entry", "display"),
        home_fn=no_home,
        page_wait=0,
        keep=lambda *a: True,
    )
    assert driver.keys == ["f4"]  # stopped after the first key


def test_commit_is_not_pressed_by_default():
    assert "f11" not in ce.EXPLORE_KEYS_DEFAULT
    assert "9" not in ce.EXPLORE_MENU_DEFAULT and "7" not in ce.EXPLORE_MENU_DEFAULT


# ── the wiring into the scanner ──────────────────────────────────────────────


def _customer_gap(monkeypatch, *, idle=1e9, queue=None):
    import as400_capture
    import auto_scanner

    monkeypatch.setattr(auto_scanner, "_last_operator_input", None)
    monkeypatch.setattr(as400_capture, "_last_self_input", None)
    monkeypatch.setenv("SCAN_IGNORE_OPERATOR_UNTIL", "2000-01-01T00:00:00Z")
    monkeypatch.setattr(auto_scanner, "_driver_for_sku_step", lambda: object())
    monkeypatch.setattr(auto_scanner, "system_idle_seconds", lambda: idle)
    monkeypatch.setattr(auto_scanner, "hold_awake", lambda: None)
    monkeypatch.setattr(auto_scanner, "let_sleep", lambda: None)
    monkeypatch.setattr(ce, "explore_if_due", lambda d, client=None: 0)
    monkeypatch.setattr(ce, "explore_due", lambda client=None: None)
    monkeypatch.setattr(ce, "fetch_queue", lambda client=None: list(queue or []))
    homes, seen = [], []
    monkeypatch.setattr(as400_capture, "return_to_order_search", lambda d: homes.append(1))
    monkeypatch.setattr(
        ce,
        "run_customer_step",
        lambda d, row: (
            seen.append(row["as400_account"]) or {"action": "read", "account": row["as400_account"]}
        ),
    )
    return auto_scanner, seen, homes


def test_the_customer_gap_does_nothing_while_the_switch_is_off(monkeypatch):
    auto_scanner, seen, homes = _customer_gap(monkeypatch, queue=[{"as400_account": "1"}])
    monkeypatch.setattr(ce, "enabled", lambda client=None: False)
    auto_scanner._run_customer_gap()
    assert seen == [] and homes == []  # the terminal is not touched at all


def test_the_customer_gap_reads_and_walks_home_once(monkeypatch):
    auto_scanner, seen, homes = _customer_gap(
        monkeypatch, queue=[{"as400_account": "1"}, {"as400_account": "2"}]
    )
    monkeypatch.setattr(ce, "enabled", lambda client=None: True)
    monkeypatch.setattr(ce, "max_per_gap", lambda client=None: 10)
    monkeypatch.setattr(ce, "gap_budget_sec", lambda client=None: 300)
    auto_scanner._run_customer_gap()
    assert seen == ["1", "2"]
    assert homes == [1]


def test_the_customer_gap_yields_to_the_operator(monkeypatch):
    auto_scanner, seen, homes = _customer_gap(monkeypatch, idle=0, queue=[{"as400_account": "1"}])
    monkeypatch.setattr(ce, "enabled", lambda client=None: True)
    monkeypatch.setattr(ce, "max_per_gap", lambda client=None: 10)
    monkeypatch.setattr(ce, "gap_budget_sec", lambda client=None: 300)
    auto_scanner._run_customer_gap()
    assert seen == []


def test_the_expedition_also_yields_to_the_operator(monkeypatch):
    auto_scanner, seen, homes = _customer_gap(monkeypatch, idle=0)
    monkeypatch.setattr(ce, "enabled", lambda client=None: True)
    monkeypatch.setattr(ce, "explore_due", lambda client=None: "1")
    ran = []
    monkeypatch.setattr(ce, "explore_if_due", lambda d, client=None: ran.append(1))
    auto_scanner._run_customer_gap()
    assert ran == [] and homes == []
