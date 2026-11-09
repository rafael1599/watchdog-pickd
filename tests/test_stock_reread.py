"""stock_reread: PickD hands over a list, the watcher reads the AS400 stock again."""

import pytest

import stock_reread as sr
from as400_capture import StockSkuNotFound

DETAIL = """                            S T O C K   I N Q U I R Y

  Stock Number: 03 4035 BL      B-Bike/P-Part: B    Model Year: 2026
  Description:  HUDSON 17 2026 MIDNIGHT BLUE

     Inventory  NJ       FL       CA                 Price  Quantity
  On Hand       41        0        0       Each    380.95         49
  Unit Meas:   EA
  Duty %                          Weight:    36
                     Cmd10                                          Cmd7
                      NOTES                                          EXIT"""

CFG = {
    "rev": "r1",
    "skus": ["03-4035BL", "03-4035BLT", "03-3779RD"],
    "home": {"03-4035BLT": "03-4035BL"},
}


@pytest.fixture(autouse=True)
def _fresh(monkeypatch, tmp_path):
    monkeypatch.setenv("STOCK_REREAD_STATE_PATH", str(tmp_path / "state.json"))
    monkeypatch.setenv("SKU_ENRICH_WRITE", "1")
    sr._reset_for_tests()
    monkeypatch.setattr(sr, "_flag", lambda client=None: {"enabled": True, "config": CFG})
    monkeypatch.setattr(sr, "return_to_search", lambda d: True)
    yield


def capture_returns(monkeypatch, screen_or_exc):
    asked = []

    def cap(code, driver, **k):
        asked.append(code)
        if isinstance(screen_or_exc, Exception):
            raise screen_or_exc
        return screen_or_exc

    monkeypatch.setattr(sr, "capture_stock_inquiry", cap)
    return asked


def recorder():
    writes = []
    return writes, lambda code, home, parsed, rev: writes.append((code, home, parsed, rev)) or "w"


def test_off_without_the_flag(monkeypatch):
    monkeypatch.setattr(sr, "_flag", lambda client=None: {"enabled": False, "config": CFG})
    assert not sr.active() and sr.next_code() is None
    monkeypatch.setattr(sr, "_flag", lambda client=None: {"enabled": True, "config": {}})
    assert not sr.active()


def test_reads_in_pickds_order_and_never_twice(monkeypatch):
    asked = capture_returns(monkeypatch, DETAIL)
    writes, fn = recorder()
    assert sr.next_code() == "03-4035BL"
    res = sr.run_step(object(), "03-4035BL", write_fn=fn)
    assert res["action"] == "read" and res["on_search"] is True and res["returned"] is True
    assert asked == ["03-4035BL"]
    assert writes[0][0] == "03-4035BL" and writes[0][1] is None
    assert writes[0][2]["on_hand"]["NJ"] == 41
    assert sr.next_code() == "03-4035BLT"
    assert sr.progress() == (1, 3)


def test_a_code_with_no_row_is_kept_at_its_home(monkeypatch):
    capture_returns(monkeypatch, StockSkuNotFound("blank"))
    writes, fn = recorder()
    res = sr.run_step(object(), "03-4035BLT", write_fn=fn)
    assert res["action"] == "unknown" and res["returned"] is None and res["on_search"] is False
    assert writes == [("03-4035BLT", "03-4035BL", None, "r1")]
    assert sr.load_state("r1")["done"]["03-4035BLT"] == "not in AS400"


def test_the_screen_showing_another_code_writes_nothing_and_gives_up_after_two(monkeypatch):
    # asked BLT, the colour field ate the T: the screen is 03-4035BL's record
    capture_returns(monkeypatch, DETAIL)
    writes, fn = recorder()
    res = sr.run_step(object(), "03-4035BLT", write_fn=fn)
    assert res["action"] == "mismatch" and writes == []
    assert sr.next_code() == "03-4035BL"  # BL still first; BLT not done yet
    sr._finish("r1", "03-4035BL", "read")
    assert sr.next_code() == "03-4035BLT"
    sr.run_step(object(), "03-4035BLT", write_fn=fn)
    assert sr.load_state("r1")["done"]["03-4035BLT"].startswith("gave up")
    assert sr.next_code() == "03-3779RD"


def test_a_new_rev_starts_over(monkeypatch):
    sr._finish("r1", "03-4035BL", "read")
    assert sr.next_code() == "03-4035BLT"
    monkeypatch.setattr(
        sr, "_flag", lambda client=None: {"enabled": True, "config": {**CFG, "rev": "r2"}}
    )
    assert sr.next_code() == "03-4035BL"


def test_plan_own_keeps_the_variants_and_marks_the_rev():
    old = {"description": "HUDSON 17", "on_hand": {"NJ": 40}, "variants": {"X": {"found": False}}}
    parsed = {"description": "HUDSON 17 2026 MIDNIGHT BLUE", "on_hand": {"NJ": 41}}
    new = sr.plan_own(old, parsed, "r1", "T")
    assert new["on_hand"] == {"NJ": 41} and new["variants"] == {"X": {"found": False}}
    assert new["reread_rev"] == "r1" and new["reread_at"] == "T"


def test_plan_variant_found_and_not_found():
    new = sr.plan_variant({"on_hand": {"NJ": 40}}, "03-4035BLT", {"on_hand": {"NJ": 7}}, "r1", "T")
    assert new["on_hand"] == {"NJ": 40}
    assert new["variants"]["03-4035BLT"]["on_hand"] == {"NJ": 7}
    gone = sr.plan_variant(new, "03-4035BKT", None, "r1", "T")
    assert gone["variants"]["03-4035BKT"]["found"] is False
    assert gone["variants"]["03-4035BLT"]["found"] is True


class FakeTable:
    def __init__(self, db, calls):
        self.db, self.calls, self.f = db, calls, {}

    def select(self, *a):
        return self

    def update(self, upd):
        self.upd = upd
        return self

    def eq(self, k, v):
        self.f[k] = v
        return self

    def limit(self, n):
        return self

    def execute(self):
        class R:
            pass

        r = R()
        sku = self.f.get("sku")
        if hasattr(self, "upd"):
            self.calls.append((sku, self.upd))
            r.data = [{}]
        else:
            r.data = [{"sku": sku, "as400_snapshot": self.db[sku]}] if sku in self.db else []
        return r


class FakeClient:
    def __init__(self, db):
        self.db, self.calls = db, []

    def table(self, name):
        assert name == "sku_metadata"
        return FakeTable(self.db, self.calls)


def test_write_touches_only_the_snapshot_and_read_at():
    c = FakeClient({"03-4035BL": {"on_hand": {"NJ": 40}}})
    assert sr.write("03-4035BL", None, {"on_hand": {"NJ": 41}}, "r1", client=c) == "written"
    sku, upd = c.calls[0]
    assert sku == "03-4035BL" and set(upd) == {"as400_snapshot", "as400_read_at"}

    assert sr.write("03-4035BLT", "03-4035BL", {"on_hand": {"NJ": 7}}, "r1", client=c) == "written"
    sku, upd = c.calls[1]
    assert sku == "03-4035BL" and set(upd) == {"as400_snapshot"}


def test_write_is_dry_with_writes_off(monkeypatch):
    monkeypatch.setenv("SKU_ENRICH_WRITE", "0")
    c = FakeClient({"03-4035BL": {}})
    assert sr.write("03-4035BL", None, {"on_hand": {"NJ": 41}}, "r1", client=c) == "dry"
    assert c.calls == []


def test_a_missing_row_writes_nothing():
    c = FakeClient({})
    assert sr.write("03-9999BL", None, {"on_hand": {}}, "r1", client=c) == "no_row"
    assert c.calls == []


# ── in the gap loop: ahead of the catalogue ──────────────────────────────────


def test_the_gap_reads_pickds_list_first_then_the_catalogue(monkeypatch):
    import sku_enrichment
    from tests.test_sku_enrichment import _gap_harness

    monkeypatch.setenv("SKU_ENRICH_MAX_PER_GAP", "40")
    seen = _gap_harness(monkeypatch)
    monkeypatch.setattr(
        sr,
        "_flag",
        lambda client=None: {"enabled": True, "config": {**CFG, "max_per_gap": 5}},
    )
    reread = []

    def step(driver, code, **kw):
        reread.append((code, kw.get("on_search_screen")))
        sr._finish("r1", code, "read")
        return {"action": "read", "sku": code, "returned": True, "on_search": True}

    monkeypatch.setattr(sr, "run_step", step)
    import auto_scanner

    auto_scanner._run_sku_gap()
    assert [c for c, _ in reread] == CFG["skus"]
    assert [h for _, h in reread] == [False, True, True]
    # 5 per gap while rereading: 3 from the list, then 2 from the catalogue
    assert seen == ["03-3492BL", "03-3492BL"]
    assert sku_enrichment is not None


def test_a_miss_walks_to_the_menu_before_the_next_code(monkeypatch):
    import as400_capture
    from tests.test_sku_enrichment import _gap_harness

    _gap_harness(monkeypatch)
    monkeypatch.setattr(
        sr,
        "_flag",
        lambda client=None: {"enabled": True, "config": {**CFG, "max_per_gap": 2}},
    )
    walks = []
    monkeypatch.setattr(as400_capture, "return_to_menu", lambda d: walks.append(1))
    hops = []

    def step(driver, code, **kw):
        hops.append(kw.get("on_search_screen"))
        sr._finish("r1", code, "not in AS400")
        return {"action": "unknown", "sku": code, "returned": None, "on_search": False}

    monkeypatch.setattr(sr, "run_step", step)
    import auto_scanner

    auto_scanner._run_sku_gap()
    assert hops == [False, False] and len(walks) == 2
