"""
Tests for the door: the scan cache published to Pickd, Pickd's requests
executed here.

All against fakes. The parts that matter are the pure decisions — what state a
capture belongs in, what a pipeline answer means, what the board's classifier
will read — and the refusals: never overwrite a request in flight, never send
the same order twice, never let a bad capture stop the rest.
"""

from datetime import datetime, timedelta, timezone

import pytest

import door
from door import build_payload, classify, outcome_of, reconcile, send_one

NOW = datetime(2026, 9, 8, 17, 0, tzinfo=timezone.utc)


def _entry(**over):
    base = {
        "order_number": "881390",
        "raw_text": "ORDER INQUIRY ...",
        "scanned_at": NOW.isoformat(),
        "source": "auto_scan",
        "customer": "SHREWSBURY BICYCLES INC.",
        "item_count": 2,
        "total_units": 6,
        "total_mismatch": False,
    }
    base.update(over)
    return base


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    monkeypatch.setattr(door, "_published", {})
    door.sending.clear()
    door.busy.clear()
    monkeypatch.delenv("AS400_HOLD_STALE_DAYS", raising=False)
    monkeypatch.delenv("AS400_ARCHIVE_DAYS", raising=False)


# ── the junk rules ───────────────────────────────────────────────────────────


def test_a_fresh_clean_capture_is_pending():
    assert classify(_entry(), NOW) == ("pending", None)


def test_a_capture_with_no_number_or_no_items_is_never_published():
    # The 'Invalid Order Number, REENTER' screen an old scanner cached.
    assert classify(_entry(order_number=None), NOW) is None
    assert classify(_entry(item_count=0), NOW) is None


def test_ebay_is_junk_not_held():
    # Never picked here. Hidden beats held, but the row keeps the ledger whole.
    assert classify(_entry(customer="EBAY PART SALES"), NOW) == ("junk", "ebay")
    assert classify(_entry(customer="  ebay   part sales "), NOW) == ("junk", "ebay")


def test_a_lost_page_is_held_and_says_so():
    # Parsed lines don't sum to the header Sub-Total. Sending it would create
    # a wrong picking list; it needs a re-capture, not a tap.
    assert classify(_entry(total_mismatch=True), NOW) == ("held", "total_mismatch")


def test_no_customer_is_held():
    # The 12 cancelled orders with no customer were TEST / 123 noise.
    assert classify(_entry(customer=""), NOW) == ("held", "no_customer")
    assert classify(_entry(customer="Unknown"), NOW) == ("held", "no_customer")


def test_three_days_untouched_is_stale_and_eight_is_archived():
    # Real orders complete in a median of 1.1 h; three days survives a weekend.
    four_days = (NOW - timedelta(days=4)).isoformat()
    nine_days = (NOW - timedelta(days=9)).isoformat()
    assert classify(_entry(scanned_at=four_days), NOW) == ("held", "stale")
    assert classify(_entry(scanned_at=nine_days), NOW) == ("archived", "stale")


def test_the_more_specific_reason_beats_stale():
    old = (NOW - timedelta(days=5)).isoformat()
    assert classify(_entry(scanned_at=old, total_mismatch=True), NOW) == ("held", "total_mismatch")
    assert classify(_entry(scanned_at=old, customer="EBAY PART SALES"), NOW) == ("junk", "ebay")


def test_thresholds_are_retunable_from_env(monkeypatch):
    monkeypatch.setenv("AS400_HOLD_STALE_DAYS", "1")
    two_days = (NOW - timedelta(days=2)).isoformat()
    assert classify(_entry(scanned_at=two_days), NOW) == ("held", "stale")


# ── what the board will read ─────────────────────────────────────────────────


def test_items_are_published_the_way_the_classifier_reads_them():
    # Canonical SKU (the board's lookup is an exact-string match), `pickingQty`
    # not `qty`, and is_bike embedded — autoClassifyShippingType prefers the
    # embedded flag, so the modal needs no lookup and cannot read a bike as a
    # part because of a spelling.
    preview = {
        "customer": "SHREWSBURY BICYCLES INC.",
        "ship_to": "SHREWSBURY BICYCLES",
        "item_count": 2,
        "total_units": 6,
        "subtotal": 100.0,
        "total_mismatch": False,
        "items": [
            {
                "sku": "033684BR",
                "raw_sku": "03 3684 BR",
                "qty": 5,
                "description": "FAULTLINE A1 17",
            },
            {"sku": "993604", "raw_sku": "99 3604", "qty": 1, "description": "TOOL"},
        ],
    }
    payload = build_payload(_entry(), preview, bike_skus={"033684BR"})
    bike, part = payload["items"]
    assert bike == {
        "sku": "03-3684BR",
        "pickingQty": 5,
        "description": "FAULTLINE A1 17",
        "unit_price": None,
        "sku_metadata": {"is_bike": True},
    }
    assert part["sku"] == "99-3604" and part["sku_metadata"] == {"is_bike": False}
    assert payload["raw_text"] == "ORDER INQUIRY ..."
    assert payload["captured_at"] == NOW.isoformat()


def test_a_zero_quantity_line_is_not_an_item():
    preview = {"items": [{"sku": "033684BR", "qty": 0}]}
    assert build_payload(_entry(), preview, set())["items"] == []


# ── the reconciler ───────────────────────────────────────────────────────────


class FakeRpcClient:
    def __init__(self, fail_times=0):
        self.calls = []
        self.fail_times = fail_times

    def rpc(self, name, params):
        self.calls.append((name, params))
        client = self

        class R:
            def execute(self_inner):
                if client.fail_times > 0:
                    client.fail_times -= 1
                    raise RuntimeError("relation does not exist")
                return type("X", (), {"data": params["p_status"]})()

        return R()


def test_reconcile_publishes_each_cached_capture_once(monkeypatch):
    monkeypatch.setattr(door.scanned_store, "load", lambda: {"881390": _entry()})
    client = FakeRpcClient()
    preview = lambda text: {"items": [{"sku": "033684BR", "qty": 5}], "customer": "X"}  # noqa: E731

    first = reconcile(client, now=NOW, preview_fn=preview, bike_skus=set())
    second = reconcile(client, now=NOW, preview_fn=preview, bike_skus=set())

    assert first["published"] == 1 and second["published"] == 0 and second["unchanged"] == 1
    name, params = client.calls[0]
    assert name == "publish_as400_capture"
    assert params["p_order_number"] == "881390" and params["p_status"] == "pending"


def test_reconcile_republishes_when_the_state_changes(monkeypatch):
    monkeypatch.setattr(door.scanned_store, "load", lambda: {"881390": _entry()})
    client = FakeRpcClient()
    preview = lambda text: {"items": [{"sku": "033684BR", "qty": 5}], "customer": "X"}  # noqa: E731

    reconcile(client, now=NOW, preview_fn=preview, bike_skus=set())
    later = NOW + timedelta(days=4)  # it went stale in the meantime
    reconcile(client, now=later, preview_fn=preview, bike_skus=set())

    assert [c[1]["p_status"] for c in client.calls] == ["pending", "held"]
    assert client.calls[1][1]["p_hold_reason"] == "stale"


def test_one_unparseable_capture_does_not_stop_the_rest(monkeypatch):
    monkeypatch.setattr(
        door.scanned_store,
        "load",
        lambda: {"1": _entry(order_number="1"), "2": _entry(order_number="2")},
    )
    client = FakeRpcClient()

    def preview(text):
        if not hasattr(preview, "n"):
            preview.n = 0
        preview.n += 1
        if preview.n == 1:
            raise ValueError("garbage screen")
        return {"items": [{"sku": "033684BR", "qty": 1}], "customer": "X"}

    counts = reconcile(client, now=NOW, preview_fn=preview, bike_skus=set())
    assert counts["failed"] == 1 and counts["published"] == 1


def test_a_missing_table_is_said_once_not_walked_across_the_whole_cache(monkeypatch):
    entries = {str(n): _entry(order_number=str(n)) for n in range(10)}
    monkeypatch.setattr(door.scanned_store, "load", lambda: entries)
    client = FakeRpcClient(fail_times=99)
    preview = lambda text: {"items": [{"sku": "033684BR", "qty": 1}], "customer": "X"}  # noqa: E731

    counts = reconcile(client, now=NOW, preview_fn=preview, bike_skus=set())
    assert counts["failed"] == 3 and len(client.calls) == 3  # stopped, did not try all ten


# ── what a pipeline answer means ─────────────────────────────────────────────


@pytest.mark.parametrize("status", ["created", "appended", "reopened", "combined", "duplicate"])
def test_every_way_of_being_on_the_board_is_sent(status):
    # `duplicate` included: the order is already there, which is the point.
    assert outcome_of({"status": status}) == ("sent", None)


def test_waiting_locked_is_held_with_that_reason():
    # A person unmarks waiting in Pickd and taps again.
    assert outcome_of({"status": "waiting_locked"}) == ("held", "waiting_locked")


def test_anything_else_is_held_with_the_pipeline_s_own_word():
    assert outcome_of({"status": "no_items"}) == ("held", "no_items")
    assert outcome_of({}) == ("held", "error")


# ── executing a request ──────────────────────────────────────────────────────


class FakeTable:
    """Records updates and lets a test decide whether the requested→sending
    claim succeeds (somebody may have cancelled in between)."""

    def __init__(self, claim_ok=True):
        self.claim_ok = claim_ok
        self.updates = []
        self._filters = {}
        self._values = None

    def table(self, name):
        return self

    def update(self, values):
        self._values = values
        self._filters = {}
        return self

    def eq(self, col, val):
        self._filters[col] = val
        return self

    def execute(self):
        self.updates.append((dict(self._values), dict(self._filters)))
        # The claim is the update that filters on status='requested'.
        if self._filters.get("status") == "requested":
            return type("R", (), {"data": [{"ok": 1}] if self.claim_ok else []})()
        return type("R", (), {"data": [{"ok": 1}]})()


def test_a_request_is_sent_and_the_cache_forgets_it(monkeypatch):
    deleted = []
    monkeypatch.setattr(door.scanned_store, "delete", lambda n: deleted.append(n))
    client = FakeTable()
    sent_with = []

    def fake_send(text, source_name):
        sent_with.append(source_name)
        return {"status": "created", "picking_list": {"id": "pl-1"}, "message": "ok"}

    out = send_one(client, {"order_number": "881390", "raw_text": "X"}, send_fn=fake_send, now=NOW)

    assert out["action"] == "sent"
    assert sent_with == ["pickd_door:881390"]  # so picking_lists.source becomes 'as400'
    final = client.updates[-1][0]
    assert final["status"] == "sent" and final["picking_list_id"] == "pl-1"
    assert "picking_list" not in final["result"]  # the row, not a copy of the row
    assert deleted == ["881390"]
    assert not door.busy.is_set() and "881390" not in door.sending


def test_a_duplicate_looks_up_the_existing_list_id(monkeypatch):
    monkeypatch.setattr(door.scanned_store, "delete", lambda n: None)
    client = FakeTable()
    out = send_one(
        client,
        {"order_number": "881390", "raw_text": "X"},
        send_fn=lambda t, source_name: {"status": "duplicate", "message": "already"},
        find_fn=lambda n: {"id": "pl-existing"},
        now=NOW,
    )
    assert out["action"] == "sent"
    assert client.updates[-1][0]["picking_list_id"] == "pl-existing"


def test_a_cancel_between_the_poll_and_the_send_wins(monkeypatch):
    # The claim (requested → sending) found nothing to claim.
    sent = []
    client = FakeTable(claim_ok=False)
    out = send_one(
        client,
        {"order_number": "881390", "raw_text": "X"},
        send_fn=lambda *a, **k: sent.append(1),
        now=NOW,
    )
    assert out["action"] == "no longer requested" and sent == []


def test_waiting_locked_goes_back_as_held_with_the_message():
    client = FakeTable()
    out = send_one(
        client,
        {"order_number": "881390", "raw_text": "X"},
        send_fn=lambda t, source_name: {
            "status": "waiting_locked",
            "message": "WAITING FOR INVENTORY",
        },
        now=NOW,
    )
    assert out == {"order_number": "881390", "action": "held", "reason": "waiting_locked"}
    final = client.updates[-1][0]
    assert final["hold_reason"] == "waiting_locked" and "WAITING" in final["last_error"]


def test_a_crash_inside_the_pipeline_is_held_not_lost():
    client = FakeTable()

    def boom(text, source_name):
        raise RuntimeError("the database is on fire")

    out = send_one(client, {"order_number": "881390", "raw_text": "X"}, send_fn=boom, now=NOW)
    assert out["action"] == "held"
    final = client.updates[-1][0]
    assert final["hold_reason"] == "error" and "on fire" in final["last_error"]
    assert not door.busy.is_set()


def test_the_same_order_is_never_sent_twice_at_once():
    # The Bay 2 button and "Traer" share this. Whoever is second gets nothing.
    assert door.claim("881390") is True
    client = FakeTable()
    out = send_one(
        client, {"order_number": "881390", "raw_text": "X"}, send_fn=lambda *a, **k: 1 / 0, now=NOW
    )
    assert out["action"] == "already sending" and client.updates == []
    door.release("881390")


def test_busy_is_raised_only_while_a_send_is_in_flight():
    # auto_update refuses to restart while this is set.
    seen = []
    client = FakeTable()
    send_one(
        client,
        {"order_number": "881390", "raw_text": "X"},
        send_fn=lambda t, source_name: (
            seen.append(door.busy.is_set()) or {"status": "created", "picking_list": {"id": "x"}}
        ),
        now=NOW,
    )
    assert seen == [True] and not door.busy.is_set()


# ── the switches ─────────────────────────────────────────────────────────────


def test_the_door_is_on_by_default_and_off_is_one_line(monkeypatch):
    monkeypatch.delenv("AS400_DOOR", raising=False)
    assert door.enabled() is True
    monkeypatch.setenv("AS400_DOOR", "0")
    assert door.enabled() is False


def test_auto_send_is_off_until_the_numbers_say_otherwise(monkeypatch):
    monkeypatch.delenv("AS400_AUTO_SEND", raising=False)
    assert door.auto_send() is False
