"""
The Maintenance panel: actions run from the UI with a dry-run first. The backfill
must write nothing on Preview, write exactly the blanks on Apply, and the endpoint
must refuse unknown actions and concurrent runs. No DB, no AS400 — mocks only.
"""

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import maintenance  # noqa: E402

HEADER = """                            O R D E R   I N Q U I R Y
 Order Number: 880036                       Account Number: 0010495 00

 Bill TUCKER CYCLES                    Ship TUCKER CYCLES
      3544 ST JOHNS AVE                     3544 ST JOHNS AVE

      JACKSONVILLE    FL  32205             JACKSONVILLE    FL  32205

   Ord   Ship
      1     1  03 4664 BR   F  DAKAR A1 17 2025 BROWN       1234.00    1234.00
                                   END OF ORDER   1234.00
"""

VOID = " Order Number: 880037     Account Number: VOID\n"

LIST_ROW = {
    "id": "list-1",
    "order_number": "880036",
    "customer_id": "cust-1",
    "as400_account_number": None,
    "ship_to_address_id": None,
}
ADDRESS = {
    "id": "addr-1",
    "street": "3544 ST JOHNS AVE",
    "city": "JACKSONVILLE",
    "state": "FL",
    "zip_code": "32205",
    "as400_ship_to": None,
}


def _run(apply, cache=None, customer=None):
    client = MagicMock()
    customer = customer or {"account": None, "varies": False, "addresses": [dict(ADDRESS)]}
    with (
        patch("maintenance.get_client", return_value=client),
        patch(
            "maintenance.scanned_store.load", return_value=cache or {"880036": {"raw_text": HEADER}}
        ),
        patch("maintenance._find_list", return_value=dict(LIST_ROW)),
        patch("maintenance._load_customer", return_value=customer),
        patch("maintenance._slot_taken_elsewhere", return_value=False),
    ):
        return client, maintenance.backfill_account_numbers(apply)


def test_preview_counts_everything_and_writes_nothing():
    client, result = _run(apply=False)
    c = result["counts"]
    assert (c["found"], c["headers"], c["accounts"], c["addresses"], c["links"]) == (1, 1, 1, 1, 1)
    assert result["lines"] == ["880036: header '0010495 00', account 10495, ship-to 00, link"]
    client.table.return_value.update.assert_not_called()


def test_apply_writes_the_account_the_suffix_and_the_link():
    client, result = _run(apply=True)
    assert result["counts"]["errors"] == 0
    tables = [call.args[0] for call in client.table.call_args_list]
    assert "customers" in tables and "customer_addresses" in tables and "picking_lists" in tables
    update_payloads = [c.args[0] for c in client.table.return_value.update.call_args_list]
    assert {"as400_account": "10495"} in update_payloads
    assert {"as400_ship_to": "00"} in update_payloads
    assert {"as400_account_number": "0010495 00", "ship_to_address_id": "addr-1"} in update_payloads


def test_channel_customer_is_never_tagged():
    _, result = _run(
        apply=False, customer={"account": None, "varies": True, "addresses": [dict(ADDRESS)]}
    )
    assert result["counts"]["addresses"] == 0
    assert result["counts"]["links"] == 1  # the order still points at its ship-to


def test_void_and_headerless_orders_are_skipped_not_errors():
    _, result = _run(apply=False, cache={"880037": {"raw_text": VOID}, "880038": {"raw_text": ""}})
    assert result["counts"]["skipped"] == 2
    assert result["counts"]["errors"] == 0
    assert result["lines"] == []


def test_registry_and_runner():
    actions = maintenance.list_actions()
    assert actions and actions[0]["id"] == "backfill_account_numbers"
    assert "Safe to repeat" in actions[0]["what"]
    with pytest.raises(KeyError):
        maintenance.run_action("nope", apply=False)
    with patch.dict(
        maintenance.ACTIONS,
        {
            "noop": {
                "title": "t",
                "what": "w",
                "run": lambda a: {"counts": {}, "lines": [], "truncated": 0},
            }
        },
    ):
        r = maintenance.run_action("noop", apply=True)
        assert r["action"] == "noop" and r["apply"] is True and "seconds" in r
        assert r["labels"] is maintenance.COUNT_LABELS


def test_only_one_action_at_a_time():
    assert maintenance._busy.acquire(blocking=False)
    try:
        with pytest.raises(maintenance.Busy):
            maintenance.run_action("backfill_account_numbers", apply=False)
    finally:
        maintenance._busy.release()


# --- the endpoint --------------------------------------------------------------


@pytest.fixture
def client():
    import app as web

    web.app.config["TESTING"] = True
    with web.app.test_client() as c:
        yield c, web


def test_endpoint_lists_actions(client):
    c, web = client
    r = c.get("/api/maintenance", headers={"Host": f"127.0.0.1:{web.PORT}"})
    assert r.status_code == 200
    assert r.get_json()["actions"][0]["id"] == "backfill_account_numbers"


def test_endpoint_runs_action_and_passes_the_mode(client):
    c, web = client
    fake = {
        "counts": {"cache": 0},
        "lines": [],
        "truncated": 0,
        "action": "backfill_account_numbers",
        "apply": True,
        "seconds": 0.1,
    }
    with patch("maintenance.run_action", return_value=fake) as run:
        r = c.post(
            "/api/maintenance/backfill_account_numbers",
            json={"apply": True},
            headers={"Host": f"127.0.0.1:{web.PORT}"},
        )
    assert r.status_code == 200
    assert r.get_json()["apply"] is True
    run.assert_called_once_with("backfill_account_numbers", True)


def test_endpoint_refuses_unknown_and_busy(client):
    c, web = client
    host = {"Host": f"127.0.0.1:{web.PORT}"}
    assert c.post("/api/maintenance/nope", json={}, headers=host).status_code == 404
    with patch("maintenance.run_action", side_effect=maintenance.Busy("busy")):
        r = c.post("/api/maintenance/backfill_account_numbers", json={"apply": False}, headers=host)
    assert r.status_code == 409
