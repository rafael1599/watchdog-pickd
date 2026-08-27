"""
Tests for the AS400 account on the customer side, with a MagicMock client:

  _resolve_customer
    (a) a customer sealed with the account is returned at once, without the
        name/street scan;
    (b) no account match → name path, then the account is SEALED onto the matched
        row with a fill-if-NULL update (never an overwrite);
    (c) a customer created on the name path is inserted WITH the account.

  _save_shipping_address
    (d) the slot moved: a row already carries this Recipient ID with another
        address → that row is updated in place and its id returned (no 2nd row);
    (e) same slot, same address → nothing written, id returned;
    (f) no slot → the upsert carries as400_ship_to and NOT fedex_recipient_id
        (the DB trigger derives it);
    (g) a ship_to_varies customer never gets as400_ship_to and is never looked
        up by Recipient ID.
"""

import os
import sys
from unittest.mock import MagicMock, call

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from supabase_client import _resolve_customer, _save_shipping_address  # noqa: E402

SHIP = {
    "name": "TUCKER CYCLES",
    "street": "3544 ST JOHNS AVE",
    "city": "JACKSONVILLE",
    "state": "FL",
    "zip_code": "32205",
}
ROW = {"id": "c-name", "name": "TUCKER CYCLES", **{k: v for k, v in SHIP.items() if k != "name"}}


def _client(tables: dict) -> MagicMock:
    """A client whose .table(name) hands back the given mock for that table."""
    client = MagicMock()
    client.table.side_effect = lambda name: tables[name]
    return client


def _customers(by_account: list, by_name: list = ()) -> MagicMock:
    t = MagicMock()
    # select(...).eq(...).limit(...).execute()  → the lookup by as400_account
    t.select.return_value.eq.return_value.limit.return_value.execute.return_value.data = by_account
    # select(...).execute()                     → the full-table name/street scan
    t.select.return_value.execute.return_value.data = list(by_name)
    t.insert.return_value.execute.return_value.data = [{"id": "new-id"}]
    return t


# ── _resolve_customer ────────────────────────────────────────────────────────────


def test_account_match_short_circuits_without_name_logic():
    customers = _customers(by_account=[{"id": "c-acct", "as400_account": "10495"}])
    client = _client({"customers": customers})

    assert _resolve_customer(client, "TUCKER CYCLES", street="1 OTHER RD", account="10495") == (
        "c-acct"
    )
    customers.select.assert_called_once_with("id, as400_account")
    customers.select.return_value.eq.assert_called_once_with("as400_account", "10495")
    customers.insert.assert_not_called()
    customers.update.assert_not_called()


def test_name_match_seals_account_only_when_null():
    customers = _customers(by_account=[], by_name=[ROW])
    client = _client({"customers": customers})

    assert _resolve_customer(
        client, "Tucker Cycles", street="3544 St Johns Ave", account="10495"
    ) == ("c-name")
    customers.update.assert_called_once_with({"as400_account": "10495"})
    chain = customers.update.return_value
    chain.eq.assert_called_once_with("id", "c-name")
    chain.eq.return_value.is_.assert_called_once_with("as400_account", "null")
    chain.eq.return_value.is_.return_value.execute.assert_called_once()
    customers.insert.assert_not_called()


def test_name_only_match_seals_account_too():
    generic = {"id": "c-generic", "name": "TUCKER CYCLES", "street": None}
    customers = _customers(by_account=[], by_name=[generic])
    client = _client({"customers": customers})

    assert _resolve_customer(client, "TUCKER CYCLES", account="10495") == "c-generic"
    customers.update.assert_called_once_with({"as400_account": "10495"})


def test_new_customer_is_inserted_with_account():
    customers = _customers(by_account=[], by_name=[])
    client = _client({"customers": customers})

    cid = _resolve_customer(
        client,
        "New Dealer",
        street="1 Main St",
        city="Reno",
        state="NV",
        zip_code="89501",
        account="7099",
    )
    assert cid == "new-id"
    payload = customers.insert.call_args[0][0]
    assert payload["as400_account"] == "7099"
    assert payload["name"] == "NEW DEALER"
    customers.update.assert_not_called()  # born with it: nothing to seal


def test_without_account_nothing_is_looked_up_or_sealed():
    customers = _customers(by_account=[], by_name=[ROW])
    client = _client({"customers": customers})

    assert _resolve_customer(client, "TUCKER CYCLES", street="3544 ST JOHNS AVE") == "c-name"
    customers.select.assert_called_once_with("id, name, street, city, state, zip_code")
    customers.update.assert_not_called()
    assert "as400_account" not in str(customers.insert.call_args)


def test_account_lookup_failure_falls_back_to_name_path():
    customers = _customers(by_account=[], by_name=[ROW])
    customers.select.return_value.eq.return_value.limit.return_value.execute.side_effect = (
        Exception("column does not exist")
    )
    client = _client({"customers": customers})

    assert _resolve_customer(
        client, "TUCKER CYCLES", street="3544 ST JOHNS AVE", account="10495"
    ) == ("c-name")


# ── _save_shipping_address ───────────────────────────────────────────────────────


def _address_tables(account="10495", varies=False, slot=None, upsert_id="a-new"):
    customers = MagicMock()
    customers.select.return_value.eq.return_value.limit.return_value.execute.return_value.data = [
        {"as400_account": account, "ship_to_varies": varies}
    ]
    addresses = MagicMock()
    sel = addresses.select.return_value
    # select(...).eq("fedex_recipient_id", rid).limit(1).execute()   → the slot lookup
    sel.eq.return_value.limit.return_value.execute.return_value.data = [slot] if slot else []
    # select("id").eq(customer_id).eq(is_default).limit(1).execute() → existing default
    sel.eq.return_value.eq.return_value.limit.return_value.execute.return_value.data = []
    addresses.upsert.return_value.execute.return_value.data = [{"id": upsert_id}]
    return customers, addresses


def test_slot_moved_updates_that_row_and_returns_its_id():
    old = {"id": "a-slot", "street": "1 OLD RD", "city": "RENO", "state": "NV", "zip_code": "89501"}
    customers, addresses = _address_tables(slot=old)
    client = _client({"customers": customers, "customer_addresses": addresses})

    assert _save_shipping_address(client, "cust-1", SHIP, ship_to="00") == "a-slot"
    addresses.select.return_value.eq.assert_any_call("fedex_recipient_id", "1049500")
    addresses.update.assert_called_once_with(
        {
            "street": "3544 ST JOHNS AVE",
            "city": "JACKSONVILLE",
            "state": "FL",
            "zip_code": "32205",
            "label": "TUCKER CYCLES",
        }
    )
    addresses.update.return_value.eq.assert_called_once_with("id", "a-slot")
    addresses.upsert.assert_not_called()  # the slot moved; no second row


def test_same_slot_same_address_writes_nothing():
    same = {"id": "a-slot", **{k: v for k, v in SHIP.items() if k != "name"}}
    same["city"] = " jacksonville "  # what the DB's normalized_address ignores
    customers, addresses = _address_tables(slot=same)
    client = _client({"customers": customers, "customer_addresses": addresses})

    assert _save_shipping_address(client, "cust-1", SHIP, ship_to="00") == "a-slot"
    addresses.update.assert_not_called()
    addresses.upsert.assert_not_called()


def test_no_slot_upserts_with_ship_to_and_lets_the_trigger_derive_the_id():
    customers, addresses = _address_tables(slot=None, upsert_id="a-new")
    client = _client({"customers": customers, "customer_addresses": addresses})

    assert _save_shipping_address(client, "cust-1", SHIP, ship_to="01") == "a-new"
    addresses.select.return_value.eq.assert_any_call("fedex_recipient_id", "1049501")
    entry = addresses.upsert.call_args[0][0]
    assert entry["as400_ship_to"] == "01"
    assert "fedex_recipient_id" not in entry
    assert entry["label"] == "TUCKER CYCLES"
    assert entry["is_default"] is True  # the customer had none yet
    assert addresses.upsert.call_args.kwargs["on_conflict"] == "customer_id,normalized_address"
    addresses.update.assert_not_called()


def test_ship_to_varies_customer_never_sends_ship_to():
    customers, addresses = _address_tables(varies=True)
    client = _client({"customers": customers, "customer_addresses": addresses})

    assert _save_shipping_address(client, "cust-1", SHIP, ship_to="00") == "a-new"
    assert "as400_ship_to" not in addresses.upsert.call_args[0][0]
    # And never looked up by Recipient ID: a channel has no slot to move.
    assert call("fedex_recipient_id", "1049500") not in (
        addresses.select.return_value.eq.call_args_list
    )


def test_customer_without_account_keeps_the_plain_upsert():
    customers, addresses = _address_tables(account=None)
    client = _client({"customers": customers, "customer_addresses": addresses})

    assert _save_shipping_address(client, "cust-1", SHIP, ship_to="00") == "a-new"
    assert "as400_ship_to" not in addresses.upsert.call_args[0][0]


def test_without_suffix_keeps_the_plain_upsert():
    customers, addresses = _address_tables()
    client = _client({"customers": customers, "customer_addresses": addresses})

    assert _save_shipping_address(client, "cust-1", SHIP) == "a-new"
    assert "as400_ship_to" not in addresses.upsert.call_args[0][0]


def test_upsert_without_data_falls_back_to_lookup_by_fields():
    customers, addresses = _address_tables(slot=None)
    addresses.upsert.return_value.execute.return_value.data = []
    # select("id").eq(customer_id).eq(street).eq(city).eq(state).eq(zip).limit(1)
    eq5 = addresses.select.return_value
    for _ in range(5):
        eq5 = eq5.eq.return_value
    eq5.limit.return_value.execute.return_value.data = [{"id": "a-found"}]
    client = _client({"customers": customers, "customer_addresses": addresses})

    assert _save_shipping_address(client, "cust-1", SHIP, ship_to="00") == "a-found"


def test_customer_read_failure_is_non_blocking():
    customers, addresses = _address_tables()
    customers.select.return_value.eq.return_value.limit.return_value.execute.side_effect = (
        Exception("boom")
    )
    client = _client({"customers": customers, "customer_addresses": addresses})

    assert _save_shipping_address(client, "cust-1", SHIP, ship_to="00") == "a-new"
    assert "as400_ship_to" not in addresses.upsert.call_args[0][0]


def test_no_street_returns_none_without_touching_the_db():
    client = MagicMock()
    assert _save_shipping_address(client, "cust-1", {"name": "X", "street": ""}, "00") is None
    client.table.assert_not_called()
