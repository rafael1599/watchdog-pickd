"""
El cliente de una orden es el Ship-to (Rafael, 29 oct 2026).

La cuenta del header es del Bill-to y sólo identifica la ficha cuando Bill y
Ship son la misma tienda. Si no coinciden —una cuenta de rep mandando una
muestra, eBay a una persona, Consumer All Access a un dealer— el Ship-to es el
cliente, la cuenta no se usa ni se sella, y el Bill-to pasa a contacto de la
orden. La 882087 (cuenta 0550369 = JOHN EPPLEY, Ship SIC TRANSIT CYCLES) nació a
nombre del rep por buscar la ficha por cuenta antes que por nombre.

Cubre: parse_parties, create_order (resolve sin cuenta + contacto), el guard de
direcciones, la puerta (no_customer mira al Ship-to, junk al Bill-to), el filtro
del Bike Buyer y la cola del enriquecimiento.
"""

import os
import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import customer_enrichment as ce  # noqa: E402
import door  # noqa: E402
from parser import buyer_as_contact, parse_order, parse_parties  # noqa: E402
from supabase_client import _save_shipping_address, looks_like_address  # noqa: E402

# Captura real (as400_captures.raw_text de la 881863): cabecera + página de detalle.
EPPLEY = """                            O R D E R   I N Q U I R Y
 Order Number: 881863                       Account Number: 0550369 00

 Bill JOHN EPPLEY                      Ship CYCLICAL NATURE BICYCLES
      23646 WILSON AVE                      4045 MAIN ST
                                            SUITE 200
      DEARBORN        MI  48128             HILLIARD        OH  43026

 Terms: 46        Cr Lim:        .00   Invoice Comments
 Sales ID: 125    Order Taken By: MAX
 Order Date: 100926 P/O: SAMPLE RENS1
          Sub-Total         2592.95
 Order Comments: SAMPLE BIKE NET 180 25% DISCOUNT
                            O R D E R   I N Q U I R Y
 Order Number: 881863                       Account Number: 0550369 00

 Bill JOHN EPPLEY

 Quant  Quant  Stock #   W/H   Description                       Unit    Extend
   Ord   Ship                                                   Price
     1      1  03 4693 GY  N   RENEGADE S1 56 2026 ADOBE CLAY 2592.95   2592.95
                                END OF ORDER                            2592.95
"""

DEALER = """ Order Number: 880036                       Account Number: 0010495 00

 Bill TUCKER CYCLES                    Ship TUCKER CYCLES
      3544 ST JOHNS AVE                     3544 ST JOHNS AVE
      JACKSONVILLE    FL  32205             JACKSONVILLE    FL  32205

 Terms: 46
 Quant  Quant  Stock #   W/H   Description                       Unit    Extend
   Ord   Ship                                                   Price
     1      1  03 4693 GY  N   RENEGADE S1 56 2026 ADOBE CLAY 2592.95   2592.95
                                END OF ORDER                            2592.95
"""

WARRANTY = """ Order Number: 881111                       Account Number: 0000000 00

 Bill DEALER WARRANTY 2009             Ship CHICAGO LAND BICYCLES
      1 JAMIS WAY                           123 MAIN ST
      NORTHVALE       NJ  07647             CHICAGO         IL  60601

 Terms: 46
 Quant  Quant  Stock #   W/H   Description                       Unit    Extend
   Ord   Ship                                                   Price
     1      1  03 4693 GY  N   RENEGADE S1 56 2026 ADOBE CLAY 2592.95   2592.95
                                END OF ORDER                            2592.95
"""

PICKUP = """ Order Number: 881222                       Account Number: 0001346 00

 Bill THE BICYCLE RACK                 Ship PICK UP
      10 BIKE ST                            HAMISH TO PICK UP 9/16/26
      TRENTON         NJ  08601

 Terms: 46
 Quant  Quant  Stock #   W/H   Description                       Unit    Extend
   Ord   Ship                                                   Price
     1      1  03 4693 GY  N   RENEGADE S1 56 2026 ADOBE CLAY 2592.95   2592.95
                                END OF ORDER                            2592.95
"""

# Captura que perdió la cabecera: lo que sigue a «Ship» es la columna de la tabla.
HEADLESS = """ Order Number: 881333                       Account Number: 0002982 00

 Bill BEETON'S CYCLERY

 Quant  Quant  Stock #   W/H   Description                       Unit    Extend
   Ord   Ship                                                   Price
     1      1  03 3985 GY  N   CITIZEN 2 23 2025 STORM GREY     428.95   428.95
                                END OF ORDER                             428.95
"""


# ── parser ────────────────────────────────────────────────────────────────────


def test_a_rep_sample_goes_to_the_store_with_the_rep_as_contact():
    p = parse_parties(EPPLEY)
    assert p["same_party"] is False
    assert p["customer_name"] == "CYCLICAL NATURE BICYCLES"
    assert p["contact_name"] == "JOHN EPPLEY"
    o = parse_order(EPPLEY)
    assert o["seal_account"] is False
    assert o["as400_account"] == "550369"  # sigue ahí, para as400_account_number crudo
    assert o["shipping"]["street"] == "4045 MAIN ST SUITE 200"


def test_a_dealer_is_the_same_party_and_keeps_the_account():
    p = parse_parties(DEALER)
    assert p["same_party"] is True
    assert p["customer_name"] == "TUCKER CYCLES"
    assert p["contact_name"] is None
    assert parse_order(DEALER)["seal_account"] is True


def test_warranty_ships_to_the_store_and_a_channel_is_no_contact():
    p = parse_parties(WARRANTY)
    assert p["same_party"] is False
    assert p["customer_name"] == "CHICAGO LAND BICYCLES"
    assert p["contact_name"] is None  # «DEALER WARRANTY 2009» no es nadie a quien llamar


def test_a_sales_channel_is_no_contact_either():
    text = DEALER.replace("Bill TUCKER CYCLES", "Bill JAMIS CONSUMER ALL ACCESS").replace(
        "Ship TUCKER CYCLES", "Ship LUCKY BIKES"
    )
    p = parse_parties(text)
    assert p["same_party"] is False
    assert p["customer_name"] == "LUCKY BIKES"
    assert p["contact_name"] is None
    for bill in ("EBAY PARTS SALES", "FACEBOOK SALES", "CARINE JOANNOU PERSONAL", "DONATIONS 2026"):
        assert (
            parse_parties(text.replace("JAMIS CONSUMER ALL ACCESS", bill))["contact_name"] is None
        )


def test_pick_up_is_not_another_store():
    p = parse_parties(PICKUP)
    assert p["same_party"] is True
    assert p["customer_name"] == "THE BICYCLE RACK"
    assert parse_order(PICKUP)["seal_account"] is True


def test_the_items_column_header_is_not_a_ship_to():
    o = parse_order(HEADLESS)
    assert o["shipping"] is None
    assert o["ship_to_name"] is None
    assert o["customer_name"] == "BEETON'S CYCLERY"
    assert o["same_party"] is True


@pytest.mark.parametrize(
    "bill, ship",
    [("Archer's Bikes", "ARCHERS BIKES"), ("KOZY'S CYCLERY", "KOZY'S CYCLERY")],
)
def test_names_compare_on_letters_and_digits_only(bill, ship):
    text = DEALER.replace("Bill TUCKER CYCLES ", f"Bill {bill:<19}").replace(
        "Ship TUCKER CYCLES", f"Ship {ship}"
    )
    assert parse_parties(text)["same_party"] is True


# ── create_order ──────────────────────────────────────────────────────────────


def _run_create(order_data, phone_rows=()):
    from supabase_client import create_order

    lists = MagicMock()
    lists.insert.return_value.execute.return_value.data = [{"id": "uuid-1"}]
    customers = MagicMock()
    customers.select.return_value.eq.return_value.limit.return_value.execute.return_value.data = (
        list(phone_rows)
    )
    tables = {"picking_lists": lists, "customers": customers}

    with (
        patch("supabase_client.get_client") as mock_client,
        patch("supabase_client._to_cart_items", return_value=[{"sku": "X", "pickingQty": 1}]),
        patch("supabase_client._resolve_customer", return_value="cust-uuid") as resolve,
        patch("supabase_client._save_shipping_address", return_value="addr-1"),
        patch("supabase_client._log_import"),
    ):
        mock_client.return_value.table.side_effect = lambda name: tables[name]
        create_order(order_data, "hash", "as400_app")
    return lists.insert.call_args[0][0], resolve


def test_the_order_of_a_rep_is_resolved_without_the_account_and_carries_the_contact():
    data = parse_order(EPPLEY)
    data["items"] = [{"sku": "03-4693GY", "qty": 1}]
    payload, resolve = _run_create(data, phone_rows=[{"phone": "(734) 555-0100"}])
    assert resolve.call_args.args[1] == "CYCLICAL NATURE BICYCLES"
    assert resolve.call_args.kwargs["account"] is None
    assert resolve.call_args.kwargs["street"] == "4045 MAIN ST SUITE 200"
    assert payload["contact_name"] == "JOHN EPPLEY"
    assert payload["contact_phone"] == "(734) 555-0100"
    assert payload["as400_account_number"] == "0550369 00"


def test_a_dealer_order_still_resolves_by_account_and_has_no_contact():
    data = parse_order(DEALER)
    data["items"] = [{"sku": "03-4693GY", "qty": 1}]
    payload, resolve = _run_create(data)
    assert resolve.call_args.kwargs["account"] == "10495"
    assert "contact_name" not in payload
    assert "contact_phone" not in payload


def test_no_phone_on_file_means_no_contact_phone_column():
    data = parse_order(EPPLEY)
    data["items"] = [{"sku": "03-4693GY", "qty": 1}]
    payload, _ = _run_create(data, phone_rows=[])
    assert payload["contact_name"] == "JOHN EPPLEY"
    assert "contact_phone" not in payload


# ── the address guard ─────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "ship, ok",
    [
        ({"street": "4045 MAIN ST SUITE 200", "city": "HILLIARD"}, True),
        ({"street": "03 4868 BL N HUDSON E1 S/O 18 2026 MIDNIGHT 1368.95", "city": None}, False),
        ({"street": "03 4868 BL N HUDSON E1 S/O 18 2026 MIDNIGHT 1368.95", "city": "X"}, False),
        ({"street": "1245 DERR VALLEY DR PARK CITY UT", "city": None}, False),
        ({"street": "", "city": "HILLIARD"}, False),
    ],
)
def test_only_an_address_is_an_address(ship, ok):
    assert looks_like_address(ship) is ok


def test_a_line_of_items_never_becomes_the_customers_address():
    client = MagicMock()
    rid = _save_shipping_address(
        client, "c1", {"name": "Price", "street": "03 3985 GY N CITIZEN 2 23 2025 428.95"}, "00"
    )
    assert rid is None
    client.table.assert_not_called()


# ── the door ──────────────────────────────────────────────────────────────────

NOW = datetime(2026, 10, 29, 17, 0, tzinfo=timezone.utc)


def _entry(**over):
    base = {
        "order_number": "882087",
        "scanned_at": NOW.isoformat(),
        "customer": "JOHN EPPLEY",
        "ship_to": "SIC TRANSIT CYCLES",
        "item_count": 1,
        "total_mismatch": False,
    }
    base.update(over)
    return base


def test_no_customer_is_about_who_receives(monkeypatch):
    monkeypatch.setattr(door, "_published", {})
    assert door.classify(_entry(), NOW) == ("pending", None)
    assert door.classify(_entry(customer="", ship_to="SIC TRANSIT CYCLES"), NOW) == (
        "pending",
        None,
    )
    assert door.classify(_entry(customer="JOHN EPPLEY", ship_to=None), NOW) == ("pending", None)
    assert door.classify(_entry(customer="", ship_to=None), NOW) == ("held", "no_customer")
    assert door.classify(_entry(customer="Unknown", ship_to=""), NOW) == ("held", "no_customer")


def test_junk_is_still_who_pays():
    assert door.classify(_entry(customer="EBAY PART SALES", ship_to="MARK SMITH"), NOW) == (
        "junk",
        "ebay",
    )


# ── the Bike Buyer filter and the enrichment queue ────────────────────────────


@pytest.mark.parametrize(
    "value",
    [
        "SAMPLE ACCOUNT",
        "ALEX OR DANNY ARE SONS CAN ORDER",
        "BRIAN OFF FRIDAYS",
        "AMY OR JAMES",
        "JOHN OR BARBARA",
        "MISH & JODY",
        "AUDREY -BARRY",
        "AMANDA PASHEA OR RUSS",
    ],
)
def test_a_note_in_bike_buyer_is_not_a_contact(value):
    assert buyer_as_contact(value) is None


@pytest.mark.parametrize(
    "value",
    ["MICHAEL PORRARO-OWNER", "ROBERT O'NEILL (OWNER)", "EDWARD B CARSKADON III", "HOLT TUCKER"],
)
def test_a_person_still_is(value):
    assert buyer_as_contact(value) == value


def test_rep_accounts_are_never_queued():
    customers = [
        {"id": "rep", "as400_account": "550369", "ship_to_varies": False},
        {"id": "dealer", "as400_account": "8307", "ship_to_varies": False},
    ]
    orders = [{"customer_id": "rep"}, {"customer_id": "dealer"}]
    assert [c["id"] for c in ce.rank_customers(customers, orders)] == ["dealer"]
    assert ce.is_rep_account("0550369") is True
    assert ce.is_rep_account("550369") is True
    assert ce.is_rep_account("8307") is False
    assert ce.is_rep_account("9995525") is False


def test_the_contact_lands_only_on_the_default_address():
    client = MagicMock()
    chain = client.table.return_value.update.return_value
    chain.eq.return_value.eq.return_value.is_.return_value.execute.return_value.data = [{}]
    ce.apply_write("c1", {"_addresses": {"contact_name": "HOLT TUCKER"}}, client)
    client.table.assert_called_with("customer_addresses")
    client.table.return_value.update.assert_called_once_with({"contact_name": "HOLT TUCKER"})
    chain.eq.assert_called_once_with("customer_id", "c1")
    chain.eq.return_value.eq.assert_called_once_with("is_default", True)
    chain.eq.return_value.eq.return_value.is_.assert_called_once_with("contact_name", "null")
