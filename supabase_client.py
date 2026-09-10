"""
supabase_client.py — Direct Supabase operations for the PDF watcher.

Uses the SERVICE_ROLE_KEY to bypass RLS (runs locally only).
Inserts orders directly into picking_lists so the web app picks them up via Realtime.
"""

import logging
import os
import re
import threading
from datetime import datetime, timedelta, timezone
from typing import Optional

from dotenv import load_dotenv
from supabase import Client, create_client

# ClientOptions has moved between supabase-py versions — import it defensively so a
# dependency bump never breaks startup. None → fall back to a plain client.
try:  # recent supabase-py re-exports it at the top level
    from supabase import ClientOptions  # type: ignore
except Exception:  # pragma: no cover - import-path shim
    try:
        from supabase.client import ClientOptions  # type: ignore
    except Exception:
        try:
            from supabase.lib.client_options import ClientOptions  # type: ignore
        except Exception:
            ClientOptions = None  # type: ignore

from parser import canonical_sku, normalize_sku

load_dotenv()

log = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL", "http://localhost:54321")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
PDF_IMPORT_USER_ID = os.getenv("PDF_IMPORT_USER_ID", "")

# Negative order number counter file
COUNTER_FILE = os.path.join(os.path.dirname(__file__), ".negative_counter")


_client: Optional[Client] = None
_client_lock = threading.Lock()


def _make_client() -> Client:
    """Build the Supabase client.

    The service-role key never expires, so auto-refresh and session persistence are
    turned off — gotrue starts a background auto-refresh worker per client when they
    are on, and spinning up several clients in a row (as one send does) deadlocked on
    macOS: '[Errno 11] Resource deadlock avoided'."""
    if ClientOptions is not None:
        try:
            return create_client(
                SUPABASE_URL,
                SUPABASE_KEY,
                options=ClientOptions(auto_refresh_token=False, persist_session=False),
            )
        except TypeError:
            # Option/signature mismatch across versions — fall back to a plain client.
            pass
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def get_client() -> Client:
    """Return the shared Supabase client (service role key), creating it once.

    Memoized for the whole process. A fresh client per call leaked a gotrue
    auto-refresh worker every time and, after a dependency bump, deadlocked on macOS
    when a send created several clients in a row. One reused client is also the
    supabase-py–recommended pattern and is safe to share across threads."""
    global _client
    if not SUPABASE_KEY:
        raise ValueError("SUPABASE_SERVICE_ROLE_KEY not set in .env")
    if _client is None:
        with _client_lock:
            if _client is None:
                _client = _make_client()
    return _client


def _next_negative_order_number() -> str:
    """Generate next negative order number: -000001, -000002, etc."""
    counter = 1
    if os.path.exists(COUNTER_FILE):
        with open(COUNTER_FILE, "r") as f:
            try:
                counter = int(f.read().strip()) + 1
            except ValueError:
                counter = 1

    with open(COUNTER_FILE, "w") as f:
        f.write(str(counter))

    return f"-{counter:06d}"


def check_duplicate(pdf_hash: str) -> Optional[dict]:
    """
    Check if a PDF with this hash has already been processed.
    Returns the existing log entry if found, None otherwise.
    """
    client = get_client()
    result = client.table("pdf_import_log").select("*").eq("pdf_hash", pdf_hash).execute()
    if result.data and len(result.data) > 0:
        return result.data[0]
    return None


def split_order_numbers(db_order_number: Optional[str]) -> list:
    """Split a picking_lists.order_number into its member numbers.

    Combined orders in PickD store their numbers joined with ' / '
    (e.g. "880106 / 880107"). A single order yields a one-element list.
    """
    return [s.strip() for s in (db_order_number or "").split(" / ") if s.strip()]


def find_existing_order(order_number: str) -> Optional[dict]:
    """
    Find an existing picking list by order number.
    Returns the most recent one (could be active or completed).

    Matches the number EXACTLY or as a member of a combined order
    ("880106 / 880107") — an eq-only lookup misses combined membership, which
    let a re-send of 880107 slip past the existing-order path. The LIKE narrows
    server-side; membership is verified client-side via split_order_numbers so a
    substring like '1880107' can't false-positive.
    """
    client = get_client()
    result = (
        client.table("picking_lists")
        .select("*")
        .eq("order_number", order_number)
        .order("updated_at", desc=True)
        .limit(1)
        .execute()
    )
    if result.data and len(result.data) > 0:
        return result.data[0]

    result = (
        client.table("picking_lists")
        .select("*")
        .like("order_number", f"%{order_number}%")
        .order("updated_at", desc=True)
        .limit(10)
        .execute()
    )
    for row in result.data or []:
        if str(order_number) in split_order_numbers(row.get("order_number")):
            return row
    return None


# How far back the batched existence check looks. The scanner only walks recent
# numbers, so a short window keeps the query tiny (one column, ~100-200 rows).
PICKD_RECENT_DAYS = int(os.getenv("PICKD_RECENT_DAYS", "14"))


# Bike catalog cache: the set of normalized bike SKUs changes rarely (new models),
# so one query per TTL is plenty. Used to estimate pallet counts like PickD does.
BIKE_SKUS_TTL_SEC = int(os.getenv("BIKE_SKUS_TTL_SEC", "3600"))
_bike_skus_cache: dict = {"at": 0.0, "skus": None}


def get_bike_skus() -> set:
    """Normalized SKUs of all bikes in sku_metadata (cached, one query per TTL).

    Normalized (via parser.normalize_sku) because the watcher's parsed SKUs are
    normalized ('033684BR') while the catalog stores canonical ('03-3684BR').
    """
    import time

    now = time.monotonic()
    if _bike_skus_cache["skus"] is not None and (now - _bike_skus_cache["at"]) < BIKE_SKUS_TTL_SEC:
        return _bike_skus_cache["skus"]
    client = get_client()
    result = client.table("sku_metadata").select("sku").eq("is_bike", True).execute()
    skus = {normalize_sku(row["sku"]) for row in result.data or [] if row.get("sku")}
    _bike_skus_cache["at"] = now
    _bike_skus_cache["skus"] = skus
    return skus


def find_orders_in_pickd(numbers: list) -> set:
    """Return the subset of `numbers` that already exist in PickD (one query).

    Pulls only order_number+status for recent picking_lists and matches each
    candidate exactly OR as a member of a combined order — the same membership
    rule as find_existing_order, so the send pipeline and the watcher UI agree
    on what "already in PickD" means. Cancelled orders don't count (a cancelled
    order is re-orderable, so its number stays a valid candidate).
    """
    wanted = {str(n) for n in numbers if n}
    if not wanted:
        return set()
    client = get_client()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=PICKD_RECENT_DAYS)).isoformat()
    result = (
        client.table("picking_lists")
        .select("order_number, status")
        .gte("created_at", cutoff)
        .execute()
    )
    present = set()
    for row in result.data or []:
        if row.get("status") == "cancelled":
            continue
        present.update(split_order_numbers(row.get("order_number")))
    return wanted & present


def source_for(file_name: str) -> str:
    """`picking_lists.source` from where the text came.

    Until 2026-09-08 every order the watcher created said `pdf_import`, so Pickd
    could not tell an AS400 capture from a dropped PDF (only 60 of 179 carried
    an AS400 account to hint at it). The create path is shared by both, so the
    label is derived from the `file_name` each caller already passes: the Bay 2
    UI sends `as400_app`, the folder watcher sends `scanned:<n>` when it reuses
    a cached capture, the door sends `pickd_door:<n>`. A real PDF keeps its
    file name and its old label. Nothing in Pickd filters on this column.
    """
    name = str(file_name or "")
    if name.startswith(("as400_app", "scanned:", "pickd_door:")):
        return "as400"
    return "pdf_import"


def create_order(order_data: dict, pdf_hash: str, file_name: str, group_id: str = None) -> dict:
    """
    Create a new picking list from parsed PDF data.
    Inserts with status='ready_to_double_check' and source='pdf_import'.

    order_data format:
    {
        'order_number': str | None,
        'account_number': str | None,   # raw AS400 header value ('0010495 00')
        'as400_account': str | None,    # '10495' — seals customers.as400_account
        'as400_ship_to': str | None,    # '00'    — tags the ship-to address
        'customer_name': str | None,
        'shipping': dict | None,
        'items': [ { sku, qty, ... } ]
    }

    group_id, when given, is set in this same INSERT statement so it links
    the row to an order_groups cluster from the moment it's created — required
    for auto_group_fedex_orders to see NEW.group_id IS NOT NULL and skip it.
    """
    client = get_client()

    order_number = order_data.get("order_number")
    if not order_number:
        order_number = _next_negative_order_number()

    # Convert items to CartItem-compatible format for the web app
    cart_items = _to_cart_items(client, order_data["items"])

    # Look up or create customer. The AS400 account from the header is the
    # identity that survives a rename — see _resolve_customer.
    customer_id = None
    customer_name = order_data.get("customer_name")
    if customer_name:
        addr = order_data.get("shipping") or {}
        customer_id = _resolve_customer(
            client,
            customer_name,
            street=addr.get("street"),
            city=addr.get("city"),
            state=addr.get("state"),
            zip_code=addr.get("zip_code"),
            account=order_data.get("as400_account"),
        )

    # Mirror the web Orders view: persist the Ship-to address on the customer
    # (main address) and in customer_addresses (history). Non-blocking. The row
    # it returns is the ship-to THIS order goes to — a dealer with two stores has
    # two rows, and "the customer's default" is the wrong one half the time.
    ship_to_address_id = None
    if customer_id and order_data.get("shipping"):
        ship_to_address_id = _save_shipping_address(
            client, customer_id, order_data["shipping"], ship_to=order_data.get("as400_ship_to")
        )

    # Insert picking list
    insert_data = {
        "user_id": PDF_IMPORT_USER_ID or None,
        "order_number": order_number,
        "status": "ready_to_double_check",
        "source": source_for(file_name),
        "is_addon": False,
        "items": cart_items,
        "customer_id": customer_id,
    }

    # Order Comments → notes (only on create, so we never clobber manual notes).
    order_comments = order_data.get("order_comments")
    if order_comments:
        insert_data["notes"] = order_comments

    # AS400 'Order Date' → source_order_date (additive date column on picking_lists).
    # Only written when present; omitted otherwise so PostgREST leaves it NULL.
    order_date = order_data.get("order_date")
    if order_date:
        insert_data["source_order_date"] = order_date

    # Raw AS400 'Account Number' ('0010495 00') for audit, and the ship-to row.
    # Both omitted when unknown, so PostgREST leaves them NULL.
    account_number = order_data.get("account_number")
    if account_number:
        insert_data["as400_account_number"] = account_number
    if ship_to_address_id:
        insert_data["ship_to_address_id"] = ship_to_address_id

    if group_id:
        insert_data["group_id"] = group_id

    result = client.table("picking_lists").insert(insert_data).execute()
    picking_list = result.data[0]

    # Log the import
    _log_import(client, pdf_hash, order_number, file_name, len(cart_items), picking_list["id"])

    return picking_list


def get_new_items_delta(existing_items: list, new_parsed_items: list, client: Client) -> list:
    """
    Compare new parsed items with existing cart items in the database.
    Returns only the items from `new_parsed_items` whose normalized SKU
    is not present in the `existing_items` list.
    """
    if not existing_items:
        return new_parsed_items

    # Convert existing items to a set of SKUs for quick lookup
    # existing_items are in CartItem format, so they have a 'sku' (which is the DB sku)
    # or a 'raw_sku' if DB sku wasn't found. We'll track both to be safe.
    existing_skus = set()
    for item in existing_items:
        if "sku" in item and item["sku"]:
            existing_skus.add(item["sku"])
        if "raw_sku" in item and item["raw_sku"]:
            existing_skus.add(item["raw_sku"])
            existing_skus.add(normalize_sku(item["raw_sku"]))

    # Find the delta
    delta_items = []
    for new_item in new_parsed_items:
        norm_sku = normalize_sku(new_item["sku"])
        raw_sku = new_item.get("raw_sku", norm_sku)

        # Check if this new item's SKU matches any existing SKU
        # We need to consider that _to_cart_items might resolve the sku to a different DB sku.
        # But for delta checking, normalized PDF sku is our best guess before hitting the DB.

        # A more robust check: What if the DB sku is "03-3684BL" but PDF is "03 3684 BL"?
        # existing_skus has "03-3684BL" and "033684BL" (normalized from raw_sku).
        # norm_sku will be "033684BL".
        if norm_sku not in existing_skus and raw_sku not in existing_skus:
            # Maybe the DB sku exists in our set? Let's check against stripped versions just in case
            found = False
            for ext_sku in existing_skus:
                if normalize_sku(ext_sku) == norm_sku:
                    found = True
                    break

            if not found:
                delta_items.append(new_item)

    return delta_items


def append_to_order(
    list_id: str,
    existing_items: list,
    delta_items: list,
    order_number: str,
    pdf_hash: str,
    file_name: str,
) -> dict:
    """
    Append DELTA items to an existing active/ready picking list.
    """
    client = get_client()

    cart_items = _to_cart_items(client, delta_items)
    merged = _merge_items(existing_items, cart_items)

    update_data = {"items": merged}

    # If any new item or existing item is unknown, the list should indicate it
    # Status handling will be done in watcher.py for new creations

    result = client.table("picking_lists").update(update_data).eq("id", list_id).execute()

    _log_import(client, pdf_hash, order_number, file_name, len(cart_items), list_id)

    return result.data[0]


def reopen_completed_order(
    list_id: str,
    existing_items: list,
    delta_items: list,
    order_number: str,
    pdf_hash: str,
    file_name: str,
) -> dict:
    """
    Reopen a completed order as an add-on.
    Sets is_addon=True, status back to 'ready_to_double_check'.
    Appends DELTA items to existing ones.
    """
    client = get_client()

    cart_items = _to_cart_items(client, delta_items)
    merged = _merge_items(existing_items, cart_items)

    result = (
        client.table("picking_lists")
        .update(
            {
                "items": merged,
                "status": "ready_to_double_check",
                "is_addon": True,
                "checked_by": None,
            }
        )
        .eq("id", list_id)
        .execute()
    )

    _log_import(client, pdf_hash, order_number, file_name, len(cart_items), list_id)

    return result.data[0]


def resolve_customer(
    client: Client,
    name: str,
    street: str = None,
    city: str = None,
    state: str = None,
    zip_code: str = None,
    account: str = None,
) -> Optional[str]:
    """Public wrapper for _resolve_customer."""
    return _resolve_customer(client, name, street, city, state, zip_code, account=account)


# The statuses in which an order still holds stock on a shelf: it has been
# planned but not yet completed or cancelled, so its lines are reserved and
# must not be handed to another order. Used by the location planner below.
#
# This list used to be COMBINABLE_STATUSES and had a second job: deciding which
# order a new arrival could be auto-combined into, by customer, within 24h. That
# door is gone (9 sep 2026) — the watcher does not decide that two orders are
# one shipment. PickD does, with a person confirming. See `combine` in the app.
STOCK_HOLDING_STATUSES = [
    "active",
    "ready_to_double_check",
    "needs_correction",
    "double_checking",
]


_VARIANT_BASE_RE = re.compile(r"^(\d{6}[A-Z]{2})[A-Z]?$")


def _variant_base(norm_sku: str) -> Optional[str]:
    """Family base of a normalized bike SKU: dept + number + 2-letter color.

    '033768BL' and '033768BLD' → '033768BL'. Anything that is not exactly that
    shape plus at most ONE finish/variant letter (parts, UPCs, longer codes) has
    no family and returns None, so the sibling rule never touches it.
    """
    m = _VARIANT_BASE_RE.match(norm_sku or "")
    return m.group(1) if m else None


def _pick_by_stock(matches: list, available: dict, requested_qty: int) -> Optional[str]:
    """Choose the catalog SKU for one order line among its variant siblings.

    `matches` is in preference order (the source's own spelling first, then the
    2-letter canonical, then the rest of the family). Stock decides, not the
    spelling: the same bike lives under '03-3768BL' or '03-3768BLD' depending on
    which row the operator last renamed, and the catalog keeps BOTH names alive
    (the old sku_metadata row cannot go — qty-0 inventory rows reference it), so
    "the first name that exists in the catalog" kept landing on the dead one and
    the order arrived flagged LOW STOCK with 145 units on the shelf.

    Returns the first match whose available stock covers the line; failing that
    the one with the most stock; failing that the first — a line nobody can fill
    stays flagged under the name the source used, exactly as before.
    """
    if not matches:
        return None
    needed = max(int(requested_qty or 0), 1)
    for sku in matches:
        if available.get(sku, 0) >= needed:
            return sku
    best = max(matches, key=lambda sku: available.get(sku, 0))
    return best if available.get(best, 0) > 0 else matches[0]


def _to_cart_items(client: Client, parsed_items: list) -> list:
    """
    Convert parsed PDF items to CartItem-compatible format.
    Checks SKU existence in the database.
    """
    if not parsed_items:
        return []

    # Batch check all SKUs in metadata (handling pagination)
    all_metadata = []
    page_size = 1000
    offset = 0
    while True:
        res = (
            client.table("sku_metadata")
            .select("sku")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        if not res.data:
            break
        all_metadata.extend(res.data)
        if len(res.data) < page_size:
            break
        offset += page_size

    # Normalize DB SKUs for loose matching (Map normalized -> original).
    # Ambiguity guard (idea-101): if TWO catalog SKUs share a normalized form
    # (e.g. '034-666-BR' and '03-4666BR'), auto-substituting would risk picking
    # the wrong one — drop the collision so the item stays unresolved and the
    # picker decides manually. (Identical strings are not a collision.)
    sku_map = {}
    ambiguous = set()
    for row in all_metadata:
        norm = normalize_sku(row["sku"])
        if norm in sku_map and sku_map[norm] != row["sku"]:
            ambiguous.add(norm)
        else:
            sku_map[norm] = row["sku"]
    for norm in ambiguous:
        log.warning("Ambiguous catalog SKUs share normalized form %s — leaving manual", norm)
        sku_map.pop(norm, None)

    # Variant-sibling index: normalized bike SKUs grouped by dept+number+color
    # base, so '033768BL' and '033768BLD' are looked at together (_variant_base).
    family_index = {}
    for norm in sku_map:
        base = _variant_base(norm)
        if base:
            family_index.setdefault(base, []).append(norm)

    found_db_skus = []
    item_results = []
    for item in parsed_items:
        normalized_pdf_sku = item["sku"]

        # Every catalog name this line could mean, most specific first: the full
        # raw SKU including any finish/variant suffix (e.g. '03 3769 BLD' →
        # '033769BLD'), then the parser's 2-letter-color canonical guess
        # ('033769BL'), then any other sibling in the family ('033769BLT'…). The
        # catalog is inconsistent — some SKUs keep the 3rd letter ('03-3769BLD',
        # operator-reported 2026-06-11), others don't ('03-3768BL' for a source
        # 'BLD') — and which sibling holds the stock changes with operator renames,
        # so the choice among them is made by STOCK below (_pick_by_stock), not
        # here. (A blind 'strip trailing T' fallback stays out: it could mangle a
        # real 2-letter color like 'WT'/'GT'.)
        candidates = []
        raw_norm = normalize_sku(item.get("raw_sku") or "")
        if raw_norm:
            candidates.append(raw_norm)
        if normalized_pdf_sku not in candidates:
            candidates.append(normalized_pdf_sku)
        base = _variant_base(normalized_pdf_sku)
        for norm in sorted(family_index.get(base, [])) if base else []:
            if norm not in candidates:
                candidates.append(norm)

        matches = []
        for c in candidates:
            db_sku = sku_map.get(c)
            if db_sku and db_sku not in matches:
                matches.append(db_sku)

        found_db_skus.extend(m for m in matches if m not in found_db_skus)
        item_results.append(
            {
                "normalized_pdf_sku": normalized_pdf_sku,
                "matches": matches,  # preference order; stock picks one below
                "not_found": not matches,
                "item": item,
            }
        )

    # Step 2: total stock per SKU, and a name to show. Two things only — the
    # shelves, their distribution and their hints stopped being read here when
    # PickD took over the address, and fetching them per import was work nobody
    # spent.
    inventory_data_map = {}  # SKU -> its inventory rows, read only for item_name
    total_stock_map = {}

    if found_db_skus:
        inv_res = (
            client.table("inventory")
            .select("sku, quantity, item_name")
            .in_("sku", found_db_skus)
            .eq("warehouse", "LUDLOW")
            .eq("is_active", True)
            .execute()
        )

        # Group entries by SKU and aggregate total stock
        raw_entries = inv_res.data or []
        for inv in raw_entries:
            sku = inv["sku"]
            qty = inv["quantity"] or 0
            total_stock_map[sku] = total_stock_map.get(sku, 0) + qty

            if sku not in inventory_data_map:
                inventory_data_map[sku] = []
            inventory_data_map[sku].append(inv)

    # Step 2b: what open orders already hold, per SKU. Not per location any
    # more — PickD decides addresses now, so an order on the board may have none
    # yet, and requiring one here would make it invisible to the availability
    # below (which decides the SKU and the insufficient_stock flag).
    reserved_by_sku = {}  # sku -> total reserved across all locations

    if found_db_skus:
        active_lists = (
            client.table("picking_lists")
            .select("items")
            .in_("status", STOCK_HOLDING_STATUSES)
            .execute()
        )
        for pl in active_lists.data or []:
            for pl_item in pl.get("items") or []:
                sku = pl_item.get("sku", "")
                qty = pl_item.get("pickingQty", 0)
                if sku in found_db_skus and qty > 0:
                    reserved_by_sku[sku] = reserved_by_sku.get(sku, 0) + qty

        # Adjust total_stock_map to reflect reservations
        for sku in total_stock_map:
            total_stock_map[sku] = max(0, total_stock_map[sku] - reserved_by_sku.get(sku, 0))

    # Step 3: Build final cart items

    cart_items = []
    for res in item_results:
        normalized_pdf_sku = res["normalized_pdf_sku"]
        item = res["item"]
        requested_qty = item["qty"]
        # total_stock_map is already net of reservations, so the sibling that
        # wins here is the one a picker can actually take from.
        db_sku = _pick_by_stock(res["matches"], total_stock_map, requested_qty)

        # A line the catalog does not know is written in the canonical spelling
        # of the parser's 2-letter-colour guess ('01-0530', '03-3768BL'), not the
        # bare matching key ('010530') and not the paper's third letter
        # ('03-3768BLD'): the box says BL (Rafael, 2026-08-26) — AS400's third
        # letter is a finish suffix, not another bike. When the operator
        # registers the line, the catalog row gets this same name and the DB
        # marks it found by exact match (pickd idea-154 / bug-020).
        line_sku = db_sku if db_sku else canonical_sku(normalized_pdf_sku)

        # Availability check
        available_qty = total_stock_map.get(db_sku, 0) if db_sku else 0
        insufficient_stock = requested_qty > available_qty

        # The catalogue name, for display. Any inventory row of this SKU carries
        # it; WHICH one stopped mattering when PickD took over the address.
        #
        # Everything that used to live here — flattening each location's
        # distribution into candidates, subtracting reservations per shelf,
        # ranking RETURN TO STOCK first, then PALLET > LINE > TOWER, then fewest
        # units_each — is gone. It was a second implementation of pickd's
        # utils/pickLocation.ts that had to be kept in step with it by hand, and
        # it answered at import time a question that is only answerable when
        # somebody actually walks: PickD replans from live stock the moment the
        # order is taken up (planPickForList).
        sku_entries = inventory_data_map.get(db_sku, []) if db_sku else []
        assigned_item_name = next(
            (e.get("item_name") for e in sku_entries if e.get("item_name")), None
        )

        cart_items.append(
            {
                "sku": line_sku,
                "pickingQty": requested_qty,
                "item_name": assigned_item_name or item.get("description", ""),
                "description": item.get("description", ""),
                "raw_sku": item.get("raw_sku", normalized_pdf_sku),
                "unit_price": item.get("unit_price", 0),
                # Left for PickD to fill. `location_hint`, `sublocation` and
                # `distribution` are gone with it: PickD reads all three off the
                # live inventory row it plans onto, and nothing ever read
                # `available_qty` — it was a snapshot that went stale on arrival.
                "location": None,
                "warehouse": "LUDLOW",
                "source": "pdf_import",
                "sku_not_found": res["not_found"],
                # Total across every shelf, minus what open orders hold. Never
                # depended on a location, so it means exactly what it always did.
                "insufficient_stock": insufficient_stock,
            }
        )
    return cart_items


def _merge_items(existing: list, new_items: list) -> list:
    """
    Merge new items into existing list.
    If same SKU exists, keep both entries (don't sum, since they may be from
    different locations — the web app handles location assignment).
    """
    merged = list(existing) if existing else []
    for new_item in new_items:
        # Check if exact same SKU already exists
        found = False
        for i, existing_item in enumerate(merged):
            if existing_item.get("sku") == new_item.get("sku"):
                # Same SKU: add quantities
                merged[i]["pickingQty"] = merged[i].get("pickingQty", 0) + new_item.get(
                    "pickingQty", 0
                )
                found = True
                break
        if not found:
            merged.append(new_item)

    return merged


def _normalize_customer_name(name: str) -> str:
    """Collapse a name to uppercase alphanumerics so 'ACME, INC.' == 'acme inc'."""
    return re.sub(r"[^A-Z0-9]", "", (name or "").upper())


def _seal_customer_account(client: Client, customer_id: str, account: str) -> None:
    """Fill customers.as400_account when it is still NULL. Never overwrites: the
    first order that names an account seals it, and a later header that disagrees
    (a re-keyed account, a shared customer row) does not move it. Non-blocking."""
    try:
        (
            client.table("customers")
            .update({"as400_account": account})
            .eq("id", customer_id)
            .is_("as400_account", "null")
            .execute()
        )
    except Exception as e:  # noqa: BLE001
        log.warning("Could not seal as400_account %s on customer %s: %s", account, customer_id, e)


def _resolve_customer(
    client: Client,
    name: str,
    street: str = None,
    city: str = None,
    state: str = None,
    zip_code: str = None,
    account: str = None,
) -> Optional[str]:
    """
    Look up a customer, creating one only if there is no match.

    With an AS400 `account` (the bill-to number without leading zeros — see
    parser.split_account_number) the lookup goes by customers.as400_account FIRST:
    that is the identity the ERP uses, so a renamed or re-punctuated dealer still
    resolves to the same row. Only without a sealed match does it fall back to the
    name + street matching below, and when that path finds or creates a row the
    account is SEALED onto it — filled when NULL, never overwritten — so the next
    order from the same dealer takes the short path.

    Defensively normalizes customer names to avoid duplicates in the UI.
    """
    clean_name = name.strip()
    target_name = _normalize_customer_name(clean_name)
    normalized_street = street.strip() if street else None

    if not target_name:
        return None

    if account:
        try:
            by_account = (
                client.table("customers")
                .select("id, as400_account")
                .eq("as400_account", account)
                .limit(1)
                .execute()
            )
            if by_account.data:
                return by_account.data[0]["id"]
        except Exception as e:  # noqa: BLE001 — degrade to name matching, never block
            log.warning("Customer lookup by as400_account %s failed: %s", account, e)

    # Helper to normalize street address for comparison
    def clean_street_str(s):
        return re.sub(r"[^a-z0-9]", "", s.lower()) if s else ""

    customer_id = None
    if normalized_street:
        # Match by name and street address.
        existing = (
            client.table("customers").select("id, name, street, city, state, zip_code").execute()
        )
        if existing.data:
            target_street_clean = clean_street_str(normalized_street)
            for row in existing.data:
                if _normalize_customer_name(row.get("name", "")) != target_name:
                    continue
                if clean_street_str(row.get("street")) == target_street_clean:
                    # Match found! If other fields are missing, let's update them.
                    updates = {}
                    if not row.get("street") and street:
                        updates["street"] = street.strip()
                    if not row.get("city") and city:
                        updates["city"] = city.strip()
                    if not row.get("state") and state:
                        updates["state"] = state.strip()
                    if not row.get("zip_code") and zip_code:
                        updates["zip_code"] = zip_code.strip()
                    if updates:
                        client.table("customers").update(updates).eq("id", row["id"]).execute()
                    customer_id = row["id"]
                    break

        if customer_id is None:
            # If no match, insert new customer with address details
            insert_data = {
                "name": clean_name.upper(),
                "street": street.strip() if street else None,
                "city": city.strip() if city else None,
                "state": state.strip() if state else None,
                "zip_code": zip_code.strip() if zip_code else None,
            }
            if account:
                insert_data["as400_account"] = account
            result = client.table("customers").insert(insert_data).execute()
            return result.data[0]["id"] if result.data else None
    else:
        # Fallback to name-only match.
        existing = client.table("customers").select("id, name, street").execute()
        if existing.data:
            # First look for a row with matching name and NO street address (generic)
            for row in existing.data:
                if _normalize_customer_name(row.get("name", "")) == target_name and not row.get(
                    "street"
                ):
                    customer_id = row["id"]
                    break
            # Otherwise return the first matching name row
            if customer_id is None:
                for row in existing.data:
                    if _normalize_customer_name(row.get("name", "")) == target_name:
                        customer_id = row["id"]
                        break

        if customer_id is None:
            # Create new customer with name only
            insert_data = {"name": clean_name.upper()}
            if account:
                insert_data["as400_account"] = account
            result = client.table("customers").insert(insert_data).execute()
            return result.data[0]["id"] if result.data else None

    if customer_id and account:
        _seal_customer_account(client, customer_id, account)
    return customer_id


def _normalized_address(fields: dict) -> str:
    """Local mirror of customer_addresses.normalized_address — a generated column,
    lower(trim(street)) || '|' || city || '|' || state || '|' || zip_code, each
    lowered and trimmed, NULL → ''. Lets a stored row be compared with a freshly
    parsed Ship-to without a round-trip; keep it in sync with the DB definition
    (pickd migration 20260403220000_customer_addresses.sql)."""
    return "|".join(
        (fields.get(k) or "").strip().lower() for k in ("street", "city", "state", "zip_code")
    )


def _find_address_id(client: Client, customer_id: str, fields: dict) -> Optional[str]:
    """id of the customer's address row equal to `fields` (street/city/state/zip),
    for an upsert whose response came back without data. NULL columns need `is`,
    not `eq` — PostgREST has no eq.null."""
    query = client.table("customer_addresses").select("id").eq("customer_id", customer_id)
    for col in ("street", "city", "state", "zip_code"):
        value = fields.get(col)
        query = query.is_(col, "null") if value is None else query.eq(col, value)
    result = query.limit(1).execute()
    return result.data[0]["id"] if result.data else None


def _save_shipping_address(
    client: Client, customer_id: str, ship: dict, ship_to: str = None
) -> Optional[str]:
    """
    Persist the parsed Ship-to address, mirroring the web Orders view, and return
    the customer_addresses.id this order ships to (None on failure / no street):
      1. customers   — update the customer's main address fields.
      2. customer_addresses — the row for this Ship-to.

    Which row depends on whether the address has a FedEx key. `ship_to` is the
    2-digit AS400 suffix; with it, and a customer that has a sealed as400_account
    and is not ship_to_varies, (account, suffix) names a ship-to SLOT whose
    fedex_recipient_id is account + suffix ('1049500' — the convention FedEx Ship
    Manager already keys 951 recipients on). The watcher never WRITES that id — a
    DB trigger derives it from as400_ship_to — it only computes it to LOOK UP the
    slot:
      - slot found with the same address → nothing to write, return its id;
      - slot found with another address → the dealer moved: update that row in
        place, no second row (the trigger nulls fedex_synced_at → re-sync);
      - no slot → upsert by (customer_id, normalized_address) carrying
        as400_ship_to, and the trigger fills the id.
    A ship_to_varies customer (consumer direct, warranty, Facebook…) or an order
    without the key keeps the plain upsert by address and never gets
    as400_ship_to: a one-off recipient must not claim the channel's Recipient ID.

    Non-blocking: any failure is logged and swallowed so order creation succeeds.
    'street' is required (customer_addresses.street is NOT NULL).
    """
    street = (ship.get("street") or "").strip()
    if not street:
        return None

    address_fields = {
        "street": street,
        "city": ship.get("city"),
        "state": ship.get("state"),
        "zip_code": ship.get("zip_code"),
    }

    # Always overwrite the customer's main address (mirrors the web Orders view,
    # reflecting a Moved/Renamed customer).
    try:
        client.table("customers").update(address_fields).eq("id", customer_id).execute()
    except Exception as e:  # noqa: BLE001
        log.warning(f"Could not update customer address: {e}")

    label = (ship.get("name") or "").strip() or None

    # One read: does this customer carry the key, and is it a channel?
    account, varies = None, False
    try:
        cust = (
            client.table("customers")
            .select("as400_account, ship_to_varies")
            .eq("id", customer_id)
            .limit(1)
            .execute()
        )
        if cust.data:
            account = cust.data[0].get("as400_account")
            varies = bool(cust.data[0].get("ship_to_varies"))
    except Exception as e:  # noqa: BLE001
        log.warning(f"Could not read customer {customer_id} for the FedEx key: {e}")

    keyed = bool(ship_to and account and not varies)
    try:
        if keyed:
            rid = account.lstrip("0") + ship_to
            slot = (
                client.table("customer_addresses")
                .select("id, street, city, state, zip_code")
                .eq("fedex_recipient_id", rid)
                .limit(1)
                .execute()
            )
            if slot.data:
                row = slot.data[0]
                if _normalized_address(row) != _normalized_address(address_fields):
                    client.table("customer_addresses").update(
                        {**address_fields, "label": label}
                    ).eq("id", row["id"]).execute()
                    log.info("Ship-to slot %s moved: %s", rid, street)
                return row["id"]

        entry = {"customer_id": customer_id, "label": label, **address_fields}
        if keyed:
            entry["as400_ship_to"] = ship_to

        # Mark this address as the default only when the customer has none yet —
        # never downgrade an existing default (respects one_default_per_customer).
        existing_default = (
            client.table("customer_addresses")
            .select("id")
            .eq("customer_id", customer_id)
            .eq("is_default", True)
            .limit(1)
            .execute()
        )
        if not existing_default.data:
            entry["is_default"] = True

        result = (
            client.table("customer_addresses")
            .upsert(entry, on_conflict="customer_id,normalized_address")
            .execute()
        )
        if result.data:
            return result.data[0]["id"]
        return _find_address_id(client, customer_id, address_fields)
    except Exception as e:  # noqa: BLE001
        log.warning(f"Could not save customer_addresses entry: {e}")
        return None


def _log_import(
    client: Client,
    pdf_hash: str,
    order_number: Optional[str],
    file_name: str,
    items_count: int,
    picking_list_id: str,
):
    """Log the PDF import for audit and duplicate detection.

    Best-effort: a self-healing re-send (same capture, topping up an existing order
    with newly-parsed SKUs) re-logs the same pdf_hash. If pdf_hash is unique-
    constrained that insert raises — swallow it, since the original log row already
    records the import and the audit trail is non-critical to the operation.
    """
    try:
        client.table("pdf_import_log").insert(
            {
                "pdf_hash": pdf_hash,
                "order_number": order_number,
                "file_name": file_name,
                "items_count": items_count,
                "picking_list_id": picking_list_id,
                "status": "processed",
            }
        ).execute()
    except Exception as e:
        log.warning("pdf_import_log insert skipped for hash %s: %s", pdf_hash, e)
