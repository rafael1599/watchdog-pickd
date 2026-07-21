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

from parser import normalize_sku

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


def create_order(order_data: dict, pdf_hash: str, file_name: str) -> dict:
    """
    Create a new picking list from parsed PDF data.
    Inserts with status='ready_to_double_check' and source='pdf_import'.

    order_data format:
    {
        'order_number': str | None,
        'customer_name': str | None,
        'items': [ { sku, qty, ... } ]
    }
    """
    client = get_client()

    order_number = order_data.get("order_number")
    if not order_number:
        order_number = _next_negative_order_number()

    # Convert items to CartItem-compatible format for the web app
    cart_items = _to_cart_items(client, order_data["items"])

    # Look up or create customer
    customer_id = None
    customer_name = order_data.get("customer_name")
    if customer_name:
        addr = order_data.get("customer_address") or {}
        customer_id = _resolve_customer(
            client,
            customer_name,
            street=addr.get("street"),
            city=addr.get("city"),
            state=addr.get("state"),
            zip_code=addr.get("zip_code")
        )

    # Mirror the web Orders view: persist the Ship-to address on the customer
    # (main address) and in customer_addresses (history). Non-blocking.
    if customer_id and order_data.get("shipping"):
        _save_shipping_address(client, customer_id, order_data["shipping"])

    # Insert picking list
    insert_data = {
        "user_id": PDF_IMPORT_USER_ID or None,
        "order_number": order_number,
        "status": "ready_to_double_check",
        "source": "pdf_import",
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
    zip_code: str = None
) -> Optional[str]:
    """Public wrapper for _resolve_customer."""
    return _resolve_customer(client, name, street, city, state, zip_code)


COMBINABLE_STATUSES = ["active", "ready_to_double_check", "needs_correction", "double_checking"]


def find_combinable_order_by_customer(
    customer_id: str, exclude_order_number: str = None
) -> Optional[dict]:
    """
    Find an existing picking list for the same customer that can be combined.
    Only returns orders in combinable statuses, created within the last 24 hours.
    Returns the most recently created one.

    Waiting orders (is_waiting_inventory = true — they live in needs_correction,
    a combinable status) are EXCLUDED: an order parked waiting for inventory must
    never be auto-combined with a new arrival. Joining one is a manual,
    user-confirmed action in PickD only (operator rule, 2026-06-11).
    """
    client = get_client()
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()

    query = (
        client.table("picking_lists")
        .select("*")
        .eq("customer_id", customer_id)
        .in_("status", COMBINABLE_STATUSES)
        .or_("is_waiting_inventory.is.null,is_waiting_inventory.eq.false")
        .gte("created_at", cutoff)
        .order("created_at", desc=True)
        .limit(1)
    )

    if exclude_order_number:
        query = query.neq("order_number", exclude_order_number)

    result = query.execute()
    if result.data and len(result.data) > 0:
        return result.data[0]
    return None


def combine_into_order(
    target_order: dict, new_order_data: dict, pdf_hash: str, file_name: str
) -> dict:
    """
    Combine a new PDF order into an existing picking list for the same customer.
    - Tags items with source_order for future splitting
    - Concatenates order numbers: "878279 / 878280"
    - Updates combine_meta with provenance data
    - If target was in double_checking: resets to ready_to_double_check, releases checker
    """
    client = get_client()

    target_id = target_order["id"]
    existing_items = target_order.get("items", []) or []
    existing_order_number = target_order["order_number"] or ""
    new_order_number = new_order_data.get("order_number") or "UNKNOWN"

    # Tag existing items with source_order if not already tagged
    for item in existing_items:
        if "source_order" not in item:
            # Use the first order number segment (handles already-combined orders)
            base_order = (
                existing_order_number.split(" / ")[0]
                if " / " in existing_order_number
                else existing_order_number
            )
            item["source_order"] = base_order

    # Convert new items to cart format and tag with source_order
    cart_items = _to_cart_items(client, new_order_data["items"])
    for item in cart_items:
        item["source_order"] = new_order_number

    # Delta check: only add items not already present
    delta_items = []
    existing_skus = set()
    for item in existing_items:
        sku = item.get("sku", "")
        if sku:
            existing_skus.add(sku)
            existing_skus.add(normalize_sku(sku))

    for new_item in cart_items:
        sku = new_item.get("sku", "")
        norm = normalize_sku(sku) if sku else ""
        if sku not in existing_skus and norm not in existing_skus:
            delta_items.append(new_item)
        else:
            # Same SKU but different source_order: keep as separate line item
            delta_items.append(new_item)
            # Mark that this is a cross-order duplicate so we DON'T merge quantities
            new_item["_cross_order"] = True

    # Merge: append delta items without merging same-SKU across source orders
    merged = list(existing_items)
    for new_item in delta_items:
        cross_order = new_item.pop("_cross_order", False)
        if cross_order:
            # Keep as separate line item (different source_order)
            merged.append(new_item)
        else:
            # New SKU, just append
            merged.append(new_item)

    # Concatenate order numbers
    combined_order_number = f"{existing_order_number} / {new_order_number}"

    # Build/update combine_meta
    existing_meta = target_order.get("combine_meta") or {}
    if not existing_meta.get("source_orders"):
        existing_meta["source_orders"] = [
            {
                "order_number": existing_order_number,
                "added_at": target_order.get("created_at", datetime.now(timezone.utc).isoformat()),
                "item_count": len(existing_items),
            }
        ]
    existing_meta["is_combined"] = True
    existing_meta["source_orders"].append(
        {
            "order_number": new_order_number,
            "pdf_hash": pdf_hash,
            "file_name": file_name,
            "added_at": datetime.now(timezone.utc).isoformat(),
            "item_count": len(cart_items),
        }
    )

    update_data = {
        "items": merged,
        "order_number": combined_order_number,
        "combine_meta": existing_meta,
    }

    # If checker had the order open, release it
    if target_order.get("status") == "double_checking":
        update_data["status"] = "ready_to_double_check"
        update_data["checked_by"] = None

    result = client.table("picking_lists").update(update_data).eq("id", target_id).execute()

    _log_import(client, pdf_hash, new_order_number, file_name, len(cart_items), target_id)

    return result.data[0]


# Statuses that count as "in verification" (the PickD double-check pipeline).
# 'completed' and 'cancelled' are terminal and excluded; 'reopened' orders are
# back in the editing loop, so they count as in-verification too.
VERIFICATION_STATUSES = [
    "active",
    "ready_to_double_check",
    "double_checking",
    "needs_correction",
    "reopened",
]


def get_verification_count() -> int:
    """Number of picking_lists currently in verification (see VERIFICATION_STATUSES).

    Goes UP when an order is sent into the queue and DOWN when one is completed
    or cancelled.
    """
    client = get_client()
    result = (
        client.table("picking_lists")
        .select("id", count="exact")
        .in_("status", VERIFICATION_STATUSES)
        .execute()
    )
    if getattr(result, "count", None) is not None:
        return result.count
    return len(result.data or [])


def get_verification_board() -> dict:
    """Read-only snapshot of recent verification orders grouped by status.

    One query over the last ~14 days. Each entry carries a minimal shape:
    order_number, customer, status, shipping_type, items (count).
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=14)).isoformat()
    client = get_client()
    result = (
        client.table("picking_lists")
        .select("order_number, status, shipping_type, items, customers(name), updated_at")
        .in_("status", VERIFICATION_STATUSES)
        .gte("updated_at", cutoff)
        .order("updated_at", desc=True)
        .execute()
    )

    board: dict[str, list] = {s: [] for s in VERIFICATION_STATUSES}
    for row in result.data or []:
        status = row.get("status")
        if status not in board:
            board[status] = []
        customer = row.get("customers") or {}
        board[status].append(
            {
                "order_number": row.get("order_number"),
                "customer": (customer.get("name") if isinstance(customer, dict) else None)
                or "Unknown",
                "status": status,
                "shipping_type": row.get("shipping_type"),
                "items": len(row.get("items") or []),
            }
        )
    return board


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

    found_db_skus = []
    item_results = []
    for item in parsed_items:
        normalized_pdf_sku = item["sku"]

        # Most-specific match FIRST: the full raw SKU including any finish/variant
        # suffix (e.g. '03 3769 BLD' → '033769BLD'), then the parser's 2-letter-color
        # canonical guess ('033769BL'). The catalog is inconsistent: some SKUs keep
        # the 3rd letter ('03-3769BLD' — operator-reported 2026-06-11), others don't
        # ('03-3768BL' for a source 'BLD'), so we let the catalog decide instead of
        # guessing at parse time. (A blind 'strip trailing T' fallback stays out:
        # it could mangle a real 2-letter color like 'WT'/'GT'.)
        candidates = []
        raw_norm = normalize_sku(item.get("raw_sku") or "")
        if raw_norm:
            candidates.append(raw_norm)
        if normalized_pdf_sku not in candidates:
            candidates.append(normalized_pdf_sku)
        db_sku = next((sku_map[c] for c in candidates if c in sku_map), None)

        not_found = db_sku is None
        found_db_skus.append(db_sku) if db_sku else None
        item_results.append(
            {
                "normalized_pdf_sku": normalized_pdf_sku,
                "db_sku": db_sku,
                "not_found": not_found,
                "item": item,
            }
        )

    # Step 2: Fetch locations and total stock from inventory for found SKUs
    inventory_data_map = {}  # SKU -> List of all inventory entries
    total_stock_map = {}

    if found_db_skus:
        # Fetch inventory for LUDLOW including distribution and hints
        inv_res = (
            client.table("inventory")
            .select("sku, location, quantity, distribution, location_hint, item_name, sublocation")
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

    # Step 2b: Query active picking lists to calculate already-reserved stock.
    # This prevents two concurrent orders from over-assigning the same location.
    reserved_map = {}  # (sku, location) -> reserved_qty
    reserved_by_sku = {}  # sku -> total reserved across all locations

    if found_db_skus:
        active_lists = (
            client.table("picking_lists")
            .select("items")
            .in_("status", COMBINABLE_STATUSES)
            .execute()
        )
        for pl in active_lists.data or []:
            for pl_item in pl.get("items") or []:
                sku = pl_item.get("sku", "")
                loc = pl_item.get("location", "")
                qty = pl_item.get("pickingQty", 0)
                if sku in found_db_skus and loc and qty > 0:
                    key = (sku, loc)
                    reserved_map[key] = reserved_map.get(key, 0) + qty
                    reserved_by_sku[sku] = reserved_by_sku.get(sku, 0) + qty

        # Adjust total_stock_map to reflect reservations
        for sku in total_stock_map:
            total_stock_map[sku] = max(0, total_stock_map[sku] - reserved_by_sku.get(sku, 0))

    # Step 3: Build final cart items using prioritization logic
    # PALLET (0) > LINE (1) > TOWER (2) > OTHER (3)
    PRIORITY = {"PALLET": 0, "LINE": 1, "TOWER": 2, "OTHER": 3}

    cart_items = []
    for res in item_results:
        db_sku = res["db_sku"]
        normalized_pdf_sku = res["normalized_pdf_sku"]
        item = res["item"]
        requested_qty = item["qty"]

        # Availability check
        available_qty = total_stock_map.get(db_sku, 0) if db_sku else 0
        insufficient_stock = requested_qty > available_qty

        # Find best location for this SKU
        assigned_location = None
        assigned_hint = None
        assigned_sublocation = None
        assigned_distribution = []
        assigned_item_name = None

        sku_entries = inventory_data_map.get(db_sku, []) if db_sku else []
        if sku_entries:
            # Flatten all distribution options per location to compare them
            candidates = []
            for entry in sku_entries:
                dist_list = entry.get("distribution") or []
                if not isinstance(dist_list, list) or not dist_list:
                    candidates.append(
                        {
                            "entry": entry,
                            "priority": 4,
                            "units_each": entry["quantity"],
                            "has_dist": False,
                        }
                    )
                    continue

                for d in dist_list:
                    candidates.append(
                        {
                            "entry": entry,
                            "priority": PRIORITY.get(d.get("type"), 3),
                            "units_each": d.get("units_each", 999999),
                            "has_dist": True,
                        }
                    )

            # Calculate effective available stock per candidate (physical - reserved)
            for c in candidates:
                entry = c["entry"]
                loc = entry.get("location") or ""
                reserved = reserved_map.get((db_sku, loc), 0)
                c["effective_qty"] = max(0, (entry.get("quantity") or 0) - reserved)

            # Filter: only locations with effective stock > 0
            in_stock = [c for c in candidates if c["effective_qty"] > 0]

            # If no location has stock, leave location=None (item stays flagged
            # with insufficient_stock=True and the picker sees the warning)
            active_candidates = in_stock if in_stock else None

            if active_candidates:
                # Sort: Priority first (Pallet=0), then units_each (fewer is better),
                # then effective quantity (more is better)
                active_candidates.sort(
                    key=lambda x: (x["priority"], x["units_each"], -x["effective_qty"])
                )

                best_match = active_candidates[0]["entry"]
                assigned_location = best_match["location"]
                assigned_hint = best_match.get("location_hint")
                assigned_sublocation = best_match.get("sublocation")
                assigned_distribution = best_match.get("distribution") or []
                assigned_item_name = best_match.get("item_name")
            else:
                # No stock anywhere — grab item_name from any entry for display
                assigned_item_name = sku_entries[0].get("item_name")

        cart_items.append(
            {
                "sku": db_sku if db_sku else normalized_pdf_sku,
                "pickingQty": requested_qty,
                "item_name": assigned_item_name or item.get("description", ""),
                "description": item.get("description", ""),
                "raw_sku": item.get("raw_sku", normalized_pdf_sku),
                "unit_price": item.get("unit_price", 0),
                "location": assigned_location,
                "location_hint": assigned_hint,
                "sublocation": assigned_sublocation,
                "distribution": assigned_distribution,
                "warehouse": "LUDLOW",
                "source": "pdf_import",
                "sku_not_found": res["not_found"],
                "insufficient_stock": insufficient_stock,
                "available_qty": available_qty,
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


def _resolve_customer(
    client: Client,
    name: str,
    street: str = None,
    city: str = None,
    state: str = None,
    zip_code: str = None
) -> Optional[str]:
    """
    Look up a customer by name and address, creating one only if there is no match.
    Defensively normalizes customer names to avoid duplicates in the UI.
    """
    clean_name = name.strip()
    target_name = _normalize_customer_name(clean_name)
    normalized_street = street.strip() if street else None

    if not target_name:
        return None

    import re

    # Helper to normalize street address for comparison
    def clean_street_str(s):
        return re.sub(r'[^a-z0-9]', '', s.lower()) if s else ''

    if normalized_street:
        # Match by name and street address.
        existing = client.table("customers").select("id, name, street, city, state, zip_code").execute()
        if existing.data:
            target_street_clean = clean_street_str(normalized_street)
            for row in existing.data:
                if _normalize_customer_name(row.get("name", "")) == target_name:
                    if clean_street_str(row.get("street")) == target_street_clean:
                        # Match found! If other fields are missing, let's update them.
                        updates = {}
                        if not row.get("street") and street: updates["street"] = street.strip()
                        if not row.get("city") and city: updates["city"] = city.strip()
                        if not row.get("state") and state: updates["state"] = state.strip()
                        if not row.get("zip_code") and zip_code: updates["zip_code"] = zip_code.strip()
                        
                        if updates:
                            client.table("customers").update(updates).eq("id", row["id"]).execute()
                            
                        return row["id"]
        
        # If no match, insert new customer with address details
        insert_data = {
            "name": clean_name.upper(),
            "street": street.strip() if street else None,
            "city": city.strip() if city else None,
            "state": state.strip() if state else None,
            "zip_code": zip_code.strip() if zip_code else None,
        }
        result = client.table("customers").insert(insert_data).execute()
        if result.data:
            return result.data[0]["id"]
    else:
        # Fallback to name-only match.
        existing = client.table("customers").select("id, name, street").execute()
        if existing.data:
            # First look for a row with matching name and NO street address (generic)
            for row in existing.data:
                if _normalize_customer_name(row.get("name", "")) == target_name and not row.get("street"):
                    return row["id"]
            # Otherwise return the first matching name row
            for row in existing.data:
                if _normalize_customer_name(row.get("name", "")) == target_name:
                    return row["id"]
            
        # Create new customer with name only
        result = client.table("customers").insert({"name": clean_name.upper()}).execute()
        if result.data:
            return result.data[0]["id"]

    return None


def _save_shipping_address(client: Client, customer_id: str, ship: dict) -> None:
    """
    Persist the parsed Ship-to address, mirroring the web Orders view:
      1. customers   — update the customer's main address fields.
      2. customer_addresses — upsert into the address history (dedup via the
         unique (customer_id, normalized_address) constraint); label = Ship-to name.

    Non-blocking: any failure is logged and swallowed so order creation succeeds.
    'street' is required (customer_addresses.street is NOT NULL).
    """
    street = (ship.get("street") or "").strip()
    if not street:
        return

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

    entry = {
        "customer_id": customer_id,
        "label": (ship.get("name") or "").strip() or None,
        **address_fields,
    }
    try:
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

        client.table("customer_addresses").upsert(
            entry, on_conflict="customer_id,normalized_address"
        ).execute()
    except Exception as e:  # noqa: BLE001
        log.warning(f"Could not save customer_addresses entry: {e}")


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
