"""
supabase_client.py — Direct Supabase operations for the PDF watcher.

Uses the SERVICE_ROLE_KEY to bypass RLS (runs locally only).
Inserts orders directly into picking_lists so the web app picks them up via Realtime.
"""

import os
import json
from typing import Optional, List, Dict
from datetime import datetime
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL", "http://localhost:54321")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
PDF_IMPORT_USER_ID = os.getenv("PDF_IMPORT_USER_ID", "")

# Negative order number counter file
COUNTER_FILE = os.path.join(os.path.dirname(__file__), ".negative_counter")


def get_client() -> Client:
    """Create and return a Supabase client using service role key."""
    if not SUPABASE_KEY:
        raise ValueError("SUPABASE_SERVICE_ROLE_KEY not set in .env")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


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
    result = (
        client.table("pdf_import_log")
        .select("*")
        .eq("pdf_hash", pdf_hash)
        .execute()
    )
    if result.data and len(result.data) > 0:
        return result.data[0]
    return None


def find_existing_order(order_number: str) -> Optional[dict]:
    """
    Find an existing picking list by order number.
    Returns the most recent one (could be active or completed).
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
    return None


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
        customer_id = _resolve_customer(client, customer_name)

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

    result = client.table("picking_lists").insert(insert_data).execute()
    picking_list = result.data[0]

    # Log the import
    _log_import(client, pdf_hash, order_number, file_name, len(cart_items), picking_list["id"])

    return picking_list


def append_to_order(list_id: str, existing_items: list, new_items: list,
                    order_number: str, pdf_hash: str, file_name: str) -> dict:
    """
    Append new items to an existing active/ready picking list.
    Merges items: if same SKU exists, adds quantities.
    """
    client = get_client()

    cart_items = _to_cart_items(client, new_items)
    merged = _merge_items(existing_items, cart_items)

    update_data = {"items": merged}
    
    # If any new item or existing item is unknown, the list should indicate it
    # Status handling will be done in watcher.py for new creations
    
    result = (
        client.table("picking_lists")
        .update(update_data)
        .eq("id", list_id)
        .execute()
    )

    _log_import(client, pdf_hash, order_number, file_name, len(cart_items), list_id)

    return result.data[0]


def reopen_completed_order(list_id: str, existing_items: list, new_items: list,
                           order_number: str, pdf_hash: str, file_name: str) -> dict:
    """
    Reopen a completed order as an add-on.
    Sets is_addon=True, status back to 'ready_to_double_check'.
    Appends new items to existing ones.
    """
    client = get_client()

    cart_items = _to_cart_items(client, new_items)
    merged = _merge_items(existing_items, cart_items)

    result = (
        client.table("picking_lists")
        .update({
            "items": merged,
            "status": "ready_to_double_check",
            "is_addon": True,
            "checked_by": None,
        })
        .eq("id", list_id)
        .execute()
    )

    _log_import(client, pdf_hash, order_number, file_name, len(cart_items), list_id)

    return result.data[0]


from parser import normalize_sku

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

    # Normalize DB SKUs for loose matching (Map normalized -> original)
    sku_map = {normalize_sku(row["sku"]): row["sku"] for row in all_metadata}

    found_db_skus = []
    item_results = []
    for item in parsed_items:
        normalized_pdf_sku = item["sku"]
        
        # Try finding exact normalized match first
        db_sku = sku_map.get(normalized_pdf_sku)
        
        # Fuzzy Fallback: Many PDF SKUs have extra suffixes like 'T' or 'PALLET'
        # e.g., '033994BLT' (PDF) vs '03-3994BL' (DB)
        if not db_sku:
            # Try removing common suffixes
            for suffix in ["T", "PALLET"]:
                if normalized_pdf_sku.endswith(suffix):
                    stripped = normalized_pdf_sku[:-len(suffix)]
                    if stripped in sku_map:
                        db_sku = sku_map[stripped]
                        break

        not_found = db_sku is None
        found_db_skus.append(db_sku) if db_sku else None
        item_results.append({
            "normalized_pdf_sku": normalized_pdf_sku,
            "db_sku": db_sku,
            "not_found": not_found,
            "item": item
        })

    # Step 2: Fetch locations and total stock from inventory for found SKUs
    inventory_map = {}
    total_stock_map = {}
    if found_db_skus:
        # Fetch inventory for LUDLOW
        inv_res = (
            client.table("inventory")
            .select("sku, location, quantity")
            .in_("sku", found_db_skus)
            .eq("warehouse", "LUDLOW")
            .order("quantity", desc=True) # Prefer locations with more stock
            .execute()
        )
        # Map SKU to its primary location and aggregate total stock
        for inv in inv_res.data:
            sku = inv["sku"]
            qty = inv["quantity"] or 0
            
            # Aggregate total stock
            total_stock_map[sku] = total_stock_map.get(sku, 0) + qty
            
            # Map primary location (one with highest stock)
            if sku not in inventory_map and inv["location"]:
                inventory_map[sku] = inv["location"]

    # Step 3: Build final cart items
    cart_items = []
    for res in item_results:
        db_sku = res["db_sku"]
        normalized_pdf_sku = res["normalized_pdf_sku"]
        item = res["item"]
        requested_qty = item["qty"]
        
        # Use database location if available
        assigned_location = inventory_map.get(db_sku) if db_sku else None
        
        # Check availability
        available_qty = total_stock_map.get(db_sku, 0) if db_sku else 0
        insufficient_stock = requested_qty > available_qty

        cart_items.append({
            "sku": db_sku if db_sku else normalized_pdf_sku, # Use official format if found
            "pickingQty": requested_qty,
            "description": item.get("description", ""),
            "raw_sku": item.get("raw_sku", normalized_pdf_sku),
            "unit_price": item.get("unit_price", 0),
            "location": assigned_location,
            "warehouse": "LUDLOW",
            "source": "pdf_import",
            "sku_not_found": res["not_found"],
            "insufficient_stock": insufficient_stock,
            "available_qty": available_qty,
        })
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
                merged[i]["pickingQty"] = (
                    merged[i].get("pickingQty", 0) + new_item.get("pickingQty", 0)
                )
                found = True
                break
        if not found:
            merged.append(new_item)

    return merged


def _resolve_customer(client: Client, name: str) -> Optional[str]:
    """
    Look up customer by name. If not found, create a new one.
    Returns the customer ID.
    """
    normalized = name.strip()

    # Try exact match first
    result = (
        client.table("customers")
        .select("id")
        .eq("name", normalized)
        .execute()
    )

    if result.data and len(result.data) > 0:
        return result.data[0]["id"]

    # Create new customer
    result = (
        client.table("customers")
        .insert({"name": normalized})
        .execute()
    )

    if result.data:
        return result.data[0]["id"]

    return None


def _log_import(client: Client, pdf_hash: str, order_number: Optional[str],
                file_name: str, items_count: int, picking_list_id: str):
    """Log the PDF import for audit and duplicate detection."""
    client.table("pdf_import_log").insert({
        "pdf_hash": pdf_hash,
        "order_number": order_number,
        "file_name": file_name,
        "items_count": items_count,
        "picking_list_id": picking_list_id,
        "status": "processed",
    }).execute()
