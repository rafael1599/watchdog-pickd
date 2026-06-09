"""
pipeline.py — Core ingestion pipeline shared by the PDF watcher and the AS400 app.

Takes already-extracted ORDER TEXT (from a PDF or from an AS400 screen capture),
parses it and pushes it into Supabase (create / append / reopen / combine),
applying the same duplicate-detection and needs_correction rules.

This is the single source of truth for "turn order text into a PickD picking list".
"""

import logging

from extractor import compute_hash
from parser import parse_order
from supabase_client import (
    _to_cart_items,
    append_to_order,
    check_duplicate,
    combine_into_order,
    create_order,
    find_combinable_order_by_customer,
    find_existing_order,
    get_client,
    get_new_items_delta,
    reopen_completed_order,
    resolve_customer,
)

log = logging.getLogger("pickd-pipeline")


def preview_order(text: str) -> dict:
    """
    Parse order text WITHOUT touching Supabase. Used to show a preview
    (customer, order number, total item count) before the user sends it.
    """
    data = parse_order(text)
    items = data.get("items", [])

    # Reconciliation guard: the AS400 header carries the order Sub-Total, which is
    # the sum of the line items' extended prices. If our parsed lines don't add up
    # to it, a line was silently dropped or misparsed — surface it BEFORE sending so
    # an incomplete order is caught at capture time, not discovered downstream.
    subtotal = data.get("subtotal")
    parsed_total = round(sum(float(i.get("extend_price") or 0) for i in items), 2)
    total_mismatch = subtotal is not None and abs(parsed_total - subtotal) > 0.01

    return {
        "order_number": data.get("order_number"),
        "customer": data.get("customer_name") or "Unknown",
        "item_count": len(items),  # number of distinct line items / SKUs
        "total_units": sum(int(i.get("qty") or 0) for i in items),  # sum of quantities
        "subtotal": subtotal,  # order Sub-Total from the header (None if not found)
        "parsed_total": parsed_total,  # sum of parsed line extends
        "total_mismatch": total_mismatch,  # True → likely a missing/misparsed line
        "is_last_page": data.get("is_last_page", False),
        "order_comments": data.get("order_comments"),
        "shipping_address": data.get("shipping_address"),
        "ship_via": data.get("ship_via"),
        "items": items,
    }


def resolve_order_items(text: str) -> list:
    """Resolve a captured order's items to pick locations/stock — READ-ONLY.

    Reuses the exact resolver that 'Send to PickD' uses (`_to_cart_items`): it
    reads sku_metadata / inventory / active picking lists and assigns the best
    location per SKU, but creates and reserves NOTHING. Used to preview an order's
    detail (locations, distribution, problem flags) before it is sent.

    Each returned item carries: sku, raw_sku, pickingQty, item_name, description,
    warehouse, location, location_hint, sublocation, distribution, unit_price,
    sku_not_found, insufficient_stock, available_qty.
    """
    data = parse_order(text)
    items = data.get("items", [])
    if not items:
        return []
    return _to_cart_items(get_client(), items)


def process_order_text(text: str, source_name: str = "as400_capture") -> dict:
    """
    Full ingestion pipeline for a single order's text.

    Returns a result dict:
        {
            "status": "created" | "appended" | "reopened" | "combined"
                      | "duplicate" | "no_items" | "empty_text",
            "order_number": str | None,
            "customer": str,
            "item_count": int,          # items in the resulting picking list (when applicable)
            "needs_correction": bool,
            "picking_list": dict | None,
            "message": str,
        }

    Never raises for expected outcomes (duplicate / no items) — those come back as
    a status. Unexpected errors propagate to the caller.
    """
    if not text or len(text.strip()) < 20:
        return _result("empty_text", message="No usable text in capture.")

    pdf_hash = compute_hash(text)

    # 1. Is this exact content already processed? We do NOT bail here. A re-send of
    # the identical capture must still be able to TOP UP an existing order with SKUs
    # that a newer parser now extracts (e.g. after fixing a parse gap). The hash only
    # guards the create/combine paths below — never the delta-append to an existing
    # order, and never silently recreates an order whose content was seen before.
    existing_log = check_duplicate(pdf_hash)
    is_duplicate_content = existing_log is not None
    dup_date = existing_log.get("processed_at", "unknown date") if existing_log else None

    # 2. Parse
    order_data = parse_order(text)
    items = order_data.get("items", [])
    if not items:
        return _result("no_items", message="No items found in the capture.")

    order_number = order_data.get("order_number")
    customer = order_data.get("customer_name") or "Unknown"

    result = None
    status = None  # action taken

    # 3. Existing order by number → delta append / reopen (self-healing re-send)
    if order_number:
        existing = find_existing_order(order_number)
        if existing:
            list_id = existing["id"]
            existing_items = existing.get("items", []) or []
            existing_status = existing.get("status", "")

            client = get_client()
            delta_items = get_new_items_delta(existing_items, items, client)

            if not delta_items:
                msg = f"Order #{order_number}: no new SKUs. Nothing to add."
                if is_duplicate_content:
                    msg = (
                        f"Order #{order_number}: identical content already processed "
                        f"on {dup_date}. No new SKUs to add."
                    )
                return _result(
                    "duplicate",
                    order_number=order_number,
                    customer=customer,
                    message=msg,
                )

            if existing_status == "completed":
                result = reopen_completed_order(
                    list_id, existing_items, delta_items, order_number, pdf_hash, source_name
                )
                status = "reopened"
            elif existing_status in (
                "active",
                "ready_to_double_check",
                "double_checking",
                "needs_correction",
            ):
                result = append_to_order(
                    list_id, existing_items, delta_items, order_number, pdf_hash, source_name
                )
                status = "appended"
            elif not is_duplicate_content:
                result = create_order(order_data, pdf_hash, source_name)
                status = "created"
            else:
                # Terminal/other status (e.g. cancelled) AND content already seen —
                # don't recreate it from a re-send.
                return _result(
                    "duplicate",
                    order_number=order_number,
                    customer=customer,
                    message=f"Identical content already processed on {dup_date}.",
                )

    # 3b. No existing order to append to, but this exact content was already
    # processed before (e.g. the order was deleted) — report duplicate instead of
    # silently recreating or combining it.
    if result is None and is_duplicate_content:
        return _result(
            "duplicate",
            order_number=order_number,
            customer=customer,
            message=f"Identical content already processed on {dup_date}.",
        )

    # 4. Auto-combine by customer
    if result is None and order_data.get("customer_name"):
        client = get_client()
        customer_id = resolve_customer(client, order_data["customer_name"])
        if customer_id:
            combinable = find_combinable_order_by_customer(
                customer_id, exclude_order_number=order_number
            )
            if combinable:
                result = combine_into_order(combinable, order_data, pdf_hash, source_name)
                status = "combined"

    # 5. Fallback: create new
    if result is None:
        result = create_order(order_data, pdf_hash, source_name)
        status = "created"
        if not order_number:
            get_client().table("picking_lists").update({"status": "needs_correction"}).eq(
                "id", result["id"]
            ).execute()

    # 6. Flag needs_correction on unknown SKUs / low stock
    updated_items = result.get("items", [])
    has_unknown = any(i.get("sku_not_found") for i in updated_items)
    has_low_stock = any(i.get("insufficient_stock") for i in updated_items)
    needs_correction = has_unknown or has_low_stock

    if needs_correction:
        get_client().table("picking_lists").update({"status": "needs_correction"}).eq(
            "id", result["id"]
        ).execute()

    return _result(
        status,
        order_number=result.get("order_number"),
        customer=customer,
        item_count=len(updated_items),
        needs_correction=needs_correction,
        picking_list=result,
        message=f"Order #{result.get('order_number')} ({len(updated_items)} items).",
    )


def _result(
    status,
    order_number=None,
    customer="Unknown",
    item_count=0,
    needs_correction=False,
    picking_list=None,
    message="",
):
    return {
        "status": status,
        "order_number": order_number,
        "customer": customer,
        "item_count": item_count,
        "needs_correction": needs_correction,
        "picking_list": picking_list,
        "message": message,
    }
