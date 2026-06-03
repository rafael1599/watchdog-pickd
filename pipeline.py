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
    return {
        "order_number": data.get("order_number"),
        "customer": data.get("customer_name") or "Unknown",
        "item_count": len(items),
        "is_last_page": data.get("is_last_page", False),
        "items": items,
    }


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

    # 1. Exact duplicate by content hash
    existing_log = check_duplicate(pdf_hash)
    if existing_log:
        return _result(
            "duplicate",
            order_number=existing_log.get("order_number"),
            message=(
                f"Contenido idéntico ya procesado el "
                f"{existing_log.get('processed_at', 'fecha desconocida')}."
            ),
        )

    # 2. Parse
    order_data = parse_order(text)
    items = order_data.get("items", [])
    if not items:
        return _result("no_items", message="No se encontraron ítems en la captura.")

    order_number = order_data.get("order_number")
    customer = order_data.get("customer_name") or "Unknown"

    result = None
    status = None  # action taken

    # 3. Existing order by number → delta append / reopen
    if order_number:
        existing = find_existing_order(order_number)
        if existing:
            list_id = existing["id"]
            existing_items = existing.get("items", []) or []
            existing_status = existing.get("status", "")

            client = get_client()
            delta_items = get_new_items_delta(existing_items, items, client)

            if not delta_items:
                return _result(
                    "duplicate",
                    order_number=order_number,
                    customer=customer,
                    message=f"Orden #{order_number}: sin SKUs nuevos. Nada que agregar.",
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
            else:
                result = create_order(order_data, pdf_hash, source_name)
                status = "created"

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
        message=f"Orden #{result.get('order_number')} ({len(updated_items)} ítems).",
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
