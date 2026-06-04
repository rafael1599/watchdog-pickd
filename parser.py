"""
parser.py — Parse extracted PDF text into structured order data.

Handles the specific format of the order inquiry PDFs:
    O R D E R  I N Q U I R Y
    Order Number: 878279  Account Number: 0007099 00
    Bill MATTHEWS BICYCLE MART, INC
    Quant Quant  Stock #      W/H  Description          Unit     Extend
    Ord   Ship                                          Price
    4     4      03 3684 BR   N    FAULTLINE A1 17...   1299.95  5199.80
"""

import re
from typing import Dict, List, Optional


def normalize_sku(raw_sku: str) -> str:
    """
    Normalize SKU by removing all non-alphanumeric characters.
    '03 3684 BR' → '033684BR'
    '03-3985GY'  → '033985GY'
    """
    return re.sub(r"[^A-Z0-9]+", "", raw_sku.upper())


def parse_order_number(text: str) -> Optional[str]:
    """
    Extract order number from text. Position-independent regex.
    Handles: 'Order Number: 878279' anywhere in the text.
    """
    match = re.search(r"Order\s*Number:\s*(\d+)", text, re.IGNORECASE)
    return match.group(1) if match else None


def parse_account_number(text: str) -> Optional[str]:
    """
    Extract account number from text.
    Handles: 'Account Number: 0007099 00'
    """
    match = re.search(r"Account\s*Number:\s*([\d\s]+)", text, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return None


def parse_customer_name(text: str) -> Optional[str]:
    """
    Extract customer name from the 'Bill' line.
    The customer name follows 'Bill ' on its own line.
    Handles: 'Bill MATTHEWS BICYCLE MART, INC'
    """
    # Look for a line starting with "Bill " followed by the customer name
    match = re.search(r"^Bill\s+(.+)$", text, re.MULTILINE | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return None


def parse_items(text: str) -> List[Dict]:
    """
    Parse order line items from the structured table.

    Each line format (after header):
        Qty_Ord  Qty_Ship  Stock#(with spaces)  W/H  Description  UnitPrice  ExtendPrice

    Example:
        4     4      03 3684 BR   N    FAULTLINE A1 17 2025 SANDSTONE    1299.95  5199.80

    The Stock# has internal spaces (e.g., '03 3684 BR') which we normalize.
    """
    items = []

    # Split text into lines for processing
    lines = text.split("\n")

    # Pattern to match item lines:
    # (qty_ord) (qty_ship) (stock# with spaces up to single letter W/H) (W/H) (description) (unit_price) (extend_price)
    #
    # The Stock# portion ends when we hit a single uppercase letter (the W/H code)
    # followed by a space and the description text.
    item_pattern = re.compile(
        r"^\s*(\d+)\s+(\d+)\s+"  # qty_ord, qty_ship
        r"([\d]{2}\s[\d]{4}\s?\w*)\s+"  # stock number (e.g., '03 3684 BR' or '03 3684 BLT')
        r"([A-Z])\s+"  # warehouse code (single letter like N)
        r"(.+?)\s+"  # description (non-greedy)
        r"([\d,]*\.\d{2})\s+"  # unit price (allow ".00" with no leading digits)
        r"([\d,]*\.\d{2})\s*$"  # extended price (allow ".00" with no leading digits)
    )

    for line in lines:
        match = item_pattern.match(line)
        if match:
            qty_ordered = int(match.group(1))
            raw_sku = match.group(3).strip()
            warehouse = match.group(4).strip()
            description = match.group(5).strip()
            unit_price = float(match.group(6).replace(",", ""))
            extend_price = float(match.group(7).replace(",", ""))

            items.append(
                {
                    "sku": normalize_sku(raw_sku),
                    "qty": qty_ordered,  # Use ordered qty (backorders qty_ship=0 still need user decision)
                    "qty_ordered": qty_ordered,
                    "raw_sku": raw_sku,
                    "warehouse": warehouse,
                    "description": description,
                    "unit_price": unit_price,
                    "extend_price": extend_price,
                }
            )

    return items


def has_end_of_order(text: str) -> bool:
    """
    Check if the text contains the 'END OF ORDER' marker,
    indicating this is the last page/PDF of the order.
    """
    return bool(re.search(r"END\s+OF\s+ORDER", text, re.IGNORECASE))


def parse_order_comments(text: str) -> Optional[str]:
    """
    Extract the 'Order Comments:' value from the header.
    Handles: 'Order Comments: SEE EMAIL FOR CC PAYMENT' (same line).
    Returns None if absent or empty. ('Invoice Comments' is a different field.)
    """
    match = re.search(r"Order\s+Comments:\s*(.+)", text, re.IGNORECASE)
    if match:
        comment = re.sub(r"\s+", " ", match.group(1)).strip()
        return comment or None
    return None


def parse_shipping_address(text: str) -> Optional[str]:
    """
    Extract the Ship-to block (right column of the header) as a single string.

    The header is a fixed-width two-column layout: 'Bill ...' on the left and
    'Ship ...' on the right. We locate the 'Ship' label, take the name after it,
    then collect the right-column text of the following lines until a footer field
    (Terms:, Sales ID:, etc.) appears. Best-effort; returns None if not found.
    """
    lines = text.split("\n")
    ship_col = None
    parts = []
    blanks = 0

    for line in lines:
        if ship_col is None:
            m = re.search(r"\bShip\b", line)
            # The Ship-to label, not 'Ship Via' / 'Ship Date' / 'Shipped From'.
            if m and not re.search(r"Ship\s+(Via|Date)|Shipped", line):
                ship_col = m.start()
                name = line[m.end() :].strip()
                if name:
                    parts.append(re.sub(r"\s+", " ", name))
            continue

        # Stop when the left column starts a footer field, or we reach the items
        # table / end marker / a new page's inquiry title.
        if re.match(r"\s*(Terms:|Sales ID:|Credit Hold:|Order Date:|Order Comments:)", line):
            break
        if re.search(r"END\s+OF\s+ORDER|Quant|Stock\s*#|I\s*N\s*Q\s*U\s*I\s*R\s*Y", line, re.IGNORECASE):
            break

        right = re.sub(r"\s+", " ", line[ship_col:]).strip()
        if not right:
            blanks += 1
            if blanks >= 2:  # two blank right-column lines → end of block
                break
            continue
        parts.append(right)
        if len(parts) >= 4:
            break

    return ", ".join(parts) if parts else None


def parse_order(text: str) -> dict:
    """
    Main entry point: parse all data from extracted PDF text.

    Returns:
        {
            'order_number': str | None,
            'account_number': str | None,
            'customer_name': str | None,
            'items': [ { sku, qty, qty_ordered, raw_sku, warehouse, description, unit_price, extend_price } ],
            'is_last_page': bool,
            'order_comments': str | None,
            'shipping_address': str | None,
            'raw_text': str
        }
    """
    return {
        "order_number": parse_order_number(text),
        "account_number": parse_account_number(text),
        "customer_name": parse_customer_name(text),
        "items": parse_items(text),
        "is_last_page": has_end_of_order(text),
        "order_comments": parse_order_comments(text),
        "shipping_address": parse_shipping_address(text),
        "raw_text": text,
    }
