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
from datetime import datetime
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


def parse_order_subtotal(text: str) -> Optional[float]:
    """
    Extract the order Sub-Total from the header, e.g. 'Sub-Total   4850.35'.

    The Sub-Total is the sum of the line items' extended prices (before freight),
    so it is the reconciliation anchor: if our parsed items don't add up to it, a
    line was dropped or misparsed. We prefer 'Sub-Total'; if absent we fall back to
    'Order Total' (equal when there is no freight). Returns None if neither is found.
    """
    for label in (r"Sub-?Total", r"Order\s+Total"):
        m = re.search(rf"{label}\s+([\d,]*\.\d{{2}})", text, re.IGNORECASE)
        if m:
            return float(m.group(1).replace(",", ""))
    return None


def parse_customer_name(text: str) -> Optional[str]:
    """
    Extract the customer (Bill-to) name from the 'Bill' line.

    PDFs put it at the line start ('Bill MATTHEWS BICYCLE MART, INC'), but AS400
    screen captures indent it AND place the Ship-to name on the SAME line:
        ' Bill DEALER WARRANTY 2009             Ship CHICAGO LAND BICYCLES'
    So we allow leading whitespace and cut off the right-hand Ship-to column.
    """
    match = re.search(r"^\s*Bill\b[ \t]*(.*)$", text, re.MULTILINE | re.IGNORECASE)
    if not match:
        return None
    # Drop the Ship-to column when Bill and Ship share one line (the columns are
    # separated by a gap of 2+ spaces before 'Ship').
    name = re.split(r"\s{2,}Ship\b", match.group(1), flags=re.IGNORECASE)[0]
    name = re.sub(r"\s+", " ", name).strip()
    # Blank Bill: the whole line was '  Bill            Ship NAME', so what is
    # left collapses to 'Ship NAME' — treat that as no Bill-to name.
    if re.match(r"^Ship\b", name, re.IGNORECASE):
        return None
    return name or None


def parse_customer_address(text: str) -> Optional[dict]:
    """
    Extract customer address following the 'Bill' line.
    Looks at up to 4 lines below the customer name to find the city/state/zip line.
    Concats intervening lines as the street.
    """
    lines = text.split("\n")
    for idx, line in enumerate(lines):
        if re.match(r"^Bill\s+", line, re.IGNORECASE):
            for offset in range(1, 5):
                if idx + offset < len(lines):
                    candidate_line = lines[idx + offset].strip()
                    zip_match = re.search(r"\b(\d{5}(?:-\d{4})?)\s*$", candidate_line)
                    if zip_match:
                        street_lines = [lines[idx + o].strip() for o in range(1, offset)]
                        street = " ".join(street_lines)
                        
                        zip_code = zip_match.group(1)
                        rem = candidate_line[:zip_match.start()].strip()
                        state_match = re.search(r"\b([A-Z]{2})\s*$", rem, re.IGNORECASE)
                        if state_match:
                            state = state_match.group(1).upper()
                            city = rem[:state_match.start()].replace(",", "").strip()
                            return {
                                "street": street,
                                "city": city,
                                "state": state,
                                "zip_code": zip_code
                            }
                        else:
                            return {
                                "street": street,
                                "city": rem.replace(",", "").strip(),
                                "state": "",
                                "zip_code": zip_code
                            }
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
    #   qty_ord  qty_ship  <dept> <number> [<color><variant?>]  W/H  description  unit  extend
    #
    # The Stock# is captured CANONICALLY: a 2-digit dept, a 4-digit number, and an
    # OPTIONAL 2-letter color. Bikes carry a color (e.g. '03 3927 BK'); parts and
    # generic stock have none (e.g. '01 0449', '12 2502', '66 1200') and go straight
    # from the number to the W/H column — those must still be captured. Some PDFs
    # append an extra finish/variant letter glued to the color (e.g. '03 3768 BLD'
    # where the canonical SKU is '03-3768BL'); that suffix is captured separately so
    # it never pollutes the SKU. The W/H is the next single letter, keeping the
    # description out of the SKU.
    item_pattern = re.compile(
        r"^\s*(\d+)\s+(\d+)\s+"  # 1 qty_ord, 2 qty_ship
        r"(\d{2})\s?(\d{4})\s+"  # 3 dept, 4 number (column gap)
        r"(?:([A-Z]{2})([A-Z]*)\s+)?"  # 5 color (optional, 2 letters), 6 finish/variant suffix
        r"([A-Z])\s+"  # 7 warehouse code (single letter like N)
        r"(.+?)\s+"  # 8 description (non-greedy)
        r"([\d,]*\.\d{2})\s+"  # 9 unit price (allow ".00" with no leading digits)
        r"([\d,]*\.\d{2})\s*$"  # 10 extended price
    )

    for line in lines:
        match = item_pattern.match(line)
        if match:
            qty_ordered = int(match.group(1))
            # color/variant are optional (parts have no color code) → default to "".
            dept, number, color, variant = (
                match.group(3),
                match.group(4),
                match.group(5) or "",
                match.group(6) or "",
            )
            warehouse = match.group(7).strip()
            description = match.group(8).strip()
            unit_price = float(match.group(9).replace(",", ""))
            extend_price = float(match.group(10).replace(",", ""))

            # Canonical SKU = dept + number + 2-letter color. Any extra finish/variant
            # letters the PDF glues onto the color are NOT part of the canonical SKU.
            canonical_sku = f"{dept}{number}{color}"
            raw_sku = f"{dept} {number} {color}{variant}".strip()

            items.append(
                {
                    "sku": normalize_sku(canonical_sku),
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

    AS400 screens render the function-key legend ('Cmd5 Cmd6 Cmd7 ...') on the
    same row, so we strip that legend; an order with no real comment → None.
    """
    match = re.search(r"Order\s+Comments:\s*(.+)", text, re.IGNORECASE)
    if not match:
        return None
    comment = re.sub(r"\s+", " ", match.group(1)).strip()
    # Remove the AS400 command-key legend (Cmd5, Cmd6=..., etc.) and anything after it.
    comment = re.sub(r"\s*\bCmd\d+\b.*$", "", comment, flags=re.IGNORECASE).strip()
    return comment or None


def _extract_ship_to_lines(text: str) -> list:
    """
    Collect the raw lines of the Ship-to block (right column of the header).

    The header is a fixed-width two-column layout: 'Bill ...' on the left and
    'Ship ...' on the right. We locate the 'Ship' label, take the name after it,
    then collect the right-column text of the following lines until a footer field
    (Terms:, Sales ID:, etc.), the items table, or an end marker appears.
    Returns [name, *address_lines]; empty list if no Ship-to found.
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
        if re.search(
            r"END\s+OF\s+ORDER|Quant|Stock\s*#|I\s*N\s*Q\s*U\s*I\s*R\s*Y", line, re.IGNORECASE
        ):
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

    return parts


def parse_shipping_address(text: str) -> Optional[str]:
    """Ship-to block as a single comma-joined string (for previews/display)."""
    parts = _extract_ship_to_lines(text)
    return ", ".join(parts) if parts else None


# Canonical USPS abbreviations, mirroring PickD's parseUSAddress so a watcher
# import and a manual paste produce the same normalized_address (dedup key).
_STREET_SUFFIXES = {
    "STREET": "ST",
    "STR": "ST",
    "ST": "ST",
    "AVENUE": "AVE",
    "AVEN": "AVE",
    "AV": "AVE",
    "AVE": "AVE",
    "BOULEVARD": "BLVD",
    "BLVD": "BLVD",
    "ROAD": "RD",
    "RD": "RD",
    "DRIVE": "DR",
    "DRV": "DR",
    "DR": "DR",
    "LANE": "LN",
    "LN": "LN",
    "COURT": "CT",
    "CT": "CT",
    "CIRCLE": "CIR",
    "CIR": "CIR",
    "PLACE": "PL",
    "PL": "PL",
    "TERRACE": "TER",
    "TER": "TER",
    "PARKWAY": "PKWY",
    "PKWY": "PKWY",
    "HIGHWAY": "HWY",
    "HWY": "HWY",
    "TRAIL": "TRL",
    "TRL": "TRL",
    "SQUARE": "SQ",
    "SQ": "SQ",
    "WAY": "WAY",
}
_DIRECTIONALS = {
    "NORTH": "N",
    "SOUTH": "S",
    "EAST": "E",
    "WEST": "W",
    "NORTHEAST": "NE",
    "NORTHWEST": "NW",
    "SOUTHEAST": "SE",
    "SOUTHWEST": "SW",
    "N": "N",
    "S": "S",
    "E": "E",
    "W": "W",
    "NE": "NE",
    "NW": "NW",
    "SE": "SE",
    "SW": "SW",
}
_UNIT_DESIGNATORS = {"APT", "STE", "SUITE", "UNIT", "FL", "FLOOR", "BLDG", "RM", "DEPT", "#"}


def normalize_street(street: Optional[str]) -> Optional[str]:
    """
    Canonicalize a street line so it matches what PickD's parseUSAddress stores:
    spell-out suffixes/directionals collapse to their USPS abbreviation
    ('123 SAINT JOHNS AVENUE' → '123 SAINT JOHNS AVE'). 'FL' as a unit
    designator (e.g. '2ND FL') is left alone, never treated as a directional.
    Unknown tokens pass through unchanged. Returns None unchanged.
    """
    if not street:
        return street

    tokens = street.split()
    out = []
    for i, tok in enumerate(tokens):
        core = re.sub(r"[.,]+$", "", tok)
        key = core.upper()
        prev = out[-1].upper() if out else ""
        # Don't fold a directional/suffix that is actually a unit value
        # (e.g. '2ND FL', 'APT W'): skip when the previous token is a designator.
        if prev in _UNIT_DESIGNATORS:
            out.append(core)
        elif key in _STREET_SUFFIXES and i > 0:
            out.append(_STREET_SUFFIXES[key])
        elif key in _DIRECTIONALS and i > 0:
            out.append(_DIRECTIONALS[key])
        else:
            out.append(core)
    return " ".join(out)


def parse_shipping_address_struct(text: str) -> Optional[dict]:
    """
    Ship-to block split into structured fields for the customers /
    customer_addresses tables:

        { 'name': str, 'street': str|None, 'city': str|None,
          'state': str|None, 'zip_code': str|None }

    First line is the Ship-to name; the last line is parsed as 'CITY ST ZIP'
    when it matches; everything in between is the street. Returns None if no
    Ship-to block is found.
    """
    parts = _extract_ship_to_lines(text)
    if not parts:
        return None

    name = parts[0]
    rest = parts[1:]
    city = state = zip_code = None

    if rest:
        m = re.match(r"^(.*?),?\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$", rest[-1])
        if m:
            city, state, zip_code = m.group(1).strip(), m.group(2), m.group(3)
            rest = rest[:-1]

    street = normalize_street(", ".join(rest)) if rest else None
    return {
        "name": name,
        "street": street,
        "city": city,
        "state": state,
        "zip_code": zip_code,
    }


def parse_order_date(text: str) -> Optional[str]:
    """
    Extract the AS400 'Order Date' from the header and return it as an ISO date
    string ('YYYY-MM-DD'), or None if absent/unparseable.

    The header carries it as 6-digit MMDDYY, e.g.:
        'Order Date: 060226 P/O: SO1608'  →  month=06, day=02, year=2026
    The 2-digit year is interpreted as 2000-2099 (these are current AS400 orders).
    Invalid month/day combinations (e.g. month 13, day 00) return None.
    """
    match = re.search(r"Order\s+Date:\s*(\d{6})\b", text, re.IGNORECASE)
    if not match:
        return None
    raw = match.group(1)
    month, day, year = int(raw[0:2]), int(raw[2:4]), 2000 + int(raw[4:6])
    try:
        return datetime(year, month, day).strftime("%Y-%m-%d")
    except ValueError:
        return None


def parse_ship_via(text: str) -> Optional[str]:
    """
    Extract the shipping carrier from the 'Ship Via' header field, verbatim.

    The header row is fixed-width with several columns, e.g.:
        '           Ship Via    FEDEX           Shipped From Florida'
    We take the value right after 'Ship Via' up to the next column gap (2+ spaces),
    so a multi-word carrier ('UPS GROUND') stays intact while the neighbouring
    'Shipped From ...' column is not swallowed. Returns None if absent/empty.
    """
    match = re.search(r"Ship\s+Via\b[ \t]*(.+)$", text, re.IGNORECASE | re.MULTILINE)
    if not match:
        return None
    # First column only: split on a 2+ space gap (the column separator).
    value = re.split(r"\s{2,}", match.group(1).strip())[0].strip()
    return value or None


def parse_order(text: str) -> dict:
    """
    Main entry point: parse all data from extracted PDF text.

    Returns:
        {
            'order_number': str | None,
            'account_number': str | None,
            'customer_name': str | None,
            'customer_address': dict | None,
            'items': [ { sku, qty, qty_ordered, raw_sku, warehouse, description, unit_price, extend_price } ],
            'is_last_page': bool,
            'order_comments': str | None,
            'shipping_address': str | None,
            'ship_via': str | None,
            'order_date': str | None,   # ISO 'YYYY-MM-DD' from the AS400 header
            'raw_text': str
        }
    """
    return {
        "order_number": parse_order_number(text),
        "account_number": parse_account_number(text),
        "customer_name": parse_customer_name(text),
        "customer_address": parse_customer_address(text),
        "items": parse_items(text),
        "subtotal": parse_order_subtotal(text),
        "is_last_page": has_end_of_order(text),
        "order_comments": parse_order_comments(text),
        "shipping_address": parse_shipping_address(text),
        "shipping": parse_shipping_address_struct(text),
        "ship_via": parse_ship_via(text),
        "order_date": parse_order_date(text),
        "raw_text": text,
    }
