"""
scanned_store.py — Local cache of orders the auto-scanner has captured from AS400.

The auto-scanner walks order numbers from a start (e.g. 880112) upward and stores
each captured order here, WITHOUT sending it to PickD. Sending happens later when a
trigger arrives: a PDF dropped in the watch folder, or a manual capture in the UI —
both look the number up here first and reuse the cached AS400 capture.

This file is the hand-off between the two processes (the capture UI writes it, the
folder watcher reads it), so it lives on disk as a single JSON object keyed by order
number. Writes are atomic (tmp + os.replace) so a reader never sees a half-written
file. It is local working state — gitignored, never committed.

Shape:
    {
      "880112": {
        "order_number": "880112",
        "raw_text": "<full AS400 capture text>",
        "customer": "...", "item_count": 13, "total_units": 13,
        "subtotal": 4850.35, "parsed_total": 4850.35, "total_mismatch": false,
        "scanned_at": "2026-06-09T...Z", "source": "auto_scan" | "manual_capture"
      },
      ...
    }
"""

import json
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("pickd-scanned-store")

# The first order number the auto-scanner should try when the cache is empty.
SCAN_START = int(os.getenv("SCAN_START", "880112"))

_lock = threading.Lock()


def _path() -> Path:
    return Path(
        os.getenv(
            "SCANNED_STORE_PATH",
            str(Path(__file__).resolve().parent / ".scanned_orders.json"),
        )
    )


def load() -> dict:
    """Read the whole cache (best-effort; returns {} on missing/corrupt file)."""
    p = _path()
    try:
        if p.exists():
            data = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
    except Exception as e:
        log.warning("Could not read scanned store: %s", e)
    return {}


def _save(data: dict) -> None:
    """Atomically write the cache so cross-process readers never see a partial file."""
    p = _path()
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, p)


def get(order_number) -> dict | None:
    """Return the cached entry for an order number, or None."""
    if not order_number:
        return None
    return load().get(str(order_number))


def put(order_number, raw_text: str, meta: dict | None = None, source: str = "auto_scan") -> dict:
    """Insert/update a scanned order. Returns the stored entry."""
    key = str(order_number)
    entry = {
        "order_number": key,
        "raw_text": raw_text,
        "scanned_at": datetime.now(timezone.utc).isoformat(),
        "source": source,
        **(meta or {}),
    }
    with _lock:
        data = load()
        data[key] = entry
        _save(data)
    return entry


def next_scan_number(start: int = SCAN_START) -> int:
    """The next order number to scan: one past the highest cached number, floored
    at `start`. With an empty cache this is `start` (e.g. 880112)."""
    data = load()
    highest = start - 1
    for k in data:
        try:
            n = int(k)
        except (TypeError, ValueError):
            continue
        if n >= start and n > highest:
            highest = n
    return highest + 1


def count() -> int:
    return len(load())
