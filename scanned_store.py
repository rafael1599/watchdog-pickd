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

from __future__ import annotations  # PEP 563: keep "dict | None" annotations working on Python 3.9

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


def _cursor_path() -> Path:
    # The scan cursor is a separate, monotonic high-water mark so removing/archiving
    # a cached order never rewinds the scanner (which would re-capture that number).
    return Path(
        os.getenv(
            "SCAN_CURSOR_PATH",
            str(Path(__file__).resolve().parent / ".scan_cursor"),
        )
    )


def _read_cursor() -> int | None:
    try:
        return int(_cursor_path().read_text(encoding="utf-8").strip())
    except Exception:
        return None


def _write_cursor(n: int) -> None:
    try:
        _cursor_path().write_text(str(int(n)), encoding="utf-8")
    except Exception as e:
        log.warning("Could not write scan cursor: %s", e)


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
    """Insert/update a scanned order. Returns the stored entry.

    For auto-scan captures, also advances the persistent cursor past this number so
    the scanner never rewinds — even if this entry is later archived/removed.
    """
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
        # Only auto-scan advances the cursor; a manual capture of an arbitrary number
        # must not make the scanner skip the orders in between.
        if source == "auto_scan" and key.isdigit() and int(key) >= SCAN_START:
            if int(key) + 1 > (_read_cursor() or 0):
                _write_cursor(int(key) + 1)
    return entry


def update_meta(order_number, **meta) -> None:
    """Merge extra fields into a cached entry (e.g. in_pickd=True). No-op if absent."""
    key = str(order_number)
    with _lock:
        data = load()
        if key in data:
            data[key].update(meta)
            _save(data)


def skip(order_number) -> None:
    """Advance the scan cursor past a number WITHOUT caching anything.

    For orders that exist in AS400 but must never become candidates — e.g. a VOID
    order (complete screen, zero items). Without this the scanner would treat the
    number as not-found and retry it forever, never reaching the next order.
    """
    key = str(order_number)
    with _lock:
        if key.isdigit() and int(key) >= SCAN_START:
            if int(key) + 1 > (_read_cursor() or 0):
                _write_cursor(int(key) + 1)


def delete(order_number) -> None:
    """Remove an order from the cache (e.g. after archiving it). Does NOT rewind the
    cursor, so the scanner won't re-capture the removed number."""
    key = str(order_number)
    with _lock:
        data = load()
        if key in data:
            del data[key]
            _save(data)


def next_scan_number(start: int = SCAN_START) -> int:
    """The next order number the scanner should try.

    The persistent cursor is authoritative; we also floor at `start` and at one past
    the highest cached number (so a cache from before the cursor existed still
    advances correctly). With nothing scanned this is `start` (e.g. 880112)."""
    candidates = [start]
    cur = _read_cursor()
    if cur is not None:
        candidates.append(cur)
    # Floor at one past the highest AUTO-scanned number (back-compat for caches from
    # before the cursor existed). Manual captures of arbitrary numbers are excluded —
    # they must not make the scanner skip the orders in between.
    for k, e in load().items():
        if str(k).isdigit() and int(k) >= start and (e or {}).get("source") != "manual_capture":
            candidates.append(int(k) + 1)
    return max(candidates)


def count() -> int:
    return len(load())
