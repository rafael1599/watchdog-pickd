"""
stock_reread.py — Read the AS400 stock again, for a list PickD hands over.

Rafael, 9 nov 2026: «Necesito el sku de una bicicleta con mayor discrepancia en
cantidad entre as400 y pickd … a veces el as400 lo tiene con terminaciones
diferentes aparte de la regular, por ejemplo blt o bl por bk … y quiero una
lectura de hoy».

The catalogue queue never asks twice: a SKU with `as400_description` is done for
ever, so the on-hand PickD compares against was the one read on 12 sep. This
reads it again — and reads the codes AS400 keeps beside PickD's own (`03-4035BLT`
next to `03-4035BL`), which PickD has no row for and so could never compare.

PickD decides WHAT is read, the watcher only reads it. The list lives in the
`app_flags` row `as400_stock_reread`:

    enabled: true
    config:  {"rev": "2026-11-09",              # a new rev starts over
              "skus": ["03-4035BL", "03-4035BLT", ...],   # in this order
              "home": {"03-4035BLT": "03-4035BL"},        # where a code with no
                                                          # row of its own is kept
              "gap_budget_sec": 3600, "max_per_gap": 1000}

What is written, and only with SKU_ENRICH_WRITE on:
  - a code with its own row: `as400_snapshot` gets the fresh screen (the keys it
    already had are kept, `variants` included), plus `reread_rev`/`reread_at`;
    `as400_read_at` moves to now.
  - a code kept at a `home`: `as400_snapshot.variants[code]` on the home row —
    on_hand, available, description, read_at, rev; `found: false` when AS400
    has no such number.
Never a quantity, a weight, a name or `as400_description`: AS400 does not
reconcile PickD (9 oct 2026); this only says what AS400 says, and when.

It runs in the gap loop, ahead of the catalogue, with the same brakes: orders
asked for, the operator, a pending update. Done codes are kept in a local file
per rev, so a restart does not read them twice.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from as400_capture import (
    AS400Disconnected,
    AS400ManualLoginRequired,
    CaptureError,
    StockScreenMismatch,
    StockSkuNotFound,
    capture_stock_inquiry,
    return_to_search,
)
from parser import parse_stock_inquiry

log = logging.getLogger("pickd-stock-reread")

FLAG_KEY = "as400_stock_reread"
FLAG_TTL_SEC = 60.0
# A code that fails this many times (mismatch, error) is given up on for the rev:
# one bad number must not hold the list.
MAX_TRIES = 2

SNAPSHOT_KEYS = (
    "description",
    "kind",
    "model_year",
    "weight_lbs",
    "on_hand",
    "on_order",
    "available",
    "open_po",
    "price_breaks",
    "unit_meas",
    "status_code",
    "bin_location",
    "stock_location",
    "commission_pct",
    "vendor_no",
)

_flag_cache: dict = {"at": 0.0, "row": None}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _client(client=None):
    if client is not None:
        return client
    from supabase_client import get_client

    return get_client()


def _flag(client=None) -> dict:
    """The `app_flags` row, re-read at most once a minute. Unreadable is off."""
    now = time.monotonic()
    if _flag_cache["row"] is not None and now - _flag_cache["at"] < FLAG_TTL_SEC:
        return _flag_cache["row"]
    row = {}
    try:
        res = (
            _client(client)
            .table("app_flags")
            .select("enabled, config")
            .eq("key", FLAG_KEY)
            .limit(1)
            .execute()
        )
        row = (res.data or [{}])[0] or {}
    except Exception as e:  # noqa: BLE001 — unreadable means off, never on
        log.warning("stock reread: could not read app_flags (%s) — treating as off", e)
    _flag_cache.update(at=now, row=row)
    return row


def _reset_for_tests() -> None:
    _flag_cache.update(at=0.0, row=None)


def config(client=None) -> dict:
    row = _flag(client)
    if not row.get("enabled"):
        return {}
    cfg = row.get("config")
    if not isinstance(cfg, dict) or not cfg.get("rev") or not cfg.get("skus"):
        return {}
    return cfg


def active(client=None) -> bool:
    return bool(config(client))


def gap_budget_sec(default: float) -> float:
    """While a reread is going, its own budget wins (it is the reason for the gap)."""
    try:
        return float(config().get("gap_budget_sec", default))
    except (TypeError, ValueError):
        return default


def max_per_gap(default: int) -> int:
    try:
        return max(1, int(config().get("max_per_gap", default)))
    except (TypeError, ValueError):
        return default


# ── what is done, per rev, on disk ───────────────────────────────────────────


def _state_path() -> Path:
    return Path(
        os.getenv(
            "STOCK_REREAD_STATE_PATH",
            str(Path(__file__).resolve().parent / ".stock_reread.json"),
        )
    )


def load_state(rev: str) -> dict:
    """{"rev", "done": {code: verdict}, "tries": {code: n}} — empty for a new rev."""
    try:
        data = json.loads(_state_path().read_text())
        if data.get("rev") == rev:
            data.setdefault("done", {})
            data.setdefault("tries", {})
            return data
    except Exception:  # noqa: BLE001 — no file or a bad one: start the rev clean
        pass
    return {"rev": rev, "done": {}, "tries": {}}


def save_state(state: dict) -> None:
    try:
        _state_path().write_text(json.dumps(state))
    except Exception as e:  # noqa: BLE001
        log.warning("stock reread: could not save state (%s)", e)


def next_code(client=None) -> str | None:
    """The next code on PickD's list that this rev has not finished."""
    cfg = config(client)
    if not cfg:
        return None
    state = load_state(str(cfg["rev"]))
    for code in cfg["skus"]:
        code = str(code or "").strip().upper()
        if code and code not in state["done"]:
            return code
    return None


def progress(client=None) -> tuple[int, int]:
    cfg = config(client)
    if not cfg:
        return 0, 0
    state = load_state(str(cfg["rev"]))
    return len(state["done"]), len(cfg["skus"])


def _finish(rev: str, code: str, verdict: str) -> None:
    state = load_state(rev)
    state["done"][code] = verdict
    state["tries"].pop(code, None)
    save_state(state)


def _failed(rev: str, code: str, why: str) -> None:
    """Count a failure; past MAX_TRIES the code is given up for this rev."""
    state = load_state(rev)
    n = int(state["tries"].get(code, 0)) + 1
    if n >= MAX_TRIES:
        state["done"][code] = f"gave up: {why}"[:200]
        state["tries"].pop(code, None)
    else:
        state["tries"][code] = n
    save_state(state)


# ── the write ────────────────────────────────────────────────────────────────


def _writes_enabled() -> bool:
    return os.getenv("SKU_ENRICH_WRITE", "0") in ("1", "true", "True", "yes")


def _read_snapshot(sku: str, client) -> dict | None:
    """The row's snapshot, `{}` when it has none, None when there is no row."""
    res = (
        client.table("sku_metadata").select("sku, as400_snapshot").eq("sku", sku).limit(1).execute()
    )
    rows = res.data or []
    if not rows:
        return None
    snap = rows[0].get("as400_snapshot")
    return dict(snap) if isinstance(snap, dict) else {}


def plan_own(old: dict, parsed: dict, rev: str, at: str) -> dict:
    """The new snapshot for a code read on its own row. Pure."""
    new = dict(old or {})
    for k in SNAPSHOT_KEYS:
        new[k] = parsed.get(k)
    if not (parsed.get("description") or "").strip() and (old or {}).get("description"):
        new["description"] = old["description"]
    new["reread_rev"] = rev
    new["reread_at"] = at
    return new


def plan_variant(old: dict, code: str, parsed: dict | None, rev: str, at: str) -> dict:
    """The home row's snapshot with `variants[code]` set. Pure. `parsed=None` is
    «AS400 has no such number»."""
    new = dict(old or {})
    variants = dict(new.get("variants") or {})
    if parsed is None:
        variants[code] = {"found": False, "rev": rev, "read_at": at}
    else:
        variants[code] = {
            "found": True,
            "on_hand": parsed.get("on_hand"),
            "available": parsed.get("available"),
            "description": parsed.get("description"),
            "rev": rev,
            "read_at": at,
        }
    new["variants"] = variants
    return new


def write(code: str, home: str | None, parsed: dict | None, rev: str, client=None) -> str:
    """Write one reading. Returns what was done, for the log and the tests."""
    if not _writes_enabled():
        log.info("stock reread %s WOULD write (SKU_ENRICH_WRITE off): %s", code, parsed)
        return "dry"
    client = _client(client)
    at = _now()
    target = home or code
    old = _read_snapshot(target, client)
    if old is None:
        log.warning("stock reread %s: no sku_metadata row %s — nothing written", code, target)
        return "no_row"
    if target == code:
        if parsed is None:
            return "not_found"  # its own row stays as it was
        update = {"as400_snapshot": plan_own(old, parsed, rev, at), "as400_read_at": at}
    else:
        update = {"as400_snapshot": plan_variant(old, code, parsed, rev, at)}
    client.table("sku_metadata").update(update).eq("sku", target).execute()
    return "written"


def keep_screen(code: str, screen: str, parsed: dict, client=None) -> bool:
    """Keep the screen as read, beside what was taken from it (Rafael, 13 Nov 2026:
    «estudiemos cada una de esas pantallas para ver si pickd está preparado para sacar
    de manera acurated la info precisa»). Only the parsed values used to be kept, so a
    misread (`Status` as a bin, `:` as a vendor) could not be checked against the
    screen nor fixed by parsing it again. `as400_screens`, `classified = 'stock_inquiry'`.
    Same switch as every other write; a failure is logged, never fatal."""
    if not _writes_enabled():
        return False
    try:
        _client(client).table("as400_screens").insert(
            {
                "sku": code,
                "classified": "stock_inquiry",
                "after": json.dumps(parsed, default=str),
                "raw": screen,
            }
        ).execute()
        return True
    except Exception as e:  # noqa: BLE001 — keeping the screen must not stop the read
        log.warning("stock reread %s: screen not kept (%s)", code, e)
        return False


# ── one step, in the gap loop ────────────────────────────────────────────────


def run_step(driver, code: str, *, on_search_screen=False, client=None, write_fn=None) -> dict:
    """Read one code and write it. Same result shape as `run_sku_step`:
    {"action", "sku", "returned", "on_search", ...}."""
    cfg = config(client)
    rev = str(cfg.get("rev") or "")
    home = (cfg.get("home") or {}).get(code)
    write_fn = write_fn or write
    result: dict
    try:
        screen = (
            capture_stock_inquiry(code, driver, on_search_screen=True)
            if on_search_screen
            else capture_stock_inquiry(code, driver)
        )
        parsed = parse_stock_inquiry(screen)
        keep_screen(code, screen, parsed, client)
        if parsed.get("sku") != code:
            # The colour field may eat a third letter (01-8791SPT shown as SP):
            # then the screen is somebody else's record and nothing is written.
            why = f"the screen showed {parsed.get('sku')}"
            _failed(rev, code, why)
            result = {"action": "mismatch", "sku": code, "why": why}
        else:
            done = write_fn(code, home, parsed, rev)
            _finish(rev, code, "read")
            log.info("stock reread %s: on_hand=%s (%s)", code, parsed.get("on_hand"), done)
            result = {"action": "read", "sku": code, "parsed": parsed}
    except StockSkuNotFound as e:
        write_fn(code, home, None, rev)
        _finish(rev, code, "not in AS400")
        log.info("stock reread %s: AS400 has no such number", code)
        result = {"action": "unknown", "sku": code, "why": str(e)}
    except StockScreenMismatch as e:
        _failed(rev, code, str(e))
        result = {"action": "mismatch", "sku": code, "why": str(e)}
    except (AS400Disconnected, AS400ManualLoginRequired) as e:
        return {"action": "unavailable", "sku": code, "why": str(e), "returned": None}
    except CaptureError as e:
        _failed(rev, code, str(e))
        result = {"action": "error", "sku": code, "why": str(e)}
    except Exception as e:  # noqa: BLE001 — a write that fails is counted, not fatal
        _failed(rev, code, f"{type(e).__name__}: {e}")
        result = {"action": "error", "sku": code, "why": f"{type(e).__name__}: {e}"}

    # Same return rule as run_sku_step: a clean read goes back to the search form
    # (Cmd7) so the next code is typed where we stand; anything else leaves the
    # screen alone and the caller walks the verified way.
    if result["action"] == "read":
        try:
            return_to_search(driver)
            result["returned"] = True
            result["on_search"] = True
        except Exception as e:  # noqa: BLE001
            log.warning("stock reread %s: could not get back to the search (%s)", code, e)
            result["returned"] = False
            result["on_search"] = False
    else:
        result["returned"] = None
        result["on_search"] = False
    return result
