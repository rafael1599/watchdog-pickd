"""
customer_enrichment.py — The dealer's phone and e-mail from AS400, in the gaps
between orders, and the map of where the CONTACT lives.

Rafael, 29 sep 2026: «empieza con la fase de los datos alcanzables, luego tienes
que mandar al watcher a explorar cada opción para descubrir el mapa completo. Lo
que queremos es lo que dice contact en esta orden». The printed pack slip of
881753 carries `TELEPHONE (201) 891-5500` and `CONTACT MICHAEL PORRARO-OWNER`;
ORDER INQUIRY shows neither, and nobody has found the screen that does.

Two jobs, same rules as the SKU queue (sku_enrichment.py), which is the
precedent this copies on purpose:

  1. The reachable data (docs/customer-enrichment.md): CUSTOMER DISPLAY has
     `Phone No` and `EMAIL Address`. One account at a time, inside the scanner's
     not-found gap, under the same `capture_lock` and operator-idle gate. The
     account on screen must be the one asked for, or nothing is written; and a
     value already in PickD is never overwritten.
  2. The expedition: once per `explore_rev`, open every function key CUSTOMER
     DISPLAY advertises and the read-only menu options nobody has opened (04, 06,
     10), and keep each screen raw in `as400_screens`. Reading those is how the
     CONTACT gets found; the code does not guess where it is.

Switched on from PickD, not from Bay 2's .env: `app_flags` row
`as400_customer_enrich` (`enabled`, `config.write`, `config.explore`,
`config.explore_rev`, `config.explore_account`). An env var, when set, wins —
`CUSTOMER_ENRICH=0` is the kill switch on the Mac itself.
"""

from __future__ import annotations  # "dict | None" hints on Python 3.9 (Bay 2 Mac)

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from as400_capture import (
    AS400Disconnected,
    AS400ManualLoginRequired,
    CaptureError,
    capture_customer_display,
    enter_menu_option,
    return_to_order_search,
)
from parser import mask_bank_numbers, parse_customer_display

log = logging.getLogger("pickd-customer-enrichment")

FLAG_KEY = "as400_customer_enrich"
FLAG_TTL_SEC = 60.0

# Every key CUSTOMER DISPLAY advertises except Cmd7 EXIT (the way out, already
# known) and Cmd11 Commit: «Commit» is the one legend that reads like an action,
# and a key that commits something in the ERP is not a thing to press to see what
# happens. ❓ Rafael can add it with `config.explore_keys` once he has looked.
EXPLORE_KEYS_DEFAULT = ("f1", "f2", "f3", "f4", "f5", "f6", "f10", "f12")
# 01, 02 and 03 are mapped; 07 changes the terminal and 09 writes; 06 (spool
# control) and 10 (pick slip UPDATE) were opened once and struck off.
EXPLORE_MENU_DEFAULT = ("4",)
# 881753's customer — the one whose printed CONTACT we know, so a screen that
# carries it is recognisable by the name MICHAEL PORRARO.
EXPLORE_ACCOUNT_DEFAULT = "6034"

_lock = threading.Lock()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _client(client=None):
    if client is not None:
        return client
    from supabase_client import get_client

    return get_client()


# ── the switch, from PickD ───────────────────────────────────────────────────
_flag_cache: dict = {"at": 0.0, "row": None}


def _flag(client=None) -> dict:
    """The `app_flags` row, re-read at most once a minute. No row, or no way to
    read it, is OFF: a feature that drives the terminal starts only when asked."""
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
        log.warning("customer enrich: could not read app_flags (%s) — treating as off", e)
    _flag_cache.update(at=now, row=row)
    return row


def _config(client=None) -> dict:
    cfg = _flag(client).get("config")
    return cfg if isinstance(cfg, dict) else {}


def _env_bool(name: str):
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return None
    return raw.strip() in ("1", "true", "True", "yes")


def enabled(client=None) -> bool:
    env = _env_bool("CUSTOMER_ENRICH")
    return env if env is not None else bool(_flag(client).get("enabled"))


def writes_enabled(client=None) -> bool:
    """E2's switch. Off: the step reads, logs what it would write, and writes nothing."""
    env = _env_bool("CUSTOMER_ENRICH_WRITE")
    return env if env is not None else bool(_config(client).get("write"))


def explore_enabled(client=None) -> bool:
    env = _env_bool("CUSTOMER_EXPLORE")
    return env if env is not None else bool(_config(client).get("explore"))


def gap_budget_sec(client=None) -> float:
    return max(0.0, float(_config(client).get("gap_budget_sec", 300)))


def max_per_gap(client=None) -> int:
    return max(1, int(_config(client).get("max_per_gap", 10)))


# ── what has been read, so a read-only phase does not ask twice ──────────────


def _seen_path() -> Path:
    return Path(
        os.getenv(
            "CUSTOMER_SEEN_PATH",
            str(Path(__file__).resolve().parent / ".customer_seen.json"),
        )
    )


def load_seen() -> dict:
    try:
        p = _seen_path()
        if p.exists():
            data = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
    except Exception as e:  # noqa: BLE001
        log.warning("customer enrich: could not read the seen list: %s", e)
    return {}


MAX_FAILED_TRIES = 3


def remember(key: str, entry: dict) -> None:
    with _lock:
        data = load_seen()
        tries = (data.get(key) or {}).get("tries", 0) + 1
        data[key] = {**entry, "tries": tries, "at": _now()}
        p = _seen_path()
        tmp = p.with_suffix(p.suffix + ".tmp")
        # Masked as a whole: entries written before the masking existed (the
        # first reads of 29 sep) are cleaned by the next write, not kept for ever.
        tmp.write_text(
            mask_bank_numbers(json.dumps(data, ensure_ascii=False, indent=2)), encoding="utf-8"
        )
        os.replace(tmp, p)


def account_key(account, suffix="00") -> str:
    return f"{str(account).strip()}-{str(suffix or '00').strip().zfill(2)}"


# ── the queue ────────────────────────────────────────────────────────────────


def rank_customers(customers, orders, seen=None, today=None) -> list:
    """Who to read next, pure so it is testable.

    `customers`: rows with id, as400_account, ship_to_varies, phone, email.
    `orders`: picking_lists rows with customer_id and created_at (last 90 days).
    Order (docs/customer-enrichment.md §5): customers with an order today first,
    then by how many orders they placed in 90 days. Only accounts AS400 can be
    asked for, never `ship_to_varies` (end consumer, eBay, warranty), never one
    already read, and never one that has both phone and e-mail already.
    """
    seen = seen or {}
    today = today or datetime.now(timezone.utc).date().isoformat()
    count: dict = {}
    has_today: set = set()
    for o in orders or []:
        cid = o.get("customer_id")
        if not cid:
            continue
        count[cid] = count.get(cid, 0) + 1
        if str(o.get("created_at") or "")[:10] == today:
            has_today.add(cid)
    out = []
    for c in customers or []:
        acct = str(c.get("as400_account") or "").strip()
        if not acct.isdigit() or c.get("ship_to_varies"):
            continue
        if c.get("phone") and c.get("email"):
            continue
        prior = seen.get(account_key(acct))
        # A read is final (❓2: no refresh). A failure is retried a few times —
        # the first run on Bay 2 failed on every account because of how the
        # number was typed, and that must not bury them for ever.
        if prior and (
            prior.get("action") in ("read", "written") or prior.get("tries", 1) >= MAX_FAILED_TRIES
        ):
            continue
        out.append(c)
    out.sort(key=lambda c: (c["id"] not in has_today, -count.get(c["id"], 0), c.get("name") or ""))
    return out


def fetch_queue(client=None) -> list:
    cl = _client(client)
    customers = (
        cl.table("customers")
        .select("id, name, as400_account, ship_to_varies, phone, email")
        .limit(5000)
        .execute()
        .data
        or []
    )
    # `Z`, never `+00:00`: the `+` travels unencoded in the query string, PostgREST
    # reads it as a space and rejects the filter — the first run on Bay 2 died
    # here, before a single customer (29 sep 2026).
    since = datetime.fromtimestamp(time.time() - 90 * 86400, timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    orders = (
        cl.table("picking_lists")
        .select("customer_id, created_at")
        .gte("created_at", since)
        .limit(5000)
        .execute()
        .data
        or []
    )
    return rank_customers(customers, orders, load_seen())


# ── what a read would write ──────────────────────────────────────────────────


def plan_write(row: dict, parsed: dict) -> dict:
    """The columns a read fills. Pure, and it is the whole write rule:

    - the screen's account must be the one asked for, or nothing at all;
    - only an empty column is filled — a phone a person typed is never replaced;
    - `contact_name` is the Bike Buyer when it is a person (the CONTACT of the
      pack slip — 881753 ⇄ WYCKOFF, 29 sep 2026). It lives on the customer's
      addresses (`customer_addresses.contact_name`, the column FedEx reads), so
      it is planned apart, under `_addresses`, and filled only where empty.
    """
    asked = str(row.get("as400_account") or "").strip()
    got = str(parsed.get("account") or "").strip()
    if not asked or not got or int(asked) != int(got):
        return {}
    plan = {}
    if parsed.get("phone") and not (row.get("phone") or "").strip():
        plan["phone"] = parsed["phone"]
    if parsed.get("email") and not (row.get("email") or "").strip():
        plan["email"] = parsed["email"]
    if parsed.get("contact"):
        plan["_addresses"] = {"contact_name": parsed["contact"]}
    return plan


def apply_write(customer_id: str, plan: dict, client=None) -> int:
    """An update by id that only lands on columns still empty (the `is null`
    filters repeat the rule in the database, so a phone typed between the read
    and the write is not overwritten either)."""
    written = 0
    cl = _client(client)
    addresses = plan.get("_addresses") or {}
    for col, value in addresses.items():
        res = (
            cl.table("customer_addresses")
            .update({col: value})
            .eq("customer_id", customer_id)
            .is_(col, "null")
            .execute()
        )
        written += len(res.data or [])
    for col, value in plan.items():
        if col == "_addresses":
            continue
        res = (
            cl.table("customers")
            .update({col: value})
            .eq("id", customer_id)
            .is_(col, "null")
            .execute()
        )
        written += len(res.data or [])
    return written


def keep_screen(label: str, classified: str, after: str, raw: str, client=None) -> bool:
    """Every customer screen is kept, not a sample: they are the evidence for
    where the CONTACT is, and there are a few hundred at most."""
    if not raw:
        return False
    try:
        _client(client).table("as400_screens").insert(
            {"sku": label, "classified": classified, "after": after, "raw": mask_bank_numbers(raw)}
        ).execute()
        return True
    except Exception as e:  # noqa: BLE001 — looking cannot take the step down
        log.warning("customer enrich: could not keep the %s screen (%s)", classified, e)
        return False


# ── the step ─────────────────────────────────────────────────────────────────


def run_customer_step(driver, row: dict, *, capture_fn=capture_customer_display, client=None):
    """Read ONE customer's CUSTOMER DISPLAY and report. It does not walk home:
    the gap does that once, at the end (the next lookup starts from anywhere
    `capture_customer_display` accepts)."""
    acct = str(row.get("as400_account") or "").strip()
    suffix = "00"
    key = account_key(acct, suffix)
    started = time.monotonic()
    try:
        entry, screen = capture_fn(acct, suffix, driver)
    except (AS400Disconnected, AS400ManualLoginRequired) as e:
        return {"action": "unavailable", "account": acct, "why": str(e)}
    except CaptureError as e:
        if getattr(e, "screen", None):
            keep_screen(f"acct:{key}", "customer_fail", str(e), e.screen, client)
        remember(key, {"action": "mismatch", "why": str(e)})
        return {"action": "mismatch", "account": acct, "why": str(e)}
    except Exception as e:  # noqa: BLE001
        log.exception("customer %s: the lookup crashed", acct)
        return {"action": "error", "account": acct, "why": str(e)}

    keep_screen(f"acct:{key}", "customer_entry", "1+ENTER", entry, client)
    keep_screen(f"acct:{key}", "customer_display", f"{acct} TAB {suffix} ENTER", screen, client)
    parsed = parse_customer_display(screen)
    if not parsed.get("account") or int(parsed["account"]) != int(acct):
        log.warning("customer %s: the screen shows %s — not ours", acct, parsed.get("account"))
        remember(key, {"action": "mismatch", "screen_account": parsed.get("account")})
        return {"action": "mismatch", "account": acct, "why": f"screen {parsed.get('account')}"}

    log.info(
        "customer %s in %.1fs — phone=%r email=%r contact=%r bike_buyer=%r parts_buyer=%r other_buyer=%r",
        acct,
        time.monotonic() - started,
        parsed.get("phone"),
        parsed.get("email"),
        parsed.get("contact"),
        parsed.get("bike_buyer"),
        parsed.get("parts_buyer"),
        parsed.get("other_buyer"),
    )
    plan = plan_write(row, parsed)
    action = "read"
    if plan and writes_enabled(client):
        n = apply_write(row["id"], plan, client)
        action = "written"
        log.info("customer %s wrote %s (%d column(s))", acct, plan, n)
    elif plan:
        log.info("customer %s WOULD write: %s", acct, plan)
    remember(key, {"action": action, "plan": plan, "parsed": parsed})
    return {"action": action, "account": acct, "parsed": parsed, "plan": plan}


# ── the expedition ───────────────────────────────────────────────────────────


def _explored_rev_path() -> Path:
    return _seen_path().with_name(".customer_explored.json")


def explore_due(client=None):
    """The rev to run, or None. Once per `config.explore_rev`: bumping it in
    app_flags is how to send the watcher exploring again, without a deploy."""
    if not explore_enabled(client):
        return None
    rev = str(_config(client).get("explore_rev", "1"))
    try:
        done = json.loads(_explored_rev_path().read_text(encoding="utf-8")).get("rev")
    except Exception:  # noqa: BLE001
        done = None
    return None if done == rev else rev


def run_expedition(
    driver,
    *,
    account=None,
    keys=None,
    menu_options=None,
    capture_fn=capture_customer_display,
    home_fn=return_to_order_search,
    page_wait: float = 1.0,
    keep=None,
    client=None,
) -> int:
    """One key, one read, then home by the verified path — for every key and
    option. Returns how many screens it kept. Aborts the moment it cannot get
    home: an expedition may not cost the scanner its next order."""
    cfg = _config(client)
    account = str(account or cfg.get("explore_account") or EXPLORE_ACCOUNT_DEFAULT)
    # None = the configured / default list; an explicit empty tuple = none at all.
    if keys is None:
        keys = cfg.get("explore_keys") or EXPLORE_KEYS_DEFAULT
    if menu_options is None:
        menu_options = cfg.get("explore_menu") or EXPLORE_MENU_DEFAULT
    keys, menu_options = tuple(keys), tuple(menu_options)
    keep = keep or (
        lambda label, classified, after, raw: keep_screen(label, classified, after, raw, client)
    )
    label = f"acct:{account_key(account)}"
    saved = 0

    def home() -> bool:
        # The screen's own advertised way out first — every screen seen so far
        # says Cmd7 EXIT, ACCOUNTS RECEIVABLE INQUIRY says ONLY that — then the
        # verified walk. On the order search or the menu an extra F7 is harmless:
        # the walk types 3 again.
        try:
            driver.key("f7")
            time.sleep(page_wait)
        except Exception as e:  # noqa: BLE001
            log.debug("customer explore: F7 before home failed (%s)", e)
        try:
            home_fn(driver)
            return True
        except Exception as e:  # noqa: BLE001
            # The walk counts its tries, and the extra screens of a sub-page can
            # spend them with the terminal already home — Bay 2, 29 sep 2026, gave
            # up «after 3 tries» sitting on the order search. Look before quitting.
            try:
                screen = driver.copy_screen()
            except Exception:  # noqa: BLE001
                screen = ""
            from as400_capture import _READY_STATES, classify_screen

            if classify_screen(screen) in _READY_STATES:
                return True
            log.warning("customer explore: could not get home (%s) — aborting", e)
            keep(label, "explore:lost", str(e), screen)
            return False

    def keep_failure(what, e):
        screen = getattr(e, "screen", None)
        if screen:
            keep(label, f"explore:fail:{what}", str(e), screen)

    for key in keys:
        try:
            _, screen = capture_fn(account, "00", driver)
            if saved == 0 and keep(label, "explore:customer_display", "base", screen):
                saved += 1
            driver.key(key)
            time.sleep(page_wait)
            if keep(
                label,
                f"explore:customer:{key}",
                f"Cmd{key[1:]} on CUSTOMER DISPLAY",
                driver.copy_screen(),
            ):
                saved += 1
        except Exception as e:  # noqa: BLE001
            log.warning("customer explore: %s failed (%s)", key, e)
            keep_failure(key, e)
        if not home():
            return saved

    for option in menu_options:
        try:
            screen = enter_menu_option(driver, option, page_wait=page_wait)
            if keep(
                label, f"explore:menu:{int(option):02d}", f"{option}+ENTER on the menu", screen
            ):
                saved += 1
        except Exception as e:  # noqa: BLE001
            log.warning("customer explore: menu %s failed (%s)", option, e)
            keep_failure(f"menu:{int(option):02d}", e)
        if not home():
            return saved

    log.info("customer explore: %d screens kept", saved)
    return saved


def explore_if_due(driver, client=None) -> int:
    rev = explore_due(client)
    if rev is None:
        return 0
    try:
        n = run_expedition(driver, client=client)
    except Exception:  # noqa: BLE001
        log.exception("customer explore crashed — the scanner carries on")
        n = 0
    try:
        _explored_rev_path().write_text(
            json.dumps({"rev": rev, "at": _now(), "kept": n}), encoding="utf-8"
        )
    except Exception as e:  # noqa: BLE001
        log.warning("customer explore: could not note the rev (%s)", e)
    return n
