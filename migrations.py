"""
migrations.py — Apply the database schema the watcher depends on.

The watcher writes columns into PickD's shared `picking_lists` table (e.g.
`source_order_date`). PostgREST silently DROPS unknown columns on insert, so if a
column hasn't been added yet the data just vanishes — no error. To make the
watcher self-sufficient, this runner applies the required schema directly.

DDL can't go through the service-role key (PostgREST exposes no DDL), so we open
a direct Postgres connection via `SUPABASE_DB_URL`. Every statement is idempotent
(`ADD COLUMN IF NOT EXISTS`), so running it repeatedly — e.g. on every app update
— is safe and a no-op once applied. It also coexists with PickD's own migration:
both use `IF NOT EXISTS`, so whichever runs first wins and the other no-ops.

Wired into `scripts/update.sh` (after the git pull), so clicking "⟳ Update app"
in the UI applies whatever schema shipped with the new code. If `SUPABASE_DB_URL`
is unset it SKIPS cleanly (never fails the update).

Run standalone:  ./venv/bin/python3 migrations.py
"""

import logging
import os

from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

# Direct Postgres connection string (NOT the service-role key — that's for
# PostgREST). Copy it from Supabase → Project Settings → Database → Connection
# string (URI). Leave unset to skip migrations entirely.
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL", "")

# Idempotent DDL the watcher relies on. Order matters; keep each statement safe to
# re-run. Add new required columns here as the watcher starts writing them.
MIGRATIONS: list[tuple[str, str]] = [
    (
        "picking_lists.source_order_date",
        """
        ALTER TABLE picking_lists
            ADD COLUMN IF NOT EXISTS source_order_date date;
        COMMENT ON COLUMN picking_lists.source_order_date IS
            'AS400 document Order Date (ISO yyyy-mm-dd) captured at import. NULL when unknown.';
        """,
    ),
]


def apply_migrations() -> dict:
    """Apply MIGRATIONS over a direct Postgres connection. Idempotent.

    Returns a summary dict: {"status": "applied"|"skipped"|"error", ...}. Never
    raises — the caller (update flow) treats a failure as non-fatal so a DB hiccup
    can't block the app from updating.
    """
    if not SUPABASE_DB_URL:
        log.info("SUPABASE_DB_URL not set — skipping schema migrations.")
        return {"status": "skipped", "reason": "SUPABASE_DB_URL not set"}

    try:
        import psycopg2
    except ImportError:
        log.warning("psycopg2 not installed — skipping schema migrations.")
        return {"status": "skipped", "reason": "psycopg2 not installed"}

    applied: list[str] = []
    try:
        conn = psycopg2.connect(SUPABASE_DB_URL)
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                for name, ddl in MIGRATIONS:
                    cur.execute(ddl)
                    applied.append(name)
                    log.info("✓ migration applied: %s", name)
        finally:
            conn.close()
    except Exception as e:  # noqa: BLE001 — surfaced to caller, never re-raised
        log.warning("Schema migration failed: %s", e)
        return {"status": "error", "error": str(e), "applied": applied}

    return {"status": "applied", "applied": applied}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    result = apply_migrations()
    if result["status"] == "applied":
        print(f"✅ Schema up to date ({len(result['applied'])} statement(s)).")
    elif result["status"] == "skipped":
        print(f"• Migrations skipped: {result['reason']}.")
    else:
        print(f"⚠ Migration error (non-fatal): {result['error']}")
