"""
Tests for migrations.apply_migrations — the idempotent schema runner wired into
the Update button. psycopg2 is mocked, so no real DB connection is made.
"""

import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import migrations  # noqa: E402


def test_skips_cleanly_when_no_db_url(monkeypatch):
    monkeypatch.setattr(migrations, "SUPABASE_DB_URL", "")
    result = migrations.apply_migrations()
    assert result["status"] == "skipped"
    assert "SUPABASE_DB_URL" in result["reason"]


def test_applies_every_migration_when_db_url_set(monkeypatch):
    monkeypatch.setattr(migrations, "SUPABASE_DB_URL", "postgresql://x")
    fake_cur = MagicMock()
    fake_conn = MagicMock()
    fake_conn.cursor.return_value.__enter__.return_value = fake_cur
    fake_psycopg2 = MagicMock()
    fake_psycopg2.connect.return_value = fake_conn

    with patch.dict(sys.modules, {"psycopg2": fake_psycopg2}):
        result = migrations.apply_migrations()

    assert result["status"] == "applied"
    # Every declared migration ran exactly once, and the connection autocommits
    # (no half-applied DDL) and is closed.
    assert result["applied"] == [name for name, _ in migrations.MIGRATIONS]
    assert fake_cur.execute.call_count == len(migrations.MIGRATIONS)
    assert fake_conn.autocommit is True
    fake_conn.close.assert_called_once()


def test_db_error_is_non_fatal(monkeypatch):
    monkeypatch.setattr(migrations, "SUPABASE_DB_URL", "postgresql://x")
    fake_psycopg2 = MagicMock()
    fake_psycopg2.connect.side_effect = Exception("connection refused")

    with patch.dict(sys.modules, {"psycopg2": fake_psycopg2}):
        result = migrations.apply_migrations()

    # Never raises — the update flow treats this as a non-fatal warning.
    assert result["status"] == "error"
    assert "connection refused" in result["error"]


def test_source_order_date_is_in_the_migration_set():
    # The whole point: the column the watcher writes must be ensured here.
    names = [name for name, _ in migrations.MIGRATIONS]
    assert "picking_lists.source_order_date" in names
    ddl = dict(migrations.MIGRATIONS)["picking_lists.source_order_date"]
    assert "IF NOT EXISTS" in ddl  # idempotent — safe to run on every update


def test_fedex_recipient_columns_are_in_the_migration_set():
    # Every column the watcher writes for the AS400 account / FedEx key must be
    # ensured here too, so an update ahead of Pickd's migration never writes into
    # a missing column. Column-only: the CHECKs, the unique index and the trigger
    # that derives fedex_recipient_id belong to Pickd's migration, not here.
    ddl = dict(migrations.MIGRATIONS)
    for name in (
        "customers.as400_account",
        "customers.ship_to_varies",
        "customer_addresses.as400_ship_to",
        "customer_addresses.fedex_recipient_id",
        "picking_lists.as400_account_number",
        "picking_lists.ship_to_address_id",
    ):
        assert name in ddl, name
        assert "IF NOT EXISTS" in ddl[name]  # idempotent — safe to run on every update
        assert "TRIGGER" not in ddl[name].upper()
        assert "CONSTRAINT" not in ddl[name].upper()
