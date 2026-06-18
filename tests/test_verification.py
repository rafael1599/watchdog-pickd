"""
Tests for the read-only verification-board mirror:
  - supabase_client.get_verification_count: which statuses count (and don't).
  - the /api/verification endpoint returns {count, board} with Supabase mocked.

No real network is hit: get_client (and the reader functions for the endpoint)
are monkeypatched, following the project's mock conventions.
"""

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import app as appmod  # noqa: E402

HDR = {"Host": f"localhost:{appmod.PORT}"}  # satisfy the loopback-only guard


# ---------- get_verification_count (statuses included/excluded) --------------


def test_verification_count_uses_in_filter_and_count():
    from supabase_client import VERIFICATION_STATUSES, get_verification_count

    mock_table = MagicMock()
    chain = mock_table.select.return_value.in_.return_value
    chain.execute.return_value = MagicMock(count=4, data=None)

    with patch("supabase_client.get_client") as mock_client:
        mock_client.return_value.table.return_value = mock_table
        assert get_verification_count() == 4

    # The query must filter on exactly the in-verification statuses and exclude
    # the terminal ones.
    in_args = mock_table.select.return_value.in_.call_args
    assert in_args[0][0] == "status"
    statuses = in_args[0][1]
    assert set(statuses) == set(VERIFICATION_STATUSES)
    for excluded in ("completed", "cancelled"):
        assert excluded not in statuses
    for included in ("active", "needs_correction", "reopened"):
        assert included in statuses


def test_verification_count_falls_back_to_len_without_count():
    from supabase_client import get_verification_count

    mock_table = MagicMock()
    chain = mock_table.select.return_value.in_.return_value
    chain.execute.return_value = MagicMock(count=None, data=[{"id": 1}, {"id": 2}])

    with patch("supabase_client.get_client") as mock_client:
        mock_client.return_value.table.return_value = mock_table
        assert get_verification_count() == 2


# ---------- /api/verification endpoint ---------------------------------------


@pytest.fixture
def client():
    appmod.app.testing = True
    # Reset the throttle cache between tests so the mock is actually consulted.
    appmod._verification_cache["ts"] = 0.0
    appmod._verification_cache["data"] = None
    return appmod.app.test_client()


def test_api_verification_returns_count_and_board(client, monkeypatch):
    board = {
        "needs_correction": [
            {
                "order_number": "880009",
                "customer": "BIKES AND MORE",
                "status": "needs_correction",
                "shipping_type": "fedex",
                "items": 3,
            }
        ]
    }
    monkeypatch.setattr(appmod, "get_verification_count", lambda: 7)
    monkeypatch.setattr(appmod, "get_verification_board", lambda: board)

    r = client.get("/api/verification", headers=HDR)
    assert r.status_code == 200
    data = r.get_json()
    assert data["count"] == 7
    assert data["board"] == board


def test_api_verification_survives_supabase_error(client, monkeypatch):
    def boom():
        raise RuntimeError("no supabase env")

    monkeypatch.setattr(appmod, "get_verification_count", boom)
    monkeypatch.setattr(appmod, "get_verification_board", dict)

    r = client.get("/api/verification", headers=HDR)
    assert r.status_code == 200
    data = r.get_json()
    assert data["count"] == 0
    assert data["board"] == {}
