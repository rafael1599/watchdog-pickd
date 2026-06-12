"""
Build identity: the UI footer and /api/version expose the running build's git
SHA so update.sh can verify a restart actually picked up the new code (an
answering server alone is not proof — a stale process or a LaunchAgent that
points at another clone serves the old UI with a 200).
"""

import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import app as appmod  # noqa: E402

HDR = {"Host": "localhost:5000"}


def test_api_version_serves_the_build_sha():
    appmod.app.testing = True
    client = appmod.app.test_client()
    r = client.get("/api/version", headers=HDR)
    assert r.status_code == 200
    v = r.get_json()["version"]
    assert v == "unknown" or re.fullmatch(r"[0-9a-f]{7,12}", v)


def test_ui_footer_shows_the_build():
    appmod.app.testing = True
    client = appmod.app.test_client()
    r = client.get("/", headers=HDR)
    assert r.status_code == 200
    assert b"build " in r.data
