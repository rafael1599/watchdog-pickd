"""
INDEX_HTML is a Python triple-quoted string, so a single-backslash escape written
into the JS ("join('\\n')" in the file) is EATEN BY PYTHON and served as a real
newline inside a JS string — a SyntaxError that kills the whole script block and
every button with it (2026-08-28: the UI was a dead static page for a day).
These tests run against the EVALUATED string — what the browser actually gets —
never against the raw source, which is exactly how the bug hid from node --check.
"""

import os
import re
import shutil
import subprocess
import sys
import tempfile

import jinja2
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import app as web  # noqa: E402


def _served_script() -> str:
    html = jinja2.Template(web.INDEX_HTML).render(version="test")
    blocks = re.findall(r"<script>(.*?)</script>", html, re.S)
    assert len(blocks) == 1, "the page has exactly one inline script block"
    return blocks[0]


def test_js_strings_hold_no_raw_newline():
    """A ' or " string that crosses a real newline = an escape Python ate. Template
    literals may span lines; // and /* */ comments are skipped outside strings."""
    script = _served_script()
    line, opened, i = 1, None, 0
    while i < len(script):
        ch = script[i]
        nxt = script[i + 1] if i + 1 < len(script) else ""
        if ch == "\n":
            assert opened not in ("'", '"'), f"line {line}: {opened}-string crosses a newline"
            line += 1
        elif ch == "\\":
            i += 2
            continue
        elif opened is None:
            if ch == "/" and nxt == "/":
                i = script.find("\n", i)
                i = len(script) if i == -1 else i
                continue
            if ch == "/" and nxt == "*":
                j = script.find("*/", i + 2)
                seg = script[i : j if j != -1 else len(script)]
                line += seg.count("\n")
                i = len(script) if j == -1 else j + 2
                continue
            if ch in ("'", '"', "`"):
                opened = ch
        elif ch == opened:
            opened = None
        i += 1


def test_the_newline_escape_survives_python():
    assert "join('\\n')" in _served_script()


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_served_script_parses_with_node():
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as f:
        f.write(_served_script())
    try:
        r = subprocess.run(["node", "--check", f.name], capture_output=True, text=True)
        assert r.returncode == 0, r.stderr[:800]
    finally:
        os.unlink(f.name)
