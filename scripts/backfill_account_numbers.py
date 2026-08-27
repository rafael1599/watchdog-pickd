"""
backfill_account_numbers.py — CLI for the "Backfill AS400 accounts" maintenance
action. The same thing is one click away in the UI (⋯ → Maintenance → Preview /
Apply); this exists for a terminal session on the watcher machine.

Seals the AS400 account and ship-to onto the orders captured before the watcher
kept them, from .scanned_orders.json (the AS400 is never driven). Dry-run by
default; --apply writes. Safe to re-run: every write fills a NULL and leaves
anything already set alone. The logic lives in maintenance.py.

    ./venv/bin/python3 scripts/backfill_account_numbers.py          # dry run
    ./venv/bin/python3 scripts/backfill_account_numbers.py --apply
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import scanned_store  # noqa: E402
from maintenance import COUNT_LABELS, backfill_account_numbers  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--apply", action="store_true", help="write to Pickd (default: dry run)")
    apply = ap.parse_args().apply
    print(f"{'APPLY' if apply else 'DRY RUN'} — scan cache: {scanned_store._path()}")
    result = backfill_account_numbers(apply)
    for line in result["lines"]:
        print(f"  {line}")
    if result["truncated"]:
        print(f"  … and {result['truncated']} more")
    would = "" if apply else " (would be)"
    counts = result["counts"]
    print()
    for key, label in COUNT_LABELS.items():
        suffix = would if key in ("headers", "accounts", "addresses", "links") else ""
        print(f"{label}{suffix}: {counts.get(key, 0)}")
    return 1 if counts.get("errors") else 0


if __name__ == "__main__":
    sys.exit(main())
