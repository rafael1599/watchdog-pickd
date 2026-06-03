"""
watcher.py — Folder watcher daemon for the PDF-to-order automation.

Monitors ~/send-to-pickd/ for new PDF files and processes them:
1. Extract text from PDF (pdfplumber)
2. Parse order data (order number, customer, items)
3. Check for duplicates (SHA-256 hash)
4. Insert/append/reopen order in Supabase
5. Move PDF to processed/ or errors/

Usage: python3 watcher.py
"""

import logging
import os
import plistlib
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from extractor import extract_text
from pipeline import process_order_text

load_dotenv()

# Configuration
WATCH_FOLDER = os.path.expanduser(os.getenv("WATCH_FOLDER", "~/send-to-pickd"))
PROCESSED_FOLDER = os.path.join(WATCH_FOLDER, "processed")
ERRORS_FOLDER = os.path.join(WATCH_FOLDER, "errors")

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pickd-watcher")


def ensure_folders():
    """Create watch, processed, and errors folders if they don't exist."""
    for folder in [WATCH_FOLDER, PROCESSED_FOLDER, ERRORS_FOLDER]:
        os.makedirs(folder, exist_ok=True)


def move_file(src: str, dest_folder: str):
    """Move file to destination folder, adding timestamp to avoid collisions."""
    base = os.path.basename(src)
    name, ext = os.path.splitext(base)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = os.path.join(dest_folder, f"{name}_{timestamp}{ext}")
    shutil.move(src, dest)
    return dest


def process_pdf(pdf_path: str):
    """
    Main processing pipeline for a single PDF file.
    """
    file_name = os.path.basename(pdf_path)
    log.info(f"📄 Processing: {file_name}")

    try:
        # 1. Extract text from the PDF
        text = extract_text(pdf_path)

        # 2. Run the shared ingestion pipeline (parse → Supabase)
        result = process_order_text(text, source_name=file_name)
        status = result["status"]

        # 3. Route the file based on the outcome
        if status == "empty_text":
            log.warning("   ⚠️  Could not extract text from PDF. Moving to errors/")
            move_file(pdf_path, ERRORS_FOLDER)
            return
        if status == "no_items":
            log.warning("   ⚠️  No items found in PDF. Moving to errors/")
            move_file(pdf_path, ERRORS_FOLDER)
            return
        if status == "duplicate":
            log.warning(f"   ⚠️  DUPLICATE: {result['message']} Skipping.")
            move_file(pdf_path, PROCESSED_FOLDER)
            return

        log.info(f"   📋 Order: #{result.get('order_number') or 'NO NUMBER'}")
        log.info(f"   👤 Customer: {result.get('customer')}")
        if result.get("needs_correction"):
            log.warning(f"   ⚠️  Order #{result.get('order_number')} set to 'needs_correction'.")
        log.info(
            f"   ✅ PROCESSED ({status}): Order #{result.get('order_number')} "
            f"({result.get('item_count')} total items)"
        )

        dest = move_file(pdf_path, PROCESSED_FOLDER)
        log.info(f"   📂 Moved to: {os.path.basename(dest)}")

    except Exception as e:
        log.error(f"   ❌ ERROR: {e}")
        try:
            move_file(pdf_path, ERRORS_FOLDER)
            log.info("   📂 Moved to errors/")
        except Exception:
            pass


class PDFHandler(FileSystemEventHandler):
    """Handles new PDF files appearing in the watch folder."""

    def __init__(self):
        super().__init__()
        self._processing = set()

    def on_created(self, event):
        if event.is_directory:
            return

        path = event.src_path
        if not path.lower().endswith(".pdf"):
            return

        # Skip files in subfolders (processed/, errors/)
        parent = os.path.dirname(path)
        if parent != WATCH_FOLDER:
            return

        # Avoid double-processing
        if path in self._processing:
            return
        self._processing.add(path)

        # Small delay to ensure file is fully written
        time.sleep(1)

        try:
            process_pdf(path)
        finally:
            self._processing.discard(path)


def process_existing_files():
    """Process any PDF files already in the watch folder at startup."""
    for file_name in sorted(os.listdir(WATCH_FOLDER)):
        if file_name.lower().endswith(".pdf"):
            pdf_path = os.path.join(WATCH_FOLDER, file_name)
            if os.path.isfile(pdf_path):
                process_pdf(pdf_path)


PLIST_LABEL = "com.antigravity.watchdog-pickd"


def install_launchd():
    """Install launchd plist so watcher starts automatically on login.
    Skips silently if already installed."""
    plist_path = Path.home() / "Library" / "LaunchAgents" / f"{PLIST_LABEL}.plist"

    if plist_path.exists():
        return  # already installed

    script_dir = Path(__file__).resolve().parent
    python_path = sys.executable
    log_dir = script_dir / "logs"
    log_dir.mkdir(exist_ok=True)

    plist = {
        "Label": PLIST_LABEL,
        "ProgramArguments": [str(python_path), str(script_dir / "watcher.py")],
        "WorkingDirectory": str(script_dir),
        "RunAtLoad": True,
        "KeepAlive": True,
        "StandardOutPath": str(log_dir / "stdout.log"),
        "StandardErrorPath": str(log_dir / "stderr.log"),
        "EnvironmentVariables": {
            "PATH": os.environ.get("PATH", "/usr/bin:/usr/local/bin"),
        },
    }

    plist_path.parent.mkdir(parents=True, exist_ok=True)
    with open(plist_path, "wb") as f:
        plistlib.dump(plist, f)

    subprocess.run(["launchctl", "load", str(plist_path)], check=False)
    log.info(f"✅ Auto-start installed: {plist_path}")


def main():
    ensure_folders()
    install_launchd()

    log.info("=" * 60)
    log.info("🚀 PickD Watcher v1.0")
    log.info(f"📂 Watching: {WATCH_FOLDER}")
    log.info(f"📦 Processed → {PROCESSED_FOLDER}")
    log.info(f"❌ Errors    → {ERRORS_FOLDER}")
    log.info("=" * 60)

    # Process any existing files first
    process_existing_files()

    # Start watching
    handler = PDFHandler()
    observer = Observer()
    observer.schedule(handler, WATCH_FOLDER, recursive=False)
    observer.start()

    log.info("👀 Watching for new PDFs... (Ctrl+C to stop)")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("🛑 Stopping watcher...")
        observer.stop()

    observer.join()
    log.info("👋 Bye!")


if __name__ == "__main__":
    main()
