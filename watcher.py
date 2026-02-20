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

import os
import sys
import time
import shutil
import logging
from pathlib import Path
from datetime import datetime

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from dotenv import load_dotenv

from extractor import extract_text, compute_hash
from parser import parse_order
from supabase_client import (
    get_client,
    check_duplicate,
    find_existing_order,
    create_order,
    append_to_order,
    reopen_completed_order,
)

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
        # 1. Compute hash for duplicate detection
        pdf_hash = compute_hash(pdf_path)
        log.info(f"   🔑 Hash: {pdf_hash[:16]}...")

        # 2. Check for exact duplicate
        existing_log = check_duplicate(pdf_hash)
        if existing_log:
            processed_at = existing_log.get("processed_at", "unknown date")
            log.warning(
                f"   ⚠️  DUPLICATE: This exact PDF was already processed on {processed_at}. "
                f"Order #{existing_log.get('order_number', '?')}. Skipping."
            )
            move_file(pdf_path, PROCESSED_FOLDER)
            return

        # 3. Extract text
        text = extract_text(pdf_path)
        if not text or len(text.strip()) < 20:
            log.warning(f"   ⚠️  Could not extract text from PDF. Moving to errors/")
            move_file(pdf_path, ERRORS_FOLDER)
            return

        # 4. Parse order data
        order_data = parse_order(text)
        items = order_data.get("items", [])

        if not items:
            log.warning(f"   ⚠️  No items found in PDF. Moving to errors/")
            move_file(pdf_path, ERRORS_FOLDER)
            return

        order_number = order_data.get("order_number")
        customer = order_data.get("customer_name", "Unknown")
        is_last = order_data.get("is_last_page", False)

        log.info(f"   📋 Order: #{order_number or 'NO NUMBER'}")
        log.info(f"   👤 Customer: {customer}")
        log.info(f"   📦 Items: {len(items)}")
        log.info(f"   🏁 Last page: {is_last}")

        # 5. Check if order already exists in the system
        if order_number:
            existing = find_existing_order(order_number)

            if existing:
                status = existing.get("status", "")
                list_id = existing["id"]
                existing_items = existing.get("items", []) or []

                if status == "completed":
                    # ADDON: Reopen completed order
                    log.info(f"   🔄 Order #{order_number} was COMPLETED. Reopening as ADD-ON...")
                    result = reopen_completed_order(
                        list_id, existing_items, items,
                        order_number, pdf_hash, file_name
                    )
                elif status in ("active", "ready_to_double_check", "double_checking", "needs_correction"):
                    # APPEND: Add items to existing active order
                    log.info(f"   ➕ Appending to existing order #{order_number} (status: {status})...")
                    result = append_to_order(
                        list_id, existing_items, items,
                        order_number, pdf_hash, file_name
                    )
                else:
                    # Unknown status, create new
                    log.info(f"   🆕 Order #{order_number} has status '{status}'. Creating new...")
                    result = create_order(order_data, pdf_hash, file_name)
            else:
                # No existing order, create new
                result = create_order(order_data, pdf_hash, file_name)
        else:
            # No order number in PDF, create with negative number
            log.info("   ⚠️  No order number found. Generating negative number...")
            result = create_order(order_data, pdf_hash, file_name)
            # Force needs_correction for negative numbers anyway
            get_client().table("picking_lists").update({"status": "needs_correction"}).eq("id", result["id"]).execute()

        # 5b. Post-process result for warnings (Unknown SKUs or Low Stock)
        updated_items = result.get("items", [])
        has_unknown = any(i.get("sku_not_found") for i in updated_items)
        has_low_stock = any(i.get("insufficient_stock") for i in updated_items)

        if has_unknown or has_low_stock:
            msg = "Unknown SKUs" if has_unknown else "Insufficient stock"
            if has_unknown and has_low_stock: msg = "Unknown SKUs & Low stock"
            
            log.warning(f"   ⚠️  {msg} detected in Order #{result.get('order_number')}. Setting to 'needs_correction'.")
            get_client().table("picking_lists").update({"status": "needs_correction"}).eq("id", result["id"]).execute()
        
        log.info(f"   ✅ PROCESSED: Order #{result.get('order_number')} ({len(updated_items)} total items)")

        # 6. Move to processed
        dest = move_file(pdf_path, PROCESSED_FOLDER)
        log.info(f"   📂 Moved to: {os.path.basename(dest)}")

    except Exception as e:
        log.error(f"   ❌ ERROR: {e}")
        try:
            move_file(pdf_path, ERRORS_FOLDER)
            log.info(f"   📂 Moved to errors/")
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


def main():
    ensure_folders()

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
