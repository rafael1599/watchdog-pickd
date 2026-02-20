
import os
import shutil
import logging
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

# Configuration
WATCH_FOLDER = os.path.expanduser(os.getenv("WATCH_FOLDER", "~/send-to-pickd"))
PROCESSED_FOLDER = os.path.join(WATCH_FOLDER, "processed")
ERRORS_FOLDER = os.path.join(WATCH_FOLDER, "errors")
TEST_ORDER_NUMBER = "878279"

# Supabase setup
SUPABASE_URL = os.getenv("SUPABASE_URL", "http://localhost:54321")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY")

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger("cleanup")

def cleanup_files():
    log.info(f"🧹 [LOCAL] Cleaning up files in {WATCH_FOLDER}...")
    for folder in [WATCH_FOLDER, PROCESSED_FOLDER, ERRORS_FOLDER]:
        if not os.path.exists(folder):
            continue
        for file_name in os.listdir(folder):
            file_path = os.path.join(folder, file_name)
            if os.path.isfile(file_path) and file_name.lower().endswith(".pdf"):
                try:
                    os.remove(file_path)
                    log.info(f"   🗑️  Deleted: {file_name}")
                except Exception as e:
                    log.error(f"   ❌ Error deleting {file_name}: {e}")

def cleanup_database():
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.error("❌ Supabase credentials missing (SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY). Skipping DB cleanup.")
        return

    log.info(f"🧹 [LOCAL DB] Cleaning up Order #{TEST_ORDER_NUMBER} at {SUPABASE_URL}...")
    
    try:
        client: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        # 1. Delete from pdf_import_log
        log_res = client.table("pdf_import_log").delete().eq("order_number", TEST_ORDER_NUMBER).execute()
        log.info(f"   ✅ Deleted {len(log_res.data)} entries from pdf_import_log")
        
        # 2. Delete from picking_lists
        list_res = client.table("picking_lists").delete().eq("order_number", TEST_ORDER_NUMBER).execute()
        log.info(f"   ✅ Deleted {len(list_res.data)} entries from picking_lists")
        
    except Exception as e:
        log.error(f"   ❌ Error during database cleanup: {e}")

def main():
    cleanup_files()
    cleanup_database()
    log.info("✨ Cleanup complete!")

if __name__ == "__main__":
    main()
