import pandas as pd
from sqlalchemy import create_engine, text
import os
import logging
from dotenv import load_dotenv

# --- 1. CONFIGURATION & SETUP ---

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Load environment variables
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.join(SCRIPT_DIR, '..')
load_dotenv(os.path.join(ROOT_DIR, '.env'))

# Paths
CSV_PATH = os.path.join(ROOT_DIR, 'data')

# Database Credentials (Source DB)
try:
    DB_USER = os.environ["SOURCE_POSTGRES_USER"]
    DB_PASS = os.environ["SOURCE_POSTGRES_PASSWORD"]
    DB_NAME = os.environ["SOURCE_POSTGRES_DB"]
except KeyError as e:
    raise RuntimeError(f"CONFIGURATION ERROR: Missing environment variable {e}. Check your .env file!")

DB_HOST = "localhost"
DB_PORT = "5433"


DB_CONNECTION_STR = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

# Mapping: CSV Filename -> Database Table Name
# Must match the logic used in 'simulate_production.py'
FILES_TO_TABLES = {
    'olist_customers_dataset.csv': 'olist_customers_dataset',
    'olist_geolocation_dataset.csv': 'olist_geo_dataset',  # Note: 'geo' shortened in simulation script
    'olist_orders_dataset.csv': 'olist_orders_dataset',
    'olist_order_items_dataset.csv': 'olist_order_items_dataset',
    'olist_order_payments_dataset.csv': 'olist_order_payments_dataset',
    'olist_order_reviews_dataset.csv': 'olist_order_reviews_dataset',
    'olist_products_dataset.csv': 'olist_products_dataset',
    'olist_sellers_dataset.csv': 'olist_sellers_dataset'
}


# --- 2. HELPER FUNCTIONS ---

def get_db_row_count(engine, table_name):
    """
    Executes a SELECT COUNT(*) query on the specified table.
    Returns -1 if the table does not exist or an error occurs.
    """
    try:
        with engine.connect() as conn:
            query = text(f"SELECT COUNT(*) FROM {table_name}")
            result = conn.execute(query).scalar()
            return result
    except Exception as e:
        logger.debug(f"Could not count rows for {table_name}: {e}")
        return -1


def run_full_validation():
    """
    Compares row counts between source CSV files and Postgres tables.
    Prints a formatted report to the console.
    """
    logger.info("Starting Data Integrity Validation...")

    try:
        engine = create_engine(DB_CONNECTION_STR)
    except Exception as e:
        logger.critical(f"Failed to connect to database: {e}")
        return

    # Print Report Header
    print("\n" + "=" * 80)
    print(f" FINAL DATA INTEGRITY CHECK (CSV vs POSTGRES) ")
    print("=" * 80)
    print(f"{'Table Name':<35} | {'CSV Rows':<10} | {'DB Rows':<10} | {'Diff':<6} | {'Status'}")
    print("-" * 80)

    all_passed = True

    for filename, table_name in FILES_TO_TABLES.items():
        # 1. Count CSV lines
        file_path = os.path.join(CSV_PATH, filename)

        if not os.path.exists(file_path):
            print(f"{table_name:<35} | {'MISSING':<10} | {'---':<10} | {'---':<6} | FILE NOT FOUND")
            all_passed = False
            continue

        try:
            # Optimize: Load only one column to speed up counting
            df = pd.read_csv(file_path, usecols=[0])
            csv_count = len(df)
        except Exception as e:
            logger.error(f"Error reading {filename}: {e}")
            csv_count = 0

        # 2. Count DB rows
        db_count = get_db_row_count(engine, table_name)

        # 3. Compare Results
        if db_count == -1:
            diff = "N/A"
            status = "TABLE MISSING"
            all_passed = False
        else:
            diff = db_count - csv_count
            if diff == 0:
                status = "OK"
            else:
                status = "MISMATCH"
                all_passed = False

        # Print Report Row
        # Using print() here intentionally for formatted output, logging for background info
        print(f"{table_name:<35} | {csv_count:<10} | {db_count:<10} | {diff:<6} | {status}")

    print("=" * 80)

    if all_passed:
        logger.info("SUCCESS: All data in Database matches the Source CSVs.")
        print("\nVALIDATION PASSED: Data Integrity Confirmed.\n")
    else:
        logger.error("FAILURE: There are data discrepancies.")
        print("\nVALIDATION FAILED: Check the mismatches above.\n")


if __name__ == "__main__":
    run_full_validation()