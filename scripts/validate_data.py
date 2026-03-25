import logging

import pandas as pd
from sqlalchemy import create_engine, func, select, table

from db_config import ROOT_DIR, get_source_db_config




logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


CSV_PATH = ROOT_DIR / 'data'

FILES_TO_TABLES = {
    'olist_customers_dataset.csv': 'olist_customers_dataset',
    'olist_geolocation_dataset.csv': 'olist_geolocation_dataset',
    'olist_orders_dataset.csv': 'olist_orders_dataset',
    'olist_order_items_dataset.csv': 'olist_order_items_dataset',
    'olist_order_payments_dataset.csv': 'olist_order_payments_dataset',
    'olist_order_reviews_dataset.csv': 'olist_order_reviews_dataset',
    'olist_products_dataset.csv': 'olist_products_dataset',
    'olist_sellers_dataset.csv': 'olist_sellers_dataset'
}
SOURCE_SCHEMA = 'public'
ALLOWED_TABLES = frozenset(FILES_TO_TABLES.values())



def _get_source_table(table_name: str):
    if table_name not in ALLOWED_TABLES:
        raise ValueError(f"Unsupported source table requested: {table_name}")

    return table(table_name, schema=SOURCE_SCHEMA)


def get_db_row_count(engine, table_name):
    try:
        with engine.connect() as conn:
            query = select(func.count()).select_from(_get_source_table(table_name))
            result = conn.execute(query).scalar_one()
            return result
    except Exception as e:
        logger.debug(f"Could not count rows for {table_name}: {e}")
        return -1


def run_full_validation():
    logger.info("Starting Data Integrity Validation...")

    try:
        engine = create_engine(get_source_db_config().sqlalchemy_url())
    except Exception as e:
        logger.critical(f"Failed to connect to database: {e}")
        return

    print("\n" + "=" * 80)
    print(" FINAL DATA INTEGRITY CHECK (CSV vs POSTGRES) ")
    print("=" * 80)
    print(f"{'Table Name':<35} | {'CSV Rows':<10} | {'DB Rows':<10} | {'Diff':<6} | {'Status'}")
    print("-" * 80)

    all_passed = True

    for filename, table_name in FILES_TO_TABLES.items():
        file_path = CSV_PATH / filename

        if not file_path.exists():
            print(f"{table_name:<35} | {'MISSING':<10} | {'---':<10} | {'---':<6} | FILE NOT FOUND")
            all_passed = False
            continue

        try:
            df = pd.read_csv(file_path, usecols=[0])
            csv_count = len(df)
        except Exception as e:
            logger.error(f"Error reading {filename}: {e}")
            csv_count = 0

        db_count = get_db_row_count(engine, table_name)

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
