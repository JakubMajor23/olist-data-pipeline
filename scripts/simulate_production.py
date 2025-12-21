import pandas as pd
from sqlalchemy import create_engine
import os
import logging
from dotenv import load_dotenv

# --- 1. CONFIGURATION & LOGGING ---

# Load environment variables from .env file (must be in root directory)
# We go up one level from 'scripts' to find .env
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.join(SCRIPT_DIR, '..')
load_dotenv(os.path.join(ROOT_DIR, '.env'))

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Paths
CSV_PATH = os.path.join(ROOT_DIR, 'data')

# Database Credentials (Safe & Secure)
try:
    DB_USER = os.environ["SOURCE_POSTGRES_USER"]
    DB_PASS = os.environ["SOURCE_POSTGRES_PASSWORD"]
    DB_NAME = os.environ["SOURCE_POSTGRES_DB"]
except KeyError as e:
    raise RuntimeError(f"CONFIGURATION ERROR: Missing environment variable {e}. Please ensure that the .env file exists!")

DB_PORT = "5433"  # Mapped port for Source DB
DB_HOST = "localhost"

DB_CONNECTION_STR = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

# File Mapping: Logical Name -> CSV Filename
FILES = {
    'orders': 'olist_orders_dataset.csv',
    'items': 'olist_order_items_dataset.csv',
    'payments': 'olist_order_payments_dataset.csv',
    'reviews': 'olist_order_reviews_dataset.csv',
    'customers': 'olist_customers_dataset.csv',
    'products': 'olist_products_dataset.csv',
    'sellers': 'olist_sellers_dataset.csv',
    'geolocation': 'olist_geolocation_dataset.csv'
}


# --- 2. HELPER FUNCTIONS ---

def get_engine():
    """Creates and returns a SQLAlchemy engine for the source database."""
    return create_engine(DB_CONNECTION_STR)


def load_static_data():
    """
    Loads reference data that doesn't change over time (Products, Sellers, Geo).
    Should be run once at the beginning of the simulation.
    """
    engine = get_engine()
    logger.info("--- Starting Static Data Load (Reference Data) ---")

    static_tables = ['products', 'sellers', 'geolocation']

    for key in static_tables:
        logger.info(f"Processing: {key}...")
        file_path = os.path.join(CSV_PATH, FILES[key])

        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            continue

        try:
            df = pd.read_csv(file_path)
            rows_in_csv = len(df)

            # if_exists='append': We don't drop tables (to preserve Foreign Keys), just append.
            # Ideally, this should use 'replace' only if tables are empty or constraints allow.
            table_name = f'olist_{key}_dataset'
            df.to_sql(table_name, engine, schema='public', if_exists='append', index=False, chunksize=1000)

            logger.info(f" -> [OK] {table_name}: Loaded {rows_in_csv} rows.")
        except Exception as e:
            logger.error(f" -> [ERROR] Failed to load {key}: {e}")


def load_monthly_data(year, month):
    """
    Simulates monthly transaction flow.
    Loads Orders, Items, Payments, Reviews, and Customers for a specific month.
    """
    engine = get_engine()
    logger.info(f"\n--- Simulation Started for Period: {year}-{month:02d} ---")

    # --- A. DATA PREPARATION (Filtering) ---

    # 1. Load Orders
    orders_path = os.path.join(CSV_PATH, FILES['orders'])
    df_orders = pd.read_csv(orders_path)
    df_orders['order_purchase_timestamp'] = pd.to_datetime(df_orders['order_purchase_timestamp'])

    # Filter by Year/Month
    mask = (df_orders['order_purchase_timestamp'].dt.year == year) & \
           (df_orders['order_purchase_timestamp'].dt.month == month)
    monthly_orders = df_orders[mask]

    if monthly_orders.empty:
        logger.warning(f"No orders found for {year}-{month:02d}. Skipping.")
        return

    # Get relevant IDs to filter related tables
    target_order_ids = monthly_orders['order_id'].unique()
    target_customer_ids = monthly_orders['customer_id'].unique()

    logger.info(f"Processing {len(monthly_orders)} orders...")

    # 2. Load Related Data (Filtered)
    # Customers
    df_customers = pd.read_csv(os.path.join(CSV_PATH, FILES['customers']))
    monthly_customers = df_customers[df_customers['customer_id'].isin(target_customer_ids)]

    # Items
    df_items = pd.read_csv(os.path.join(CSV_PATH, FILES['items']))
    monthly_items = df_items[df_items['order_id'].isin(target_order_ids)]

    # Payments
    df_payments = pd.read_csv(os.path.join(CSV_PATH, FILES['payments']))
    monthly_payments = df_payments[df_payments['order_id'].isin(target_order_ids)]

    # Reviews
    df_reviews = pd.read_csv(os.path.join(CSV_PATH, FILES['reviews']))
    monthly_reviews = df_reviews[df_reviews['order_id'].isin(target_order_ids)]

    counts = {
        'Customers': len(monthly_customers),
        'Orders': len(monthly_orders),
        'Items': len(monthly_items),
        'Payments': len(monthly_payments),
        'Reviews': len(monthly_reviews)
    }

    # --- B. DATABASE INGESTION ---
    # Insertion order matters due to Foreign Key constraints:
    # 1. Customers (must exist before Orders)
    # 2. Orders (must exist before Items/Payments/Reviews)
    # 3. Details (Items, Payments, Reviews)

    try:
        monthly_customers.to_sql('olist_customers_dataset', engine, schema='public', if_exists='append', index=False)
        logger.info(f" -> Added Customers: {counts['Customers']}")

        monthly_orders.to_sql('olist_orders_dataset', engine, schema='public', if_exists='append', index=False)
        logger.info(f" -> Added Orders:    {counts['Orders']}")

        monthly_items.to_sql('olist_order_items_dataset', engine, schema='public', if_exists='append', index=False)
        logger.info(f" -> Added Items:     {counts['Items']}")

        monthly_payments.to_sql('olist_order_payments_dataset', engine, schema='public', if_exists='append',
                                index=False)
        logger.info(f" -> Added Payments:  {counts['Payments']}")

        monthly_reviews.to_sql('olist_order_reviews_dataset', engine, schema='public', if_exists='append', index=False)
        logger.info(f" -> Added Reviews:   {counts['Reviews']}")

    except Exception as e:
        logger.critical(f"Database insertion failed: {e}")
        return

    logger.info("SUCCESS: Simulation completed for this month.")


if __name__ == "__main__":
    print("\n--- OLIST DATA SIMULATOR ---")
    print(" [1] Initialization (Load Products, Sellers, Geo - Run ONCE)")
    print(" [2] Simulate Specific Month")

    mode = input("Select mode: ")

    if mode == '1':
        load_static_data()
    elif mode == '2':
        try:
            y = int(input("Enter year (e.g., 2017): "))
            m = int(input("Enter month (e.g., 1): "))
            load_monthly_data(y, m)
        except ValueError:
            print("Invalid input! Please enter numbers.")
    else:
        print("Unknown option.")