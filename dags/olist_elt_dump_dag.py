import os
import logging
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Logger setup
logger = logging.getLogger(__name__)

# --- 1. CONFIGURATION & ENVIRONMENT VARIABLES (Fail Fast Strategy) ---
try:
    # We retrieve environment variables passed via docker-compose.
    # We use os.environ[] to force a KeyError if a variable is missing.
    # This prevents connecting with default/insecure credentials.

    # Source DB Credentials
    SRC_USER = os.environ["SOURCE_POSTGRES_USER"]
    SRC_PASS = os.environ["SOURCE_POSTGRES_PASSWORD"]
    SRC_DB = os.environ["SOURCE_POSTGRES_DB"]

    # DWH DB Credentials
    DWH_USER = os.environ["POSTGRES_USER"]
    DWH_PASS = os.environ["POSTGRES_PASSWORD"]
    DWH_DB = os.environ["POSTGRES_DB"]

except KeyError as e:
    # Airflow will show this error in logs if you forget the .env file
    raise RuntimeError(f"AIRFLOW CONFIG ERROR: Missing environment variable {e}. Check your docker-compose.yml!")

# Docker Service Names (Fixed within the Docker network)
SRC_HOST = "postgres-olist-source"
DWH_HOST = "postgres-dwh"

# Connection Strings
SOURCE_DB_STR = f'postgresql://{SRC_USER}:{SRC_PASS}@{SRC_HOST}:5432/{SRC_DB}'
DWH_DB_STR = f'postgresql://{DWH_USER}:{DWH_PASS}@{DWH_HOST}:5432/{DWH_DB}'

DBT_PROJECT_DIR = '/opt/airflow/olist_dbt'

# --- 2. SQL TEMPLATES ---

SQL_INCREMENTAL_ORDERS = """
                         SELECT * \
                         FROM public.olist_orders_dataset
                         WHERE EXTRACT(YEAR FROM order_purchase_timestamp) = {year}
                           AND EXTRACT (MONTH FROM order_purchase_timestamp) = {month} \
                         """

# Helper subquery to filter related tables based on orders from the specific month
_SUBQUERY_WHERE_ORDERS = """
    WHERE order_id IN (
        SELECT order_id FROM public.olist_orders_dataset
        WHERE EXTRACT(YEAR FROM order_purchase_timestamp) = {year}
          AND EXTRACT(MONTH FROM order_purchase_timestamp) = {month}
    )
"""

SQL_INCREMENTAL_OTHERS = {
    'olist_order_items_dataset': f"SELECT * FROM public.olist_order_items_dataset {_SUBQUERY_WHERE_ORDERS}",
    'olist_order_payments_dataset': f"SELECT * FROM public.olist_order_payments_dataset {_SUBQUERY_WHERE_ORDERS}",
    'olist_order_reviews_dataset': f"SELECT * FROM public.olist_order_reviews_dataset {_SUBQUERY_WHERE_ORDERS}",
    'olist_customers_dataset': f"""
        SELECT * FROM public.olist_customers_dataset
        WHERE customer_id IN (
            SELECT customer_id FROM public.olist_orders_dataset
            WHERE EXTRACT(YEAR FROM order_purchase_timestamp) = {{year}}
              AND EXTRACT(MONTH FROM order_purchase_timestamp) = {{month}}
        )
    """
}


# --- 3. HELPER FUNCTIONS ---

def get_source_engine():
    return create_engine(SOURCE_DB_STR)


def get_dwh_engine():
    return create_engine(DWH_DB_STR)


def _cleanup_dwh_data(conn, year, month):
    """
    Removes data from DWH for the specific period to ensure Idempotency.
    Uses SAVEPOINTS (begin_nested) to safely handle 'Table doesn't exist' errors
    during the first run.
    """
    logger.info(f">> Cleaning up DWH data for {year}-{month:02d}...")

    dwh_orders_filter = f"""
        SELECT order_id FROM raw_data.olist_orders_dataset
        WHERE EXTRACT(YEAR FROM order_purchase_timestamp) = {year}
          AND EXTRACT(MONTH FROM order_purchase_timestamp) = {month}
    """

    cleanup_queries = [
        # Delete related records first (Foreign Key constraints)
        f"DELETE FROM raw_data.olist_order_items_dataset WHERE order_id IN ({dwh_orders_filter})",
        f"DELETE FROM raw_data.olist_order_payments_dataset WHERE order_id IN ({dwh_orders_filter})",
        f"DELETE FROM raw_data.olist_order_reviews_dataset WHERE order_id IN ({dwh_orders_filter})",
        f"""
        DELETE FROM raw_data.olist_customers_dataset 
        WHERE customer_id IN (
            SELECT customer_id FROM raw_data.olist_orders_dataset 
            WHERE EXTRACT(YEAR FROM order_purchase_timestamp) = {year}
              AND EXTRACT(MONTH FROM order_purchase_timestamp) = {month}
        )
        """,
        # Delete orders last
        f"""
        DELETE FROM raw_data.olist_orders_dataset
        WHERE EXTRACT(YEAR FROM order_purchase_timestamp) = {year}
          AND EXTRACT(MONTH FROM order_purchase_timestamp) = {month}
        """
    ]

    for q in cleanup_queries:
        try:
            # Create a "safe zone" (Savepoint) for each delete query.
            # If the query fails (e.g. table doesn't exist), only this part is rolled back.
            with conn.begin_nested():
                conn.execute(text(q))
        except Exception as e:
            # Ignore errors if tables don't exist yet (e.g., during the first run)
            if 'UndefinedTable' in str(e) or 'does not exist' in str(e):
                pass
            else:
                logger.warning(f"   [WARNING] Cleanup error: {e}")


def elt_load_data(**context):
    """
    Main ELT function: Extracts data from Source DB and Loads it into DWH (Raw Layer).
    Handles both static data (Full Refresh) and transactional data (Incremental).
    """
    execution_date = context['logical_date']
    year = execution_date.year
    month = execution_date.month

    logger.info(f"--- START RAW DUMP: {year}-{month:02d} ---")

    src_engine = get_source_engine()
    dwh_engine = get_dwh_engine()

    # A. STATIC DATA (Full Load / Replace)
    static_tables = ['olist_products_dataset', 'olist_sellers_dataset', 'olist_geolocation_dataset']

    logger.info(">> Processing static tables...")
    for table in static_tables:
        try:
            # Full Refresh for dimensions
            df = pd.read_sql(f"SELECT * FROM public.{table}", src_engine)
            df.to_sql(table, dwh_engine, schema='raw_data', if_exists='replace', index=False)
            logger.info(f"   [OK] {table}: loaded {len(df)} rows.")
        except Exception as e:
            # Log error but don't stop the pipeline (table might not exist in source yet)
            logger.error(f"   [ERROR] {table}: {e}")

    # B. INCREMENTAL DATA (Transactions)
    logger.info(f">> Processing transactional tables...")

    with dwh_engine.begin() as conn:
        # 1. Cleanup (Ensure Idempotency)
        _cleanup_dwh_data(conn, year, month)

        # 2. Loading Orders
        query_orders = SQL_INCREMENTAL_ORDERS.format(year=year, month=month)
        df_orders = pd.read_sql(query_orders, src_engine)

        if not df_orders.empty:
            df_orders.to_sql('olist_orders_dataset', conn, schema='raw_data', if_exists='append', index=False)
            logger.info(f"   [OK] olist_orders_dataset: appended {len(df_orders)} rows.")

            # 3. Loading Related Tables (only if orders exist)
            for table_name, query_template in SQL_INCREMENTAL_OTHERS.items():
                query = query_template.format(year=year, month=month)
                df = pd.read_sql(query, src_engine)

                if not df.empty:
                    df.to_sql(table_name, conn, schema='raw_data', if_exists='append', index=False)
                    logger.info(f"   [OK] {table_name}: appended {len(df)} rows.")
                else:
                    logger.info(f"   [INFO] {table_name}: no related data found.")
        else:
            logger.info("   [INFO] No orders found for this month. Skipping related tables.")

    logger.info("SUCCESS: Data transfer completed.")


def _create_schema():
    """Creates the 'raw_data' schema in DWH if it doesn't exist."""
    engine = get_dwh_engine()
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw_data;"))


# --- 4. DAG DEFINITION ---

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016, 1, 1),
    'retries': 0,
}

with DAG(
        'olist_elt_dump',
        default_args=default_args,
        max_active_runs=1,  # Limit to 1 to prevent DB overload during backfill
        schedule_interval=None,  # Triggered externally (API Trigger)
        catchup=False,
        tags=['olist', 'elt']
) as dag:
    create_schema_task = PythonOperator(
        task_id='create_raw_schema',
        python_callable=_create_schema
    )

    elt_task = PythonOperator(
        task_id='extract_and_load_data',
        python_callable=elt_load_data,
        provide_context=True
    )

    # We use 'docker' target in dbt to connect via port 5432 inside the network
    dbt_task = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt seed --target docker && dbt run --target docker && dbt test --target docker'
    )

    create_schema_task >> elt_task >> dbt_task