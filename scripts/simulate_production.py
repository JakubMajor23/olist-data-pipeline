import logging
from typing import Callable, Dict, List, Sequence, Tuple

import pandas as pd
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Connection, Engine

from db_config import ROOT_DIR, get_source_db_config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

CSV_DIR = ROOT_DIR / 'data'


DATASET_FILES: Dict[str, str] = {
    'orders': 'olist_orders_dataset.csv',
    'items': 'olist_order_items_dataset.csv',
    'payments': 'olist_order_payments_dataset.csv',
    'reviews': 'olist_order_reviews_dataset.csv',
    'customers': 'olist_customers_dataset.csv',
    'products': 'olist_products_dataset.csv',
    'sellers': 'olist_sellers_dataset.csv',
    'geolocation': 'olist_geolocation_dataset.csv'
}


def get_engine() -> Engine:
    return create_engine(get_source_db_config().sqlalchemy_url())


def _read_csv(dataset_key: str, **kwargs: object) -> pd.DataFrame:
    file_path = CSV_DIR / DATASET_FILES[dataset_key]
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    return pd.read_csv(file_path, **kwargs)


def _build_upsert_method(conflict_columns: Sequence[str]) -> Callable[..., int]:
    """Builds a pandas to_sql callback that performs PostgreSQL upserts."""

    def upsert_method(table: object, conn: Connection, keys: List[str], data_iter: object) -> int:
        rows = [dict(zip(keys, row)) for row in data_iter]
        if not rows:
            return 0

        stmt = insert(table.table).values(rows)
        update_columns = {
            column.name: stmt.excluded[column.name]
            for column in table.table.columns
            if column.name not in conflict_columns
        }

        if update_columns:
            stmt = stmt.on_conflict_do_update(
                index_elements=list(conflict_columns),
                set_=update_columns
            )
        else:
            stmt = stmt.on_conflict_do_nothing(index_elements=list(conflict_columns))

        result = conn.execute(stmt)
        return result.rowcount if result.rowcount is not None else 0

    return upsert_method


def _write_dataframe_to_sql(
        df: pd.DataFrame,
        table_name: str,
        conn: Connection,
        method: Callable[..., int] | None = None
) -> None:
    """Helper to write a DataFrame to PostgreSQL using the current transaction."""
    if df.empty:
        return

    df.to_sql(
        name=table_name,
        con=conn,
        schema='public',
        if_exists='append',
        index=False,
        chunksize=1000,
        method=method
    )


def _delete_rows_by_ids(
        conn: Connection,
        table_name: str,
        id_column: str,
        ids: Sequence[str]
) -> None:
    """Deletes rows matching the provided identifiers."""
    if not ids:
        return

    delete_stmt = text(
        f"DELETE FROM public.{table_name} WHERE {id_column} IN :ids"
    ).bindparams(bindparam("ids", expanding=True))
    conn.execute(delete_stmt, {"ids": list(ids)})


def _cleanup_monthly_data(
        conn: Connection,
        target_order_ids: Sequence[str],
        target_customer_ids: Sequence[str],
        year: int,
        month: int
) -> None:
    """Removes the target month from the source DB before reloading it."""
    logger.info(f"Cleaning existing source data for {year}-{month:02d} before reload...")

    _delete_rows_by_ids(conn, 'olist_order_items_dataset', 'order_id', target_order_ids)
    _delete_rows_by_ids(conn, 'olist_order_payments_dataset', 'order_id', target_order_ids)
    _delete_rows_by_ids(conn, 'olist_order_reviews_dataset', 'order_id', target_order_ids)
    _delete_rows_by_ids(conn, 'olist_orders_dataset', 'order_id', target_order_ids)

    if target_customer_ids:
        delete_customers_stmt = text("""
            DELETE FROM public.olist_customers_dataset AS c
            WHERE c.customer_id IN :customer_ids
              AND NOT EXISTS (
                  SELECT 1
                  FROM public.olist_orders_dataset AS o
                  WHERE o.customer_id = c.customer_id
              )
        """).bindparams(bindparam("customer_ids", expanding=True))
        conn.execute(delete_customers_stmt, {"customer_ids": list(target_customer_ids)})


def load_static_data() -> None:
    """
    Loads reference data (Products, Sellers, Geolocation) that doesn't change over time.
    Should be run once at the beginning of the simulation.
    """
    engine = get_engine()
    logger.info("--- Starting Static Data Load (Reference Data) ---")

    products_df = _read_csv('products')
    sellers_df = _read_csv('sellers')
    geolocation_df = _read_csv('geolocation')

    try:
        with engine.begin() as conn:
            logger.info("Processing: products...")
            _write_dataframe_to_sql(
                products_df,
                'olist_products_dataset',
                conn,
                method=_build_upsert_method(['product_id'])
            )
            logger.info(f" -> [OK] olist_products_dataset: synchronized {len(products_df)} rows.")

            logger.info("Processing: sellers...")
            _write_dataframe_to_sql(
                sellers_df,
                'olist_sellers_dataset',
                conn,
                method=_build_upsert_method(['seller_id'])
            )
            logger.info(f" -> [OK] olist_sellers_dataset: synchronized {len(sellers_df)} rows.")

            logger.info("Processing: geolocation...")
            conn.execute(text("TRUNCATE TABLE public.olist_geolocation_dataset"))
            _write_dataframe_to_sql(geolocation_df, 'olist_geolocation_dataset', conn)
            logger.info(f" -> [OK] olist_geolocation_dataset: reloaded {len(geolocation_df)} rows.")
    except Exception:
        logger.exception("Static data load failed. Rolled back all reference data changes.")
        raise


def load_monthly_data(year: int, month: int) -> None:
    """
    Simulates monthly transaction flow.
    Loads Orders, Items, Payments, Reviews, and Customers for a specific month.
    """
    engine = get_engine()
    logger.info(f"\n--- Simulation Started for Period: {year}-{month:02d} ---")

    # --- A. DATA PREPARATION (Filtering) ---
    df_orders = _read_csv('orders')
    df_orders['order_purchase_timestamp'] = pd.to_datetime(df_orders['order_purchase_timestamp'])

    mask = (
        (df_orders['order_purchase_timestamp'].dt.year == year) &
        (df_orders['order_purchase_timestamp'].dt.month == month)
    )
    monthly_orders = df_orders[mask]

    if monthly_orders.empty:
        logger.warning(f"No orders found for {year}-{month:02d}. Skipping.")
        return

    target_order_ids = monthly_orders['order_id'].drop_duplicates().tolist()
    target_customer_ids = monthly_orders['customer_id'].drop_duplicates().tolist()

    logger.info(f"Processing {len(monthly_orders)} orders...")

    # Load Related Data (Filtered)
    df_customers = _read_csv('customers')
    monthly_customers = df_customers[df_customers['customer_id'].isin(target_customer_ids)]

    df_items = _read_csv('items')
    monthly_items = df_items[df_items['order_id'].isin(target_order_ids)]

    df_payments = _read_csv('payments')
    monthly_payments = df_payments[df_payments['order_id'].isin(target_order_ids)]

    df_reviews = _read_csv('reviews')
    monthly_reviews = df_reviews[df_reviews['order_id'].isin(target_order_ids)]

    # --- B. DATABASE INGESTION ---
    # Insertion order matters due to Foreign Key constraints:
    # 1. Customers -> 2. Orders -> 3. Details (Items, Payments, Reviews)
    ingestion_plan: List[Tuple[str, pd.DataFrame, str, Callable[..., int] | None]] = [
        (
            'Customers',
            monthly_customers,
            'olist_customers_dataset',
            _build_upsert_method(['customer_id'])
        ),
        ('Orders', monthly_orders, 'olist_orders_dataset', None),
        ('Items', monthly_items, 'olist_order_items_dataset', None),
        ('Payments', monthly_payments, 'olist_order_payments_dataset', None),
        ('Reviews', monthly_reviews, 'olist_order_reviews_dataset', None)
    ]

    try:
        with engine.begin() as conn:
            _cleanup_monthly_data(conn, target_order_ids, target_customer_ids, year, month)

            for label, df, table_name, method in ingestion_plan:
                _write_dataframe_to_sql(df, table_name, conn, method=method)
                logger.info(f" -> Added {label}: {len(df)}")
    except Exception:
        logger.exception(
            "Monthly simulation failed for %s-%02d. Rolled back all writes for this period.",
            year,
            month
        )
        raise

    logger.info("SUCCESS: Simulation completed for this month.")


def _run_interactive_mode() -> None:
    """Handles the CLI interaction if script is run directly."""
    print("\n--- OLIST DATA SIMULATOR ---")
    print(" [1] Initialization (Load Products, Sellers, Geolocation - Run ONCE)")
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
            print("Invalid input! Please enter valid integer numbers.")
    else:
        print("Unknown option. Exiting.")


if __name__ == "__main__":
    try:
        _run_interactive_mode()
    except KeyboardInterrupt:
        logger.warning("\nSimulator stopped by user.")
