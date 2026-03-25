import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.sql.elements import TextClause

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'scripts'
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.append(str(SCRIPTS_DIR))

from db_config import get_dwh_db_config, get_source_db_config

logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = '/opt/airflow/olist_dbt'


def _get_source_engine() -> Engine:
    return create_engine(get_source_db_config().sqlalchemy_url())


def _get_dwh_engine() -> Engine:
    return create_engine(get_dwh_db_config().sqlalchemy_url())


SQL_INCREMENTAL_ORDERS = text("""
    SELECT * FROM public.olist_orders_dataset
    WHERE EXTRACT(YEAR FROM order_purchase_timestamp) = :year
      AND EXTRACT(MONTH FROM order_purchase_timestamp) = :month
""")

SQL_STATIC_TABLES: Dict[str, TextClause] = {
    'olist_products_dataset': text("SELECT * FROM public.olist_products_dataset"),
    'olist_sellers_dataset': text("SELECT * FROM public.olist_sellers_dataset"),
    'olist_geolocation_dataset': text("SELECT * FROM public.olist_geolocation_dataset"),
}

_SUBQUERY_WHERE_ORDERS = """
    WHERE order_id IN (
        SELECT order_id FROM public.olist_orders_dataset
        WHERE EXTRACT(YEAR FROM order_purchase_timestamp) = :year
          AND EXTRACT(MONTH FROM order_purchase_timestamp) = :month
    )
"""

SQL_INCREMENTAL_OTHERS: Dict[str, TextClause] = {
    'olist_order_items_dataset': text(
        f"SELECT * FROM public.olist_order_items_dataset {_SUBQUERY_WHERE_ORDERS}"
    ),
    'olist_order_payments_dataset': text(
        f"SELECT * FROM public.olist_order_payments_dataset {_SUBQUERY_WHERE_ORDERS}"
    ),
    'olist_order_reviews_dataset': text(
        f"SELECT * FROM public.olist_order_reviews_dataset {_SUBQUERY_WHERE_ORDERS}"
    ),
    'olist_customers_dataset': text("""
        SELECT * FROM public.olist_customers_dataset
        WHERE customer_id IN (
            SELECT customer_id FROM public.olist_orders_dataset
            WHERE EXTRACT(YEAR FROM order_purchase_timestamp) = :year
              AND EXTRACT(MONTH FROM order_purchase_timestamp) = :month
        )
    """)
}

SQL_CLEANUP_QUERIES: Dict[str, TextClause] = {
    'olist_order_items_dataset': text("""
        DELETE FROM raw_data.olist_order_items_dataset
        WHERE order_id IN (
            SELECT order_id FROM raw_data.olist_orders_dataset
            WHERE EXTRACT(YEAR FROM order_purchase_timestamp) = :year
              AND EXTRACT(MONTH FROM order_purchase_timestamp) = :month
        )
    """),
    'olist_order_payments_dataset': text("""
        DELETE FROM raw_data.olist_order_payments_dataset
        WHERE order_id IN (
            SELECT order_id FROM raw_data.olist_orders_dataset
            WHERE EXTRACT(YEAR FROM order_purchase_timestamp) = :year
              AND EXTRACT(MONTH FROM order_purchase_timestamp) = :month
        )
    """),
    'olist_order_reviews_dataset': text("""
        DELETE FROM raw_data.olist_order_reviews_dataset
        WHERE order_id IN (
            SELECT order_id FROM raw_data.olist_orders_dataset
            WHERE EXTRACT(YEAR FROM order_purchase_timestamp) = :year
              AND EXTRACT(MONTH FROM order_purchase_timestamp) = :month
        )
    """),
    'olist_customers_dataset': text("""
        DELETE FROM raw_data.olist_customers_dataset
        WHERE customer_id IN (
            SELECT DISTINCT customer_id FROM raw_data.olist_orders_dataset
            WHERE EXTRACT(YEAR FROM order_purchase_timestamp) = :year
              AND EXTRACT(MONTH FROM order_purchase_timestamp) = :month
        )
          AND NOT EXISTS (
              SELECT 1
              FROM raw_data.olist_orders_dataset AS o
              WHERE o.customer_id = raw_data.olist_customers_dataset.customer_id
                AND NOT (
                    EXTRACT(YEAR FROM o.order_purchase_timestamp) = :year
                    AND EXTRACT(MONTH FROM o.order_purchase_timestamp) = :month
                )
          )
    """),
    'olist_orders_dataset': text("""
        DELETE FROM raw_data.olist_orders_dataset
        WHERE EXTRACT(YEAR FROM order_purchase_timestamp) = :year
          AND EXTRACT(MONTH FROM order_purchase_timestamp) = :month
    """),
}


def _create_schema() -> None:
    engine = _get_dwh_engine()
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw_data;"))


def _table_exists(conn: Connection, schema: str, table_name: str) -> bool:
    qualified_name = f"{schema}.{table_name}"
    result = conn.execute(
        text("SELECT to_regclass(:qualified_name) IS NOT NULL"),
        {"qualified_name": qualified_name}
    )
    return bool(result.scalar())


def _cleanup_dwh_data(conn: Connection, year: int, month: int) -> None:
    """
    Removes data from DWH for the specific period to ensure idempotency.
    Missing tables on the first run are skipped, but any real cleanup error aborts the task.
    """
    logger.info(f">> Cleaning up DWH data for {year}-{month:02d}...")
    period_params = {"year": year, "month": month}

    existing_tables = {
        table_name: _table_exists(conn, 'raw_data', table_name)
        for table_name in [
            'olist_orders_dataset',
            'olist_customers_dataset',
            'olist_order_items_dataset',
            'olist_order_payments_dataset',
            'olist_order_reviews_dataset'
        ]
    }

    if not any(existing_tables.values()):
        logger.info("   [INFO] No transactional raw_data tables exist yet. Skipping cleanup.")
        return

    if not existing_tables['olist_orders_dataset']:
        raise RuntimeError(
            "Cleanup aborted: raw_data.olist_orders_dataset is missing while other "
            "transactional raw_data tables exist."
        )

    for table_name, query in SQL_CLEANUP_QUERIES.items():
        if not existing_tables.get(table_name, False):
            logger.info(f"   [INFO] {table_name} is missing. Skipping cleanup for this table.")
            continue

        result = conn.execute(query, period_params)
        rowcount = result.rowcount if result.rowcount is not None and result.rowcount >= 0 else 'unknown number of'
        logger.info(f"   [OK] {table_name}: removed {rowcount} rows.")


def _load_static_data(src_engine: Engine, dwh_engine: Engine) -> None:
    """Loads reference data performing an atomic full refresh."""
    logger.info(">> Processing static tables...")

    static_frames = {
        table: pd.read_sql(query, src_engine)
        for table, query in SQL_STATIC_TABLES.items()
    }

    with dwh_engine.begin() as conn:
        for table, df in static_frames.items():
            df.to_sql(table, conn, schema='raw_data', if_exists='replace', index=False)
            logger.info(f"   [OK] {table}: loaded {len(df)} rows.")


def _load_incremental_data(year: int, month: int, src_engine: Engine, dwh_engine: Engine) -> None:
    """Loads transactional data incrementally for a specific month."""
    logger.info(">> Processing transactional tables...")
    period_params = {"year": year, "month": month}

    with dwh_engine.begin() as conn:
        _cleanup_dwh_data(conn, year, month)

        df_orders = pd.read_sql(SQL_INCREMENTAL_ORDERS, src_engine, params=period_params)

        if df_orders.empty:
            logger.info("   [INFO] No orders found for this month. Skipping related tables.")
            return

        df_orders.to_sql('olist_orders_dataset', conn, schema='raw_data', if_exists='append', index=False)
        logger.info(f"   [OK] olist_orders_dataset: appended {len(df_orders)} rows.")

        for table_name, query_template in SQL_INCREMENTAL_OTHERS.items():
            df = pd.read_sql(query_template, src_engine, params=period_params)

            if not df.empty:
                df.to_sql(table_name, conn, schema='raw_data', if_exists='append', index=False)
                logger.info(f"   [OK] {table_name}: appended {len(df)} rows.")
            else:
                logger.info(f"   [INFO] {table_name}: no related data found.")


def elt_load_data(**kwargs) -> None:
    """
    Main ELT function: Extracts data from Source DB and Loads it into DWH.
    Handles static (Full Refresh) and transactional (Incremental) data.
    """
    execution_date = kwargs['logical_date']
    year = execution_date.year
    month = execution_date.month

    logger.info(f"--- START RAW: {year}-{month:02d} ---")

    src_engine = _get_source_engine()
    dwh_engine = _get_dwh_engine()

    _load_static_data(src_engine, dwh_engine)
    _load_incremental_data(year, month, src_engine, dwh_engine)

    logger.info("SUCCESS: Data transfer completed.")


DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016, 1, 1),
    'retries': 0,
}


DBT_BASH_CMD = (
    f"cd {DBT_PROJECT_DIR} && "
    "if [ ! -d dbt_packages ] || [ -z \"$(ls -A dbt_packages)\" ]; then dbt deps; fi && "
    "dbt seed --target docker && "
    "dbt run --target docker && "
    "dbt test --target docker"
)

with DAG(
        dag_id='olist_elt_pipeline',
        default_args=DEFAULT_ARGS,
        max_active_runs=1,
        schedule_interval=None,
        catchup=False,
        tags=['olist', 'elt'],
        is_paused_upon_creation=False
) as dag:

    create_schema_task = PythonOperator(
        task_id='create_raw_schema',
        python_callable=_create_schema
    )

    elt_task = PythonOperator(
        task_id='extract_and_load_data',
        python_callable=elt_load_data
    )

    dbt_task = BashOperator(
        task_id='dbt_run',
        bash_command=DBT_BASH_CMD
    )

    create_schema_task >> elt_task >> dbt_task
