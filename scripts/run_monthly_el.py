# run_monthly_etl.py
import psycopg2
import psycopg2.extras
import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta

RUN_YEAR = 2017
RUN_MONTH = 11

SOURCE_CONFIG = {
    "host": "localhost", "port": "5433", "dbname": "olist_source",
    "user": "olist", "password": "olist"
}

TARGET_CONFIG = {
    "host": "localhost", "port": "5434", "dbname": "dwh",
    "user": "dwh_user", "password": "dwh_pass"
}

# === KROK 2: WPISZ TUTAJ NAZWĘ SCHEMATU ZNALEZIONĄ W KROKU 1 ===
SOURCE_SCHEMA = "public"  # Np. "public" lub "raw_data"
# ==============================================================

FULL_REFRESH_TABLES = [
    'olist_products_dataset',
    'olist_customers_dataset',
    'olist_sellers_dataset',
    'olist_geolocation_dataset',
]

INCREMENTAL_TABLES = {
    'olist_orders_dataset': 'order_purchase_timestamp',
    'olist_order_reviews_dataset': 'order_purchase_timestamp',
    'olist_order_items_dataset': 'order_purchase_timestamp',
    'olist_order_payments_dataset': 'order_purchase_timestamp'
}


def get_db_connection(config):
    return psycopg2.connect(**config)


def load_full_refresh_table(table_name, src_conn, trg_cursor):
    print(f"  [Full Refresh] Ładowanie: {table_name}...")
    try:
        with src_conn.cursor() as cur:
            # POPRAWKA: Używamy zmiennej SOURCE_SCHEMA
            cur.execute(f"SELECT * FROM {SOURCE_SCHEMA}.{table_name}")
            data = cur.fetchall()
            columns = [desc[0] for desc in cur.description]

        if not data:
            print(f"    Brak danych w źródle dla {table_name}.")
            return

        cols_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        query = f"INSERT INTO raw_data.{table_name} ({cols_str}) VALUES ({placeholders})"

        trg_cursor.execute(f"TRUNCATE TABLE raw_data.{table_name} RESTART IDENTITY")
        psycopg2.extras.execute_batch(trg_cursor, query, data)
        print(f"    Załadowano {len(data)} wierszy.")

    except Exception as e:
        print(f"    BŁĄD przy {table_name}: {e}")
        raise


def load_incremental_table(table_name, date_column, start_date, end_date, src_conn, trg_cursor):
    print(f"  [Incremental] Ładowanie: {table_name} (dla {start_date.date()})...")

    # POPRAWKA: Używamy zmiennej SOURCE_SCHEMA
    query_template = "SELECT * FROM {schema}.{table_name} WHERE {date_column} >= %s AND {date_column} < %s"
    params = (start_date, end_date)

    if table_name in ['olist_order_items_dataset', 'olist_order_payments_dataset', 'olist_order_reviews_dataset']:
        # POPRAWKA: Używamy zmiennej SOURCE_SCHEMA
        query_template = """
                         SELECT t.* \
                         FROM {schema}.{table_name} t
            JOIN {schema}.olist_orders_dataset o \
                         ON t.order_id = o.order_id
                         WHERE o.{date_column} >= %s \
                           AND o.{date_column} \
                             < %s \
                         """

    try:
        with src_conn.cursor() as cur:
            query = query_template.format(schema=SOURCE_SCHEMA, table_name=table_name, date_column=date_column)
            cur.execute(query, params)
            data = cur.fetchall()
            columns = [desc[0] for desc in cur.description]

        if not data:
            print(f"    Brak danych w tym miesiącu dla {table_name}.")
            return

        cols_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        insert_query = f"INSERT INTO raw_data.{table_name} ({cols_str}) VALUES ({placeholders})"

        delete_query_template = """
                                DELETE \
                                FROM raw_data.{table_name} t
            USING raw_data.olist_orders_dataset o
                                WHERE t.order_id = o.order_id
                                  AND o.{date_column} >= %s \
                                  AND o.{date_column} \
                                    < %s \
                                """

        if table_name == 'olist_orders_dataset':
            delete_query_template = "DELETE FROM raw_data.{table_name} WHERE {date_column} >= %s AND {date_column} < %s"

        delete_query = delete_query_template.format(table_name=table_name, date_column=date_column)
        trg_cursor.execute(delete_query, (start_date, end_date))
        print(f"    Usunięto {trg_cursor.rowcount} starych wierszy powiązanych z tym miesiącem.")

        psycopg2.extras.execute_batch(trg_cursor, insert_query, data)
        print(f"    Załadowano {len(data)} nowych wierszy.")

    except Exception as e:
        print(f"    BBŁĄD przy {table_name}: {e}")
        raise


def main():
    start_date = datetime(RUN_YEAR, RUN_MONTH, 1)
    end_date = start_date + relativedelta(months=1)

    print(f"--- Rozpoczynam ETL dla: {start_date.strftime('%Y-%m')} ---")

    # Sprawdzenie, czy schemat został ustawiony
    if SOURCE_SCHEMA == "TUTAJ_WSTAW_NAZWE_SCHEMATU":
        print("--- BŁĄD KRYTYCZNY: Nie ustawiono zmiennej SOURCE_SCHEMA na górze skryptu! ---")
        sys.exit(1)

    try:
        with get_db_connection(SOURCE_CONFIG) as src_conn:
            with get_db_connection(TARGET_CONFIG) as trg_conn:
                with trg_conn.cursor() as trg_cursor:

                    print("\n[Blok 1: Tabele Full-Refresh]")
                    for table_name in FULL_REFRESH_TABLES:
                        load_full_refresh_table(table_name, src_conn, trg_cursor)

                    print("\n[Blok 2: Tabele Inkrementalne]")
                    for table_name, date_column in INCREMENTAL_TABLES.items():
                        load_incremental_table(table_name, date_column, start_date, end_date, src_conn, trg_cursor)

                print("\nZatwierdzanie transakcji (Commit)...")
                trg_conn.commit()

    except Exception as e:
        print(f"\n--- BŁĄD KRYTYCZNY ---")
        print(f"Transakcja została wycofana (Rollback). Szczegóły: {e}")
        sys.exit(1)

    print(f"--- Proces E+L dla {start_date.strftime('%Y-%m')} zakończony sukcesem! ---")


if __name__ == "__main__":
    main()