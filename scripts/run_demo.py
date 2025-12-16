import sys
import requests
import time
import logging
from simulate_production import load_monthly_data, load_static_data
from validate_data import run_full_validation

# --- 1. CONFIGURATION & LOGGING ---

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Airflow API Configuration
# Assumes Airflow Webserver is running locally on port 8080
AIRFLOW_URL = "http://localhost:8080/api/v1"
DAG_ID = "olist_elt_dump"
# Basic Auth credentials (default for the docker-compose setup)
AUTH = ('airflow', 'airflow')


def trigger_airflow_dag(year, month):
    """
    Sends a POST request to the Airflow API to trigger the DAG for a specific logical date.
    This simulates the DAG running immediately after data arrives.
    """
    # Logical date is set to the 1st day of the simulated month
    logical_date = f"{year}-{month:02d}-01T00:00:00Z"

    endpoint = f"{AIRFLOW_URL}/dags/{DAG_ID}/dagRuns"

    payload = {
        "logical_date": logical_date,
        "conf": {}  # Can pass runtime configuration here if needed
    }

    logger.info(f"   [Airflow] Triggering DAG for logical date: {logical_date}...")

    try:
        response = requests.post(endpoint, json=payload, auth=AUTH)

        if response.status_code == 200:
            logger.info("   [Airflow]SUCCESS: DAG triggered successfully.")
        elif response.status_code == 409:
            logger.warning("   [Airflow] DAG run already exists for this date (Skipping trigger).")
        else:
            logger.error(f"   [Airflow] API ERROR: {response.status_code} - {response.text}")

    except Exception as e:
        logger.critical(f"   [Airflow] Connection Failed: {e}")


def main():
    logger.info("=== STARTING FULL HISTORY SIMULATION & ETL ORCHESTRATION ===")

    # --- STEP 1: INITIALIZATION ---
    # Load reference data (Products, Sellers, Geo) - happens only once.
    logger.info("\n>>> STEP 1: Initializing Static Data (Source DB)...")
    load_static_data()

    # --- STEP 2: TRANSACTION LOOP ---
    # We simulate time passing month by month.
    logger.info("\n>>> STEP 2: Processing Monthly Transactions...")

    # Define the Timeline
    timeline = []

    # 2016: Data starts in September (09)
    for m in range(9, 13):
        timeline.append((2016, m))

    # 2017: Full year
    for m in range(1, 13):
        timeline.append((2017, m))

    # 2018: Full year (up to August usually in dataset, but we iterate full)
    for m in range(1, 13):
        timeline.append((2018, m))

    # Main Execution Loop
    for year, month in timeline:
        # A. Ingest Data into Source DB (Simulate Production)
        load_monthly_data(year, month)

        # B. Trigger Airflow Pipeline (ETL)
        trigger_airflow_dag(year, month)

        # C. Wait (Throttling)
        # Prevents overwhelming the Airflow Scheduler/Webserver
        logger.info("   [System] Waiting 10 seconds before next batch...")
        time.sleep(10)

        # --- STEP 3: VALIDATION ---
    # Final check to ensure no data was lost during the process.
    logger.info("\n>>> STEP 3: Verifying Data Integrity...")
    run_full_validation()

    logger.info("\n=== SIMULATION COMPLETED SUCCESSFULLY ===")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("\nSimulation stopped by user.")
        sys.exit(0)