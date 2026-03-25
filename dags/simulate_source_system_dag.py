import logging
import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'scripts'
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.append(str(SCRIPTS_DIR))

from simulate_production import load_monthly_data, load_static_data

logger = logging.getLogger(__name__)

def _load_static_if_first_run(**context):
    """Loads static reference data ONLY on the first simulated DAG run (Sep 2016)"""
    logical_date = context['logical_date']
    if logical_date.year == 2016 and logical_date.month == 9:
        logger.info("First run detected: Loading static reference data (Products, Sellers, Geolocation)...")
        load_static_data()
    else:
        logger.info(f"Run for {logical_date.strftime('%Y-%m')}. Skipping static data load.")

def _simulate_monthly_transactions(**context):
    """Simulates production transactions for the current logical month"""
    logical_date = context['logical_date']
    year = logical_date.year
    month = logical_date.month
    
    logger.info(f"Simulating production data injection for: {year}-{month:02d}")
    load_monthly_data(year, month)

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': True,
}

with DAG(
    dag_id='simulate_source_system',
    default_args=DEFAULT_ARGS,
    start_date=datetime(2016, 9, 1),
    end_date=datetime(2018, 12, 1),
    schedule_interval='@monthly',
    catchup=True,                  
    max_active_runs=1,             
    tags=['olist', 'simulation']
) as dag:

    load_static_task = PythonOperator(
        task_id='load_static_reference_data',
        python_callable=_load_static_if_first_run
    )

    simulate_monthly_task = PythonOperator(
        task_id='load_monthly_transactions',
        python_callable=_simulate_monthly_transactions
    )

    trigger_etl_dag = TriggerDagRunOperator(
        task_id="trigger_olist_elt_pipeline",
        trigger_dag_id="olist_elt_pipeline",
        logical_date="{{ logical_date }}",
        wait_for_completion=True,
        poke_interval=10
    )

    load_static_task >> simulate_monthly_task >> trigger_etl_dag
