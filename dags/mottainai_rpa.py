"""Optional Airflow DAG. Install airflow separately only for scheduled runs."""
from datetime import datetime, timedelta

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:  # keeps local development/tests independent from Airflow
    DAG = PythonOperator = None

from relational_migration import migrate
from settings import Settings

def execute():
    return migrate(Settings(), dry_run=False)

if DAG:
    with DAG("mottainai_rpa", start_date=datetime(2026, 1, 1), schedule="*/5 * * * *", catchup=False,
             default_args={"retries": 3, "retry_delay": timedelta(minutes=5)}, tags=["mottainai", "integration"]):
        PythonOperator(task_id="sync_database", python_callable=execute)
