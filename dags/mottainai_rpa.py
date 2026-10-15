"""Legacy-to-operational synchronization every 30 minutes."""
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
    with DAG(
        "mottainai_legacy_to_operational",
        start_date=datetime(2026, 1, 1),
        schedule="*/30 * * * *",
        catchup=False,
        max_active_runs=1,
        default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
        tags=["mottainai", "legacy", "operational"],
    ):
        PythonOperator(task_id="sync_legacy_to_operational", python_callable=execute)
