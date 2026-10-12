"""Daily BI batch at 02:00 in the America/Sao_Paulo timezone."""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:  # Allows local tests without installing Airflow.
    DAG = PythonOperator = None

from analytics.load import run

if DAG:
    with DAG(
        "mottainai_operational_to_analytics",
        start_date=datetime(2026, 1, 1, tzinfo=ZoneInfo("America/Sao_Paulo")),
        schedule="0 2 * * *",
        catchup=False,
        max_active_runs=1,
        default_args={"retries": 3, "retry_delay": timedelta(minutes=10)},
        tags=["mottainai", "bi", "daily"],
    ):
        PythonOperator(task_id="load_analytics", python_callable=run)
