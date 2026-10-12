"""DAG operacional -> analitico, diariamente as 02:00 em Sao Paulo."""
from datetime import datetime, timedelta
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:
    DAG = PythonOperator = None
from analytics_daily import run_daily_analytics

def execute():
    return run_daily_analytics(dry_run=False)

if DAG:
    with DAG("mottainai_operational_to_analytics_daily", start_date=datetime(2026,1,1),
             schedule="0 2 * * *", catchup=False, max_active_runs=1,
             dagrun_timeout=timedelta(minutes=30),
             default_args={"retries":3, "retry_delay":timedelta(minutes=10)},
             tags=["mottainai","analytics","daily"]):
        PythonOperator(task_id="load_previous_day", python_callable=execute)
