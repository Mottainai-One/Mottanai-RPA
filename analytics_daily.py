"""Carga diaria do operacional para o mart analitico."""
from __future__ import annotations
import logging
import os
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from db import connection

LOGGER = logging.getLogger("mottainai.analytics_daily")
SAO_PAULO = ZoneInfo("America/Sao_Paulo")

def previous_day(reference: datetime | None = None) -> date:
    now = reference.astimezone(SAO_PAULO) if reference else datetime.now(SAO_PAULO)
    return now.date() - timedelta(days=1)

def run_daily_analytics(*, reference: datetime | None = None, dry_run: bool | None = None):
    target_date = previous_day(reference)
    if dry_run is None:
        dry_run = os.getenv("ANALYTICS_DRY_RUN", "true").lower() in {"1", "true", "yes"}
    with connection("operational") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_xact_lock(hashtext('mottainai.analytics.daily'))")
            if not cur.fetchone()[0]:
                raise RuntimeError("A carga analitica diaria ja esta em execucao")
            cur.execute("""CREATE TABLE IF NOT EXISTS analytics.etl_run_log (
                run_id bigserial PRIMARY KEY, pipeline_name text NOT NULL,
                reference_date date NOT NULL, status text NOT NULL
                    CHECK (status IN ('RUNNING','SUCCEEDED','FAILED')),
                started_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz,
                rows_written integer, error_message text,
                UNIQUE (pipeline_name, reference_date))""")
            cur.execute("""INSERT INTO analytics.etl_run_log
                (pipeline_name, reference_date, status)
                VALUES ('operational_to_analytics_daily', %s, 'RUNNING')
                ON CONFLICT (pipeline_name, reference_date) DO UPDATE SET
                  status=CASE WHEN analytics.etl_run_log.status='SUCCEEDED'
                    THEN 'SUCCEEDED' ELSE 'RUNNING' END,
                  started_at=CASE WHEN analytics.etl_run_log.status='SUCCEEDED'
                    THEN analytics.etl_run_log.started_at ELSE now() END,
                  error_message=NULL RETURNING status""", (target_date,))
            if cur.fetchone()[0] == "SUCCEEDED":
                conn.rollback()
                return {"reference_date": str(target_date), "status": "already_loaded", "dry_run": dry_run}
            try:
                cur.execute("CALL analytics.refresh_facts(%s)", (target_date,))
                cur.execute("""SELECT COALESCE(SUM(employees_total + users_total),0)
                    FROM analytics.fact_store_daily
                    WHERE date_key=to_char(%s,'YYYYMMDD')::integer""", (target_date,))
                rows_written = int(cur.fetchone()[0])
                if dry_run:
                    conn.rollback()
                    return {"reference_date": str(target_date), "status": "dry_run", "rows_written": rows_written, "dry_run": True}
                cur.execute("""UPDATE analytics.etl_run_log
                    SET status='SUCCEEDED', finished_at=now(), rows_written=%s
                    WHERE pipeline_name='operational_to_analytics_daily' AND reference_date=%s""", (rows_written, target_date))
                conn.commit()
                return {"reference_date": str(target_date), "status": "succeeded", "rows_written": rows_written, "dry_run": False}
            except Exception as exc:
                conn.rollback()
                LOGGER.exception("Falha na carga analitica de %s", target_date)
                raise RuntimeError(f"Falha na carga analitica de {target_date}: {exc}") from exc
