"""Copy the existing two-day store history from the old operational analytics schema.

Run once before replacing the old dashboard. Re-running is safe because the
physical analytics table has a (snapshot_date, store_id) primary key.
"""

from contextlib import closing

import psycopg2
from psycopg2.extras import execute_values

from config import get_analytics_db_config, get_operational_db_config


SOURCE_SQL = """
SELECT d.full_date, dc.company_id, ds.store_id,
       f.employees_total, f.employees_active,
       f.users_total, f.users_active, f.captured_at
FROM analytics.fact_store_daily f
JOIN analytics.dim_date d ON d.date_key = f.date_key
JOIN analytics.dim_company dc ON dc.company_key = f.company_key
JOIN analytics.dim_store ds ON ds.store_key = f.store_key
"""

TARGET_SQL = """
INSERT INTO mottainai_analytics.fact_store_daily
    (snapshot_date, company_id, store_id, employees_total,
     employees_active, users_total, users_active, captured_at)
VALUES %s
ON CONFLICT (snapshot_date, store_id) DO UPDATE SET
    company_id=EXCLUDED.company_id,
    employees_total=EXCLUDED.employees_total,
    employees_active=EXCLUDED.employees_active,
    users_total=EXCLUDED.users_total,
    users_active=EXCLUDED.users_active,
    captured_at=EXCLUDED.captured_at
"""


def run() -> int:
    with closing(psycopg2.connect(**get_operational_db_config())) as source:
        source.set_session(readonly=True)
        with source.cursor() as src:
            src.execute(SOURCE_SQL)
            rows = src.fetchall()
    with closing(psycopg2.connect(**get_analytics_db_config())) as target:
        try:
            with target.cursor() as dst:
                if rows:
                    execute_values(dst, TARGET_SQL, rows, page_size=1000)
            target.commit()
            return len(rows)
        except Exception:
            target.rollback()
            raise


if __name__ == "__main__":
    print({"historical_store_days": run()})
