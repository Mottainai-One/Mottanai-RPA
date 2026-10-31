"""Idempotent daily load from the physical operational DB to the analytics DB.

The present data volume is small, so each run reconciles all historical source
rows. This also captures late corrections when operational updated_at fields
are not reliably maintained. No personal identifiers or credentials are read.
"""

from __future__ import annotations

import logging
import json
import os
import re
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from uuid import uuid4
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

from config import get_analytics_db_config, get_operational_db_config

LOG = logging.getLogger(__name__)
ZONE = ZoneInfo("America/Sao_Paulo")
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class Dataset:
    target: str
    columns: tuple[str, ...]
    key: tuple[str, ...]
    source_query: str
    snapshot: bool = False


# Queries only select fields needed by the Data Mart. In particular, no CPF,
# CNPJ, address, email, phone, password, or customer name crosses databases.
DATASETS = (
    Dataset("dim_company", ("company_id", "plan_id", "legal_name", "trade_name", "active", "source_updated_at", "plan_name"), ("company_id",),
            "SELECT c.company_id, c.plan_id, c.official_name, c.trade_name, c.active AND c.deleted_at IS NULL, c.updated_at, p.name FROM {src}.company c JOIN {src}.subscription_plan p ON p.plan_id=c.plan_id"),
    Dataset("dim_store", ("store_id", "company_id", "name", "city", "state_code", "latitude", "longitude", "active", "source_updated_at"), ("store_id",),
            "SELECT s.store_id, s.company_id, s.name, a.city, a.state, s.latitude, s.longitude, s.active AND s.deleted_at IS NULL, s.updated_at FROM {src}.retail_store s JOIN {src}.address a ON a.address_id=s.address_id"),
    Dataset("dim_category", ("category_id", "parent_category_id", "name", "active", "source_updated_at"), ("category_id",),
            "SELECT category_id, NULL::bigint, name, active AND deleted_at IS NULL, updated_at FROM {src}.product_category"),
    Dataset("dim_product", ("product_id", "category_id", "tax_profile_id", "sku", "barcode", "name", "brand", "unit_of_measure", "avg_cost", "suggested_price", "active", "source_updated_at"), ("product_id",),
            "SELECT product_id, category_id, tax_profile_id, sku, barcode, name, brand, unit_measure, avg_cost, suggested_price, active AND deleted_at IS NULL, updated_at FROM {src}.product"),
    Dataset("dim_employee", ("employee_id", "store_id", "role_id", "role_name", "active", "source_updated_at"), ("employee_id",),
            "SELECT e.employee_id, e.store_id, e.role_id, r.name, e.active AND e.deleted_at IS NULL, e.updated_at FROM {src}.employee e JOIN {src}.employee_role r ON r.role_id=e.role_id"),
    Dataset("dim_customer", ("customer_id", "birth_year", "active", "marketing_consent", "source_updated_at"), ("customer_id",),
            "SELECT customer_id, birth_year, active AND deleted_at IS NULL, marketing_consent, updated_at FROM {src}.customer"),
    Dataset("fact_sales", ("sale_id", "sale_date", "sale_timestamp", "company_id", "store_id", "employee_id", "customer_id", "item_count", "gross_amount", "discount_amount", "net_amount", "status"), ("sale_id", "sale_date"),
            "SELECT s.sale_id, s.sale_date::date, s.sale_date, st.company_id, s.store_id, s.employee_id, s.customer_id, (SELECT count(*) FROM {src}.sale_item i WHERE i.sale_id=s.sale_id AND i.sale_date=s.sale_date AND i.status::text='SOLD'), s.total_amount, 0::numeric, s.total_amount, s.status::text FROM {src}.sales_transaction s JOIN {src}.retail_store st ON st.store_id=s.store_id"),
    Dataset("fact_sale_item", ("sale_item_id", "sale_id", "sale_date", "store_id", "product_id", "batch_id", "quantity", "unit_price", "discount_amount", "subtotal"), ("sale_item_id", "sale_date"),
            "SELECT i.sale_item_id, i.sale_id, i.sale_date::date, s.store_id, i.product_id, i.batch_id, i.quantity_sold, i.unit_price, 0::numeric, i.subtotal FROM {src}.sale_item i JOIN {src}.sales_transaction s ON s.sale_id=i.sale_id AND s.sale_date=i.sale_date WHERE i.status::text='SOLD'"),
    Dataset("fact_sale_payment", ("payment_id", "sale_id", "sale_date", "store_id", "payment_method", "amount", "installments"), ("payment_id",),
            "SELECT p.sale_payment_id, p.sale_id, p.sale_date::date, s.store_id, p.payment_method::text, p.amount, p.installments FROM {src}.sale_payment p JOIN {src}.sales_transaction s ON s.sale_id=p.sale_id AND s.sale_date=p.sale_date"),
    Dataset("fact_inventory_movement", ("movement_id", "movement_date", "movement_timestamp", "company_id", "store_id", "inventory_id", "product_id", "batch_id", "employee_id", "movement_type", "quantity", "unit_cost"), ("movement_id", "movement_date"),
            "SELECT m.movement_id, m.movement_date::date, m.movement_date, st.company_id, i.store_id, i.inventory_id, b.product_id, i.batch_id, m.employee_id, m.movement_type::text, m.moved_quantity, b.unit_cost FROM {src}.inventory_movement m JOIN {src}.inventory i ON i.inventory_id=m.inventory_id JOIN {src}.batch b ON b.batch_id=i.batch_id JOIN {src}.retail_store st ON st.store_id=i.store_id"),
    Dataset("fact_inventory_count_line", ("inventory_count_item_id", "inventory_count_id", "count_date", "store_id", "product_id", "inventory_id", "batch_id", "status", "system_quantity", "counted_quantity", "unit_cost", "finished_at"), ("inventory_count_item_id",),
            "SELECT ci.inventory_count_item_id, c.inventory_count_id, (c.started_at AT TIME ZONE 'America/Sao_Paulo')::date, c.store_id, b.product_id, i.inventory_id, i.batch_id, c.status::text, ci.system_quantity, ci.counted_quantity, b.unit_cost, c.finished_at FROM {src}.inventory_count_item ci JOIN {src}.inventory_count c ON c.inventory_count_id=ci.inventory_count_id JOIN {src}.inventory i ON i.inventory_id=ci.inventory_id JOIN {src}.batch b ON b.batch_id=i.batch_id WHERE ci.deleted_at IS NULL"),
    Dataset("fact_inventory_snapshot", ("snapshot_date", "company_id", "store_id", "product_id", "batch_id", "inventory_type", "quantity", "reserved_quantity", "average_cost", "expiration_date", "inventory_id", "minimum_quantity"), ("snapshot_date", "store_id", "product_id", "batch_id", "inventory_type"),
            "SELECT %s::date, st.company_id, i.store_id, b.product_id, i.batch_id, i.inventory_type::text, i.current_quantity, 0::numeric, b.unit_cost, b.expiration_date, i.inventory_id, i.minimum_quantity FROM {src}.inventory i JOIN {src}.batch b ON b.batch_id=i.batch_id JOIN {src}.retail_store st ON st.store_id=i.store_id WHERE i.deleted_at IS NULL", True),
    Dataset("fact_alert_snapshot", ("snapshot_date", "alert_id", "store_id", "status"), ("snapshot_date", "alert_id"),
            "SELECT %s::date, a.alert_id, a.store_id, a.status::text FROM {src}.alert a", True),
    Dataset("fact_alert_lifecycle", ("alert_id", "store_id", "alert_type", "priority", "status", "generated_at", "resolved_at", "source_updated_at"), ("alert_id",),
            "SELECT alert_id, store_id, alert_type::text, priority::text, status::text, generated_at, resolved_at, updated_at FROM {src}.alert"),
    Dataset("fact_action_lifecycle", ("suggested_action_id", "alert_id", "store_id", "action_type", "status", "generated_at", "source_recommendation_uuid", "source_updated_at"), ("suggested_action_id",),
            "SELECT a.suggested_action_id, a.alert_id, l.store_id, a.action_type::text, a.status::text, a.generated_at, a.source_recommendation_uuid, a.updated_at FROM {src}.suggested_action a JOIN {src}.alert l ON l.alert_id=a.alert_id"),
    Dataset("fact_transfer_line", ("transfer_item_id", "transfer_id", "request_date", "requested_at", "completed_at", "source_store_id", "destination_store_id", "product_id", "batch_id", "suggested_action_id", "status", "transferred_quantity", "unit_cost"), ("transfer_item_id",),
            "SELECT ti.transfer_item_id, t.transfer_id, t.request_date::date, t.request_date, t.completion_date, t.source_store_id, t.destination_store_id, b.product_id, ti.batch_id, t.suggested_action_id, t.status::text, ti.transferred_quantity, b.unit_cost FROM {src}.transfer_item ti JOIN {src}.transfer t ON t.transfer_id=ti.transfer_id JOIN {src}.batch b ON b.batch_id=ti.batch_id"),
    Dataset("fact_replenishment_plan_line", ("pre_list_item_id", "pre_list_id", "store_id", "product_id", "generated_at", "status", "suggested_quantity"), ("pre_list_item_id",),
            "SELECT pi.pre_list_item_id, p.pre_list_id, p.store_id, pi.product_id, p.generated_at, p.status::text, pi.suggested_quantity FROM {src}.replenishment_pre_list_item pi JOIN {src}.replenishment_pre_list p ON p.pre_list_id=pi.pre_list_id"),
    Dataset("fact_replenishment_execution_line", ("execution_item_id", "execution_id", "pre_list_id", "store_id", "product_id", "batch_id", "started_at", "completed_at", "replenished_quantity"), ("execution_item_id",),
            "SELECT ei.execution_item_id, e.execution_id, e.pre_list_id, p.store_id, b.product_id, ei.batch_id, e.start_date, e.end_date, ei.replenished_quantity FROM {src}.replenishment_execution_item ei JOIN {src}.replenishment_execution e ON e.execution_id=ei.execution_id JOIN {src}.replenishment_pre_list p ON p.pre_list_id=e.pre_list_id JOIN {src}.batch b ON b.batch_id=ei.batch_id"),
    Dataset("fact_donation_line", ("donation_item_id", "donation_id", "donation_date", "store_id", "product_id", "batch_id", "suggested_action_id", "status", "donated_quantity", "unit_cost"), ("donation_item_id",),
            "SELECT di.donation_item_id, d.donation_id, d.donation_date::date, d.store_id, b.product_id, di.batch_id, d.suggested_action_id, d.status::text, di.donated_quantity, b.unit_cost FROM {src}.donation_item di JOIN {src}.donation d ON d.donation_id=di.donation_id JOIN {src}.batch b ON b.batch_id=di.batch_id"),
    Dataset("fact_disposal_line", ("disposal_item_id", "disposal_id", "disposal_date", "store_id", "product_id", "batch_id", "suggested_action_id", "disposed_quantity", "unit_cost"), ("disposal_item_id",),
            "SELECT di.disposal_item_id, d.disposal_id, d.disposal_date::date, d.store_id, b.product_id, di.batch_id, d.suggested_action_id, di.disposed_quantity, b.unit_cost FROM {src}.disposal_item di JOIN {src}.disposal d ON d.disposal_id=di.disposal_id JOIN {src}.batch b ON b.batch_id=di.batch_id"),
    Dataset("fact_store_daily", ("snapshot_date", "company_id", "store_id", "employees_total", "employees_active", "users_total", "users_active"), ("snapshot_date", "store_id"),
            "SELECT %s::date, s.company_id, s.store_id, count(DISTINCT e.employee_id)::integer, count(DISTINCT e.employee_id) FILTER (WHERE e.active AND e.deleted_at IS NULL)::integer, count(DISTINCT u.user_id)::integer, count(DISTINCT u.user_id) FILTER (WHERE u.active AND u.deleted_at IS NULL)::integer FROM {src}.retail_store s LEFT JOIN {src}.employee e ON e.store_id=s.store_id LEFT JOIN {src}.app_user u ON u.employee_id=e.employee_id WHERE s.deleted_at IS NULL GROUP BY s.company_id,s.store_id", True),
)

OPERATIONAL_KPI_FACTS = (
    "fact_inventory_count_line", "fact_transfer_line",
    "fact_replenishment_plan_line", "fact_replenishment_execution_line",
    "fact_donation_line", "fact_disposal_line",
    "fact_alert_lifecycle", "fact_action_lifecycle",
)


def identifier(value: str) -> sql.Identifier:
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"Invalid schema identifier: {value!r}")
    return sql.Identifier(value)


def _upsert_query(dataset: Dataset, schema: str) -> sql.Composed:
    columns = sql.SQL(", ").join(map(sql.Identifier, dataset.columns))
    keys = sql.SQL(", ").join(map(sql.Identifier, dataset.key))
    updates = sql.SQL(", ").join(
        sql.SQL("{}=EXCLUDED.{}").format(sql.Identifier(name), sql.Identifier(name))
        for name in dataset.columns if name not in dataset.key
    )
    return sql.SQL("INSERT INTO {}.{} ({}) VALUES %s ON CONFLICT ({}) DO UPDATE SET {}").format(
        identifier(schema), sql.Identifier(dataset.target), columns, keys, updates
    )


def run(*, now: datetime | None = None) -> dict[str, int]:
    """Reconcile source rows in one destination transaction, then record success."""
    run_at = now.astimezone(ZONE) if now else None
    source_schema = os.getenv("ANALYTICS_SOURCE_SCHEMA", "mottainai")
    target_schema = os.getenv("ANALYTICS_TARGET_SCHEMA", "mottainai_analytics")
    identifier(source_schema)
    identifier(target_schema)
    batch_size = int(os.getenv("ANALYTICS_BATCH_SIZE", "1000"))
    if batch_size < 1:
        raise ValueError("ANALYTICS_BATCH_SIZE must be positive")
    for name, config in (("OPERATIONAL_DB", get_operational_db_config()), ("ANALYTICS_DB", get_analytics_db_config())):
        missing = [field for field in ("dbname", "user", "password") if not config[field]]
        if missing:
            raise ValueError(f"Missing {name} connection settings: {', '.join(missing)}")
    operational_config = get_operational_db_config()
    analytics_config = get_analytics_db_config()
    if (operational_config["host"], operational_config["port"], operational_config["dbname"]) == (
        analytics_config["host"], analytics_config["port"], analytics_config["dbname"]
    ):
        raise ValueError("Operational and analytics must be different physical databases")
    counts: dict[str, int] = {}
    # Aiven Free has few connection slots. Read one consistent source snapshot,
    # release its connection, then apply everything in one target transaction.
    source_rows: list[tuple[Dataset, list[tuple]]] = []
    with closing(psycopg2.connect(**operational_config)) as source:
        source.set_session(readonly=True)
        with source.cursor() as source_cursor:
            source_cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            source_cursor.execute("SELECT transaction_timestamp()")
            source_cutoff = source_cursor.fetchone()[0].astimezone(ZONE)
            run_at = run_at or source_cutoff
            for dataset in DATASETS:
                query = dataset.source_query.format(src=sql.Identifier(source_schema).as_string(source))
                source_cursor.execute(query, (run_at.date(),) if dataset.snapshot else ())
                rows = source_cursor.fetchall()
                if dataset.target in ("fact_sales", "fact_inventory_snapshot"):
                    table = "sales_transaction" if dataset.target == "fact_sales" else "inventory"
                    predicate = sql.SQL("") if dataset.target == "fact_sales" else sql.SQL(" WHERE deleted_at IS NULL")
                    source_cursor.execute(
                        sql.SQL("SELECT count(*) FROM {}.{}{}").format(
                            identifier(source_schema), sql.Identifier(table), predicate
                        )
                    )
                    expected = source_cursor.fetchone()[0]
                    if len(rows) != expected:
                        raise RuntimeError(
                            f"Source extraction lost {table} rows: expected={expected}, extracted={len(rows)}"
                        )
                source_rows.append((dataset, rows))
    with closing(psycopg2.connect(**analytics_config)) as target:
        try:
            with target.cursor() as target_cursor:
                target_cursor.execute("SET LOCAL TIME ZONE 'America/Sao_Paulo'")
                target_cursor.execute(
                    """SELECT obj_description(c.oid, 'pg_class')
                       FROM pg_class c
                       JOIN pg_namespace n ON n.oid = c.relnamespace
                       WHERE n.nspname = %s AND c.relname = 'vw_published_batch'""",
                    (target_schema,),
                )
                publication_view = target_cursor.fetchone()
                if not publication_view or publication_view[0] != "publication_contract=analytics_mart_v1":
                    raise RuntimeError(
                        "Analytics publication view is not migrated; apply 14_publish_mart_view.sql first"
                    )
                target_cursor.execute(
                    """SELECT tablename FROM pg_tables
                       WHERE schemaname = %s AND tablename = ANY(%s)""",
                    (target_schema, list(OPERATIONAL_KPI_FACTS)),
                )
                installed = {row[0] for row in target_cursor.fetchall()}
                missing = sorted(set(OPERATIONAL_KPI_FACTS) - installed)
                if missing:
                    raise RuntimeError(
                        "Analytics KPI facts are not migrated; run "
                        "database/analytics/15_operational_kpi_facts.sql "
                        f"against MOTTAINAI_ANALITICO first (missing: {', '.join(missing)})"
                    )
                for dataset, rows in source_rows:
                    if dataset.snapshot:
                        target_cursor.execute(
                            sql.SQL("DELETE FROM {}.{} WHERE snapshot_date=%s").format(identifier(target_schema), sql.Identifier(dataset.target)),
                            (run_at.date(),),
                        )
                    elif dataset.target.startswith("fact_"):
                        # Full reconciliation also removes late cancellations and
                        # source deletions that an upsert alone would leave behind.
                        target_cursor.execute(
                            sql.SQL("DELETE FROM {}.{}").format(identifier(target_schema), sql.Identifier(dataset.target))
                        )
                    written = len(rows)
                    upsert = _upsert_query(dataset, target_schema).as_string(target)
                    for start in range(0, written, batch_size):
                        batch = rows[start:start + batch_size]
                        execute_values(target_cursor, upsert, batch, page_size=batch_size)
                    target_cursor.execute(
                        sql.SQL("SELECT count(*) FROM {}.{}").format(
                            identifier(target_schema), sql.Identifier(dataset.target)
                        )
                    )
                    target_count = target_cursor.fetchone()[0]
                    if dataset.snapshot:
                        target_cursor.execute(
                            sql.SQL("SELECT count(*) FROM {}.{} WHERE snapshot_date=%s").format(
                                identifier(target_schema), sql.Identifier(dataset.target)
                            ),
                            (run_at.date(),),
                        )
                        target_count = target_cursor.fetchone()[0]
                    if target_count != written:
                        raise RuntimeError(
                            f"Reconciliation failed for {dataset.target}: source={written}, target={target_count}"
                        )
                    counts[dataset.target] = written
                    LOG.info("loaded %s rows into %s", written, dataset.target)
                # psycopg2 does not adapt Python UUID objects unless its UUID
                # adapter is registered. Send a canonical string explicitly.
                publication_id = str(uuid4())
                target_cursor.execute(
                    sql.SQL("INSERT INTO {}.job_log (job_name, status, started_at, finished_at, rows_read, rows_written, batch_id, cutoff_at, metadata) VALUES (%s, %s::{}.job_status, %s, now(), %s, %s, %s::uuid, %s, %s::jsonb)").format(identifier(target_schema), identifier(target_schema)),
                    (
                        "operational_to_analytics_daily", "SUCCEEDED", run_at,
                        sum(counts.values()), sum(counts.values()), publication_id,
                        source_cutoff,
                        json.dumps({"publication_contract": "analytics_mart_v1", "dataset_counts": counts}),
                    ),
                )
                target_cursor.execute(
                    sql.SQL("REFRESH MATERIALIZED VIEW {}.mv_dashboard_metrics").format(identifier(target_schema))
                )
            target.commit()
        except Exception:
            target.rollback()
            LOG.exception("daily analytics load failed; target transaction rolled back")
            raise
    return counts
