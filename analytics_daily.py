"""Operational -> mottainai_analytics daily batch for PR 31."""
from __future__ import annotations
import hashlib, json, logging, os
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from psycopg2.extras import Json, RealDictCursor
from db import connection
from pipeline import quote_identifier

LOGGER = logging.getLogger("mottainai.analytics_daily")
SAO_PAULO = ZoneInfo("America/Sao_Paulo")
OPERATIONAL_SCHEMA = os.getenv("OPERATIONAL_SCHEMA", "mottainai")
ANALYTICS_SCHEMA = os.getenv("ANALYTICS_SCHEMA", "mottainai_analytics")
PIPELINE = "operational_to_analytics_daily"
FORBIDDEN_KEYS = {"cpf","email","phone","full_name","customer_document","password","password_hash","refresh_token","refresh_token_hash","token_hash","recovery_token_hash","code_hash","firebase_uid","address_token","street","number","neighborhood","zip_code","old_data","new_data","stack_trace","parameters","payload","response","name_ciphertext","name_nonce","name_token","name_masked","email_ciphertext","email_nonce","email_token","email_masked","phone_ciphertext","phone_nonce","phone_token","phone_masked","key_version","encrypted_at"}

def previous_day(reference: datetime | None = None) -> date:
    now = reference.astimezone(SAO_PAULO) if reference else datetime.now(SAO_PAULO)
    return now.date() - timedelta(days=1)

def sanitize(table: str, row: dict) -> dict:
    result = {k: v for k, v in row.items() if k.lower() not in FORBIDDEN_KEYS}
    if table == "address":
        result = {k: row[k] for k in ("address_id","city","state","created_at","updated_at","deleted_at") if k in row}
    elif table == "employee":
        result = {k: result[k] for k in result if k != "name"}
    return result

def jsonable(value):
    return value.isoformat() if isinstance(value, (datetime, date)) else value

def source_key(table: str, row: dict) -> str:
    for key in (f"{table}_id", "id", "source_id", "code"):
        if row.get(key) is not None:
            return str(row[key])
    raw = json.dumps({k: jsonable(v) for k, v in row.items()}, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode()).hexdigest()

def run_daily_analytics(*, reference: datetime | None = None, dry_run: bool | None = None):
    business_date = previous_day(reference)
    if dry_run is None:
        dry_run = os.getenv("ANALYTICS_DRY_RUN", "true").lower() in {"1", "true", "yes"}
    with connection("operational") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (PIPELINE,))
            cur.execute(f"""SELECT source_table, extraction_mode::text, watermark_column, required
                FROM {quote_identifier(ANALYTICS_SCHEMA)}.etl_source_contract
                WHERE enabled AND payload_policy::text <> 'EXCLUDED' ORDER BY load_order""")
            contracts = cur.fetchall()
            if not contracts:
                raise RuntimeError("Contrato etl_source_contract nao encontrado ou desabilitado")
            metadata = {"timezone":"America/Sao_Paulo", "rpa":"Mottanai-RPA", "dry_run":dry_run}
            cur.execute(f"SELECT {quote_identifier(ANALYTICS_SCHEMA)}.fn_begin_daily_batch(%s, NULL, %s, %s)", (business_date, f"rpa-{business_date.isoformat()}", Json(metadata)))
            batch_id = cur.fetchone()[0]
            cur.execute(f"SELECT status::text FROM {quote_identifier(ANALYTICS_SCHEMA)}.etl_daily_batch WHERE batch_id=%s", (batch_id,))
            if cur.fetchone()[0] == "PUBLISHED":
                conn.rollback()
                return {"batch_id": str(batch_id), "business_date": str(business_date), "status": "already_published"}
            try:
                expected_counts, staged_total = {}, 0
                for source_table, extraction_mode, watermark_column, required in contracts:
                    table_sql = f"{quote_identifier(OPERATIONAL_SCHEMA)}.{quote_identifier(source_table)}"
                    if extraction_mode == "SNAPSHOT":
                        sql, params = f"SELECT * FROM {table_sql}", ()
                    else:
                        if not watermark_column:
                            raise RuntimeError(f"Tabela {source_table} sem watermark_column")
                        sql = f"SELECT * FROM {table_sql} WHERE {quote_identifier(watermark_column)} >= %s AND {quote_identifier(watermark_column)} < (%s::date + interval '1 day')"
                        params = (business_date, business_date)
                    source_cur = conn.cursor(cursor_factory=RealDictCursor)
                    try:
                        source_cur.execute(sql, params); rows = source_cur.fetchall()
                    finally:
                        source_cur.close()
                    expected_counts[source_table] = len(rows)
                    for row in rows:
                        raw = dict(row); payload = sanitize(source_table, raw)
                        updated = raw.get(watermark_column) if watermark_column else None
                        cur.execute(f"""SELECT {quote_identifier(ANALYTICS_SCHEMA)}.fn_stage_operational_row(%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (batch_id, source_table, source_key(source_table, raw), Json({k:jsonable(v) for k,v in payload.items()}), raw.get("company_id"), raw.get("store_id"), raw.get("created_at"), updated, raw.get("deleted_at")))
                        staged_total += int(bool(cur.fetchone()[0]))
                cur.execute(f"SELECT {quote_identifier(ANALYTICS_SCHEMA)}.fn_reconcile_daily_batch(%s,%s)", (batch_id, Json(expected_counts)))
                if not cur.fetchone()[0]:
                    raise RuntimeError(f"Reconciliacao rejeitada para o batch {batch_id}")
                cur.execute(f"SELECT {quote_identifier(ANALYTICS_SCHEMA)}.fn_mark_daily_batch_transformed(%s,%s,%s)", (batch_id, staged_total, Json({"source":"operational","date":str(business_date)})))
                cur.execute(f"SELECT {quote_identifier(ANALYTICS_SCHEMA)}.fn_publish_daily_batch(%s)", (batch_id,))
                result = {"batch_id":str(batch_id), "business_date":str(business_date), "rows_staged":staged_total}
                if dry_run:
                    conn.rollback(); result["status"] = "dry_run"; return result
                conn.commit(); result["status"] = "published"; return result
            except Exception as exc:
                conn.rollback(); LOGGER.exception("Falha no batch analitico %s", batch_id)
                raise RuntimeError(f"Falha no batch analitico {batch_id}: {exc}") from exc
