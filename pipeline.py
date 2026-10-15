"""Reliable, generic PostgreSQL-to-PostgreSQL integration pipeline.

The business mapping is deliberately isolated in ``transform_record`` so it can
be replaced once the source and destination schemas are confirmed. The current
generic reader is ordered and batch-based; watermark filtering is enabled after
the incremental column semantics are confirmed.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import unicodedata
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from db import connection, fetch_source_rows

LOGGER = logging.getLogger("mottainai.pipeline")
SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def quote_identifier(name: str) -> str:
    if not SAFE_IDENTIFIER.fullmatch(name):
        raise ValueError(f"Identificador SQL inválido: {name!r}")
    return f'"{name}"'


def normalize_text(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    value = unicodedata.normalize("NFKC", value).replace("\x00", "")
    return " ".join(value.strip().split()) or None


def normalize_value(value: Any) -> Any:
    if isinstance(value, str):
        return normalize_text(value)
    if isinstance(value, Decimal):
        return value.quantize(Decimal("0.01"))
    return value


def record_fingerprint(record: dict[str, Any]) -> str:
    payload = json.dumps(record, default=str, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def transform_record(record: dict[str, Any]) -> dict[str, Any]:
    """Clean one source record; extend this function with business mapping."""
    cleaned = {key: normalize_value(value) for key, value in record.items()}
    cleaned["_source_fingerprint"] = record_fingerprint(cleaned)
    cleaned["_loaded_at"] = datetime.utcnow()
    return cleaned


def ensure_control_tables(cur, schema: str) -> None:
    schema_q = quote_identifier(schema)
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_q}")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema_q}.rpa_run (
            run_id BIGSERIAL PRIMARY KEY, started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            finished_at TIMESTAMPTZ, status TEXT NOT NULL, rows_read INTEGER DEFAULT 0,
            rows_loaded INTEGER DEFAULT 0, rows_quarantined INTEGER DEFAULT 0, error TEXT
        )
    """)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema_q}.rpa_quarantine (
            id BIGSERIAL PRIMARY KEY, run_id BIGINT NOT NULL, payload JSONB NOT NULL,
            error TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema_q}.rpa_watermark (
            pipeline TEXT PRIMARY KEY, value TEXT, updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)


def validate_record(record: dict[str, Any]) -> None:
    if not record:
        raise ValueError("registro vazio")
    if any(not SAFE_IDENTIFIER.fullmatch(str(k)) for k in record):
        raise ValueError("nome de coluna inválido")


def load_batch(cur, schema: str, table: str, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    columns = list(rows[0])
    table_q = f"{quote_identifier(schema)}.{quote_identifier(table)}"
    cols_q = ", ".join(quote_identifier(c) for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    # Destination tables should have a unique constraint on _source_fingerprint.
    sql = f"INSERT INTO {table_q} ({cols_q}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
    cur.executemany(sql, [[row.get(column) for column in columns] for row in rows])
    return cur.rowcount


def run_pipeline(settings) -> dict[str, int]:
    required = (settings.source_table, settings.destination_table)
    if not all(required):
        raise ValueError("SOURCE_TABLE e DESTINATION_TABLE são obrigatórios")
    stats = {"read": 0, "loaded": 0, "quarantined": 0}
    started = datetime.utcnow()
    with connection("operational") as target:
        with target.cursor() as cur:
            ensure_control_tables(cur, settings.destination_schema)
            cur.execute(f"INSERT INTO {quote_identifier(settings.destination_schema)}.rpa_run(status) VALUES ('RUNNING') RETURNING run_id")
            run_id = cur.fetchone()[0]
        target.commit()
        try:
            for source_rows in fetch_source_rows(settings):
                stats["read"] += len(source_rows)
                valid, invalid = [], []
                for row in source_rows:
                    try:
                        transformed = transform_record(dict(row))
                        validate_record(transformed)
                        valid.append(transformed)
                    except Exception as exc:
                        invalid.append((json.dumps(dict(row), default=str), str(exc)))
                with target.cursor() as cur:
                    if invalid:
                        cur.executemany(f"INSERT INTO {quote_identifier(settings.destination_schema)}.rpa_quarantine(run_id,payload,error) VALUES (%s,%s::jsonb,%s)", [(run_id, payload, error) for payload, error in invalid])
                    stats["quarantined"] += len(invalid)
                    if not settings.dry_run:
                        stats["loaded"] += load_batch(cur, settings.destination_schema, settings.destination_table, valid)
                target.commit()
            with target.cursor() as cur:
                cur.execute(f"UPDATE {quote_identifier(settings.destination_schema)}.rpa_run SET finished_at=now(), status='SUCCESS', rows_read=%s, rows_loaded=%s, rows_quarantined=%s WHERE run_id=%s", (*stats.values(), run_id))
            target.commit()
            return stats
        except Exception as exc:
            target.rollback()
            with target.cursor() as cur:
                cur.execute(f"UPDATE {quote_identifier(settings.destination_schema)}.rpa_run SET finished_at=now(), status='FAILED', error=%s WHERE run_id=%s", (str(exc)[:4000], run_id))
            target.commit()
            raise
