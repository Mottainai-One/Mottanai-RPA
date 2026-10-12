from contextlib import contextmanager
import time
from typing import Iterator

import psycopg2
from psycopg2.extras import RealDictCursor

from config import (
    get_legacy_db_config,
    get_operational_db_config,
)


def connect(kind: str):
    """Connect with bounded exponential backoff; credentials come only from env."""
    config = get_legacy_db_config() if kind == "legacy" else get_operational_db_config()
    last_error = None
    for attempt in range(3):
        try:
            return psycopg2.connect(connect_timeout=15, application_name="mottainai-rpa", **config)
        except psycopg2.OperationalError as exc:
            last_error = exc
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)
    raise last_error  # pragma: no cover


@contextmanager
def connection(kind: str):
    conn = connect(kind)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def get_legacy_cursor():
    """Open a read-only cursor for the legacy database."""
    with connection("legacy") as conn:
        conn.set_session(readonly=True, autocommit=False)
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            yield cursor


@contextmanager
def get_operational_cursor(commit=False):
    """Open a connection to the operational database."""
    with connection("operational") as conn:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                yield cursor
            if commit:
                conn.commit()
        except Exception:
            conn.rollback()
            raise


def fetch_source_rows(settings) -> Iterator[list[dict]]:
    """Stream source records in batches, avoiding an unbounded memory read."""
    from pipeline import quote_identifier
    query = f"SELECT * FROM {quote_identifier(settings.source_schema)}.{quote_identifier(settings.source_table)} ORDER BY {quote_identifier(settings.source_incremental_column)}"
    with connection("legacy") as conn:
        with conn.cursor(name="mottainai_source", cursor_factory=RealDictCursor) as cur:
            cur.itersize = settings.batch_size
            cur.execute(query)
            while True:
                rows = cur.fetchmany(settings.batch_size)
                if not rows:
                    break
                yield rows
