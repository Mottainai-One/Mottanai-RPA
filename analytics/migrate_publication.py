"""Apply the idempotent publication-view migration before a manual mart load."""

import os
from pathlib import Path

import psycopg2


MIGRATION = Path(__file__).parent / "migrations" / "14_publish_mart_view.sql"


def main() -> None:
    required = (
        "ANALYTICS_DB_HOST",
        "ANALYTICS_DB_PORT",
        "ANALYTICS_DB_NAME",
        "ANALYTICS_DB_USER",
        "ANALYTICS_DB_PASSWORD",
    )
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise RuntimeError(f"Missing analytical database configuration: {', '.join(missing)}")

    with psycopg2.connect(
        host=os.environ["ANALYTICS_DB_HOST"],
        port=int(os.environ["ANALYTICS_DB_PORT"]),
        dbname=os.environ["ANALYTICS_DB_NAME"],
        user=os.environ["ANALYTICS_DB_USER"],
        password=os.environ["ANALYTICS_DB_PASSWORD"],
        sslmode=os.environ.get("ANALYTICS_DB_SSLMODE", "require"),
        connect_timeout=15,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(MIGRATION.read_text(encoding="utf-8"))
            cursor.execute(
                "SELECT obj_description('mottainai_analytics.vw_published_batch'::regclass)"
            )
            comment = cursor.fetchone()[0]
            if comment != "publication_contract=analytics_mart_v1":
                raise RuntimeError("Publication view migration was not verified")

    print("Analytical publication view migrated and verified")


if __name__ == "__main__":
    main()
