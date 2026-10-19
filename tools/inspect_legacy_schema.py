from pathlib import Path

from db import get_legacy_cursor


OUTPUT_FILE = Path(__file__).resolve().parents[1] / "legacy_schema_report.txt"


TABLES_QUERY = """
SELECT
    table_schema,
    table_name
FROM information_schema.tables
WHERE table_type = 'BASE TABLE'
  AND table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY table_schema, table_name;
"""


COLUMNS_QUERY = """
SELECT
    column_name,
    data_type,
    character_maximum_length,
    numeric_precision,
    numeric_scale,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = %s
  AND table_name = %s
ORDER BY ordinal_position;
"""


PRIMARY_KEYS_QUERY = """
SELECT
    kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
   AND tc.table_schema = kcu.table_schema
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND tc.table_schema = %s
  AND tc.table_name = %s
ORDER BY kcu.ordinal_position;
"""


UNIQUE_QUERY = """
SELECT
    tc.constraint_name,
    kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
   AND tc.table_schema = kcu.table_schema
WHERE tc.constraint_type = 'UNIQUE'
  AND tc.table_schema = %s
  AND tc.table_name = %s
ORDER BY tc.constraint_name, kcu.ordinal_position;
"""


FOREIGN_KEYS_QUERY = """
SELECT
    tc.constraint_name,
    kcu.column_name,
    ccu.table_schema AS referenced_schema,
    ccu.table_name AS referenced_table,
    ccu.column_name AS referenced_column
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
   AND tc.table_schema = kcu.table_schema
JOIN information_schema.constraint_column_usage ccu
    ON ccu.constraint_name = tc.constraint_name
   AND ccu.constraint_schema = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_schema = %s
  AND tc.table_name = %s
ORDER BY tc.constraint_name;
"""


def format_data_type(column):
    data_type = column["data_type"]

    if column["character_maximum_length"]:
        return f"{data_type}({column['character_maximum_length']})"

    if column["numeric_precision"]:
        precision = column["numeric_precision"]
        scale = column["numeric_scale"]

        if scale is not None:
            return f"{data_type}({precision},{scale})"

    return data_type


def inspect_table(cursor, schema, table):
    lines = []

    lines.append("=" * 80)
    lines.append(f"TABELA: {schema}.{table}")
    lines.append("=" * 80)

    cursor.execute(COLUMNS_QUERY, (schema, table))
    columns = cursor.fetchall()

    cursor.execute(PRIMARY_KEYS_QUERY, (schema, table))
    primary_keys = {
        row["column_name"]
        for row in cursor.fetchall()
    }

    cursor.execute(UNIQUE_QUERY, (schema, table))
    unique_rows = cursor.fetchall()

    unique_columns = {
        row["column_name"]
        for row in unique_rows
    }

    cursor.execute(FOREIGN_KEYS_QUERY, (schema, table))
    foreign_keys = cursor.fetchall()

    fk_by_column = {
        row["column_name"]: row
        for row in foreign_keys
    }

    lines.append("")
    lines.append("COLUNAS:")
    lines.append("")

    for column in columns:
        column_name = column["column_name"]
        data_type = format_data_type(column)

        properties = []

        if column_name in primary_keys:
            properties.append("PK")

        if column_name in unique_columns:
            properties.append("UNIQUE")

        if column["is_nullable"] == "NO":
            properties.append("NOT NULL")

        if column_name in fk_by_column:
            fk = fk_by_column[column_name]

            properties.append(
                "FK -> "
                f"{fk['referenced_schema']}."
                f"{fk['referenced_table']}."
                f"{fk['referenced_column']}"
            )

        if column["column_default"]:
            properties.append(
                f"DEFAULT {column['column_default']}"
            )

        property_text = ", ".join(properties)

        if property_text:
            lines.append(
                f"  - {column_name}: {data_type} [{property_text}]"
            )
        else:
            lines.append(
                f"  - {column_name}: {data_type}"
            )

    lines.append("")

    if foreign_keys:
        lines.append("RELACIONAMENTOS:")
        lines.append("")

        for fk in foreign_keys:
            lines.append(
                f"  - {fk['column_name']} -> "
                f"{fk['referenced_schema']}."
                f"{fk['referenced_table']}."
                f"{fk['referenced_column']}"
            )

        lines.append("")

    return lines


def main():
    report = []

    report.append("MOTTAINAI - INSPECAO DO BANCO LEGADO")
    report.append("")

    with get_legacy_cursor() as cursor:
        cursor.execute(
            """
            SELECT
                current_database() AS database,
                current_user AS usuario
            """
        )

        connection_info = cursor.fetchone()

        report.append(
            f"Banco: {connection_info['database']}"
        )
        report.append(
            f"Usuario: {connection_info['usuario']}"
        )
        report.append("")

        cursor.execute(TABLES_QUERY)
        tables = cursor.fetchall()

        report.append(
            f"Total de tabelas encontradas: {len(tables)}"
        )
        report.append("")

        report.append("TABELAS ENCONTRADAS:")
        report.append("")

        for table in tables:
            report.append(
                f"  - {table['table_schema']}."
                f"{table['table_name']}"
            )

        report.append("")
        report.append("")

        for table in tables:
            table_report = inspect_table(
                cursor,
                table["table_schema"],
                table["table_name"],
            )

            report.extend(table_report)

    output = "\n".join(report)

    print(output)

    OUTPUT_FILE.write_text(
        output,
        encoding="utf-8",
    )

    print()
    print(
        f"Relatorio salvo em: {OUTPUT_FILE.resolve()}"
    )


if __name__ == "__main__":
    main()
