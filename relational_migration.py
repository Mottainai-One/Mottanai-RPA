"""Data-only migration from the legacy public schema to operational mottainai."""
from __future__ import annotations

import logging
import unicodedata
import hashlib
import json
import os
import re
from collections import defaultdict
from typing import Any

from psycopg2.extras import RealDictCursor

from db import connection
from pipeline import quote_identifier
from settings import Settings

LOGGER = logging.getLogger("mottainai.relational_migration")
STATE_CODES = {
    "acre": "AC", "alagoas": "AL", "amapa": "AP", "amazonas": "AM", "bahia": "BA",
    "ceara": "CE", "distrito federal": "DF", "espirito santo": "ES", "goias": "GO",
    "maranhao": "MA", "mato grosso": "MT", "mato grosso do sul": "MS", "minas gerais": "MG",
    "para": "PA", "paraiba": "PB", "parana": "PR", "pernambuco": "PE", "piaui": "PI",
    "rio de janeiro": "RJ", "rio grande do norte": "RN", "rio grande do sul": "RS",
    "rondonia": "RO", "roraima": "RR", "santa catarina": "SC", "sao paulo": "SP",
    "sergipe": "SE", "tocantins": "TO",
}
BCRYPT_RE = re.compile(r"^\$2[aby]\$[0-3][0-9]\$[./A-Za-z0-9]{53}$")


def clean_digits(value: Any) -> str | None:
    if value is None:
        return None
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return digits or None


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    value = " ".join(str(value).replace("\x00", "").strip().split())
    return value or None


def protected_token(prefix: str, value: Any, *, digits: bool = False) -> str | None:
    """Return the deterministic token format required by the operational DB."""
    text = clean_digits(value) if digits else clean_text(value)
    if not text:
        return None
    digest = hashlib.sha256(text.lower().encode("utf-8")).hexdigest().upper()
    return f"{prefix}_SHA256_{digest}"


def is_legacy_masked_email(value: Any) -> bool:
    return bool(value and re.fullmatch(r"(?:email_emp|EMAIL_EMP)_[0-9]+", str(value).strip()))


def is_legacy_masked_phone(value: Any) -> bool:
    return bool(value and re.fullmatch(r"PHONE_EMP_[0-9]+", str(value).strip(), re.IGNORECASE))


def is_legacy_masked_cpf(value: Any) -> bool:
    # The legacy system stores anonymized CPF references as four digits.
    return bool(value and re.fullmatch(r"[0-9]{4}", str(value).strip()))


def preserve_or_protect_email(value: Any) -> str | None:
    text = clean_text(value)
    return text if is_legacy_masked_email(text) else protected_token("EMAIL", text)


def preserve_or_protect_phone(value: Any) -> str | None:
    text = clean_text(value)
    return text if is_legacy_masked_phone(text) else protected_token("PHONE", text)


def preserve_or_protect_cpf(value: Any) -> str | None:
    text = clean_text(value)
    return text if is_legacy_masked_cpf(text) else protected_token("CPF", text, digits=True)


def protected_email(value: Any) -> str | None:
    return protected_token("EMAIL", value)


def protected_phone(value: Any) -> str | None:
    return protected_token("PHONE", value, digits=False)


def protected_cpf(value: Any) -> str | None:
    return protected_token("CPF", value, digits=True)


def protected_employee_name(value: Any) -> str | None:
    text = clean_text(value)
    if not text:
        return None
    if re.fullmatch(r"EMP_[0-9]+", text):
        return text
    return protected_token("EMP", text)


def is_bcrypt(value: Any) -> bool:
    return bool(value and BCRYPT_RE.fullmatch(str(value)))


def normalize_state(value: Any) -> str | None:
    text = clean_text(value)
    if not text:
        return None
    plain = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode().lower()
    return STATE_CODES.get(plain, text[:2].upper())


def q(schema: str, table: str) -> str:
    return f"{quote_identifier(schema)}.{quote_identifier(table)}"


def rows(cur, schema: str, table: str) -> list[dict[str, Any]]:
    cur.execute(f"SELECT * FROM {q(schema, table)}")
    return [dict(row) for row in cur.fetchall()]


def validate_legacy_migration_contract(cur) -> None:
    """Fail before writes when PR 34 finalization removed legacy PII columns."""
    cur.execute("""
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'mottainai'
          AND table_name IN ('employee', 'app_user')
          AND column_name IN ('name', 'cpf', 'email', 'phone')
    """)
    found = {(row[0], row[1]) for row in cur.fetchall()}
    employee_columns = {"name", "cpf", "email", "phone"}
    app_user_columns = {"cpf", "email"}
    employee_found = {column for table, column in found if table == "employee"}
    app_user_found = {column for table, column in found if table == "app_user"}
    if not employee_columns.issubset(employee_found) or not app_user_columns.issubset(app_user_found):
        raise RuntimeError(
            "PR 34 finalization detected: employee/app_user PII columns were removed. "
            "The legacy migration requires an approved AES-256-GCM encryption step "
            "to populate mottainai.employee_private_data before loading. "
            "Do not run the real migration until that encryption integration is configured."
        )


def upsert(cur, target: str, values: dict[str, Any], conflict: str, primary_key: str) -> int:
    values = {key: value for key, value in values.items() if value is not None or key not in {"created_at", "updated_at"}}
    columns = list(values)
    columns_sql = ", ".join(quote_identifier(c) for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    updates = ", ".join(f"{quote_identifier(c)} = EXCLUDED.{quote_identifier(c)}" for c in columns if c != conflict)
    sql = f"INSERT INTO {target} ({columns_sql}) VALUES ({placeholders}) ON CONFLICT ({quote_identifier(conflict)}) DO UPDATE SET {updates} RETURNING {quote_identifier(primary_key)}"
    cur.execute(sql, [values[column] for column in columns])
    return cur.fetchone()[0]


def upsert_address(cur, target: str, values: dict[str, Any]) -> int:
    values = {key: value for key, value in values.items() if value is not None or key not in {"created_at", "updated_at"}}
    cur.execute(f"SELECT address_id FROM {target} WHERE zip_code=%s AND street=%s AND number=%s LIMIT 1", (values["zip_code"], values["street"], values["number"]))
    found = cur.fetchone()
    if found:
        cur.execute(f"UPDATE {target} SET complement=%s, neighborhood=%s, city=%s, state=%s, updated_at=COALESCE(%s, updated_at), deleted_at=%s WHERE address_id=%s", (values.get("complement"), values["neighborhood"], values["city"], values["state"], values.get("updated_at"), values.get("deleted_at"), found[0]))
        return found[0]
    columns = list(values)
    cur.execute(f"INSERT INTO {target} ({', '.join(quote_identifier(c) for c in columns)}) VALUES ({', '.join(['%s'] * len(columns))}) RETURNING address_id", [values[c] for c in columns])
    return cur.fetchone()[0]


def upsert_app_user(cur, target: str, values: dict[str, Any]) -> int:
    """Upsert by either unique identity because employee_id and email are unique."""
    values = {key: value for key, value in values.items() if value is not None or key not in {"created_at", "updated_at"}}
    cur.execute(f"SELECT user_id FROM {target} WHERE employee_id=%s LIMIT 1", (values["employee_id"],))
    found = cur.fetchone()
    if not found:
        cur.execute(f"SELECT user_id FROM {target} WHERE email=%s LIMIT 1", (values["email"],))
        found = cur.fetchone()
    if found:
        updates = ", ".join(f"{quote_identifier(c)}=%s" for c in values if c not in {"employee_id", "email", "created_at"})
        update_values = [values[c] for c in values if c not in {"employee_id", "email", "created_at"}]
        cur.execute(f"UPDATE {target} SET {updates} WHERE user_id=%s RETURNING user_id", (*update_values, found[0]))
        return cur.fetchone()[0]
    columns = list(values)
    cur.execute(f"INSERT INTO {target} ({', '.join(quote_identifier(c) for c in columns)}) VALUES ({', '.join(['%s'] * len(columns))}) RETURNING user_id", [values[c] for c in columns])
    return cur.fetchone()[0]


def migrate(settings: Settings, dry_run: bool = True) -> dict[str, int]:
    source_schema = settings.source_schema
    target_schema = settings.destination_schema
    counts = defaultdict(int)
    quarantine: list[dict[str, Any]] = []

    def quarantine_record(table: str, source_id: Any, reason: str) -> None:
        # Deliberately do not persist source payloads or secrets.
        quarantine.append({"table": table, "source_id": source_id, "reason": reason})
    with connection("legacy") as source, connection("operational") as target:
        source_cursor = source.cursor(cursor_factory=RealDictCursor)
        target_cursor = target.cursor()
        try:
            validate_legacy_migration_contract(target_cursor)
            source_cursor.execute("SET TRANSACTION READ ONLY")
            # Read all source rows before changing the destination. This keeps a
            # consistent source snapshot and makes dry-run fully non-mutating.
            source_data = {name: rows(source_cursor, source_schema, name) for name in (
                "address", "employeerole", "subscriptionplan", "company", "store", "employee", "useraccount", "administration"
            )}
            maps: dict[str, dict[Any, Any]] = defaultdict(dict)

            # 1) Independent entities.
            for item in source_data["address"]:
                values = {
                    "zip_code": (clean_digits(item["zipcode"]) or "")[:8],
                    "street": clean_text(item["street"]), "number": clean_text(item["number"]),
                    "complement": clean_text(item.get("complement")), "neighborhood": clean_text(item["neighborhood"]),
                    "city": clean_text(item["city"]), "state": normalize_state(item["state"]),
                    "created_at": item.get("createdat"), "updated_at": item.get("updatedat"), "deleted_at": item.get("deletedat"),
                }
                maps["address"][item["addressid"]] = upsert_address(target_cursor, q(target_schema, "address"), values)
                counts["address"] += 1

            for item in source_data["employeerole"]:
                values = {"name": clean_text(item["name"]), "description": clean_text(item.get("description")), "permission_level": item["permissionlevel"], "active": item["active"], "created_at": item.get("createdat"), "updated_at": item.get("updatedat"), "deleted_at": item.get("deletedat")}
                maps["role"][item["roleid"]] = upsert(target_cursor, q(target_schema, "employee_role"), values, "name", "role_id")
                counts["employee_role"] += 1

            # Legacy plans are attached to companies; the operational model is
            # the inverse, so first create plans keyed by their unique name.
            for item in source_data["subscriptionplan"]:
                values = {"name": clean_text(item["name"]), "description": clean_text(item["description"]), "price": item["price"], "store_limit": item["storelimit"], "user_limit": item["userlimit"], "active": item["active"], "created_at": item.get("createdat"), "updated_at": item.get("updatedat"), "deleted_at": item.get("deletedat")}
                maps["plan"][item["planid"]] = upsert(target_cursor, q(target_schema, "subscription_plan"), values, "name", "plan_id")
                counts["subscription_plan"] += 1

            # 2) Company, store, employee and account dependency chain.
            for item in source_data["company"]:
                plan = next((p for p in source_data["subscriptionplan"] if p["companyid"] == item["companyid"]), None)
                if not plan:
                    raise ValueError(f"Empresa {item['companyid']} sem plano legado")
                values = {"plan_id": maps["plan"].get(plan["planid"], 0), "official_name": clean_text(item["officialname"]), "trade_name": clean_text(item.get("tradename")), "cnpj": clean_digits(item["cnpj"]), "email": preserve_or_protect_email(item["email"]), "phone": preserve_or_protect_phone(item.get("phone")), "active": item["active"], "created_at": item.get("createdat"), "updated_at": item.get("updatedat"), "deleted_at": item.get("deletedat")}
                maps["company"][item["companyid"]] = upsert(target_cursor, q(target_schema, "company"), values, "cnpj", "company_id")
                counts["company"] += 1

            for item in source_data["store"]:
                values = {"company_id": maps["company"].get(item["companyid"], 0), "address_id": maps["address"].get(item["addressid"], 0), "name": clean_text(item["name"]), "cnpj": clean_digits(item["cnpj"]), "email": preserve_or_protect_email(item["email"]), "phone": preserve_or_protect_phone(item["phone"]), "active": item["active"], "created_at": item.get("createdat"), "updated_at": item.get("updatedat"), "deleted_at": item.get("deletedat")}
                maps["store"][item["storeid"]] = upsert(target_cursor, q(target_schema, "retail_store"), values, "cnpj", "store_id")
                counts["retail_store"] += 1

            for item in source_data["employee"]:
                source_store = next((store for store in source_data["store"] if store["storeid"] == item["storeid"]), None)
                if not source_store:
                    raise ValueError(f"Funcionário {item['employeeid']} sem loja legada")
                operational_company_id = maps["company"].get(source_store["companyid"])
                if not operational_company_id:
                    raise ValueError(f"Loja {item['storeid']} sem empresa operacional mapeada")
                target_cursor.execute(
                    "SELECT mottainai.fn_set_legacy_migration_context(%s)",
                    (operational_company_id,),
                )
                values = {"store_id": maps["store"].get(item["storeid"], 0), "role_id": maps["role"].get(item["roleid"], 0), "name": protected_employee_name(item["name"]), "cpf": preserve_or_protect_cpf(item["cpf"]), "email": preserve_or_protect_email(item["email"]), "phone": preserve_or_protect_phone(item["phone"]), "active": item["active"], "hire_date": item["hiredate"], "created_at": item.get("createdat"), "updated_at": item.get("updatedat"), "deleted_at": item.get("deletedat")}
                maps["employee"][item["employeeid"]] = upsert(target_cursor, q(target_schema, "employee"), values, "cpf", "employee_id")
                counts["employee"] += 1

            for item in source_data["useraccount"]:
                employee = next((employee for employee in source_data["employee"] if employee["employeeid"] == item["employeeid"]), None)
                if not employee:
                    raise ValueError(f"Usuário {item['userid']} sem funcionário legado")
                if not is_bcrypt(item["passwordhash"]):
                    quarantine_record("useraccount", item["userid"], "password_hash não está em formato BCrypt")
                    counts["useraccount_quarantined"] += 1
                    continue
                values = {"employee_id": maps["employee"].get(item["employeeid"], 0), "cpf": preserve_or_protect_cpf(employee["cpf"]), "email": preserve_or_protect_email(item["email"]), "password_hash": item["passwordhash"], "last_login": item.get("lastlogin"), "active": item["active"], "created_at": item.get("createdat"), "updated_at": item.get("updatedat"), "deleted_at": item.get("deletedat")}
                maps["user"][item["userid"]] = upsert_app_user(target_cursor, q(target_schema, "app_user"), values)
                counts["system_user"] += 1

            # Administration is not linked to employee in the target model.
            counts["administration"] = len(source_data["administration"])
            counts["administration_quarantined"] = len(source_data["administration"])
            quarantine_path = os.getenv("MIGRATION_QUARANTINE_FILE", "migration_quarantine.json")
            with open(quarantine_path, "w", encoding="utf-8") as output:
                json.dump(quarantine, output, ensure_ascii=False, indent=2)
            if dry_run:
                target.rollback()
            else:
                target.commit()
            return dict(counts)
        except Exception:
            target.rollback()
            raise
        finally:
            source_cursor.close(); target_cursor.close()
