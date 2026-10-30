"""Data-only migration from the legacy public schema to operational mottainai."""
from __future__ import annotations

import logging
import unicodedata
import hashlib
import hmac
import base64
import json
import os
import re
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
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


class EmployeeProtector:
    """Create reversible employee envelopes and non-reversible lookup keys."""

    def __init__(self, aes_key: bytes, lookup_key: bytes, key_version: int):
        if len(aes_key) != 32 or len(lookup_key) != 32:
            raise ValueError("Employee AES and lookup keys must each contain exactly 32 bytes")
        if key_version <= 0 or key_version > 32767:
            raise ValueError("EMPLOYEE_PII_KEY_VERSION must be between 1 and 32767")
        self._aes = AESGCM(aes_key)
        self._lookup_key = lookup_key
        self.key_version = key_version

    @classmethod
    def from_environment(cls) -> "EmployeeProtector":
        def decode(name: str) -> bytes:
            value = os.getenv(name)
            if not value:
                raise ValueError(f"{name} is required for the employee PII migration")
            try:
                return base64.b64decode(value, validate=True)
            except ValueError as exc:
                raise ValueError(f"{name} must be valid Base64") from exc

        try:
            key_version = int(os.getenv("EMPLOYEE_PII_KEY_VERSION", "1"))
        except ValueError as exc:
            raise ValueError("EMPLOYEE_PII_KEY_VERSION must be an integer") from exc
        return cls(
            decode("EMPLOYEE_PII_AES_KEY_B64"),
            decode("EMPLOYEE_PII_LOOKUP_KEY_B64"),
            key_version,
        )

    def lookup(self, field: str, value: Any, *, digits: bool = False) -> bytes | None:
        normalized = clean_digits(value) if digits else clean_text(value)
        if not normalized:
            return None
        if not digits:
            normalized = normalized.casefold()
        payload = f"mottainai:employee:{field}:v1:{normalized}".encode("utf-8")
        return hmac.new(self._lookup_key, payload, hashlib.sha256).digest()

    def fingerprint(self, source_table: str, row: dict[str, Any]) -> str:
        """Keyed fingerprint prevents disclosure of low-entropy source fields."""
        payload = json.dumps(row, sort_keys=True, default=str, ensure_ascii=False)
        message = f"mottainai:legacy-sync:{source_table}:v1:{payload}".encode("utf-8")
        return hmac.new(self._lookup_key, message, hashlib.sha256).hexdigest().upper()

    def encrypt(self, employee_id: int, field: str, value: Any) -> tuple[bytes | None, bytes | None]:
        plaintext = clean_text(value)
        if plaintext is None:
            return None, None
        nonce = os.urandom(12)
        aad = f"mottainai:employee:{employee_id}:{field}:v1".encode("utf-8")
        return self._aes.encrypt(nonce, plaintext.encode("utf-8"), aad), nonce


def mask_name(employee_id: int) -> str:
    return f"EMPLOYEE_{employee_id}"


def mask_cpf(value: Any) -> str | None:
    return None if clean_text(value) is None else "***.***.***-**"


def mask_email(value: Any) -> str | None:
    return None if clean_text(value) is None else "***@***"


def mask_phone(value: Any) -> str | None:
    return None if clean_text(value) is None else "***********"


def clean_digits(value: Any) -> str | None:
    if value is None:
        return None
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return digits or None


def valid_cnpj(value: Any) -> bool:
    """Mirror the operational check-digit rule before writing a company/store."""
    digits = clean_digits(value)
    if not digits or len(digits) != 14 or len(set(digits)) == 1:
        return False
    numbers = [int(char) for char in digits]
    for length, weights in (
        (12, (5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2)),
        (13, (6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2)),
    ):
        remainder = sum(n * w for n, w in zip(numbers[:length], weights)) % 11
        expected = 0 if remainder < 2 else 11 - remainder
        if numbers[length] != expected:
            return False
    return True


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


def legacy_plan_id(company: dict[str, Any]) -> Any:
    """Read the plan FK from the company row, where the legacy model stores it."""
    for key in ("planid", "subscriptionplanid", "subscription_plan_id"):
        if company.get(key) is not None:
            return company[key]
    raise ValueError(
        f"Empresa {company.get('companyid')} sem coluna de plano reconhecida "
        "(planid, subscriptionplanid ou subscription_plan_id)"
    )


def q(schema: str, table: str) -> str:
    return f"{quote_identifier(schema)}.{quote_identifier(table)}"


def rows(cur, schema: str, table: str) -> list[dict[str, Any]]:
    cur.execute(f"SELECT * FROM {q(schema, table)}")
    return [dict(row) for row in cur.fetchall()]


def table_columns(cur, schema: str, table: str) -> set[str]:
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema=%s AND table_name=%s",
        (schema, table),
    )
    return {row[0] for row in cur.fetchall()}


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


def upsert_employee(cur, target: str, values: dict[str, Any]) -> int:
    """Upsert employee through the stable, secret-keyed CPF lookup digest."""
    values = {key: value for key, value in values.items() if value is not None or key not in {"created_at", "updated_at"}}
    cur.execute(f"SELECT employee_id FROM {target} WHERE cpf_lookup_hmac=%s LIMIT 1", (values["cpf_lookup_hmac"],))
    found = cur.fetchone()
    if found:
        updates = ", ".join(f"{quote_identifier(c)}=%s" for c in values if c not in {"cpf_lookup_hmac", "created_at"})
        update_values = [values[c] for c in values if c not in {"cpf_lookup_hmac", "created_at"}]
        cur.execute(f"UPDATE {target} SET {updates} WHERE employee_id=%s RETURNING employee_id", (*update_values, found[0]))
        return cur.fetchone()[0]
    columns = list(values)
    cur.execute(f"INSERT INTO {target} ({', '.join(quote_identifier(c) for c in columns)}) VALUES ({', '.join(['%s'] * len(columns))}) RETURNING employee_id", [values[c] for c in columns])
    return cur.fetchone()[0]


def upsert_app_user(cur, target: str, values: dict[str, Any]) -> int:
    """Upsert an account by the stable one-to-one employee relationship."""
    values = {key: value for key, value in values.items() if value is not None or key not in {"created_at", "updated_at"}}
    cur.execute(f"SELECT user_id FROM {target} WHERE employee_id=%s LIMIT 1", (values["employee_id"],))
    found = cur.fetchone()
    if found:
        updates = ", ".join(f"{quote_identifier(c)}=%s" for c in values if c not in {"employee_id", "created_at"})
        update_values = [values[c] for c in values if c not in {"employee_id", "created_at"}]
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
    protector = EmployeeProtector.from_environment()

    def quarantine_record(table: str, source_id: Any, reason: str) -> None:
        # Deliberately do not persist source payloads or secrets.
        quarantine.append({"table": table, "source_id": source_id, "reason": reason})
    with connection("legacy") as source, connection("operational") as target:
        source_cursor = source.cursor(cursor_factory=RealDictCursor)
        target_cursor = target.cursor()
        try:
            source_cursor.execute("SET TRANSACTION READ ONLY")
            target_cursor.execute("SET LOCAL ROLE mottainai_migrator")
            # Read all source rows before changing the destination. This keeps a
            # consistent source snapshot and makes dry-run fully non-mutating.
            source_data = {name: rows(source_cursor, source_schema, name) for name in (
                "address", "employeerole", "subscriptionplan", "company", "store", "employee", "useraccount", "administration"
            )}
            LOGGER.info("Legacy snapshot read: %s", {name: len(items) for name, items in source_data.items()})
            maps: dict[str, dict[Any, Any]] = defaultdict(dict)
            employee_columns = table_columns(target_cursor, target_schema, "employee")
            app_user_columns = table_columns(target_cursor, target_schema, "app_user")
            target_cursor.execute(
                f"SELECT source_table, source_record_id, target_record_id, payload_sha256 "
                f"FROM {q(target_schema, 'legacy_sync_record')} "
                "WHERE source_system='LEGACY_POSTGRES' AND sync_status='APPLIED'"
            )
            sync_index = {(table, source_id): (int(target_id), digest)
                          for table, source_id, target_id, digest in target_cursor.fetchall()}
            force_full = os.getenv("MIGRATION_FORCE_FULL", "false").lower() in {"1", "true", "yes"}

            def prior_target(table: str, item: dict[str, Any], source_id: Any) -> int | None:
                previous = sync_index.get((table, str(source_id)))
                if previous and not force_full and previous[1] == protector.fingerprint(table, item):
                    counts[f"{table}_unchanged"] += 1
                    return previous[0]
                return None

            def record_sync(table: str, item: dict[str, Any], source_id: Any,
                            target_table: str, target_id: int) -> None:
                target_cursor.execute(
                    f"INSERT INTO {q(target_schema, 'legacy_sync_record')} "
                    "(source_system, source_table, source_record_id, target_table, "
                    "target_record_id, payload_sha256, sync_status) "
                    "VALUES ('LEGACY_POSTGRES', %s, %s, %s, %s, %s, 'APPLIED') "
                    "ON CONFLICT (source_system, source_table, source_record_id) "
                    "DO UPDATE SET target_table=EXCLUDED.target_table, "
                    "target_record_id=EXCLUDED.target_record_id, "
                    "payload_sha256=EXCLUDED.payload_sha256, sync_status='APPLIED', "
                    "synchronized_at=now(), error_code=NULL",
                    (table, str(source_id), target_table, str(target_id),
                     protector.fingerprint(table, item)),
                )

            # 1) Independent entities.
            for item in source_data["address"]:
                prior = prior_target("address", item, item["addressid"])
                if prior is not None:
                    maps["address"][item["addressid"]] = prior
                    continue
                values = {
                    "zip_code": (clean_digits(item["zipcode"]) or "")[:8],
                    "street": clean_text(item["street"]), "number": clean_text(item["number"]),
                    "complement": clean_text(item.get("complement")), "neighborhood": clean_text(item["neighborhood"]),
                    "city": clean_text(item["city"]), "state": normalize_state(item["state"]),
                    "created_at": item.get("createdat"), "updated_at": item.get("updatedat"), "deleted_at": item.get("deletedat"),
                }
                maps["address"][item["addressid"]] = upsert_address(target_cursor, q(target_schema, "address"), values)
                record_sync("address", item, item["addressid"], "address", maps["address"][item["addressid"]])
                counts["address"] += 1

            for item in source_data["employeerole"]:
                prior = prior_target("employeerole", item, item["roleid"])
                if prior is not None:
                    maps["role"][item["roleid"]] = prior
                    continue
                values = {"name": clean_text(item["name"]), "description": clean_text(item.get("description")), "permission_level": item["permissionlevel"], "active": item["active"], "created_at": item.get("createdat"), "updated_at": item.get("updatedat"), "deleted_at": item.get("deletedat")}
                maps["role"][item["roleid"]] = upsert(target_cursor, q(target_schema, "employee_role"), values, "name", "role_id")
                record_sync("employeerole", item, item["roleid"], "employee_role", maps["role"][item["roleid"]])
                counts["employee_role"] += 1

            # Legacy plans are attached to companies; the operational model is
            # the inverse, so first create plans keyed by their unique name.
            for item in source_data["subscriptionplan"]:
                prior = prior_target("subscriptionplan", item, item["planid"])
                if prior is not None:
                    maps["plan"][item["planid"]] = prior
                    continue
                values = {"name": clean_text(item["name"]), "description": clean_text(item["description"]), "price": item["price"], "store_limit": item["storelimit"], "user_limit": item["userlimit"], "active": item["active"], "created_at": item.get("createdat"), "updated_at": item.get("updatedat"), "deleted_at": item.get("deletedat")}
                maps["plan"][item["planid"]] = upsert(target_cursor, q(target_schema, "subscription_plan"), values, "name", "plan_id")
                record_sync("subscriptionplan", item, item["planid"], "subscription_plan", maps["plan"][item["planid"]])
                counts["subscription_plan"] += 1

            # 2) Company, store, employee and account dependency chain.
            for item in source_data["company"]:
                if not valid_cnpj(item.get("cnpj")):
                    quarantine_record("company", item["companyid"], "CNPJ inválido")
                    counts["company_quarantined"] += 1
                    continue
                prior = prior_target("company", item, item["companyid"])
                if prior is not None:
                    maps["company"][item["companyid"]] = prior
                    continue
                plan_id = legacy_plan_id(item)
                plan = next((p for p in source_data["subscriptionplan"] if p["planid"] == plan_id), None)
                if not plan:
                    raise ValueError(f"Empresa {item['companyid']} referencia plano legado inexistente: {plan_id}")
                values = {"plan_id": maps["plan"].get(plan["planid"], 0), "official_name": clean_text(item["officialname"]), "trade_name": clean_text(item.get("tradename")), "cnpj": clean_digits(item["cnpj"]), "email": preserve_or_protect_email(item["email"]), "phone": preserve_or_protect_phone(item.get("phone")), "active": item["active"], "created_at": item.get("createdat"), "updated_at": item.get("updatedat"), "deleted_at": item.get("deletedat")}
                maps["company"][item["companyid"]] = upsert(target_cursor, q(target_schema, "company"), values, "cnpj", "company_id")
                record_sync("company", item, item["companyid"], "company", maps["company"][item["companyid"]])
                counts["company"] += 1

            LOGGER.info("Company phase completed: %s loaded, %s quarantined", counts["company"], counts["company_quarantined"])

            for item in source_data["store"]:
                if item["companyid"] not in maps["company"]:
                    quarantine_record("store", item["storeid"], "empresa não migrada")
                    counts["store_quarantined"] += 1
                    continue
                if not valid_cnpj(item.get("cnpj")):
                    quarantine_record("store", item["storeid"], "CNPJ inválido")
                    counts["store_quarantined"] += 1
                    continue
                prior = prior_target("store", item, item["storeid"])
                if prior is not None:
                    maps["store"][item["storeid"]] = prior
                    continue
                target_cursor.execute(
                    "SELECT mottainai.fn_set_legacy_migration_context(%s)",
                    (maps["company"][item["companyid"]],),
                )
                values = {"company_id": maps["company"].get(item["companyid"], 0), "address_id": maps["address"].get(item["addressid"], 0), "name": clean_text(item["name"]), "cnpj": clean_digits(item["cnpj"]), "email": preserve_or_protect_email(item["email"]), "phone": preserve_or_protect_phone(item["phone"]), "active": item["active"], "created_at": item.get("createdat"), "updated_at": item.get("updatedat"), "deleted_at": item.get("deletedat")}
                maps["store"][item["storeid"]] = upsert(target_cursor, q(target_schema, "retail_store"), values, "cnpj", "store_id")
                record_sync("store", item, item["storeid"], "retail_store", maps["store"][item["storeid"]])
                counts["retail_store"] += 1

            LOGGER.info("Store phase completed: %s loaded, %s quarantined", counts["retail_store"], counts["store_quarantined"])

            for item in source_data["employee"]:
                if item["storeid"] not in maps["store"]:
                    quarantine_record("employee", item["employeeid"], "loja não migrada")
                    counts["employee_quarantined"] += 1
                    continue
                prior = prior_target("employee", item, item["employeeid"])
                if prior is not None:
                    maps["employee"][item["employeeid"]] = prior
                    continue
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
                if clean_text(item.get("name")) is None or clean_text(item.get("cpf")) is None:
                    raise ValueError(
                        f"Funcionário {item['employeeid']} sem nome/CPF recuperável para o envelope AES-GCM"
                    )
                values = {
                    "store_id": maps["store"].get(item["storeid"], 0),
                    "role_id": maps["role"].get(item["roleid"], 0),
                    "cpf_lookup_hmac": protector.lookup("cpf", item["cpf"], digits=True),
                    "email_lookup_hmac": protector.lookup("email", item.get("email")),
                    "active": item["active"], "hire_date": item["hiredate"],
                    "created_at": item.get("createdat"), "updated_at": item.get("updatedat"),
                    "deleted_at": item.get("deletedat"),
                }
                legacy_values = {
                    "name": protected_employee_name(item["name"]),
                    "cpf": preserve_or_protect_cpf(item["cpf"]),
                    "email": preserve_or_protect_email(item.get("email")),
                    "phone": preserve_or_protect_phone(item.get("phone")),
                }
                values.update({key: value for key, value in legacy_values.items() if key in employee_columns})
                employee_id = upsert_employee(target_cursor, q(target_schema, "employee"), values)
                maps["employee"][item["employeeid"]] = employee_id

                name_ciphertext, name_nonce = protector.encrypt(employee_id, "name", item["name"])
                cpf_ciphertext, cpf_nonce = protector.encrypt(employee_id, "cpf", item["cpf"])
                email_ciphertext, email_nonce = protector.encrypt(employee_id, "email", item.get("email"))
                phone_ciphertext, phone_nonce = protector.encrypt(employee_id, "phone", item.get("phone"))
                private_values = {
                    "employee_id": employee_id,
                    "name_ciphertext": name_ciphertext, "name_nonce": name_nonce,
                    "name_masked": mask_name(employee_id),
                    "cpf_ciphertext": cpf_ciphertext, "cpf_nonce": cpf_nonce,
                    "cpf_masked": mask_cpf(item["cpf"]),
                    "email_ciphertext": email_ciphertext, "email_nonce": email_nonce,
                    "email_masked": mask_email(item.get("email")),
                    "phone_ciphertext": phone_ciphertext, "phone_nonce": phone_nonce,
                    "phone_masked": mask_phone(item.get("phone")),
                    "key_version": protector.key_version,
                    "algorithm": "AES-256-GCM", "protection_state": "ENCRYPTED",
                    "encrypted_at": datetime.now(timezone.utc),
                    "updated_at": item.get("updatedat"),
                }
                upsert(
                    target_cursor,
                    q(target_schema, "employee_private_data"),
                    private_values,
                    "employee_id",
                    "employee_id",
                )
                record_sync("employee", item, item["employeeid"], "employee", employee_id)
                counts["employee"] += 1
                counts["employee_private_data"] += 1

            LOGGER.info("Employee phase completed: %s loaded, %s quarantined", counts["employee"], counts["employee_quarantined"])

            for item in source_data["useraccount"]:
                if item["employeeid"] not in maps["employee"]:
                    quarantine_record("useraccount", item["userid"], "funcionário não migrado")
                    counts["useraccount_quarantined"] += 1
                    continue
                prior = prior_target("useraccount", item, item["userid"])
                if prior is not None:
                    maps["user"][item["userid"]] = prior
                    continue
                employee = next((employee for employee in source_data["employee"] if employee["employeeid"] == item["employeeid"]), None)
                if not employee:
                    raise ValueError(f"Usuário {item['userid']} sem funcionário legado")
                if not is_bcrypt(item["passwordhash"]):
                    quarantine_record("useraccount", item["userid"], "password_hash não está em formato BCrypt")
                    counts["useraccount_quarantined"] += 1
                    continue
                source_store = next((store for store in source_data["store"] if store["storeid"] == employee["storeid"]), None)
                if not source_store:
                    raise ValueError(f"Usuário {item['userid']} referencia funcionário sem loja legada")
                operational_company_id = maps["company"].get(source_store["companyid"])
                target_cursor.execute(
                    "SELECT mottainai.fn_set_legacy_migration_context(%s)",
                    (operational_company_id,),
                )
                source_firebase_uid = clean_text(item.get("firebase_uid") or item.get("firebaseuid"))
                target_cursor.execute(
                    f"SELECT firebase_uid FROM {q(target_schema, 'app_user')} WHERE employee_id=%s LIMIT 1",
                    (maps["employee"].get(item["employeeid"], 0),),
                )
                existing_account = target_cursor.fetchone()
                effective_firebase_uid = source_firebase_uid or (
                    existing_account[0] if existing_account else None
                )
                values = {
                    "employee_id": maps["employee"].get(item["employeeid"], 0),
                    "password_hash": item["passwordhash"], "last_login": item.get("lastlogin"),
                    "active": bool(item["active"] and effective_firebase_uid),
                    "created_at": item.get("createdat"), "updated_at": item.get("updatedat"),
                    "deleted_at": item.get("deletedat"),
                }
                optional_values = {
                    "cpf": preserve_or_protect_cpf(employee["cpf"]),
                    "email": preserve_or_protect_email(item["email"]),
                    "password_set": True,
                }
                if source_firebase_uid:
                    optional_values["firebase_uid"] = source_firebase_uid
                values.update({key: value for key, value in optional_values.items() if key in app_user_columns})
                maps["user"][item["userid"]] = upsert_app_user(target_cursor, q(target_schema, "app_user"), values)
                record_sync("useraccount", item, item["userid"], "app_user", maps["user"][item["userid"]])
                counts["system_user"] += 1
                if not effective_firebase_uid:
                    counts["app_user_pending_firebase"] += 1

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
