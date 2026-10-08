from datetime import datetime, timezone

import pytest

from analytics.load import DATASETS, ZONE, identifier, run


def test_daily_batch_has_unique_dataset_keys_and_no_personal_fields():
    names = [dataset.target for dataset in DATASETS]
    assert len(names) == len(set(names))
    assert {"dim_company", "dim_store", "fact_sales", "fact_inventory_snapshot"} <= set(names)
    sensitive = {"cpf", "cnpj", "email", "phone", "password_hash", "full_name"}
    assert all(not sensitive.intersection(dataset.columns) for dataset in DATASETS)
    assert all(set(dataset.key) <= set(dataset.columns) for dataset in DATASETS)


def test_schema_name_validation_rejects_sql_injection():
    with pytest.raises(ValueError):
        identifier("mottainai; DROP SCHEMA public")


def test_load_requires_both_database_credentials(monkeypatch):
    monkeypatch.setattr("analytics.load.get_operational_db_config", lambda: {"dbname": None, "user": None, "password": None})
    monkeypatch.setattr("analytics.load.get_analytics_db_config", lambda: {"dbname": "analytics", "user": "test", "password": "test"})
    with pytest.raises(ValueError, match="OPERATIONAL_DB"):
        run(now=datetime(2026, 10, 8, 5, 0, tzinfo=timezone.utc))


def test_sao_paulo_zone_is_used_for_daily_snapshot():
    assert datetime(2026, 10, 8, 5, 0, tzinfo=timezone.utc).astimezone(ZONE).hour == 2
