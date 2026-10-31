from datetime import datetime, timezone

import pytest

from analytics.load import DATASETS, OPERATIONAL_KPI_FACTS, ZONE, identifier, run


def test_daily_batch_has_unique_dataset_keys_and_no_personal_fields():
    names = [dataset.target for dataset in DATASETS]
    assert len(names) == len(set(names))
    assert {"dim_company", "dim_store", "fact_sales", "fact_inventory_snapshot"} <= set(names)
    sensitive = {"cpf", "cnpj", "email", "phone", "password_hash", "full_name"}
    assert all(not sensitive.intersection(dataset.columns) for dataset in DATASETS)
    assert all(set(dataset.key) <= set(dataset.columns) for dataset in DATASETS)


def test_operational_kpi_sources_are_materialized_without_sensitive_columns():
    by_target = {dataset.target: dataset for dataset in DATASETS}
    assert set(OPERATIONAL_KPI_FACTS) <= by_target.keys()
    for target in OPERATIONAL_KPI_FACTS:
        dataset = by_target[target]
        assert dataset.source_query.startswith("SELECT ")
        assert "{src}." in dataset.source_query
        assert "observation" not in dataset.columns
        assert "description" not in dataset.columns


def test_partitioned_kpi_facts_include_the_partition_column_in_the_upsert_key():
    by_target = {dataset.target: dataset for dataset in DATASETS}
    partition_columns = {
        "fact_inventory_count_line": "count_date",
        "fact_transfer_line": "request_date",
        "fact_replenishment_plan_line": "generated_at",
        "fact_replenishment_execution_line": "started_at",
        "fact_donation_line": "donation_date",
        "fact_disposal_line": "disposal_date",
        "fact_alert_lifecycle": "generated_at",
        "fact_action_lifecycle": "generated_at",
    }
    for table, partition_column in partition_columns.items():
        dataset = by_target[table]
        assert partition_column in dataset.columns
        assert partition_column in dataset.key


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
