from unittest.mock import Mock

import pytest

from relational_migration import LEGACY_REQUIRED_COLUMNS, is_valid_cnpj, validate_legacy_contract


def _cursor_with_contract(columns):
    cursor = Mock()
    cursor.fetchall.return_value = [
        {"table_name": table, "column_name": column}
        for table, table_columns in columns.items()
        for column in table_columns
    ]
    return cursor


def test_current_legacy_contract_is_accepted():
    cursor = _cursor_with_contract(LEGACY_REQUIRED_COLUMNS)

    validate_legacy_contract(cursor, "public")

    cursor.execute.assert_called_once()


def test_legacy_contract_fails_before_migration_when_column_is_missing():
    columns = {table: set(values) for table, values in LEGACY_REQUIRED_COLUMNS.items()}
    columns["company"].remove("subscriptionplanid")
    cursor = _cursor_with_contract(columns)

    with pytest.raises(ValueError, match="public.company missing columns: subscriptionplanid"):
        validate_legacy_contract(cursor, "public")


@pytest.mark.parametrize("value", ["11222333000181", "04.252.011/0001-10"])
def test_valid_cnpj_is_accepted(value):
    assert is_valid_cnpj(value)


@pytest.mark.parametrize("value", [None, "", "00000000000000", "98765432000110"])
def test_invalid_cnpj_is_rejected(value):
    assert not is_valid_cnpj(value)
