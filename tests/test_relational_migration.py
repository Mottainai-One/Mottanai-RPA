"""Check the legacy normalization functions currently used by migrate()."""

from relational_migration import (
    clean_digits,
    legacy_plan_id,
    normalize_state,
    preserve_or_protect_cpf,
    protected_token,
)


def test_company_tax_id_is_normalized_to_digits():
    assert clean_digits("04.252.011/0001-10") == "04252011000110"


def test_company_plan_comes_from_company_foreign_key():
    assert legacy_plan_id({"companyid": 42, "subscriptionplanid": 7}) == 7
    assert legacy_plan_id({"companyid": 42, "planid": 9}) == 9


def test_employee_cpf_is_protected_without_exposing_the_raw_value():
    cpf = "123.456.789-09"
    stored = preserve_or_protect_cpf(cpf)
    assert stored == protected_token("CPF", cpf, digits=True)
    assert "12345678909" not in stored


def test_existing_legacy_masked_cpf_is_preserved():
    assert preserve_or_protect_cpf("1234") == "1234"


def test_accented_state_name_is_normalized():
    assert normalize_state("São Paulo") == "SP"
