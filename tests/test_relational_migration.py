"""Check the legacy normalization functions currently used by migrate()."""

import base64

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from relational_migration import (
    EmployeeProtector,
    LegacyPiiDecryptor,
    clean_digits,
    legacy_plan_id,
    normalize_state,
    preserve_or_protect_cpf,
    protected_token,
    valid_cnpj,
)


def test_company_tax_id_is_normalized_to_digits():
    assert clean_digits("04.252.011/0001-10") == "04252011000110"
    assert valid_cnpj("04.252.011/0001-10")
    assert not valid_cnpj("04.252.011/0001-11")
    assert not valid_cnpj("00000000000000")


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


def test_employee_envelope_is_aes_gcm_and_bound_to_employee_and_field():
    aes_key = bytes(range(32))
    protector = EmployeeProtector(aes_key, bytes(reversed(range(32))), 3)
    ciphertext, nonce = protector.encrypt(42, "cpf", "123.456.789-09")

    assert len(nonce) == 12
    assert len(ciphertext) > len("123.456.789-09")
    aad = b"mottainai:employee:42:cpf:v1"
    assert AESGCM(aes_key).decrypt(nonce, ciphertext, aad) == b"123.456.789-09"


def test_employee_lookup_is_deterministic_and_domain_separated():
    protector = EmployeeProtector(bytes(range(32)), bytes(reversed(range(32))), 1)
    first = protector.lookup("cpf", "123.456.789-09", digits=True)
    second = protector.lookup("cpf", "12345678909", digits=True)
    email = protector.lookup("email", "12345678909")

    assert first == second
    assert first != email
    assert len(first) == 32


def test_employee_keys_are_loaded_from_base64_environment(monkeypatch):
    monkeypatch.setenv("EMPLOYEE_PII_AES_KEY_B64", base64.b64encode(bytes(range(32))).decode())
    monkeypatch.setenv("EMPLOYEE_PII_LOOKUP_KEY_B64", base64.b64encode(bytes(reversed(range(32)))).decode())
    monkeypatch.setenv("EMPLOYEE_PII_KEY_VERSION", "2")

    assert EmployeeProtector.from_environment().key_version == 2


def test_legacy_backend_envelope_is_decrypted_from_iv_plus_gcm_payload():
    key = bytes(range(32))
    iv = bytes(reversed(range(12)))
    payload = AESGCM(key).encrypt(iv, b"123.456.789-09", None)
    envelope = base64.b64encode(iv + payload).decode()

    assert LegacyPiiDecryptor(key).decrypt(envelope) == "123.456.789-09"
