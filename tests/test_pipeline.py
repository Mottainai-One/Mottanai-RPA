from decimal import Decimal

from pipeline import normalize_text, normalize_value, quote_identifier, record_fingerprint, transform_record

def test_normalize_text_removes_noise():
    assert normalize_text("  João\u00a0  da Silva  ") == "João da Silva"
    assert normalize_text("   ") is None

def test_normalize_decimal():
    assert normalize_value(Decimal("10.1")) == Decimal("10.10")

def test_fingerprint_is_stable():
    assert record_fingerprint({"b": 2, "a": 1}) == record_fingerprint({"a": 1, "b": 2})

def test_identifier_is_safe():
    assert quote_identifier("customer_1") == '"customer_1"'

def test_transform_adds_lineage():
    row = transform_record({"name": "  Ana  "})
    assert row["name"] == "Ana"
    assert len(row["_source_fingerprint"]) == 64
