import json
from collections import namedtuple
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import Enum
from pathlib import Path

import pytest

from databricks_event_logger.serialization import (
    DEPTH_LIMIT_VALUE,
    REDACTED_VALUE,
    TRUNCATED_MARKER,
    UNSUPPORTED_VALUE,
    safe_text,
    serialize_metadata,
)


@dataclass
class Credentials:
    password: str
    nested: dict


class SecretEnum(Enum):
    PAYLOAD = {"api_token": "enum-secret", "count": 7}


class BrokenText:
    def __repr__(self):
        raise AssertionError("repr must never run")

    def __str__(self):
        raise KeyboardInterrupt("str must not interrupt error reporting")


def test_conversion_and_redaction_share_one_recursive_path():
    payload = {
        "date": date(2026, 6, 3),
        "amount": Decimal("12.30"),
        "path": Path("positions.csv"),
        "tuple": (1, "two"),
        "payload": Credentials("dataclass-secret", {"Private_Key": "nested-secret"}),
        "enum": SecretEnum.PAYLOAD,
        "unknown": BrokenText(),
        "set": {"a", "b"},
    }

    serialized = serialize_metadata(payload)
    data = json.loads(serialized)

    assert data["date"] == "2026-06-03"
    assert data["amount"] == "12.30"
    assert data["path"] == "positions.csv"
    assert data["tuple"] == [1, "two"]
    assert data["payload"] == {
        "password": REDACTED_VALUE, "nested": {"Private_Key": REDACTED_VALUE},
    }
    assert data["enum"] == {"api_token": REDACTED_VALUE, "count": 7}
    assert data["unknown"] == UNSUPPORTED_VALUE
    assert data["set"] == UNSUPPORTED_VALUE
    assert "dataclass-secret" not in serialized
    assert "nested-secret" not in serialized
    assert "enum-secret" not in serialized
    assert payload["payload"].password == "dataclass-secret"


def test_depth_limit_never_reveals_raw_repr_and_handles_cycles():
    cycle = {"child": {}}
    cycle["child"]["child"] = cycle
    data = json.loads(
        serialize_metadata({"value": {"child": {"password": "secret-at-depth"}}}, max_depth=2)
    )
    assert data["value"]["child"] == DEPTH_LIMIT_VALUE
    serialized = serialize_metadata(cycle, max_depth=3)
    assert DEPTH_LIMIT_VALUE in serialized


def test_namedtuple_fields_survive_recursive_redaction():
    Connection = namedtuple("Connection", "table password nested")
    Token = namedtuple("Token", "api_token count")
    payload = Connection("positions", "record-secret", Token("nested-secret", 7))

    serialized = serialize_metadata({"config": payload})

    assert json.loads(serialized) == {
        "config": {
            "table": "positions", "password": REDACTED_VALUE,
            "nested": {"api_token": REDACTED_VALUE, "count": 7},
        },
    }
    assert "record-secret" not in serialized
    assert "nested-secret" not in serialized


def test_pyspark_row_fields_survive_nested_redaction_without_a_spark_session():
    pytest.importorskip("pyspark")
    from pyspark.sql import Row

    payload = Row(table="positions", password="row-secret", nested=Row(api_token="nested-secret"))

    serialized = serialize_metadata({"rows": [payload]})

    assert json.loads(serialized) == {
        "rows": [{
            "table": "positions", "password": REDACTED_VALUE,
            "nested": {"api_token": REDACTED_VALUE},
        }],
    }
    assert "row-secret" not in serialized
    assert "nested-secret" not in serialized


@pytest.mark.parametrize("attribute", ["_fields", "__fields__"])
@pytest.mark.parametrize("names", [None, "password", {"password"}, [], [1], ["a", "b"]])
def test_malformed_named_records_never_fall_back_to_positional_values(attribute, names):
    Record = type("Record", (tuple,), {attribute: names})

    serialized = serialize_metadata({"config": Record(["hidden-secret"])})

    assert json.loads(serialized) == {"config": UNSUPPORTED_VALUE}
    assert "hidden-secret" not in serialized


def test_duplicate_record_field_names_are_not_silently_discarded():
    Record = type("Record", (tuple,), {"__fields__": ["password", "password"]})
    serialized = serialize_metadata({"config": Record(["first-secret", "second-secret"])})
    assert json.loads(serialized) == {"config": UNSUPPORTED_VALUE}
    assert "secret" not in serialized


def test_numpy_numeric_scalars_preserve_json_number_types():
    np = pytest.importorskip("numpy")

    serialized = serialize_metadata({
        "integer": np.int64(5), "unsigned": np.uint64(2**64 - 1),
        "float32": np.float32(1.5), "float64": np.float64(2.5),
        "nonfinite": np.float32("nan"), "boolean": True, "numpy_boolean": np.bool_(True),
    })

    assert json.loads(serialized) == {
        "integer": 5, "unsigned": 2**64 - 1, "float32": 1.5, "float64": 2.5,
        "nonfinite": "[NONFINITE]", "boolean": True, "numpy_boolean": UNSUPPORTED_VALUE,
    }


def test_nonfinite_numbers_become_json_strings():
    serialized = serialize_metadata({"nan": float("nan"), "infinite": float("inf")})
    assert json.loads(serialized) == {"infinite": "[NONFINITE]", "nan": "[NONFINITE]"}


def test_empty_metadata_and_custom_redaction():
    assert serialize_metadata(None) is None
    assert serialize_metadata({}) is None
    data = json.loads(
        serialize_metadata(
            {"CUSTOM_FIELD": "value", "password": "visible"}, redact_keys=("custom",),
        )
    )
    assert data == {"CUSTOM_FIELD": REDACTED_VALUE, "password": "visible"}


@pytest.mark.parametrize("budget", [2, 3, 18, 19, 20, 49, 60, 80, 120, 180, 300])
def test_payload_byte_budget_includes_wrapping_escaping_and_unicode(budget):
    serialized = serialize_metadata(
        {"message": '😀"\\' * 100, "password": "hidden"}, string_max_chars=None, max_bytes=budget,
    )
    assert isinstance(json.loads(serialized), dict)
    assert len(serialized.encode("utf-8")) <= budget
    assert "hidden" not in serialized


def test_large_payload_retains_a_bounded_truncation_summary():
    data = json.loads(
        serialize_metadata({"message": "x" * 500}, string_max_chars=None, max_bytes=180)
    )
    assert data["_truncated"] is True
    assert data["_original_size_bytes"] > 180
    assert "_preview" in data


@pytest.mark.parametrize("budget", [None, 2, 60, 120, 4000])
def test_surrogate_keys_and_values_are_utf8_safe_with_or_without_a_byte_limit(budget):
    metadata = {"path\ud800": "file\udcff", "password\ud800": "hidden", "text": "😀é"}

    serialized = serialize_metadata(metadata, max_bytes=budget)

    encoded = serialized.encode("utf-8")
    data = json.loads(encoded)
    assert "hidden" not in serialized
    if budget is not None:
        assert len(encoded) <= budget
    if budget is None or budget == 4000:
        assert data == {"path\ud800": "file\udcff", "password\ud800": REDACTED_VALUE, "text": "😀é"}


def test_truncation_preview_is_maximal_within_the_byte_budget():
    payload = {"message": '😀"\\' * 1000}
    full = serialize_metadata(payload, string_max_chars=None, max_bytes=None)
    budget = 200
    bounded = serialize_metadata(payload, string_max_chars=None, max_bytes=budget)
    summary = json.loads(bounded)
    preview_length = len(summary["_preview"])

    assert len(bounded.encode("utf-8")) <= budget
    assert summary["_preview"] == full[:preview_length]
    summary["_preview"] = full[:preview_length + 1]
    next_candidate = json.dumps(summary, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    assert len(next_candidate.encode("utf-8")) > budget


@pytest.mark.parametrize("limit", [0, 1, 3, 13, 20, 200])
def test_string_limit_includes_truncation_marker(limit):
    data = json.loads(
        serialize_metadata({"value": "x" * 500}, string_max_chars=limit, max_bytes=None)
    )
    assert len(data["value"]) <= limit
    if limit > len(TRUNCATED_MARKER):
        assert data["value"].endswith(TRUNCATED_MARKER)


@pytest.mark.parametrize(
    "options",
    [
        {"max_bytes": 1}, {"max_bytes": -1}, {"max_bytes": True},
        {"max_depth": 0}, {"max_depth": True}, {"string_max_chars": -1},
        {"string_max_chars": 1.5}, {"redact_keys": ("token", None)},
    ],
)
def test_invalid_configuration_is_rejected_even_for_empty_metadata(options):
    with pytest.raises(ValueError):
        serialize_metadata({}, **options)


def test_mapping_requires_string_keys_without_stringifying_objects():
    with pytest.raises(TypeError, match="keys must be strings"):
        serialize_metadata({BrokenText(): "secret"})
    with pytest.raises(TypeError, match="mapping"):
        serialize_metadata(["invalid"])


def test_safe_diagnostic_text_cannot_mask_original_error():
    assert safe_text(BrokenText()) == "[UNPRINTABLE]"
    assert len(safe_text("x" * 100, max_chars=20)) == 20
