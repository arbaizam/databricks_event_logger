import json
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
    deserialize_metadata,
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
    data = deserialize_metadata(serialized)

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
    data = deserialize_metadata(
        serialize_metadata({"value": {"child": {"password": "secret-at-depth"}}}, max_depth=2)
    )
    assert data["value"]["child"] == DEPTH_LIMIT_VALUE
    serialized = serialize_metadata(cycle, max_depth=3)
    assert DEPTH_LIMIT_VALUE in serialized


def test_nonfinite_numbers_become_json_strings():
    serialized = serialize_metadata({"nan": float("nan"), "infinite": float("inf")})
    assert json.loads(serialized) == {"infinite": "[NONFINITE]", "nan": "[NONFINITE]"}


def test_empty_metadata_and_custom_redaction():
    assert serialize_metadata(None) is None
    assert serialize_metadata({}) is None
    assert deserialize_metadata(None) == {}
    data = deserialize_metadata(
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
    data = deserialize_metadata(
        serialize_metadata({"message": "x" * 500}, string_max_chars=None, max_bytes=180)
    )
    assert data["_truncated"] is True
    assert data["_original_size_bytes"] > 180
    assert "_preview" in data


@pytest.mark.parametrize("limit", [0, 1, 3, 13, 20, 200])
def test_string_limit_includes_truncation_marker(limit):
    data = deserialize_metadata(
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
