from dataclasses import dataclass
from datetime import date

from databricks_event_logger.serialization import deserialize_metadata, serialize_metadata


@dataclass(frozen=True)
class ExamplePayload:
    value: int


def test_serialize_metadata_handles_common_non_json_values():
    """
    What: Serializes common Python values that JSON cannot encode directly.
    Why: Event logging should tolerate diagnostic metadata from application code.
    Fails when: Dates, dataclasses, sets, or unknown objects break metadata JSON.
    """
    metadata_json = serialize_metadata(
        {
            "business_date": date(2026, 6, 3),
            "payload": ExamplePayload(7),
            "values": {"b", "a"},
            "unknown": object(),
        }
    )

    metadata = deserialize_metadata(metadata_json)

    assert metadata["business_date"] == "2026-06-03"
    assert metadata["payload"] == {"value": 7}
    assert metadata["values"] == ["a", "b"]
    assert metadata["unknown"].startswith("<object object")


def test_serialize_metadata_returns_none_for_empty_metadata():
    """
    What: Treats missing and empty metadata as no metadata.
    Why: Empty JSON strings add noise to Delta rows and dashboards.
    Fails when: Empty metadata is stored as a noisy JSON object.
    """
    assert serialize_metadata(None) is None
    assert serialize_metadata({}) is None


def test_serialize_metadata_redacts_and_truncates_nested_values():
    """
    What: Applies redaction and string truncation before metadata persistence.
    Why: Production event rows should not accidentally store common secrets or huge strings.
    Fails when: Sensitive-looking keys or oversized strings leak into metadata JSON.
    """
    metadata_json = serialize_metadata(
        {
            "api_token": "secret-value",
            "nested": {
                "private_key": "abc",
                "message": "abcdef",
            },
        },
        string_max_chars=3,
        max_bytes=None,
    )

    metadata = deserialize_metadata(metadata_json)

    assert metadata["api_token"] == "[REDACTED]"
    assert metadata["nested"]["private_key"] == "[REDACTED]"
    assert metadata["nested"]["message"] == "abc...[TRUNCATED]"


def test_serialize_metadata_caps_oversized_payloads():
    """
    What: Replaces oversized serialized metadata with a bounded preview payload.
    Why: A single noisy event should not create unbounded Delta JSON payloads.
    Fails when: Metadata size limits are ignored.
    """
    metadata_json = serialize_metadata(
        {"message": "x" * 500},
        string_max_chars=None,
        max_bytes=180,
    )
    metadata = deserialize_metadata(metadata_json)

    assert metadata["_truncated"] is True
    assert metadata["_original_size_bytes"] > 180
    assert len(metadata_json.encode("utf-8")) <= 180
