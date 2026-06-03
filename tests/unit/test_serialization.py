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
