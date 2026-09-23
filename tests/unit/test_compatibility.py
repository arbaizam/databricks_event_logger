"""Public compatibility contracts that the old tests did not exercise."""

import base64
import importlib
import json
import pickle
import warnings
from datetime import datetime, timezone
from pathlib import Path

import pytest

from databricks_event_logger import (
    EventLogger,
    EventRecord,
    EventScope,
    EventSeverity,
    EventStatus,
    MemorySink,
)
from databricks_event_logger.diagnostics import TRUNCATED_MARKER, safe_text
from databricks_event_logger.enums import SEVERITY_VALUES, STATUS_VALUES
from databricks_event_logger.metadata import sanitize_metadata, serialize_metadata


def test_legacy_imports_are_warning_free_aliases():
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        event = importlib.import_module("databricks_event_logger.event")
        events = importlib.import_module("databricks_event_logger.events")
        serialization = importlib.import_module("databricks_event_logger.serialization")
    assert event.EventRecord is EventRecord
    assert event.VALID_STATUSES is STATUS_VALUES
    assert event.VALID_SEVERITIES is SEVERITY_VALUES
    assert events.EventStatus is EventStatus
    assert events.EventSeverity is EventSeverity
    assert serialization.safe_text is safe_text
    assert serialization.TRUNCATED_MARKER == TRUNCATED_MARKER
    assert serialization.sanitize_metadata is sanitize_metadata
    assert serialization.serialize_metadata is serialize_metadata


@pytest.mark.parametrize("field", ["start_ts", "end_ts"])
def test_timestamp_error_order_matches_baseline(field):
    with pytest.raises(ValueError, match=f"^{field} must be a timezone-aware datetime[.]$"):
        EventRecord("x", **{field: "bad", "created_at": "bad"})


def test_legacy_scope_constructor_and_state_remain_readable():
    stamp = datetime(2020, 1, 1, tzinfo=timezone.utc)
    record = EventRecord("x", event_ts=stamp, created_at=stamp)
    scope = EventScope(_event=record, row_count=12)
    restored = pickle.loads(pickle.dumps(scope))
    assert restored.event_id == record.event_id
    assert restored.row_count == 12
    # Also accept objects saved by the reviewed branch before this repair.
    restored.__setstate__({"_template": record, "metadata": {}, "row_count": 14})
    assert restored.event_id == record.event_id
    assert restored._result_fields()["row_count"] == 14


def test_objects_actually_pickled_by_baseline_load_without_migration():
    # Trusted, checked-in fixtures generated in a separate process at 77f33cf.
    path = Path(__file__).resolve().parents[1] / "fixtures/legacy_pickles.json"
    saved = json.loads(path.read_text(encoding="utf-8"))
    assert saved["revision"] == "77f33cf"
    objects = {name: pickle.loads(base64.b64decode(payload))
               for name, payload in saved["objects"].items()}
    assert objects["record"].event_name == "legacy"
    assert objects["record"].event_id == "saved-id"
    assert objects["scope"].event_id == "saved-id"
    assert objects["scope"]._result_fields()["row_count"] == 12
    assert objects["status"] is EventStatus.WARNING
    assert objects["severity"] is EventSeverity.ERROR
    assert objects["health"].attempted == 3
    assert objects["health"].last_error == "RuntimeError: example"


def test_strict_inner_sink_failure_keeps_legacy_sdk_frames_in_outer_record():
    failure = RuntimeError("sink failed")

    class FailInner(MemorySink):
        def emit(self, event):
            if event.event_name == "inner":
                raise failure
            super().emit(event)

    sink = FailInner()
    logger = EventLogger(sink=sink, strict_logging=True, capture_error_frames=True)
    with pytest.raises(RuntimeError) as raised:
        with logger.event("outer"):
            with logger.event("inner"):
                pass
    assert raised.value is failure
    frames = json.loads(sink.events[0].error_frames_json)
    sdk_frames = [frame for frame in frames if frame["file"] == "logger.py"]
    assert sdk_frames == [
        {"file": "logger.py", "function": "_event_scope", "line": 251},
        {"file": "logger.py", "function": "_event_scope", "line": 258},
        {"file": "logger.py", "function": "_deliver", "line": 340},
    ]
