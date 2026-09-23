"""Guard the wiring that contributors must update when adding fields/state."""

import inspect
from dataclasses import fields
from datetime import datetime, timezone

import pytest

from databricks_event_logger import EventLogger, EventScope, MemorySink

# Deliberately independent from implementation field lists. A new API parameter
# needs an example here; testing that example catches accepted-but-dropped values.
FIELD_VALUES = {
    "event_name": "example",
    "event_type": "batch",
    "status": "warning",
    "severity": "info",
    "event_id": "specified-id",
    "parent_event_id": "specified-parent",
    "source_table": "a.b.source",
    "target_table": "a.b.target",
    "row_count": 42,
    "metric_name": "total",
    "metric_value": 4.5,
    "start_ts": datetime(2020, 1, 2, tzinfo=timezone.utc),
    "end_ts": datetime(2020, 1, 3, tzinfo=timezone.utc),
    "duration_ms": 123,
}


def parameters(method):
    return set(inspect.signature(method).parameters) - {"self", "metadata"}


def test_every_direct_argument_reaches_the_record():
    assert parameters(EventLogger.record_event) == FIELD_VALUES.keys(), (
        "Add a representative value for each new event argument."
    )
    record = EventLogger(sink=MemorySink()).record_event(**FIELD_VALUES)
    for name, value in FIELD_VALUES.items():
        assert getattr(record, name) == value, f"record_event dropped {name}"


def scope_results():
    return {
        item.name
        for item in fields(EventScope)
        if not item.name.startswith("_") and item.name != "metadata"
    }


def test_every_scope_argument_initializes_and_reaches_the_record():
    names = parameters(EventLogger.event)
    assert names <= FIELD_VALUES.keys(), "Add an example for the new scope parameter."
    assert names - {"event_name", "event_type"} == scope_results()
    values = {name: FIELD_VALUES[name] for name in names}
    sink = MemorySink()
    with EventLogger(sink=sink).event(**values) as scope:
        for name in scope_results():
            assert getattr(scope, name) == values[name]
    for name, value in values.items():
        assert getattr(sink.events[0], name) == value, f"event dropped {name}"


@pytest.mark.parametrize("name", sorted(scope_results()))
def test_each_edited_scope_field_reaches_delivery(name):
    assert name in FIELD_VALUES, "Add an example value for the new editable field."
    sink = MemorySink()
    with EventLogger(sink=sink).event("edited") as scope:
        setattr(scope, name, FIELD_VALUES[name])
    assert getattr(sink.events[0], name) == FIELD_VALUES[name]


def test_binding_ownership_is_an_explicit_decision_for_every_logger_attribute():
    logger = EventLogger(sink=MemorySink(), default_metadata={"nested": []})
    # Configuration is copied by reference, shared state has one owner, and
    # top-level metadata is replaced. Adding state requires a deliberate choice.
    configuration = {
        "app_name",
        "component",
        "environment",
        "context",
        "correlation_id",
        "sink",
        "metadata_max_bytes",
        "metadata_string_max_chars",
        "error_message_max_chars",
        "redact_keys",
        "strict_logging",
        "capture_error_frames",
    }
    assert set(vars(logger)) == configuration | {"_state", "_default_metadata"}
    bound = logger.bind(batch="one")
    for name in configuration | {"_state"}:
        assert getattr(bound, name) is getattr(logger, name)
    assert bound._default_metadata is not logger._default_metadata
    assert bound.default_metadata["nested"] is logger.default_metadata["nested"]
    bound.app_name = "different"
    assert logger.app_name is None
    bound.record_event("counted")
    assert logger.health.succeeded == bound.health.succeeded == 1
