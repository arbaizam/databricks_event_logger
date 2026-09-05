from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone

import pytest

from databricks_event_logger.context import RuntimeContext
from databricks_event_logger.event import EventRecord
from databricks_event_logger.events import EventSeverity, EventStatus


def test_event_snapshot_flattens_context_and_preserves_normalized_metadata():
    event = EventRecord(
        "positions.publish",
        context=RuntimeContext(job_id=12, run_id="run-1", task_key="publish"),
        metadata_json='{"count":7}',
    )

    row = event.as_dict()
    assert row["job_id"] == "12"
    assert row["run_id"] == "run-1"
    assert row["task_key"] == "publish"
    assert "context" not in row
    assert event.context.job_id == "12"
    assert event.metadata_json == '{"count":7}'
    with pytest.raises(FrozenInstanceError):
        event.status = "failed"


def test_timestamp_and_partition_date_use_utc():
    event = EventRecord(
        "positions.publish",
        event_ts=datetime(2026, 6, 3, 23, 30, tzinfo=timezone(timedelta(hours=-5))),
    )
    assert event.event_ts == datetime(2026, 6, 4, 4, 30, tzinfo=timezone.utc)
    assert event.as_json_dict()["event_ts"] == "2026-06-04T04:30:00+00:00"
    assert event.as_json_dict()["event_date"] == "2026-06-04"

    updated = replace(event, event_ts=datetime(2026, 7, 1, tzinfo=timezone.utc))
    assert updated.event_date.isoformat() == "2026-07-01"


@pytest.mark.parametrize("field", ["event_ts", "start_ts", "end_ts", "created_at"])
def test_naive_timestamps_are_rejected(field):
    with pytest.raises(ValueError, match=field):
        EventRecord("positions.publish", **{field: datetime(2026, 6, 3)})


@pytest.mark.parametrize(
    ("fields", "error"),
    [
        ({"event_name": ""}, "event_name"),
        ({"event_name": "x" * 256}, "event_name"),
        ({"event_type": ""}, "event_type"),
        ({"event_type": "x" * 101}, "event_type"),
        ({"event_id": ""}, "event_id"),
        ({"status": "done"}, "status"),
        ({"status": []}, "status"),
        ({"severity": "urgent"}, "severity"),
        ({"source_table": 7}, "source_table"),
        ({"context": {}}, "context"),
    ],
)
def test_invalid_fields_are_rejected(fields, error):
    with pytest.raises(ValueError, match=error):
        EventRecord(**{"event_name": "positions.publish", **fields})


@pytest.mark.parametrize("field", ["row_count", "duration_ms"])
@pytest.mark.parametrize("value", [-1, True, 7.0, 1 << 63, "7"])
def test_counts_and_durations_must_fit_nonnegative_int64(field, value):
    with pytest.raises(ValueError, match=field):
        EventRecord("positions.publish", **{field: value})


def test_integer_metric_is_normalized_to_double():
    event = EventRecord("positions.count", metric_value=7)
    assert event.metric_value == 7.0
    assert type(event.metric_value) is float


def test_numpy_numbers_are_normalized_to_storage_primitives():
    np = pytest.importorskip("numpy")
    for integer_type in (np.int32, np.int64, np.uint64):
        event = EventRecord(
            "positions.count", row_count=integer_type(7),
            duration_ms=integer_type(12), metric_value=integer_type(7),
        )
        assert event.row_count == 7
        assert event.duration_ms == 12
        assert type(event.row_count) is int
        assert type(event.duration_ms) is int
        assert type(event.metric_value) is float
        assert event.metric_value == 7.0
    for float_type in (np.float32, np.float64):
        event = EventRecord("positions.ratio", metric_value=float_type(1.5))
        assert type(event.metric_value) is float
        assert event.metric_value == 1.5


def test_numpy_numbers_follow_the_same_validation_rules():
    np = pytest.importorskip("numpy")
    for name in ("row_count", "duration_ms"):
        for value in (np.int64(-1), np.uint64(1 << 63), np.float64(7), np.bool_(True)):
            with pytest.raises(ValueError, match=name):
                EventRecord("positions.count", **{name: value})
    for value in (np.bool_(True), np.float32("inf"), np.float64("nan")):
        with pytest.raises(ValueError, match="metric_value"):
            EventRecord("positions.ratio", metric_value=value)


@pytest.mark.parametrize("value", [True, "7", float("inf"), float("nan"), 10 ** 400])
def test_invalid_metrics_are_rejected(value):
    with pytest.raises(ValueError, match="metric_value"):
        EventRecord("positions.count", metric_value=value)


def test_status_and_severity_enums_store_strings():
    event = EventRecord(
        "positions.validate", status=EventStatus.WARNING, severity=EventSeverity.ERROR,
    )
    assert event.status == "warning"
    assert type(event.status) is str
    assert event.severity == "error"


def test_unknown_arguments_and_raw_metadata_are_rejected():
    with pytest.raises(TypeError):
        EventRecord("positions.publish", jobid="123")
    with pytest.raises(TypeError):
        EventRecord("positions.publish", metadata={"count": 7})
