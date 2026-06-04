from datetime import UTC, datetime
from decimal import Decimal

from databricks_event_logger.context import RuntimeContext
from databricks_event_logger.event import EventRecord
from databricks_event_logger.serialization import deserialize_metadata


def test_event_record_serializes_metadata_and_context():
    """
    What: Builds one flat event record with metadata and Databricks context.
    Why: Sinks and dashboard tables depend on stable event row fields.
    Fails when: Metadata JSON, context stamping, or date derivation regresses.
    """
    event = EventRecord(
        "reporting.step",
        event_type="business_process",
        status="success",
        event_ts=datetime(2026, 6, 3, 12, 0, tzinfo=UTC),
        app_name="app",
        component="component",
        environment="dev",
        context=RuntimeContext(
            job_id="job-1",
            run_id="run-1",
            task_key="task",
            task_run_id="task-run-1",
            task_attempt_number="1",
            job_start_time="2026-06-03T12:00:00Z",
            job_trigger_type="one_time",
            run_as_user_name="svc@example.com",
        ),
        metadata={"amount": Decimal("12.30"), "as_of_date": datetime(2026, 6, 3, tzinfo=UTC)},
    )

    row = event.as_dict()
    metadata = deserialize_metadata(event.metadata_json)

    assert row["event_name"] == "reporting.step"
    assert row["event_date"].isoformat() == "2026-06-03"
    assert row["job_id"] == "job-1"
    assert row["run_id"] == "run-1"
    assert row["task_key"] == "task"
    assert row["task_run_id"] == "task-run-1"
    assert row["task_attempt_number"] == "1"
    assert row["job_start_time"] == "2026-06-03T12:00:00Z"
    assert row["job_trigger_type"] == "one_time"
    assert row["run_as_user_name"] == "svc@example.com"
    assert metadata["amount"] == "12.30"
    assert metadata["as_of_date"] == "2026-06-03T00:00:00+00:00"


def test_event_record_json_dict_renders_datetimes_as_text():
    """
    What: Converts event timestamps to JSON-safe strings.
    Why: ConsoleSink and diagnostics need plain JSON-compatible values.
    Fails when: Datetime values leak into JSON output dictionaries.
    """
    event = EventRecord(
        "reporting.step",
        event_ts=datetime(2026, 6, 3, 12, 0, tzinfo=UTC),
    )

    row = event.as_json_dict()

    assert row["event_ts"] == "2026-06-03T12:00:00+00:00"
    assert row["event_date"] == "2026-06-03"
