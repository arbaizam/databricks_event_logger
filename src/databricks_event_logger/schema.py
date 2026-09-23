"""The storage schema: one row per event, one column per field.

``EVENT_SCHEMA`` is the only place where columns are listed. Everything
else is built from it: the ``CREATE TABLE`` SQL, the Spark schema used for
inserts, and the checks in ``DeltaSink.validate()``.

The column names must match the keys of ``EventRecord.as_dict()`` exactly.
A unit test enforces this. When you add a field to ``EventRecord`` or
``RuntimeContext``, add its column here too.
"""

from __future__ import annotations

from typing import NamedTuple


class Column(NamedTuple):
    """One storage column used to generate DDL, Spark types, and validation.

    Args:
        name: Key in EventRecord.as_dict().
        sql_type: STRING, TIMESTAMP, DATE, BIGINT or DOUBLE. A new type also
            needs a conversion in sinks.delta._spark_schema.
        nullable: Whether the column can store NULL.

    This NamedTuple stores values without validating them. It is an internal
    schema declaration, not a way to add columns to an existing logger at runtime.
    """

    name: str
    sql_type: str  # A Spark SQL type name, such as "STRING" or "BIGINT".
    nullable: bool  # True if the column can hold NULL.


EVENT_SCHEMA: tuple[Column, ...] = (
    # What happened
    Column("event_name", "STRING", False),
    Column("event_type", "STRING", False),
    Column("status", "STRING", False),
    # Identity and lineage
    Column("event_id", "STRING", False),
    Column("correlation_id", "STRING", True),
    Column("parent_event_id", "STRING", True),
    # Timing (all timestamps are UTC)
    Column("event_ts", "TIMESTAMP", False),
    Column("event_date", "DATE", False),
    Column("start_ts", "TIMESTAMP", True),
    Column("end_ts", "TIMESTAMP", True),
    Column("duration_ms", "BIGINT", True),
    Column("severity", "STRING", True),
    # Application identity (set on the logger)
    Column("app_name", "STRING", True),
    Column("component", "STRING", True),
    Column("environment", "STRING", True),
    Column("sdk_version", "STRING", False),
    # Runtime context (RuntimeContext fields)
    Column("workspace_id", "STRING", True),
    Column("workspace_url", "STRING", True),
    Column("cluster_id", "STRING", True),
    Column("job_id", "STRING", True),
    Column("run_id", "STRING", True),
    Column("task_key", "STRING", True),
    Column("task_run_id", "STRING", True),
    Column("task_attempt_number", "STRING", True),
    Column("job_start_time", "STRING", True),
    Column("job_trigger_type", "STRING", True),
    Column("notebook_path", "STRING", True),
    Column("user_name", "STRING", True),
    Column("run_as_user_name", "STRING", True),
    # Results
    Column("source_table", "STRING", True),
    Column("target_table", "STRING", True),
    Column("row_count", "BIGINT", True),
    Column("metric_name", "STRING", True),
    Column("metric_value", "DOUBLE", True),
    # Failure details
    Column("error_class", "STRING", True),
    Column("error_message", "STRING", True),
    Column("stack_trace_hash", "STRING", True),
    Column("error_frames_json", "STRING", True),
    # Free-form data and bookkeeping
    Column("metadata_json", "STRING", True),
    Column("created_at", "TIMESTAMP", False),
)

EVENT_COLUMNS: tuple[str, ...] = tuple(column.name for column in EVENT_SCHEMA)
