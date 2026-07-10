"""
Delta sink for Databricks persistence.

The v1 Delta sink performs immediate writes. This is intentionally simple and
more failure-resilient than buffering because an event is handed to Spark as
soon as it is emitted. Future buffering can be added behind the same ``emit``
interface if event volume makes small writes too expensive.
"""

from __future__ import annotations

import re
import warnings
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from databricks_event_logger.errors import EventLoggerConfigurationError
from databricks_event_logger.event import EventRecord

EVENT_COLUMNS = (
    "event_name",
    "event_type",
    "status",
    "event_id",
    "correlation_id",
    "parent_event_id",
    "event_ts",
    "event_date",
    "start_ts",
    "end_ts",
    "duration_ms",
    "severity",
    "app_name",
    "component",
    "environment",
    "sdk_version",
    "workspace_id",
    "workspace_url",
    "cluster_id",
    "job_id",
    "run_id",
    "task_key",
    "task_run_id",
    "task_attempt_number",
    "job_start_time",
    "job_trigger_type",
    "notebook_path",
    "user_name",
    "run_as_user_name",
    "source_table",
    "target_table",
    "row_count",
    "metric_name",
    "metric_value",
    "error_class",
    "error_message",
    "stack_trace_hash",
    "metadata_json",
    "created_at",
)

_IDENTIFIER_PART = r"[A-Za-z_][A-Za-z0-9_]*"
_THREE_PART_TABLE_NAME = re.compile(
    rf"{_IDENTIFIER_PART}\.{_IDENTIFIER_PART}\.{_IDENTIFIER_PART}"
)


@dataclass
class DeltaSink:
    """
    Write each event immediately to a Delta table.

    Parameters
    ----------
    spark : Any
        Spark session. It is typed as ``Any`` so the package does not require
        PySpark to be importable outside Databricks.
    table_name : str
        Fully qualified Delta table name. V1 accepts only simple three-part
        Unity Catalog identifiers such as ``catalog.schema.event_log``.
    """

    spark: Any
    table_name: str

    def __post_init__(self) -> None:
        """
        Validate required sink configuration.
        """
        if self.spark is None:
            raise EventLoggerConfigurationError("DeltaSink requires a Spark session.")
        if not self.table_name:
            raise EventLoggerConfigurationError("DeltaSink requires a table name.")
        self.table_name = self.table_name.strip()
        if not _THREE_PART_TABLE_NAME.fullmatch(self.table_name):
            raise EventLoggerConfigurationError(
                "DeltaSink table_name must be a three-part Unity Catalog identifier "
                "using only letters, numbers, and underscores, for example "
                "'catalog.schema.event_log'."
            )

    def emit(self, event: EventRecord) -> None:
        """
        Append one event to the configured Delta table.

        Parameters
        ----------
        event : EventRecord
            Event to persist.
        """
        event_dict = event.as_dict()
        row = {column: event_dict[column] for column in EVENT_COLUMNS}
        view_name = f"databricks_event_logger_event_{uuid4().hex}"
        dataframe = self.spark.createDataFrame([row], schema=_event_schema())
        dataframe.createOrReplaceTempView(view_name)

        try:
            self.spark.sql(
                f"""
                INSERT INTO {self.table_name} ({_column_list()})
                SELECT {_column_list()}
                FROM {view_name}
                """
            )
        finally:
            try:
                self.spark.sql(f"DROP VIEW IF EXISTS {view_name}")
            except Exception as cleanup_exc:
                warnings.warn(
                    "DeltaSink failed to clean up staging view "
                    f"{view_name!r}: {cleanup_exc}",
                    RuntimeWarning,
                    stacklevel=2,
                )

    def validate(self) -> None:
        """
        Validate that the configured event table is queryable and has v1 columns.

        Raises
        ------
        EventLoggerConfigurationError
            If the table cannot be described or required event columns are
            missing.
        """
        try:
            describe_result = self.spark.sql(f"DESCRIBE TABLE {self.table_name}")
        except Exception as exc:
            raise EventLoggerConfigurationError(
                f"DeltaSink could not describe event table {self.table_name!r}: {exc}"
            ) from exc
        rows = _collect_optional(describe_result)
        if rows is None:
            return
        described_columns = {
            column_name
            for row in rows
            if (column_name := _row_value(row, "col_name"))
            and not str(column_name).startswith("#")
        }
        missing_columns = sorted(set(EVENT_COLUMNS) - set(described_columns))
        if missing_columns:
            raise EventLoggerConfigurationError(
                f"DeltaSink event table {self.table_name!r} is missing required "
                f"columns: {', '.join(missing_columns)}"
            )

def _column_list() -> str:
    """
    Return the event columns as a SQL column list.
    """
    return ", ".join(EVENT_COLUMNS)


def _event_schema() -> Any:
    """
    Return the typed Spark schema used for the single-row staging view.

    The event table itself should be created from SQL DDL outside this package.
    This schema only prevents Spark from inferring ``NullType`` for optional
    fields before the row is inserted through SQL.
    """
    from pyspark.sql.types import (  # noqa: PLC0415 - PySpark is Databricks-only.
        DateType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("event_name", StringType(), nullable=False),
            StructField("event_type", StringType(), nullable=False),
            StructField("status", StringType(), nullable=False),
            StructField("event_id", StringType(), nullable=False),
            StructField("correlation_id", StringType(), nullable=True),
            StructField("parent_event_id", StringType(), nullable=True),
            StructField("event_ts", TimestampType(), nullable=False),
            StructField("event_date", DateType(), nullable=False),
            StructField("start_ts", TimestampType(), nullable=True),
            StructField("end_ts", TimestampType(), nullable=True),
            StructField("duration_ms", LongType(), nullable=True),
            StructField("severity", StringType(), nullable=True),
            StructField("app_name", StringType(), nullable=True),
            StructField("component", StringType(), nullable=True),
            StructField("environment", StringType(), nullable=True),
            StructField("sdk_version", StringType(), nullable=False),
            StructField("workspace_id", StringType(), nullable=True),
            StructField("workspace_url", StringType(), nullable=True),
            StructField("cluster_id", StringType(), nullable=True),
            StructField("job_id", StringType(), nullable=True),
            StructField("run_id", StringType(), nullable=True),
            StructField("task_key", StringType(), nullable=True),
            StructField("task_run_id", StringType(), nullable=True),
            StructField("task_attempt_number", StringType(), nullable=True),
            StructField("job_start_time", StringType(), nullable=True),
            StructField("job_trigger_type", StringType(), nullable=True),
            StructField("notebook_path", StringType(), nullable=True),
            StructField("user_name", StringType(), nullable=True),
            StructField("run_as_user_name", StringType(), nullable=True),
            StructField("source_table", StringType(), nullable=True),
            StructField("target_table", StringType(), nullable=True),
            StructField("row_count", LongType(), nullable=True),
            StructField("metric_name", StringType(), nullable=True),
            StructField("metric_value", DoubleType(), nullable=True),
            StructField("error_class", StringType(), nullable=True),
            StructField("error_message", StringType(), nullable=True),
            StructField("stack_trace_hash", StringType(), nullable=True),
            StructField("metadata_json", StringType(), nullable=True),
            StructField("created_at", TimestampType(), nullable=False),
        ]
    )


def _collect_optional(result: Any) -> list[Any] | None:
    """
    Collect a Spark result when the test double or Spark object supports it.
    """
    collect = getattr(result, "collect", None)
    if collect is None:
        return None
    return list(collect())


def _row_value(row: Any, name: str) -> str | None:
    """
    Read one named value from a Spark Row-like object.
    """
    if hasattr(row, "asDict"):
        return row.asDict().get(name)
    if isinstance(row, dict):
        return row.get(name)
    return getattr(row, name, None)
