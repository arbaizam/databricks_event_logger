"""Immediate event delivery to an existing Delta table."""

from __future__ import annotations

import re
import warnings
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from databricks_event_logger.errors import EventLoggerConfigurationError
from databricks_event_logger.event import EventRecord

# The persistence schema is defined once; SQL DDL and Spark rows derive from it.
# Each entry is (column name, SQL type, allows null).
EVENT_SCHEMA = (
    ("event_name", "STRING", False),
    ("event_type", "STRING", False),
    ("status", "STRING", False),
    ("event_id", "STRING", False),
    ("correlation_id", "STRING", True),
    ("parent_event_id", "STRING", True),
    ("event_ts", "TIMESTAMP", False),
    ("event_date", "DATE", False),
    ("start_ts", "TIMESTAMP", True),
    ("end_ts", "TIMESTAMP", True),
    ("duration_ms", "BIGINT", True),
    ("severity", "STRING", True),
    ("app_name", "STRING", True),
    ("component", "STRING", True),
    ("environment", "STRING", True),
    ("sdk_version", "STRING", False),
    ("workspace_id", "STRING", True),
    ("workspace_url", "STRING", True),
    ("cluster_id", "STRING", True),
    ("job_id", "STRING", True),
    ("run_id", "STRING", True),
    ("task_key", "STRING", True),
    ("task_run_id", "STRING", True),
    ("task_attempt_number", "STRING", True),
    ("job_start_time", "STRING", True),
    ("job_trigger_type", "STRING", True),
    ("notebook_path", "STRING", True),
    ("user_name", "STRING", True),
    ("run_as_user_name", "STRING", True),
    ("source_table", "STRING", True),
    ("target_table", "STRING", True),
    ("row_count", "BIGINT", True),
    ("metric_name", "STRING", True),
    ("metric_value", "DOUBLE", True),
    ("error_class", "STRING", True),
    ("error_message", "STRING", True),
    ("stack_trace_hash", "STRING", True),
    ("error_frames_json", "STRING", True),
    ("metadata_json", "STRING", True),
    ("created_at", "TIMESTAMP", False),
)
EVENT_COLUMNS = tuple(name for name, _, _ in EVENT_SCHEMA)
_TABLE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*")


def create_table_sql(table_name: str) -> str:
    """Return deployment DDL without executing it or creating a table.

    Names use three simple Unity Catalog identifiers: catalog.schema.table.
    The schema and write permission should be managed by the deployment owner.
    """
    table_name = _validate_table_name(table_name)
    columns = ",\n".join(
        f"  {name} {sql_type}" + ("" if nullable else " NOT NULL")
        for name, sql_type, nullable in EVENT_SCHEMA
    )
    return (
        f"CREATE TABLE IF NOT EXISTS {table_name} (\n{columns}\n)\n"
        "USING DELTA\nPARTITIONED BY (event_date);"
    )


@dataclass
class DeltaSink:
    """Insert each event synchronously using a typed staging view.

    The caller owns table creation. There is no buffering or automatic retry.
    """

    spark: Any
    table_name: str

    def __post_init__(self) -> None:
        if self.spark is None:
            raise EventLoggerConfigurationError("DeltaSink requires a Spark session.")
        self.table_name = _validate_table_name(self.table_name)

    def emit(self, event: EventRecord) -> None:
        """Append one event and release its temporary view, even on failure."""
        event_dict = event.as_dict()
        row = {column: event_dict[column] for column in EVENT_COLUMNS}
        view_name = f"databricks_event_logger_event_{uuid4().hex}"
        dataframe = self.spark.createDataFrame([row], schema=_event_schema())
        dataframe.createOrReplaceTempView(view_name)
        columns = ", ".join(EVENT_COLUMNS)
        try:
            self.spark.sql(
                f"INSERT INTO {self.table_name} ({columns}) SELECT {columns} FROM {view_name}"
            ).collect()
        finally:
            try:
                self.spark.catalog.dropTempView(view_name)
            except Exception:
                # Warnings-as-errors must not replace an INSERT failure or turn
                # an acknowledged insertion into a failure that invites retry.
                try:
                    warnings.warn(
                        f"DeltaSink could not clean up staging view {view_name!r}.",
                        RuntimeWarning,
                        stacklevel=2,
                    )
                except BaseException:
                    pass

    def validate(self) -> None:
        """Check table access and schema compatibility without writing.

        Required columns must have the specified types. Optional event columns
        and additional table columns must allow nulls. This does not establish
        INSERT permission, the table provider, or arbitrary table constraints.
        """
        try:
            actual = {item.name: item for item in self.spark.table(self.table_name).schema}
        except Exception as exc:
            raise EventLoggerConfigurationError(
                f"DeltaSink could not read the schema of event table {self.table_name!r}."
            ) from exc
        missing = sorted(set(EVENT_COLUMNS) - actual.keys())
        if missing:
            raise EventLoggerConfigurationError(
                f"DeltaSink event table is missing required columns: {', '.join(missing)}"
            )
        problems = []
        for name, sql_type, nullable in EVENT_SCHEMA:
            column = actual[name]
            if column.dataType.simpleString().upper() != sql_type:
                problems.append(f"{name} must have type {sql_type}")
            if nullable and not column.nullable:
                problems.append(f"{name} must allow nulls")
        for name in sorted(actual.keys() - set(EVENT_COLUMNS)):
            if not actual[name].nullable:
                problems.append(f"additional column {name} must allow nulls")
        if problems:
            raise EventLoggerConfigurationError(
                "DeltaSink event table has an incompatible schema: " + "; ".join(problems)
            )


def _validate_table_name(table_name: str) -> str:
    if not isinstance(table_name, str) or not table_name.strip():
        raise EventLoggerConfigurationError("DeltaSink requires a table name.")
    table_name = table_name.strip()
    if not _TABLE_NAME.fullmatch(table_name):
        raise EventLoggerConfigurationError(
            "table_name must be a three-part Unity Catalog identifier using only letters, "
            "numbers, and underscores, for example 'catalog.schema.event_log'."
        )
    return table_name


def _event_schema() -> Any:
    """Build the staging schema lazily so PySpark stays an optional dependency."""
    from pyspark.sql.types import (  # noqa: PLC0415
        DateType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    types = {
        "STRING": StringType,
        "TIMESTAMP": TimestampType,
        "DATE": DateType,
        "BIGINT": LongType,
        "DOUBLE": DoubleType,
    }
    return StructType([
        StructField(name, types[sql_type](), nullable=nullable)
        for name, sql_type, nullable in EVENT_SCHEMA
    ])
