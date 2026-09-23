"""``DeltaSink``: insert each event into an existing Delta table.

How one event is written:

1. Turn the event into a one-row Spark DataFrame, using the typed schema
   from ``schema.py``.
2. Register it as a temporary view with a unique name.
3. Run ``INSERT INTO <table> (<columns>) SELECT <columns> FROM <view>``.
   Naming the columns means the table's column order doesn't matter.
4. Drop the temporary view, even if the insert failed.

The sink never creates the table. Use ``create_table_sql()`` to get the SQL,
then run it as part of your deployment. There is no buffering and no retry.

PySpark is imported only when it's needed, so this package has no hard
dependency on it.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from databricks_event_logger.diagnostics import warn_safely
from databricks_event_logger.errors import EventLoggerConfigurationError
from databricks_event_logger.record import EventRecord
from databricks_event_logger.schema import EVENT_COLUMNS, EVENT_SCHEMA

# catalog.schema.table, where each part is a simple identifier (no quoting).
_THREE_PART_NAME = re.compile(
    r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*"
)
_VIEW_PREFIX = "databricks_event_logger_event_"


def create_table_sql(table_name: str) -> str:
    """Return the ``CREATE TABLE`` SQL for the event table. Nothing is run.

    The table is partitioned by ``event_date``. The catalog and schema must
    already exist when the SQL runs.

    Args:
        table_name: A three-part Unity Catalog name, like ``"main.observability.event_log"``.

    Raises:
        EventLoggerConfigurationError: If ``table_name`` isn't a valid three-part name.
    """
    table_name = _check_table_name(table_name)
    columns = ",\n".join(
        f"  {column.name} {column.sql_type}" + ("" if column.nullable else " NOT NULL")
        for column in EVENT_SCHEMA
    )
    return (
        f"CREATE TABLE IF NOT EXISTS {table_name} (\n{columns}\n)\n"
        "USING DELTA\nPARTITIONED BY (event_date);"
    )


@dataclass
class DeltaSink:
    """Insert each event into an existing Delta table, one insert per event.

    Attributes:
        spark: An active ``SparkSession``.
        table_name: A three-part Unity Catalog name, like ``"main.observability.event_log"``.

    Raises:
        EventLoggerConfigurationError: If ``spark`` is missing or ``table_name``
            is invalid.
    """

    spark: Any
    table_name: str

    def __post_init__(self) -> None:
        if self.spark is None:
            raise EventLoggerConfigurationError("DeltaSink requires a Spark session.")
        self.table_name = _check_table_name(self.table_name)

    def emit(self, event: EventRecord) -> None:
        """Insert one event. Any Spark error is raised to the logger."""
        row = event.as_dict()
        view_name = f"{_VIEW_PREFIX}{uuid4().hex}"
        dataframe = self.spark.createDataFrame(
            [{column: row[column] for column in EVENT_COLUMNS}], schema=_spark_schema()
        )
        dataframe.createOrReplaceTempView(view_name)
        columns = ", ".join(EVENT_COLUMNS)
        try:
            # collect() makes Spark actually run the insert before we return.
            self.spark.sql(
                f"INSERT INTO {self.table_name} ({columns}) SELECT {columns} FROM {view_name}"
            ).collect()
        finally:
            self._drop_view_quietly(view_name)

    def validate(self) -> None:
        """Check that the table exists and can store events. Nothing is written.

        Checks:
            - Every event column exists, with the expected type.
            - Columns that can be empty in an event allow NULL.
            - Any extra columns in the table allow NULL, because inserts
              leave them empty.

        This doesn't check INSERT permission, table constraints, or the
        storage format.

        Raises:
            EventLoggerConfigurationError: If the table can't be read, or its
                schema doesn't match. The message lists every problem found.
        """
        try:
            actual = {column.name: column for column in self.spark.table(self.table_name).schema}
        except Exception as exc:
            raise EventLoggerConfigurationError(
                f"DeltaSink could not read the schema of event table {self.table_name!r}."
            ) from exc

        missing = sorted(set(EVENT_COLUMNS) - actual.keys())
        if missing:
            raise EventLoggerConfigurationError(
                f"DeltaSink event table is missing required columns: {', '.join(missing)}"
            )
        problems = _schema_problems(actual)
        if problems:
            raise EventLoggerConfigurationError(
                "DeltaSink event table has an incompatible schema: " + "; ".join(problems)
            )

    def _drop_view_quietly(self, view_name: str) -> None:
        """Drop the staging view. Warn instead of raising if it fails.

        A failed cleanup must not hide an insert error. It also must not turn
        a successful insert into a failure, because that would invite a retry
        and a duplicate row.
        """
        try:
            self.spark.catalog.dropTempView(view_name)
        except Exception:
            # stacklevel=3 points the warning at the code that called emit().
            warn_safely(f"DeltaSink could not clean up staging view {view_name!r}.", stacklevel=3)


def _schema_problems(actual: dict[str, Any]) -> list[str]:
    """List the ways the table's columns (by name) don't match ``EVENT_SCHEMA``."""
    problems = []
    for expected in EVENT_SCHEMA:
        column = actual[expected.name]
        if column.dataType.simpleString().upper() != expected.sql_type:
            problems.append(f"{expected.name} must have type {expected.sql_type}")
        if expected.nullable and not column.nullable:
            problems.append(f"{expected.name} must allow nulls")
    for name in sorted(actual.keys() - set(EVENT_COLUMNS)):
        if not actual[name].nullable:
            problems.append(f"additional column {name} must allow nulls")
    return problems


def _check_table_name(table_name: Any) -> str:
    """Return the trimmed table name, or raise if it isn't ``catalog.schema.table``."""
    if not isinstance(table_name, str) or not table_name.strip():
        raise EventLoggerConfigurationError("DeltaSink requires a table name.")
    table_name = table_name.strip()
    if not _THREE_PART_NAME.fullmatch(table_name):
        raise EventLoggerConfigurationError(
            "table_name must be a three-part Unity Catalog identifier using only letters, "
            "numbers, and underscores, for example 'catalog.schema.event_log'."
        )
    return table_name


def _spark_schema() -> Any:
    """Build the PySpark ``StructType`` for ``EVENT_SCHEMA``.

    PySpark is imported here instead of at the top of the file, so that
    importing this package never requires PySpark.
    """
    from pyspark.sql.types import (  # noqa: PLC0415
        DateType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    spark_types = {
        "STRING": StringType,
        "TIMESTAMP": TimestampType,
        "DATE": DateType,
        "BIGINT": LongType,
        "DOUBLE": DoubleType,
    }
    return StructType([
        StructField(column.name, spark_types[column.sql_type](), nullable=column.nullable)
        for column in EVENT_SCHEMA
    ])
