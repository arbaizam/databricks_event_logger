import io
import json
import re
from pathlib import Path

import pytest

from databricks_event_logger import ConsoleSink, DeltaSink, MemorySink
from databricks_event_logger.errors import EventLoggerConfigurationError
from databricks_event_logger.event import EventRecord
from databricks_event_logger.sinks.delta import EVENT_COLUMNS


def test_memory_sink_stores_events_in_order():
    """
    What: Stores emitted events in insertion order.
    Why: Databricks-hosted unit tests use MemorySink for deterministic asserts.
    Fails when: MemorySink drops or reorders emitted events.
    """
    sink = MemorySink()
    first = EventRecord("first")
    second = EventRecord("second")

    sink.emit(first)
    sink.emit(second)

    assert sink.events == [first, second]


def test_console_sink_writes_one_json_line():
    """
    What: Writes one JSON object per emitted event.
    Why: ConsoleSink should be usable for notebook diagnostics.
    Fails when: Console output is not valid JSON.
    """
    stream = io.StringIO()
    sink = ConsoleSink(stream=stream)

    sink.emit(EventRecord("reporting.step", status="success"))

    payload = json.loads(stream.getvalue())
    assert payload["event_name"] == "reporting.step"
    assert payload["status"] == "success"


def test_delta_sink_requires_table_name():
    """
    What: Rejects an empty Delta table name at sink construction time.
    Why: Misconfigured persistence should fail before business code starts.
    Fails when: DeltaSink accepts missing table configuration.
    """
    with pytest.raises(EventLoggerConfigurationError, match="table name"):
        DeltaSink(spark=object(), table_name="")


def test_delta_sink_rejects_unsafe_table_name():
    """
    What: Rejects table names that are not simple three-part UC identifiers.
    Why: DeltaSink interpolates the table identifier into SQL and must fail closed.
    Fails when: Widget-sourced table names can inject arbitrary SQL.
    """
    with pytest.raises(EventLoggerConfigurationError, match="three-part"):
        DeltaSink(
            spark=object(),
            table_name="catalog.schema.event_log; DROP TABLE catalog.schema.event_log",
        )


def test_delta_sink_inserts_through_sql_with_typed_staging_view():
    """
    What: Stages one typed event row and inserts it into an existing table via SQL.
    Why: SQL DDL owns table nullability, and DeltaSink must not create tables from PySpark.
    Fails when: DeltaSink goes back to DataFrameWriter.saveAsTable or inferred schemas.
    """
    pytest.importorskip("pyspark")
    spark = _FakeSpark()
    sink = DeltaSink(spark=spark, table_name="catalog.schema.event_log")
    event = EventRecord("reporting.delta_write", metadata={"rows": 10})

    sink.emit(event)

    assert spark.data[0]["event_name"] == "reporting.delta_write"
    assert spark.schema["event_name"].nullable is False
    assert spark.schema["task_run_id"].nullable is True
    assert spark.schema["job_trigger_type"].nullable is True
    assert spark.schema["metadata_json"].nullable is True
    assert spark.dataframe.view_name.startswith("databricks_event_logger_event_")
    assert spark.sql_calls[0].startswith("INSERT INTO catalog.schema.event_log")
    assert "event_name, event_type, status, event_id" in spark.sql_calls[0]
    assert spark.sql_calls[1].startswith("DROP VIEW IF EXISTS databricks_event_logger_event_")


def test_delta_sink_validate_checks_required_columns():
    """
    What: Validates that a configured event table exposes the v1 event columns.
    Why: Production bootstrap should fail before business code runs when schema is wrong.
    Fails when: validate_sink=True cannot detect missing event-log columns.
    """
    spark = _DescribeSpark(columns=["event_name"])
    sink = DeltaSink(spark=spark, table_name="catalog.schema.event_log")

    with pytest.raises(EventLoggerConfigurationError, match="missing required columns"):
        sink.validate()


def test_delta_sink_validate_accepts_expected_columns():
    """
    What: Accepts a described table that contains the required v1 columns.
    Why: validate_sink=True should be usable against a correctly deployed table.
    Fails when: validation rejects the standard event table schema.
    """
    spark = _DescribeSpark(columns=list(EVENT_COLUMNS))
    sink = DeltaSink(spark=spark, table_name="catalog.schema.event_log")

    sink.validate()

    assert spark.sql_calls == ["DESCRIBE TABLE catalog.schema.event_log"]


def test_delta_sink_preserves_insert_error_when_cleanup_fails():
    """
    What: Keeps the SQL INSERT exception when temp-view cleanup also fails.
    Why: The actionable persistence error should not be hidden by cleanup failure.
    Fails when: DROP VIEW errors mask the original insert failure.
    """
    pytest.importorskip("pyspark")
    spark = _InsertAndCleanupFailSpark()
    sink = DeltaSink(spark=spark, table_name="catalog.schema.event_log")

    with pytest.warns(RuntimeWarning, match="clean up staging view"):
        with pytest.raises(RuntimeError, match="insert failed"):
            sink.emit(EventRecord("reporting.delta_write"))


def test_event_columns_match_create_event_log_ddl():
    """
    What: Compares DeltaSink EVENT_COLUMNS to the SQL table template.
    Why: DDL/schema drift breaks persisted inserts in Databricks.
    Fails when: A column is added to one representation but not the other.
    """
    ddl_path = Path(__file__).parents[2] / "resources" / "sql" / "create_event_log.sql"
    ddl_text = ddl_path.read_text(encoding="utf-8")
    ddl_columns = [
        match.group(1)
        for line in ddl_text.splitlines()
        if (match := re.match(r"\s{2}([a-z_]+)\s+", line))
    ]

    assert ddl_columns == list(EVENT_COLUMNS)


class _FakeSpark:
    """
    Minimal Spark test double for DeltaSink SQL-write assertions.
    """

    def __init__(self) -> None:
        self.data = None
        self.schema = None
        self.dataframe = _FakeDataFrame()
        self.sql_calls: list[str] = []

    def createDataFrame(self, data, schema=None):  # noqa: N802 - Spark API casing.
        """
        Capture the staged row and schema.
        """
        self.data = data
        self.schema = schema
        return self.dataframe

    def sql(self, query: str) -> None:
        """
        Capture SQL statements in a whitespace-normalized form.
        """
        self.sql_calls.append(" ".join(query.split()))


class _FakeDataFrame:
    """
    Minimal DataFrame test double for temp-view registration.
    """

    def __init__(self) -> None:
        self.view_name = ""

    def createOrReplaceTempView(self, name: str) -> None:
        """
        Capture the temp view name.
        """
        self.view_name = name


class _DescribeSpark:
    """
    Minimal Spark test double for DeltaSink.validate.
    """

    def __init__(self, columns: list[str]) -> None:
        self.columns = columns
        self.sql_calls: list[str] = []

    def sql(self, query: str):
        """
        Return DESCRIBE TABLE rows for the configured columns.
        """
        self.sql_calls.append(" ".join(query.split()))
        return _DescribeResult(self.columns)


class _DescribeResult:
    """
    Minimal DESCRIBE TABLE result test double.
    """

    def __init__(self, columns: list[str]) -> None:
        self.columns = columns

    def collect(self):
        """
        Return dictionaries shaped like Spark DESCRIBE TABLE rows.
        """
        return [{"col_name": column} for column in self.columns]


class _InsertAndCleanupFailSpark(_FakeSpark):
    """
    Spark test double that raises for both insert and cleanup SQL.
    """

    def sql(self, query: str) -> None:
        """
        Raise distinct errors for insert and cleanup.
        """
        normalized = " ".join(query.split())
        self.sql_calls.append(normalized)
        if normalized.startswith("INSERT INTO"):
            raise RuntimeError("insert failed")
        if normalized.startswith("DROP VIEW"):
            raise RuntimeError("drop failed")
