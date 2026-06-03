import io
import json

import pytest

from databricks_event_logger.errors import EventLoggerConfigurationError
from databricks_event_logger.event import EventRecord
from databricks_event_logger.sinks.console import ConsoleSink
from databricks_event_logger.sinks.delta import DeltaSink
from databricks_event_logger.sinks.memory import MemorySink


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


def test_delta_sink_inserts_through_sql_with_typed_staging_view():
    """
    What: Stages one typed event row and inserts it into an existing table via SQL.
    Why: SQL DDL owns table nullability, and DeltaSink must not create tables from PySpark.
    Fails when: DeltaSink goes back to DataFrameWriter.saveAsTable or inferred schemas.
    """
    spark = _FakeSpark()
    sink = DeltaSink(spark=spark, table_name="catalog.schema.event_log")
    event = EventRecord("reporting.delta_write", metadata={"rows": 10})

    sink.emit(event)

    assert spark.data[0]["event_name"] == "reporting.delta_write"
    assert spark.schema["event_name"].nullable is False
    assert spark.schema["metadata_json"].nullable is True
    assert spark.dataframe.view_name.startswith("databricks_event_logger_event_")
    assert spark.sql_calls[0].startswith("INSERT INTO catalog.schema.event_log")
    assert "event_name, event_type, status, event_id" in spark.sql_calls[0]
    assert spark.sql_calls[1].startswith("DROP VIEW IF EXISTS databricks_event_logger_event_")


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
