import io
import json
import re
import warnings
from types import SimpleNamespace

import pytest

from databricks_event_logger import ConsoleSink, DeltaSink, MemorySink, create_table_sql
from databricks_event_logger.errors import EventLoggerConfigurationError
from databricks_event_logger.event import EventRecord
from databricks_event_logger.sinks.delta import EVENT_COLUMNS, EVENT_SCHEMA, _event_schema


def test_memory_sink_stores_events_in_order():
    sink = MemorySink()
    first, second = EventRecord("first"), EventRecord("second")
    sink.emit(first)
    sink.emit(second)
    assert sink.events == [first, second]


def test_console_sink_writes_one_json_line():
    stream = io.StringIO()
    ConsoleSink(stream=stream).emit(EventRecord("positions.publish", metric_value=7))
    assert len(stream.getvalue().splitlines()) == 1
    data = json.loads(stream.getvalue())
    assert data["event_name"] == "positions.publish"
    assert data["metric_value"] == 7.0
    assert "context" not in data


@pytest.mark.parametrize(
    "name", ["", None, "table", "a.b.c; DROP TABLE a.b.c", "a.b.c\nDROP TABLE x"],
)
def test_delta_sink_and_ddl_reject_invalid_identifiers(name):
    with pytest.raises(EventLoggerConfigurationError):
        DeltaSink(spark=object(), table_name=name)
    with pytest.raises(EventLoggerConfigurationError):
        create_table_sql(name)


def test_delta_sink_requires_spark():
    with pytest.raises(EventLoggerConfigurationError, match="Spark"):
        DeltaSink(None, "catalog.schema.events")


def test_deployment_ddl_matches_flat_event_columns():
    ddl = create_table_sql("catalog.schema.events")
    columns = re.findall(r"^  ([a-z_]+) ", ddl, re.MULTILINE)
    assert columns == list(EVENT_COLUMNS)
    assert set(columns) == EventRecord("example").as_dict().keys()
    assert "metric_value DOUBLE" in ddl
    assert "row_count BIGINT" in ddl
    assert "event_ts TIMESTAMP NOT NULL" in ddl
    assert "error_frames_json STRING" in ddl
    assert "USING DELTA" in ddl
    assert "PARTITIONED BY (event_date)" in ddl


def test_delta_sink_stages_a_verified_typed_row_and_collects_insert_result():
    pytest.importorskip("pyspark")
    spark = FakeSpark()
    event = EventRecord("positions.publish", metric_value=7, metadata_json='{"count":10}')

    DeltaSink(spark, "catalog.schema.events").emit(event)

    from pyspark.sql.types import _make_type_verifier

    _make_type_verifier(spark.staging_schema)(spark.staged_rows[0])
    assert spark.staged_rows[0]["metric_value"] == 7.0
    assert spark.staging_schema.fieldNames() == list(EVENT_COLUMNS)
    assert spark.staging_schema["event_name"].nullable is False
    assert spark.staging_schema["error_frames_json"].nullable is True
    assert len(spark.sql_calls) == 1
    assert spark.sql_calls[0].startswith("INSERT INTO catalog.schema.events")
    assert spark.insert_result.collected is True
    assert spark.dropped == [spark.dataframe.view_name]


@pytest.mark.parametrize("fail_collect", [False, True])
def test_failed_insert_still_drops_view_and_preserves_error_under_warnings_as_errors(fail_collect):
    pytest.importorskip("pyspark")
    spark = FakeSpark(fail_insert=not fail_collect, fail_collect=fail_collect, fail_cleanup=True)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        with pytest.raises(RuntimeError, match="insert failed"):
            DeltaSink(spark, "catalog.schema.events").emit(EventRecord("positions.publish"))
    assert spark.dropped == [spark.dataframe.view_name]


def test_cleanup_failure_does_not_fail_acknowledged_insert():
    pytest.importorskip("pyspark")
    spark = FakeSpark(fail_cleanup=True)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        DeltaSink(spark, "catalog.schema.events").emit(EventRecord("positions.publish"))
    assert spark.insert_result.collected


def test_cleanup_failure_is_reported_as_warning():
    pytest.importorskip("pyspark")
    with pytest.warns(RuntimeWarning, match="clean up staging view"):
        sink = DeltaSink(FakeSpark(fail_cleanup=True), "catalog.schema.events")
        sink.emit(EventRecord("example"))


def test_broken_warning_handler_cannot_hide_insert_error(monkeypatch):
    pytest.importorskip("pyspark")

    def broken_warning(*args, **kwargs):
        raise KeyboardInterrupt("broken warning handler")

    monkeypatch.setattr(warnings, "warn", broken_warning)
    sink = DeltaSink(
        FakeSpark(fail_insert=True, fail_cleanup=True), "catalog.schema.events",
    )
    with pytest.raises(RuntimeError, match="insert failed"):
        sink.emit(EventRecord("example"))


def test_validate_accepts_matching_schema_with_nullable_extra_column():
    schema = fake_schema() + [fake_field("team_note", "string", True)]
    spark = FakeSpark(table_schema=schema)
    DeltaSink(spark, "catalog.schema.events").validate()
    assert spark.table_calls == ["catalog.schema.events"]
    assert spark.sql_calls == []


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ("missing", "missing required columns: row_count"),
        ("type", "metric_value must have type DOUBLE"),
        ("nullability", "metadata_json must allow nulls"),
        ("extra_required", "additional column team_note must allow nulls"),
    ],
)
def test_validate_rejects_schema_that_cannot_accept_event_rows(change, message):
    schema = fake_schema()
    if change == "missing":
        schema = [item for item in schema if item.name != "row_count"]
    elif change == "type":
        schema = [
            fake_field("metric_value", "bigint", True) if item.name == "metric_value" else item
            for item in schema
        ]
    elif change == "nullability":
        schema = [
            fake_field("metadata_json", "string", False) if item.name == "metadata_json" else item
            for item in schema
        ]
    else:
        schema.append(fake_field("team_note", "string", False))
    with pytest.raises(EventLoggerConfigurationError, match=message):
        DeltaSink(FakeSpark(table_schema=schema), "catalog.schema.events").validate()


def test_validate_wraps_table_access_failure():
    class MissingTable:
        def table(self, name):
            raise RuntimeError("not found")

    with pytest.raises(EventLoggerConfigurationError, match="could not read the schema") as error:
        DeltaSink(MissingTable(), "catalog.schema.events").validate()
    assert isinstance(error.value.__cause__, RuntimeError)


def test_validate_accepts_real_pyspark_schema_without_starting_spark():
    pytest.importorskip("pyspark")
    DeltaSink(FakeSpark(table_schema=_event_schema()), "catalog.schema.events").validate()


def fake_field(name, data_type, nullable):
    return SimpleNamespace(
        name=name, nullable=nullable, dataType=SimpleNamespace(simpleString=lambda: data_type),
    )


def fake_schema():
    return [
        fake_field(name, sql_type.lower(), nullable) for name, sql_type, nullable in EVENT_SCHEMA
    ]


class FakeResult:
    def __init__(self, fail=False):
        self.fail = fail
        self.collected = False

    def collect(self):
        if self.fail:
            raise RuntimeError("insert failed")
        self.collected = True
        return []


class FakeDataFrame:
    def __init__(self):
        self.view_name = None

    def createOrReplaceTempView(self, name):  # noqa: N802
        self.view_name = name


class FakeSpark:
    """Implements the Spark methods exercised by delivery and schema inspection."""

    def __init__(
        self, *, fail_insert=False, fail_collect=False, fail_cleanup=False, table_schema=None,
    ):
        self.fail_insert = fail_insert
        self.fail_cleanup = fail_cleanup
        self.insert_result = FakeResult(fail_collect)
        self.dataframe = FakeDataFrame()
        self.table_schema = table_schema
        self.catalog = self
        self.sql_calls = []
        self.table_calls = []
        self.dropped = []
        self.staged_rows = None
        self.staging_schema = None

    def createDataFrame(self, rows, schema):  # noqa: N802
        self.staged_rows = rows
        self.staging_schema = schema
        return self.dataframe

    def sql(self, query):
        self.sql_calls.append(" ".join(query.split()))
        if self.fail_insert:
            raise RuntimeError("insert failed")
        return self.insert_result

    def dropTempView(self, name):  # noqa: N802
        self.dropped.append(name)
        if self.fail_cleanup:
            raise RuntimeError("cleanup failed")
        return True

    def table(self, name):
        self.table_calls.append(name)
        return SimpleNamespace(schema=self.table_schema)
