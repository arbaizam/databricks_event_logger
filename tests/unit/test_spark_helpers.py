import pytest

from databricks_event_logger import EventLogger
from databricks_event_logger.serialization import deserialize_metadata
from databricks_event_logger.sinks.memory import MemorySink
from databricks_event_logger.spark import (
    count_rows,
    read_table,
    run_sql,
    table_exists,
    validate_row_count,
    write_delta,
)


def test_read_table_logs_source_and_returns_dataframe():
    """
    What: Logs a Spark table read helper event and returns the Spark DataFrame.
    Why: Notebooks need a low-friction observable replacement for spark.table.
    Fails when: read_table drops source-table context or changes return behavior.
    """
    sink = MemorySink()
    logger = EventLogger(sink=sink)
    spark = _FakeSpark(row_count=3)

    dataframe = read_table(
        "catalog.schema.source",
        logger=logger,
        spark=spark,
        as_of_date="2026-06-30",
    )

    event = sink.events[-1]
    metadata = deserialize_metadata(event.metadata_json)
    assert dataframe is spark.dataframe
    assert spark.table_calls == ["catalog.schema.source"]
    assert event.event_name == "delta.read"
    assert event.event_type == "delta_read"
    assert event.source_table == "catalog.schema.source"
    assert metadata["as_of_date"] == "2026-06-30"
    assert metadata["spark_operation"] == "table"


def test_write_delta_logs_target_and_writer_options():
    """
    What: Logs a Delta write helper event around DataFrameWriter.saveAsTable.
    Why: Write observability should not require a custom context manager at every call site.
    Fails when: write_delta stops applying mode/options or target-table metadata.
    """
    sink = MemorySink()
    logger = EventLogger(sink=sink)
    dataframe = _FakeDataFrame(row_count=3)

    write_delta(
        dataframe,
        table="catalog.schema.target",
        logger=logger,
        mode="overwrite",
        overwrite_schema=True,
        merge_schema=True,
        replace_where="AsOfDate = DATE '2026-06-30'",
        row_count=3,
        partition_by=["event_date"],
        options={"customOption": "custom"},
        metadata={"as_of_date": "2026-06-30"},
    )

    event = sink.events[-1]
    metadata = deserialize_metadata(event.metadata_json)
    assert dataframe.write.saved_table == "catalog.schema.target"
    assert dataframe.write.format_name == "delta"
    assert dataframe.write.mode_name == "overwrite"
    assert dataframe.write.options == {
        "customOption": "custom",
        "mergeSchema": "true",
        "overwriteSchema": "true",
        "replaceWhere": "AsOfDate = DATE '2026-06-30'",
    }
    assert dataframe.write.partition_columns == ("event_date",)
    assert event.event_type == "delta_write"
    assert event.target_table == "catalog.schema.target"
    assert event.row_count == 3
    assert metadata["mode"] == "overwrite"
    assert metadata["overwrite_schema"] is True
    assert metadata["merge_schema"] is True
    assert metadata["replace_where"] == "AsOfDate = DATE '2026-06-30'"
    assert metadata["as_of_date"] == "2026-06-30"


def test_run_sql_logs_hash_without_preview_by_default():
    """
    What: Logs a SQL hash without a SQL preview by default.
    Why: SQL text can contain sensitive predicates and literals.
    Fails when: run_sql stores SQL preview metadata without explicit opt-in.
    """
    sink = MemorySink()
    logger = EventLogger(sink=sink)
    spark = _FakeSpark(row_count=3)

    result = run_sql(
        "OPTIMIZE catalog.schema.target",
        logger=logger,
        spark=spark,
        action="optimize",
    )

    metadata = deserialize_metadata(sink.events[-1].metadata_json)
    assert result == "sql-result"
    assert spark.sql_calls == ["OPTIMIZE catalog.schema.target"]
    assert sink.events[-1].event_type == "sql"
    assert metadata["action"] == "optimize"
    assert len(metadata["sql_hash"]) == 64
    assert "sql_preview" not in metadata


def test_run_sql_preview_is_opt_in_and_redacts_literals():
    """
    What: Includes a redacted SQL preview only when explicitly requested.
    Why: Optional previews should not expose string or numeric literals.
    Fails when: Preview redaction leaks literal predicate values.
    """
    sink = MemorySink()
    logger = EventLogger(sink=sink)
    spark = _FakeSpark(row_count=3)

    run_sql(
        "SELECT * FROM catalog.schema.target WHERE account_id = 12345 "
        "AND token = 'secret-token' AND rate >= 12.5",
        logger=logger,
        spark=spark,
        include_sql_preview=True,
    )

    metadata = deserialize_metadata(sink.events[-1].metadata_json)
    assert "secret-token" not in metadata["sql_preview"]
    assert "12345" not in metadata["sql_preview"]
    assert "12.5" not in metadata["sql_preview"]
    assert "'[REDACTED]'" in metadata["sql_preview"]
    assert "account_id = ?" in metadata["sql_preview"]


def test_run_sql_preview_is_truncated_when_enabled():
    """
    What: Truncates redacted SQL previews to the configured length.
    Why: Optional preview metadata still needs bounded size.
    Fails when: Long SQL previews are stored unbounded.
    """
    sink = MemorySink()
    logger = EventLogger(sink=sink)
    spark = _FakeSpark(row_count=3)

    run_sql(
        "SELECT " + ", ".join(f"column_{index}" for index in range(20)),
        logger=logger,
        spark=spark,
        include_sql_preview=True,
        sql_preview_chars=40,
    )

    metadata = deserialize_metadata(sink.events[-1].metadata_json)
    assert metadata["sql_preview"].endswith("...[TRUNCATED]")
    assert len(metadata["sql_preview"]) <= 55


def test_validate_row_count_logs_success_and_returns_count():
    """
    What: Counts rows, logs a successful validation event, and returns the count.
    Why: Materialized validation should be explicit and reusable.
    Fails when: validate_row_count omits row_count or changes return behavior.
    """
    sink = MemorySink()
    logger = EventLogger(sink=sink)
    spark = _FakeSpark(row_count=3)

    count = validate_row_count(
        table="catalog.schema.target",
        logger=logger,
        spark=spark,
        expected_min=1,
        validation_name="target_not_empty",
    )

    event = sink.events[-1]
    assert count == 3
    assert event.event_name == "validation.row_count"
    assert event.status == "success"
    assert event.row_count == 3
    assert event.source_table == "catalog.schema.target"
    assert deserialize_metadata(event.metadata_json)["validation_name"] == "target_not_empty"


def test_validate_row_count_logs_failure_and_reraises():
    """
    What: Logs failed validation details when row-count expectations are not met.
    Why: Failed data checks should be visible before the task exception propagates.
    Fails when: validation failures do not emit failed events.
    """
    sink = MemorySink()
    logger = EventLogger(sink=sink)
    spark = _FakeSpark(row_count=0)

    with pytest.raises(ValueError, match="expected at least 1"):
        validate_row_count(
            "catalog.schema.target",
            logger=logger,
            spark=spark,
            expected_min=1,
        )

    event = sink.events[-1]
    assert event.status == "failed"
    assert event.severity == "error"
    assert event.row_count == 0
    assert event.error_class == "ValueError"
    assert event.stack_trace_hash


def test_validate_row_count_preserves_original_error_when_strict_logging_fails():
    """
    What: Re-raises the validation failure even when strict failure logging fails.
    Why: Helper observability must not mask the business/data-quality exception.
    Fails when: A sink error replaces the original validation exception.
    """
    logger = EventLogger(sink=_FailingSink(), strict_logging=True)
    spark = _FakeSpark(row_count=0)

    with pytest.warns(RuntimeWarning, match="helper failure event"):
        with pytest.raises(ValueError, match="expected at least 1"):
            validate_row_count(
                "catalog.schema.target",
                logger=logger,
                spark=spark,
                expected_min=1,
            )


def test_count_rows_logs_materialized_count_for_dataframe():
    """
    What: Counts a DataFrame through an explicit helper and logs row_count.
    Why: Users need an obvious, opt-in materialization helper separate from reads.
    Fails when: count_rows does not emit a Spark action event.
    """
    sink = MemorySink()
    logger = EventLogger(sink=sink)
    dataframe = _FakeDataFrame(row_count=7)

    count = count_rows(
        dataframe,
        spark=_FakeSpark(row_count=0),
        logger=logger,
        table_name="catalog.schema.target",
        metric_context="post_write",
    )

    event = sink.events[-1]
    metadata = deserialize_metadata(event.metadata_json)
    assert count == 7
    assert event.event_name == "spark.count"
    assert event.event_type == "spark_action"
    assert event.row_count == 7
    assert event.source_table == "catalog.schema.target"
    assert metadata["metric_context"] == "post_write"


def test_count_rows_logs_materialized_count_for_table_name():
    """
    What: Counts a table name by resolving it through Spark.
    Why: Table-count smoke checks should be one line in notebooks.
    Fails when: count_rows cannot count by table name.
    """
    sink = MemorySink()
    logger = EventLogger(sink=sink)
    spark = _FakeSpark(row_count=5)

    count = count_rows("catalog.schema.target", logger=logger, spark=spark)

    assert count == 5
    assert spark.table_calls == ["catalog.schema.target"]
    assert sink.events[-1].source_table == "catalog.schema.target"


def test_count_rows_preserves_original_error_when_strict_logging_fails():
    """
    What: Re-raises the Spark count error even when strict failure logging fails.
    Why: Helper failure telemetry must not hide the actual Spark action failure.
    Fails when: A sink error replaces the original count exception.
    """
    logger = EventLogger(sink=_FailingSink(), strict_logging=True)

    with pytest.warns(RuntimeWarning, match="helper failure event"):
        with pytest.raises(RuntimeError, match="count failed"):
            count_rows(
                _FailingCountDataFrame(),
                spark=_FakeSpark(row_count=0),
                logger=logger,
            )


def test_table_exists_logs_warning_when_table_is_missing():
    """
    What: Logs table-existence checks without raising for a missing table.
    Why: Bootstrap and smoke tests should make missing objects visible.
    Fails when: table_exists cannot report false with a warning event.
    """
    sink = MemorySink()
    logger = EventLogger(sink=sink)
    spark = _FakeSpark(row_count=0, existing_tables=set())

    exists = table_exists(
        table="catalog.schema.missing",
        logger=logger,
        spark=spark,
        check_context="smoke",
    )

    event = sink.events[-1]
    metadata = deserialize_metadata(event.metadata_json)
    assert exists is False
    assert event.status == "warning"
    assert event.severity == "warning"
    assert event.source_table == "catalog.schema.missing"
    assert metadata["table_exists"] is False
    assert metadata["check_context"] == "smoke"


def test_table_exists_logs_success_when_table_exists():
    """
    What: Logs table-existence success when Spark catalog finds the table.
    Why: Positive smoke checks should also leave an event trail.
    Fails when: table_exists drops successful validation events.
    """
    sink = MemorySink()
    logger = EventLogger(sink=sink)
    spark = _FakeSpark(row_count=0, existing_tables={"catalog.schema.target"})

    exists = table_exists("catalog.schema.target", logger=logger, spark=spark)

    assert exists is True
    assert sink.events[-1].status == "success"
    assert sink.events[-1].severity == "info"


def test_table_exists_falls_back_to_describe_for_simple_table_names():
    """
    What: Uses DESCRIBE TABLE when Spark catalog tableExists is unavailable.
    Why: Some Databricks runtimes/access modes may restrict catalog API behavior.
    Fails when: table_exists cannot use SQL fallback for simple UC names.
    """
    sink = MemorySink()
    logger = EventLogger(sink=sink)
    spark = _FakeSpark(row_count=0)
    spark.catalog = _FailingCatalog()

    exists = table_exists("catalog.schema.target", logger=logger, spark=spark)

    assert exists is True
    assert spark.sql_calls == ["DESCRIBE TABLE catalog.schema.target"]
    assert sink.events[-1].status == "success"


def test_table_exists_preserves_original_error_when_strict_logging_fails():
    """
    What: Re-raises catalog inspection errors when strict failure logging fails.
    Why: Catalog errors should not be replaced by sink errors.
    Fails when: A sink error masks the table-existence failure.
    """
    logger = EventLogger(sink=_FailingSink(), strict_logging=True)
    spark = _FakeSpark(row_count=0)
    spark.catalog = _FailingCatalog()

    with pytest.warns(RuntimeWarning, match="helper failure event"):
        with pytest.raises(RuntimeError, match="catalog failed"):
            table_exists("target", logger=logger, spark=spark)


class _FakeSpark:
    """
    Minimal Spark session test double for helper tests.
    """

    def __init__(self, row_count: int, existing_tables: set[str] | None = None) -> None:
        self.dataframe = _FakeDataFrame(row_count=row_count)
        self.catalog = _FakeCatalog(existing_tables or {"catalog.schema.target"})
        self.table_calls: list[str] = []
        self.sql_calls: list[str] = []

    def table(self, table_name: str):
        """
        Return a fake DataFrame and capture the requested table name.
        """
        self.table_calls.append(table_name)
        return self.dataframe

    def sql(self, sql_text: str):
        """
        Capture SQL text and return a fake result.
        """
        self.sql_calls.append(sql_text)
        return "sql-result"


class _FakeDataFrame:
    """
    Minimal Spark DataFrame test double.
    """

    def __init__(self, row_count: int) -> None:
        self.row_count = row_count
        self.write = _FakeWriter()

    def count(self) -> int:
        """
        Return the configured row count.
        """
        return self.row_count


class _FakeWriter:
    """
    Minimal DataFrameWriter test double.
    """

    def __init__(self) -> None:
        self.format_name = None
        self.mode_name = None
        self.options: dict[str, str] = {}
        self.partition_columns: tuple[str, ...] = ()
        self.saved_table = None

    def format(self, name: str):
        """
        Capture the writer format.
        """
        self.format_name = name
        return self

    def mode(self, name: str):
        """
        Capture the writer mode.
        """
        self.mode_name = name
        return self

    def option(self, key: str, value: str):
        """
        Capture one writer option.
        """
        self.options[key] = value
        return self

    def partitionBy(self, *columns: str):  # noqa: N802 - Spark API casing.
        """
        Capture partition columns.
        """
        self.partition_columns = columns
        return self

    def saveAsTable(self, table_name: str) -> None:  # noqa: N802 - Spark API casing.
        """
        Capture the target table name.
        """
        self.saved_table = table_name


class _FakeCatalog:
    """
    Minimal Spark catalog test double.
    """

    def __init__(self, existing_tables: set[str]) -> None:
        self.existing_tables = existing_tables

    def tableExists(self, table_name: str) -> bool:  # noqa: N802 - Spark API casing.
        """
        Return whether the table exists in the fake catalog.
        """
        return table_name in self.existing_tables


class _FailingCountDataFrame:
    """
    DataFrame test double whose count action fails.
    """

    def count(self) -> int:
        """
        Raise a fake Spark count failure.
        """
        raise RuntimeError("count failed")


class _FailingCatalog:
    """
    Catalog test double whose table-exists check fails.
    """

    def tableExists(self, table_name: str) -> bool:  # noqa: N802 - Spark API casing.
        """
        Raise a fake catalog failure.
        """
        raise RuntimeError("catalog failed")


class _FailingSink:
    """
    Sink test double that always fails on emit.
    """

    def emit(self, event) -> None:
        """
        Raise a fake sink failure.
        """
        raise RuntimeError("sink unavailable")
