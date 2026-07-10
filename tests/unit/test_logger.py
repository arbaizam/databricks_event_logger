import warnings
from contextvars import Context

import pytest

from databricks_event_logger import (
    CommonEvent,
    ConsoleSink,
    DeltaSink,
    EventLogger,
    EventSeverity,
    EventStatus,
    EventType,
    assert_observability_ready,
    check_observability_ready,
    get_default_logger,
    observe_notebook,
    observed,
)
from databricks_event_logger.context import RuntimeContext
from databricks_event_logger.errors import EventLoggerConfigurationError
from databricks_event_logger.serialization import deserialize_metadata
from databricks_event_logger.sinks.memory import MemorySink


def test_record_event_stamps_config_context_and_correlation():
    """
    What: Emits one custom event with logger-level identity fields.
    Why: Every event row should carry app/component/environment correlation data.
    Fails when: Logger configuration stops propagating to emitted events.
    """
    sink = MemorySink()
    logger = EventLogger(
        app_name="app",
        component="component",
        environment="dev",
        sink=sink,
        correlation_id="corr-1",
    )

    event = logger.record_event("reporting.checkpoint", metadata={"step": "ready"})

    assert sink.events == [event]
    assert event.app_name == "app"
    assert event.component == "component"
    assert event.environment == "dev"
    assert event.correlation_id == "corr-1"


def test_event_constants_are_accepted_as_event_fields():
    """
    What: Records an event using public event constants.
    Why: Constants should reduce caller typos without changing persisted values.
    Fails when: String enum constants leak enum names into event rows.
    """
    sink = MemorySink()
    logger = EventLogger(sink=sink)

    event = logger.record_event(
        CommonEvent.DELTA_WRITE,
        event_type=EventType.DELTA_WRITE,
        status=EventStatus.SUCCESS,
        severity=EventSeverity.INFO,
    )

    assert event.event_name == "delta.write"
    assert event.event_type == "delta_write"
    assert event.status == "success"
    assert event.severity == "info"


def test_logger_derives_correlation_id_from_task_context():
    """
    What: Uses Databricks task context for the default correlation id.
    Why: Events from the same task run should have a stable join key.
    Fails when: Logger always falls back to a random UUID in Databricks jobs.
    """
    logger = EventLogger(
        sink=MemorySink(),
        context=RuntimeContext(
            run_id="run-1",
            task_key="publish",
            task_attempt_number="2",
        ),
    )

    assert logger.correlation_id == "run-1:publish:2"


def test_logger_prefers_task_run_id_for_default_correlation_id():
    """
    What: Uses task_run_id when Databricks supplies it.
    Why: task_run_id is the most precise task-run correlation identifier.
    Fails when: A less specific run id is used despite task_run_id being known.
    """
    logger = EventLogger(
        sink=MemorySink(),
        context=RuntimeContext(
            run_id="run-1",
            task_key="publish",
            task_run_id="task-run-1",
        ),
    )

    assert logger.correlation_id == "task-run-1"


def test_logger_uses_run_id_when_no_task_context_is_available():
    """
    What: Falls back to run_id when task-level Databricks fields are unavailable.
    Why: Single-task jobs and some execution modes still need stable run correlation.
    Fails when: The run_id-only branch falls back to a random UUID.
    """
    logger = EventLogger(
        sink=MemorySink(),
        context=RuntimeContext(run_id="run-1"),
    )

    assert logger.correlation_id == "run-1"


def test_logged_event_records_success_and_return_value():
    """
    What: Decorates a successful function and returns the original result.
    Why: Instrumentation should not change business function behavior.
    Fails when: Decorators stop preserving return values or success status.
    """
    sink = MemorySink()
    logger = EventLogger(sink=sink)

    @logger.logged_event("reporting.build_positions")
    def build_positions():
        return 42

    assert build_positions() == 42
    assert sink.events[-1].event_name == "reporting.build_positions"
    assert sink.events[-1].status == "success"
    assert sink.events[-1].duration_ms is not None


def test_logged_event_metadata_factory_uses_call_arguments():
    """
    What: Builds decorator metadata from runtime function arguments.
    Why: Package-level instrumentation needs metadata such as ruleset/as_of_date at call time.
    Fails when: Decorators only support import-time static metadata.
    """
    sink = MemorySink()
    logger = EventLogger(sink=sink)

    @logger.logged_event(
        "reporting.apply_rules",
        event_type="business_process",
        metadata={"source": "decorator"},
        metadata_factory=lambda df, *, ruleset, as_of_date: {
            "df": df,
            "ruleset": ruleset,
            "as_of_date": as_of_date,
        },
    )
    def apply_rules(df, *, ruleset: str, as_of_date: str):
        return "ok"

    assert apply_rules("positions", ruleset="MVE", as_of_date="2026-06-30") == "ok"
    metadata = deserialize_metadata(sink.events[-1].metadata_json)

    assert metadata == {
        "as_of_date": "2026-06-30",
        "df": "positions",
        "ruleset": "MVE",
        "source": "decorator",
    }


def test_logged_event_metadata_factory_failure_does_not_block_function():
    """
    What: Continues running business code when metadata_factory raises.
    Why: Optional call-time metadata should not become adoption-breaking business logic.
    Fails when: A bad metadata_factory prevents the wrapped function from running.
    """
    sink = MemorySink()
    logger = EventLogger(sink=sink)
    calls = []

    def broken_metadata_factory(value):
        raise KeyError(f"missing-{value}")

    @logger.logged_event(
        "reporting.apply_rules",
        metadata={"source": "decorator"},
        metadata_factory=broken_metadata_factory,
    )
    def apply_rules(value):
        calls.append(value)
        return "ok"

    with pytest.warns(RuntimeWarning, match="metadata_factory"):
        assert apply_rules("positions") == "ok"
    metadata = deserialize_metadata(sink.events[-1].metadata_json)

    assert calls == ["positions"]
    assert sink.events[-1].status == "success"
    assert metadata["source"] == "decorator"
    assert metadata["metadata_factory_error"] is True
    assert metadata["metadata_factory_error_class"] == "KeyError"
    assert "missing-positions" in metadata["metadata_factory_error_message"]


def test_logged_event_records_failure_and_reraises_original_exception():
    """
    What: Decorates a failing function and re-raises its original exception.
    Why: Databricks task failure semantics must remain driven by business code.
    Fails when: Logging swallows, wraps, or masks business exceptions.
    """
    sink = MemorySink()
    logger = EventLogger(sink=sink)

    @logger.logged_event("reporting.fail")
    def fail():
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        fail()

    assert sink.events[-1].event_name == "reporting.fail"
    assert sink.events[-1].status == "failed"
    assert sink.events[-1].error_class == "ValueError"
    assert sink.events[-1].error_message == "boom"
    assert sink.events[-1].stack_trace_hash


def test_logged_event_metadata_factory_failure_preserves_business_exception():
    """
    What: Keeps the business failure when metadata_factory and business code both fail.
    Why: Databricks task status should be driven by the original business exception.
    Fails when: Factory exceptions mask the exception raised by the wrapped function.
    """
    sink = MemorySink()
    logger = EventLogger(sink=sink)

    def broken_metadata_factory():
        raise RuntimeError("metadata failed")

    @logger.logged_event(
        "reporting.fail",
        metadata_factory=broken_metadata_factory,
    )
    def fail():
        raise ValueError("business failed")

    with pytest.warns(RuntimeWarning, match="metadata_factory"):
        with pytest.raises(ValueError, match="business failed"):
            fail()
    metadata = deserialize_metadata(sink.events[-1].metadata_json)

    assert sink.events[-1].status == "failed"
    assert sink.events[-1].error_class == "ValueError"
    assert metadata["metadata_factory_error"] is True
    assert metadata["metadata_factory_error_class"] == "RuntimeError"


def test_context_manager_records_success_for_custom_block():
    """
    What: Records a custom block event when the block succeeds.
    Why: Context managers are required v1 API for non-helper notebook blocks.
    Fails when: Context manager usage stops emitting success events.
    """
    sink = MemorySink()
    logger = EventLogger(sink=sink)

    with logger.event("reporting.custom_block", target_table="silver.positions"):
        value = "done"

    assert value == "done"
    assert sink.events[-1].event_name == "reporting.custom_block"
    assert sink.events[-1].status == "success"
    assert sink.events[-1].target_table == "silver.positions"


def test_context_manager_records_failure_and_reraises():
    """
    What: Records a failed custom block and re-raises the original exception.
    Why: Notebook block instrumentation should not hide failures from Databricks.
    Fails when: Context manager failure handling changes exception behavior.
    """
    sink = MemorySink()
    logger = EventLogger(sink=sink)

    with pytest.raises(RuntimeError, match="bad block"):
        with logger.event("reporting.custom_block"):
            raise RuntimeError("bad block")

    assert sink.events[-1].status == "failed"
    assert sink.events[-1].error_class == "RuntimeError"


def test_context_manager_preserves_source_and_target_on_failure():
    """
    What: Records table fields on a failed custom block.
    Why: Failure diagnostics need the same source/target context as success events.
    Fails when: Failure handling drops I/O context fields.
    """
    sink = MemorySink()
    logger = EventLogger(sink=sink)

    with pytest.raises(RuntimeError, match="bad block"):
        with logger.event(
            "reporting.custom_block",
            source_table="bronze.positions",
            target_table="silver.positions",
        ):
            raise RuntimeError("bad block")

    assert sink.events[-1].source_table == "bronze.positions"
    assert sink.events[-1].target_table == "silver.positions"


def test_run_task_records_success_and_return_value():
    """
    What: Runs a task callable and records task success.
    Why: Explicit task wrappers are required for reliable SDK lifecycle events.
    Fails when: Task wrapper stops preserving callable return values.
    """
    sink = MemorySink()
    logger = EventLogger(sink=sink)

    result = logger.run_task("reporting.task", lambda value: value + 1, 2)

    assert result == 3
    assert sink.events[-1].event_name == "reporting.task"
    assert sink.events[-1].event_type == "task"
    assert sink.events[-1].status == "success"


def test_run_task_records_failure_and_reraises_original_exception():
    """
    What: Runs a failing task callable and re-raises its original exception.
    Why: Task wrapper logging must preserve Databricks job failure semantics.
    Fails when: run_task swallows or replaces the business exception.
    """
    sink = MemorySink()
    logger = EventLogger(sink=sink)

    def fail():
        raise ValueError("task boom")

    with pytest.raises(ValueError, match="task boom"):
        logger.run_task("reporting.task", fail)

    assert sink.events[-1].event_name == "reporting.task"
    assert sink.events[-1].event_type == "task"
    assert sink.events[-1].status == "failed"
    assert sink.events[-1].error_class == "ValueError"


def test_record_metric_defaults_event_name():
    """
    What: Uses the metric name to build a default metric event name.
    Why: Callers should not need to repeat metric names for common metrics.
    Fails when: The metric event naming convention changes unexpectedly.
    """
    sink = MemorySink()
    logger = EventLogger(sink=sink)

    event = logger.record_metric("input_rows", 25)

    assert event.event_name == "metric.input_rows"
    assert event.metric_name == "input_rows"
    assert event.metric_value == 25


def test_nested_observed_events_capture_parent_event_id():
    """
    What: Records a nested direct event under an observed function event.
    Why: Parent ids help dashboard users correlate sub-events with operations.
    Fails when: Nested events lose their parent-child relationship.
    """
    sink = MemorySink()
    logger = EventLogger(sink=sink)

    @logger.logged_event("reporting.outer")
    def outer():
        child = logger.record_event("reporting.inner")
        return child

    child = outer()
    outer_event = sink.events[-1]

    assert child.parent_event_id == outer_event.event_id
    assert outer_event.parent_event_id is None


def test_observed_decorator_uses_default_logger_at_call_time():
    """
    What: Resolves the default logger when a decorated function is called.
    Why: Import order should not decide whether decorators work in notebooks.
    Fails when: @observed captures a missing logger at import time.
    """
    def run():
        sink = MemorySink()
        observe_notebook(
            spark=object(),
            dbutils=object(),
            app_name="app",
            component="component",
            environment="dev",
            sink=sink,
        )

        @observed("reporting.default_logger")
        def work():
            return "ok"

        assert get_default_logger().config.app_name == "app"
        assert work() == "ok"
        assert sink.events[-1].event_name == "reporting.default_logger"

    Context().run(run)


def test_observed_metadata_factory_uses_default_logger_at_call_time():
    """
    What: Uses metadata_factory with the module-level default logger decorator.
    Why: Owned packages should be able to decorate public APIs without inner wrappers.
    Fails when: @observed cannot compute metadata from call arguments.
    """
    def run():
        sink = MemorySink()
        observe_notebook(
            spark=object(),
            dbutils=object(),
            app_name="app",
            component="component",
            environment="dev",
            sink=sink,
        )

        @observed(
            "reporting.default_logger.dynamic",
            metadata_factory=lambda value, *, batch_id: {
                "value": value,
                "batch_id": batch_id,
            },
        )
        def work(value, *, batch_id):
            return "ok"

        assert work("input", batch_id="batch-1") == "ok"
        metadata = deserialize_metadata(sink.events[-1].metadata_json)
        assert metadata == {"batch_id": "batch-1", "value": "input"}

    Context().run(run)


def test_event_logger_defaults_to_console_sink():
    """The default sink is visible notebook output, not implicit persistence."""
    assert isinstance(EventLogger().sink, ConsoleSink)


def test_observe_notebook_requires_explicit_runtime_dependencies():
    """Notebook bootstrap has no frame or non-Databricks fallback."""
    with pytest.raises(TypeError):
        observe_notebook()
    with pytest.raises(EventLoggerConfigurationError, match="requires both"):
        observe_notebook(spark=None, dbutils=object())


def test_observe_notebook_uses_explicit_configuration_and_correlation():
    """Bootstrap passes explicit identity, sink, and correlation settings through."""
    sink = MemorySink()

    logger = observe_notebook(
        spark=object(),
        dbutils=object(),
        app_name="app",
        component="component",
        environment="dev",
        correlation_id="workflow-123",
        sink=sink,
    )

    assert logger.config.app_name == "app"
    assert logger.config.component == "component"
    assert logger.config.environment == "dev"
    assert logger.correlation_id == "workflow-123"
    assert sink.events[-1].event_name == "notebook.started"
    assert sink.events[-1].correlation_id == "workflow-123"


def test_observe_notebook_uses_explicit_delta_sink():
    """Delta persistence is explicit instead of inferred from a table parameter."""
    pytest.importorskip("pyspark")
    spark = FakeSpark()

    logger = observe_notebook(
        spark=spark,
        dbutils=object(),
        sink=DeltaSink(spark=spark, table_name="catalog.schema.event_log"),
    )

    assert isinstance(logger.sink, DeltaSink)
    assert spark.sql_calls[0].startswith("INSERT INTO catalog.schema.event_log")
    assert spark.data[0]["event_name"] == "notebook.started"


def test_get_default_logger_requires_bootstrap():
    """
    What: Raises a clear configuration error when no default logger exists.
    Why: Missing notebook bootstrap should be explicit instead of silently lost.
    Fails when: The default logger silently creates unconfigured loggers.
    """
    with pytest.raises(EventLoggerConfigurationError):
        Context().run(get_default_logger)


def test_logging_failure_warns_and_preserves_success_return_value():
    """
    What: Lets successful business code return even when event emission fails.
    Why: V1 logging should not fail successful business workflows.
    Fails when: Sink failures replace successful function results.
    """
    logger = EventLogger(sink=FailingSink())

    @logger.logged_event("reporting.success")
    def work():
        return "ok"

    with pytest.warns(RuntimeWarning, match="Failed to emit event"):
        assert work() == "ok"


def test_strict_logging_raises_sink_failure_on_success_path():
    """
    What: Raises sink errors when strict logging is explicitly enabled.
    Why: Production controls may require event persistence to be mandatory.
    Fails when: strict_logging=True still silently warns and continues.
    """
    logger = EventLogger(sink=FailingSink(), strict_logging=True)

    @logger.logged_event("reporting.success")
    def work():
        return "ok"

    with pytest.raises(RuntimeError, match="sink unavailable"):
        work()


def test_failure_logging_failure_preserves_original_exception():
    """
    What: Re-raises the business exception when failure-event emission fails.
    Why: A broken sink must not mask the exception that should fail the task.
    Fails when: Logging exceptions replace business exceptions.
    """
    logger = EventLogger(sink=FailingSink())

    @logger.logged_event("reporting.failure")
    def fail():
        raise ValueError("business failure")

    with pytest.warns(RuntimeWarning, match="Failed to emit event"):
        with pytest.raises(ValueError, match="business failure"):
            fail()


def test_default_metadata_merges_with_event_metadata():
    """
    What: Applies logger-level default metadata to emitted events.
    Why: Production jobs need run-level metadata without repeating it at every call site.
    Fails when: default_metadata is dropped or overrides event-level metadata.
    """
    sink = MemorySink()
    logger = EventLogger(
        sink=sink,
        default_metadata={"workflow": "daily_positions", "stage": "default"},
    )

    event = logger.record_event("reporting.step", metadata={"stage": "custom"})
    metadata = deserialize_metadata(event.metadata_json)

    assert metadata == {"stage": "custom", "workflow": "daily_positions"}


def test_logger_builds_job_navigation_urls_from_context():
    """
    What: Derives Databricks job and run UI links from runtime context.
    Why: Dashboards and notebooks should offer one-click navigation to the run.
    Fails when: URL derivation drops workspace, job, or run identifiers.
    """
    logger = EventLogger(
        sink=MemorySink(),
        context=RuntimeContext(
            workspace_url="Example.Cloud.Databricks.com/",
            job_id="123",
            run_id="456",
        ),
    )

    assert logger.job_url == "https://example.cloud.databricks.com/jobs/123"
    assert logger.job_run_url == "https://example.cloud.databricks.com/jobs/123/runs/456"


def test_observe_notebook_started_event_includes_bootstrap_diagnostics():
    """
    What: Adds sink and navigation diagnostics to the startup event metadata.
    Why: Misconfiguration should be visible from the first event in a run.
    Fails when: notebook.started omits production bootstrap details.
    """
    sink = MemorySink()

    logger = observe_notebook(
        spark=object(),
        dbutils=object(),
        app_name="app",
        component="component",
        environment="dev",
        sink=sink,
        strict_logging=True,
    )
    metadata = deserialize_metadata(sink.events[-1].metadata_json)

    assert logger is get_default_logger()
    assert metadata["sink_type"] == "MemorySink"
    assert metadata["strict_logging"] is True
    assert metadata["event_table"] is None


def test_event_volume_warning_emits_once():
    """
    What: Warns once when a logger emits more events than the configured threshold.
    Why: Immediate Delta writes should discourage unexpectedly chatty instrumentation.
    Fails when: High-volume event loops stay invisible to developers.
    """
    logger = EventLogger(sink=MemorySink(), max_events_warning_threshold=1)

    logger.record_event("reporting.first")
    with pytest.warns(RuntimeWarning, match="more than 1 events"):
        logger.record_event("reporting.second")
    with warnings.catch_warnings(record=True) as warning_records:
        warnings.simplefilter("always")
        logger.record_event("reporting.third")

    assert not warning_records


def test_check_observability_ready_uses_console_sink_by_default():
    """Readiness reports the same explicit default used by EventLogger."""
    report = check_observability_ready(
        dbutils=object(),
        spark=object(),
        validate_sink=False,
    )

    assert report.ready is True
    assert report.sink_type == "ConsoleSink"
    assert report.event_table is None


def test_assert_observability_ready_returns_report_when_ready():
    """
    What: Returns a readiness report when required production inputs are present.
    Why: Production notebooks need a compact preflight check.
    Fails when: The assert helper emits events or rejects valid basic configuration.
    """
    report = assert_observability_ready(
        dbutils=object(),
        spark=object(),
        sink=MemorySink(),
        validate_sink=False,
    )

    assert report.ready is True
    assert report.sink_type == "MemorySink"
    assert report.event_table is None


def test_check_observability_ready_reports_delta_validation_failure():
    """
    What: Reports DeltaSink schema validation failures without emitting events.
    Why: Preflight checks should catch bad event table deployment before bootstrap.
    Fails when: Readiness diagnostics ignore a malformed event log table.
    """
    spark = FakeSpark(describe_columns=["event_name"])
    sink = DeltaSink(spark=spark, table_name="catalog.schema.event_log")

    report = check_observability_ready(
        dbutils=object(),
        spark=spark,
        sink=sink,
        validate_sink=True,
    )

    assert report.ready is False
    assert report.sink_type == "DeltaSink"
    assert "missing required columns" in report.issues[0]


class FakeSpark:
    def __init__(self, describe_columns=None):
        self.data = None
        self.dataframe = FakeDataFrame()
        self.sql_calls = []
        self.describe_columns = describe_columns

    def createDataFrame(self, data, schema=None):  # noqa: N802 - Spark API casing.
        self.data = data
        return self.dataframe

    def sql(self, query):
        normalized = " ".join(query.split())
        self.sql_calls.append(normalized)
        if normalized.startswith("DESCRIBE TABLE"):
            return FakeDescribeResult(self.describe_columns or [])


class FakeDataFrame:
    def createOrReplaceTempView(self, name):  # noqa: N802 - Spark API casing.
        self.view_name = name


class FakeDescribeResult:
    def __init__(self, columns):
        self.columns = columns

    def collect(self):
        return [{"col_name": column} for column in self.columns]


class FailingSink:
    def emit(self, event):
        raise RuntimeError("sink unavailable")
