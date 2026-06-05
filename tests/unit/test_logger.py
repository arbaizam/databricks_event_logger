import warnings
from contextvars import Context

import pytest

from databricks_event_logger import (
    CommonEvent,
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
        observe_notebook(app_name="app", component="component", environment="dev", sink=sink)

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
        observe_notebook(app_name="app", component="component", environment="dev", sink=sink)

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


def test_observe_notebook_from_widgets_uses_optional_context_widgets():
    """
    What: Uses optional widget values as runtime context fallbacks.
    Why: Databricks exposes task name/execution count as dynamic task parameters.
    Fails when: Job task smoke tests cannot populate task_key or attempt fields.
    """
    sink = MemorySink()
    dbutils = FakeDbutils(
        {
            "app_name": "app",
            "component": "component",
            "environment": "dev",
            "observability_event_table": "catalog.schema.event_log",
            "task_key": "smoke_task",
            "task_run_id": "task-run-1",
            "task_attempt_number": "1",
            "job_start_time": "2026-06-04T13:00:00Z",
            "job_trigger_type": "one_time",
            "notebook_path": "/Workspace/smoke",
            "run_as_user_name": "svc@example.com",
        }
    )

    logger = observe_notebook.from_widgets(dbutils=dbutils, sink=sink)

    assert logger.context.task_key == "smoke_task"
    assert logger.context.task_run_id == "task-run-1"
    assert logger.context.task_attempt_number == "1"
    assert logger.context.job_start_time == "2026-06-04T13:00:00Z"
    assert logger.context.job_trigger_type == "one_time"
    assert logger.context.notebook_path == "/Workspace/smoke"
    assert logger.context.run_as_user_name == "svc@example.com"
    assert sink.events[-1].task_key == "smoke_task"
    assert sink.events[-1].task_run_id == "task-run-1"


def test_observe_notebook_from_widgets_uses_delta_sink_when_configured():
    """
    What: Creates a DeltaSink when widgets provide an event table and Spark is supplied.
    Why: The standard job bootstrap path should persist events without extra sink wiring.
    Fails when: from_widgets falls back to MemorySink despite complete persistence config.
    """
    pytest.importorskip("pyspark")

    dbutils = FakeDbutils(
        {
            "app_name": "app",
            "component": "component",
            "environment": "dev",
            "observability_event_table": "catalog.schema.event_log",
        }
    )
    spark = FakeSpark()

    def run():
        logger = observe_notebook.from_widgets(dbutils=dbutils, spark=spark)

        assert type(logger.sink).__name__ == "DeltaSink"
        assert spark.sql_calls[0].startswith("INSERT INTO catalog.schema.event_log")
        assert spark.data[0]["event_name"] == "notebook.started"

    Context().run(run)


def test_observe_notebook_direct_uses_optional_context_widgets():
    """
    What: Uses optional widget values when bootstrapping directly.
    Why: Some notebooks pass app/table arguments directly but task context via parameters.
    Fails when: Direct observe_notebook calls ignore task parameter widgets.
    """
    sink = MemorySink()
    dbutils = FakeDbutils(
        {
            "task_key": "smoke_task",
            "task_run_id": "task-run-1",
            "task_attempt_number": "1",
            "job_start_time": "2026-06-04T13:00:00Z",
        }
    )

    logger = observe_notebook(
        app_name="app",
        component="component",
        environment="dev",
        sink=sink,
        dbutils=dbutils,
    )

    assert logger.context.task_key == "smoke_task"
    assert logger.context.task_run_id == "task-run-1"
    assert logger.context.task_attempt_number == "1"
    assert logger.context.job_start_time == "2026-06-04T13:00:00Z"
    assert sink.events[-1].task_key == "smoke_task"


def test_observe_notebook_warns_when_event_table_cannot_persist():
    """
    What: Warns when an event table is supplied without a Spark session.
    Why: Production jobs should not silently fall back to non-persistent memory events.
    Fails when: Missing Spark causes silent event loss.
    """
    def run():
        with pytest.warns(RuntimeWarning, match="observability_event_table"):
            logger = observe_notebook(
                app_name="app",
                component="component",
                environment="dev",
                event_table="catalog.schema.event_log",
            )
        assert isinstance(logger.sink, MemorySink)

    Context().run(run)


def test_observe_notebook_warns_when_spark_has_no_event_table():
    """
    What: Warns when Spark is supplied but no event table is configured.
    Why: A Spark-backed job usually expects Delta persistence.
    Fails when: Missing event table causes silent MemorySink fallback.
    """
    def run():
        with pytest.warns(RuntimeWarning, match="observability_event_table"):
            logger = observe_notebook(
                app_name="app",
                component="component",
                environment="dev",
                spark=object(),
            )
        assert isinstance(logger.sink, MemorySink)

    Context().run(run)


def test_observe_notebook_from_widgets_warns_on_frame_inspection():
    """
    What: Warns once when globals are discovered through caller-frame inspection.
    Why: Production notebooks should pass dbutils and spark explicitly.
    Fails when: Fragile implicit lookup remains invisible or emits noisy duplicate warnings.
    """
    def run():
        dbutils = FakeDbutils(
            {
                "app_name": "app",
                "component": "component",
                "environment": "dev",
            }
        )
        spark = object()
        sink = MemorySink()

        with warnings.catch_warnings(record=True) as warning_records:
            warnings.simplefilter("always")
            logger = observe_notebook.from_widgets(sink=sink)

        assert logger.config.app_name == "app"
        assert dbutils.widgets.get("app_name") == "app"
        assert spark is not None
        assert len(warning_records) == 1
        assert "dbutils and spark" in str(warning_records[0].message)

    Context().run(run)


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
    context = RuntimeContext(
        workspace_url="workspace.cloud.databricks.com",
        job_id="123",
        run_id="456",
    )

    logger = observe_notebook(
        app_name="app",
        component="component",
        environment="dev",
        sink=sink,
        context=context,
        require_persistence=False,
        validate_sink=True,
        strict_logging=True,
    )
    metadata = deserialize_metadata(sink.events[-1].metadata_json)

    assert logger is get_default_logger()
    assert metadata["sink_type"] == "MemorySink"
    assert metadata["validate_sink"] is True
    assert metadata["strict_logging"] is True
    assert metadata["job_run_url"] == "https://workspace.cloud.databricks.com/jobs/123/runs/456"


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


def test_observe_notebook_requires_persistence_when_requested():
    """
    What: Fails bootstrap when production persistence is required but unavailable.
    Why: Production jobs should not silently fall back to MemorySink.
    Fails when: require_persistence=True still allows in-memory events.
    """
    with warnings.catch_warnings(record=True) as warning_records:
        warnings.simplefilter("always")
        with pytest.raises(EventLoggerConfigurationError, match="Persistent event logging"):
            observe_notebook(
                app_name="app",
                component="component",
                environment="dev",
                event_table="catalog.schema.event_log",
                require_persistence=True,
            )

    assert not warning_records


def test_production_from_widgets_enables_production_controls():
    """
    What: Applies the production bootstrap preset from widgets.
    Why: Production jobs should not require developers to remember multiple flags.
    Fails when: production_from_widgets does not fail fast or validate the sink.
    """
    pytest.importorskip("pyspark")
    dbutils = FakeDbutils(
        {
            "app_name": "app",
            "component": "component",
            "environment": "prod",
            "observability_event_table": "catalog.schema.event_log",
        }
    )
    spark = FakeSpark(describe_columns=FULL_EVENT_COLUMNS)

    logger = observe_notebook.production_from_widgets(dbutils=dbutils, spark=spark)
    metadata = deserialize_metadata(spark.data[0]["metadata_json"])

    assert type(logger.sink).__name__ == "DeltaSink"
    assert logger.strict_logging is True
    assert metadata["require_persistence"] is True
    assert metadata["validate_sink"] is True
    assert metadata["strict_logging"] is True
    assert spark.sql_calls[0].startswith("DESCRIBE TABLE catalog.schema.event_log")
    assert spark.sql_calls[1].startswith("INSERT INTO catalog.schema.event_log")


def test_check_observability_ready_reports_missing_persistence():
    """
    What: Reports readiness issues without configuring the default logger.
    Why: Setup diagnostics should be available before emitting events.
    Fails when: Missing Spark/table configuration is not visible.
    """
    report = check_observability_ready(require_persistence=True, validate_sink=False)

    assert report.ready is False
    assert report.sink_type == "MemorySink"
    assert "Persistent event logging is required" in report.issues[0]


def test_assert_observability_ready_returns_report_when_ready():
    """
    What: Returns a readiness report when required production inputs are present.
    Why: Production notebooks need a compact preflight check.
    Fails when: The assert helper emits events or rejects valid basic configuration.
    """
    dbutils = FakeDbutils({"observability_event_table": "catalog.schema.event_log"})

    report = assert_observability_ready(
        dbutils=dbutils,
        spark=object(),
        require_persistence=True,
        validate_sink=False,
    )

    assert report.ready is True
    assert report.sink_type == "DeltaSink"
    assert report.event_table == "catalog.schema.event_log"


def test_check_observability_ready_reports_delta_validation_failure():
    """
    What: Reports DeltaSink schema validation failures without emitting events.
    Why: Preflight checks should catch bad event table deployment before bootstrap.
    Fails when: Readiness diagnostics ignore a malformed event log table.
    """
    dbutils = FakeDbutils({"observability_event_table": "catalog.schema.event_log"})
    spark = FakeSpark(describe_columns=["event_name"])

    report = check_observability_ready(
        dbutils=dbutils,
        spark=spark,
        require_persistence=True,
        validate_sink=True,
    )

    assert report.ready is False
    assert report.sink_type == "DeltaSink"
    assert "missing required columns" in report.issues[0]


class FakeDbutils:
    def __init__(self, widget_values):
        self.widgets = FakeWidgets(widget_values)


class FakeWidgets:
    def __init__(self, values):
        self._values = values

    def get(self, name):
        if name not in self._values:
            raise KeyError(name)
        return self._values[name]

    def getAll(self):  # noqa: N802 - Databricks widget API casing.
        return dict(self._values)


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

    def flush(self):
        pass

    def close(self):
        pass


FULL_EVENT_COLUMNS = (
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
