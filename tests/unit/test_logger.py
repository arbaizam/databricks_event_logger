from contextvars import Context

import pytest

from databricks_event_logger import EventLogger, get_default_logger, observe_notebook, observed
from databricks_event_logger.errors import EventLoggerConfigurationError
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
            "task_attempt_number": "1",
            "notebook_path": "/Workspace/smoke",
        }
    )

    logger = observe_notebook.from_widgets(dbutils=dbutils, sink=sink)

    assert logger.context.task_key == "smoke_task"
    assert logger.context.task_attempt_number == "1"
    assert logger.context.notebook_path == "/Workspace/smoke"
    assert sink.events[-1].task_key == "smoke_task"


def test_get_default_logger_requires_bootstrap():
    """
    What: Raises a clear configuration error when no default logger exists.
    Why: Missing notebook bootstrap should be explicit instead of silently lost.
    Fails when: The default logger silently creates unconfigured loggers.
    """
    with pytest.raises(EventLoggerConfigurationError):
        Context().run(get_default_logger)


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
