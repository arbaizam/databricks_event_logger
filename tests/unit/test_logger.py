import asyncio
import json
import warnings
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from dataclasses import FrozenInstanceError
from threading import Barrier, Lock

import pytest

from databricks_event_logger.context import RuntimeContext
from databricks_event_logger.logger import EventLogger
from databricks_event_logger.sinks.memory import MemorySink


class FailingSink:
    def __init__(self, error=None):
        self.error = error if error is not None else RuntimeError("destination unavailable")

    def emit(self, event):
        raise self.error


def test_direct_events_stamp_identity_and_normalize_numeric_metrics():
    sink = MemorySink()
    context = RuntimeContext(job_id="10", run_id="20")
    logger = EventLogger(
        sink=sink,
        context=context,
        app_name="positions",
        component="publish",
        environment="dev",
        correlation_id="batch-1",
    )
    first = logger.record_event("positions.validated", row_count=7)
    metric = logger.record_metric("rows", 7)
    assert sink.events == [first, metric]
    assert first.context == context
    assert first.app_name == "positions"
    assert first.component == "publish"
    assert first.environment == "dev"
    assert first.correlation_id == "batch-1"
    assert metric.event_type == "metric"
    assert metric.metric_value == 7.0
    assert isinstance(metric.metric_value, float)
    assert (logger.health.attempted, logger.health.succeeded, logger.health.failed) == (2, 2, 0)


def test_numpy_scalars_work_through_direct_metrics_and_editable_scope_apis():
    np = pytest.importorskip("numpy")
    logger = EventLogger(sink=MemorySink())
    direct = logger.record_event("count", row_count=np.int64(5))
    metric = logger.record_metric("rows", np.int64(5))
    floating = logger.record_metric("ratio", np.float32(1.5))
    with logger.event("scoped", row_count=np.int64(0)) as scope:
        scope.row_count = np.uint32(7)
    scoped = logger.sink.events[-1]
    assert direct.row_count == 5
    assert scoped.row_count == 7
    assert type(direct.row_count) is type(scoped.row_count) is int
    assert metric.metric_value == 5.0
    assert floating.metric_value == 1.5
    assert type(metric.metric_value) is type(floating.metric_value) is float
    assert logger.health.succeeded == 4


def test_default_correlation_is_independent_of_databricks_identity():
    context = RuntimeContext(run_id="same-run")
    assert (
        EventLogger(context=context).correlation_id != EventLogger(context=context).correlation_id
    )


def test_editable_scope_emits_one_result_with_computed_fields():
    sink = MemorySink()
    logger = EventLogger(sink=sink, default_metadata={"batch": "one", "source": "default"})
    with logger.event("positions.validate", metadata={"source": "scope"}) as scope:
        event_id = scope.event_id
        scope.metadata["partition"] = "2026-09-04"
        scope.row_count = 0
        scope.status = "warning"
        scope.severity = "warning"
        scope.source_table = "raw.positions"
        scope.target_table = "curated.positions"
        assert not sink.events
        with pytest.raises(AttributeError):
            scope.event_id = "different"
    (event,) = sink.events
    assert event.event_id == event_id
    assert event.row_count == 0
    assert event.status == event.severity == "warning"
    assert event.source_table == "raw.positions"
    assert event.target_table == "curated.positions"
    assert event.start_ts <= event.end_ts
    assert event.event_ts == event.end_ts
    assert event.duration_ms >= 0
    assert json.loads(event.metadata_json) == {
        "batch": "one",
        "source": "scope",
        "partition": "2026-09-04",
    }


def test_parent_lineage_is_shared_by_bindings_but_not_independent_loggers():
    sink = MemorySink()
    logger = EventLogger(sink=sink)
    bound = logger.bind(batch="one")
    unrelated = EventLogger(sink=sink)
    with logger.event("outer") as outer:
        bound.record_event("bound-checkpoint")
        unrelated.record_event("independent-checkpoint")
        with bound.event("inner") as inner:
            logger.record_event("inside-inner")
        logger.record_event("after-inner")
    logger.record_event("after-outer")
    events = {event.event_name: event for event in sink.events}
    assert events["bound-checkpoint"].parent_event_id == outer.event_id
    assert events["independent-checkpoint"].parent_event_id is None
    assert events["inside-inner"].parent_event_id == inner.event_id
    assert events["inner"].parent_event_id == outer.event_id
    assert events["after-inner"].parent_event_id == outer.event_id
    assert events["outer"].parent_event_id is None
    assert events["after-outer"].parent_event_id is None


def test_bound_metadata_and_scope_inputs_have_independent_top_level_snapshots():
    supplied = {"nested": {"items": [1]}, "label": "root"}
    sink = MemorySink()
    logger = EventLogger(sink=sink, default_metadata=supplied)
    bound = logger.bind(label="bound")
    supplied["label"] = "changed"
    exposed = bound.default_metadata
    exposed["label"] = "changed again"
    scope_metadata = {"items": ["before"]}
    manager = bound.event("scoped", metadata=scope_metadata)
    scope_metadata["items"] = ["replaced"]
    with manager as scope:
        scope.metadata["items"].append("inside")
    direct = logger.record_event("root")
    assert json.loads(sink.events[0].metadata_json) == {
        "nested": {"items": [1]},
        "label": "bound",
        "items": ["before", "inside"],
    }
    assert json.loads(direct.metadata_json) == {"nested": {"items": [1]}, "label": "root"}
    assert bound.sink is logger.sink
    assert bound.correlation_id == logger.correlation_id
    assert bound.health == logger.health


@pytest.mark.parametrize("metadata", [{1: "bad key"}, {"nested": [{1: "bad key"}]}])
def test_default_metadata_content_is_validated_before_logger_construction(metadata):
    sink = MemorySink()
    with pytest.raises(TypeError, match="metadata keys must be strings"):
        EventLogger(sink=sink, default_metadata=metadata, metadata_max_bytes=2)
    assert not sink.events


def test_bound_metadata_is_validated_after_merging_without_attempting_delivery():
    logger = EventLogger(sink=MemorySink(), default_metadata={"batch": "one"})
    with pytest.raises(TypeError, match="metadata keys must be strings"):
        logger.bind(nested={1: "bad key"})
    assert logger.default_metadata == {"batch": "one"}
    assert logger.health.attempted == 0
    assert not logger.sink.events


def test_bound_metadata_revalidates_inherited_values_after_external_mutation():
    nested = {"valid": "one"}
    logger = EventLogger(sink=MemorySink(), default_metadata={"nested": nested})
    nested[1] = "bad key"
    with pytest.raises(TypeError, match="metadata keys must be strings"):
        logger.bind(batch="one")
    assert logger.health.attempted == 0
    # Later mutations stay caller-owned; event delivery uses the normal failure policy.
    with pytest.warns(RuntimeWarning):
        assert logger.record_event("after-mutation") is None
    assert logger.health.failed == 1


def test_metadata_validation_uses_configured_redaction_without_replacing_raw_defaults():
    sink = MemorySink()
    logger = EventLogger(
        sink=sink,
        default_metadata={"team_private": {1: "redacted before traversal"}, "batch": "one"},
        redact_keys=("team_private",),
        metadata_string_max_chars=3,
        metadata_max_bytes=None,
    )
    child = logger.bind(team_private={2: "also redacted before traversal"}, label="abcdef")
    event = child.record_event("checkpoint")
    assert child.default_metadata["label"] == "abcdef"
    assert json.loads(event.metadata_json) == {
        "team_private": "[REDACTED]",
        "batch": "one",
        "label": "...",
    }


def test_health_is_an_immutable_snapshot_and_retains_last_failure():
    sink = FailingSink()
    logger = EventLogger(sink=sink)
    child = logger.bind(batch="one")
    original = logger.health
    with pytest.warns(RuntimeWarning, match="Event delivery failed"):
        assert child.record_event("failed") is None
    failed = logger.health
    assert (original.attempted, original.failed) == (0, 0)
    assert (failed.attempted, failed.succeeded, failed.failed) == (1, 0, 1)
    assert failed.last_error == "RuntimeError: destination unavailable"
    with pytest.raises(FrozenInstanceError):
        failed.failed = 0
    logger.sink = MemorySink()
    assert logger.record_event("recovered") is not None
    assert logger.health.succeeded == 1
    assert logger.health.last_error == failed.last_error


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize(
    "business_error",
    [ValueError("business"), KeyboardInterrupt(), SystemExit(17), asyncio.CancelledError()],
)
def test_original_business_exception_survives_any_sink_failure_and_error_warnings(
    strict,
    business_error,
):
    logger = EventLogger(sink=FailingSink(SystemExit("sink-exit")), strict_logging=strict)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        with pytest.raises(type(business_error)) as captured:
            with logger.event("operation"):
                raise business_error
    assert captured.value is business_error
    assert logger.health.failed == 1
    logger.sink = MemorySink()
    assert logger.record_event("after").parent_event_id is None


def test_nonstrict_sink_failure_does_not_interrupt_success_with_warning_errors():
    logger = EventLogger(sink=FailingSink())
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert logger.record_event("checkpoint") is None
        with logger.event("operation"):
            result = 42
    assert result == 42
    assert logger.health.failed == 2


def test_strict_delivery_failure_is_raised_once_after_work_and_parent_is_restored():
    error = RuntimeError("sink unavailable")
    logger = EventLogger(sink=FailingSink(error), strict_logging=True)
    completed = []
    with pytest.raises(RuntimeError) as captured:
        with logger.event("operation"):
            completed.append(True)
    assert captured.value is error
    assert completed == [True]
    assert (logger.health.attempted, logger.health.failed) == (1, 1)
    logger.sink = MemorySink()
    assert logger.record_event("after").parent_event_id is None


def test_nested_strict_failure_aborts_outer_work_and_records_its_failed_outcome():
    error = RuntimeError("inner event delivery unavailable")

    class FailInnerSink(MemorySink):
        def emit(self, event):
            if event.event_name == "inner":
                raise error
            super().emit(event)

    sink = FailInnerSink()
    logger = EventLogger(sink=sink, strict_logging=True)
    completed = []
    with pytest.raises(RuntimeError) as captured:
        with logger.event("outer"):
            with logger.event("inner"):
                completed.append("inner")
            completed.append("remaining outer work")
    assert captured.value is error
    assert completed == ["inner"]
    (outer,) = sink.events
    assert outer.event_name == "outer"
    assert outer.status == "failed"
    assert outer.severity == "error"
    assert outer.error_class == "RuntimeError"
    assert outer.error_message == str(error)
    assert outer.parent_event_id is None
    assert (logger.health.attempted, logger.health.succeeded, logger.health.failed) == (2, 1, 1)
    assert logger.record_event("after").parent_event_id is None


@pytest.mark.parametrize("strict", [False, True])
def test_metadata_preparation_failure_obeys_delivery_policy(monkeypatch, strict):
    logger = EventLogger(sink=MemorySink(), strict_logging=strict)

    def broken_serializer(*args, **kwargs):
        raise RuntimeError("normalization failed")

    monkeypatch.setattr("databricks_event_logger.logger.serialize_metadata", broken_serializer)
    if strict:
        with pytest.raises(RuntimeError, match="normalization failed"):
            logger.record_event("checkpoint", metadata={"value": "one"})
    else:
        with pytest.warns(RuntimeWarning):
            assert logger.record_event("checkpoint", metadata={"value": "one"}) is None
    assert (logger.health.attempted, logger.health.failed) == (1, 1)
    assert not logger.sink.events


def test_bad_exception_string_and_metadata_repr_do_not_replace_business_failure():
    class BadMessage(ValueError):
        def __str__(self):
            raise RuntimeError("bad str")

    class BadRepr:
        def __repr__(self):
            raise RuntimeError("bad repr")

    sink = MemorySink()
    logger = EventLogger(sink=sink, capture_error_frames=True)
    error = BadMessage()
    with pytest.raises(BadMessage) as captured:
        with logger.event("operation", metadata={"object": BadRepr()}):
            raise error
    assert captured.value is error
    assert sink.events[0].status == "failed"
    assert sink.events[0].error_class == "BadMessage"
    assert logger.health.succeeded == 1


def test_metadata_snapshots_do_not_copy_unsupported_python_objects():
    sink = MemorySink()
    logger = EventLogger(sink=sink, default_metadata={"lock": Lock()})
    bound = logger.bind(second_lock=Lock())
    with bound.event("operation", metadata={"third_lock": Lock()}):
        pass
    values = json.loads(sink.events[0].metadata_json)
    assert set(values) == {"lock", "second_lock", "third_lock"}
    assert all(value == "[UNSUPPORTED]" for value in values.values())


def test_delivery_diagnostics_with_raising_str_preserve_business_error():
    class UnprintableFailure(Exception):
        def __str__(self):
            raise KeyboardInterrupt()

    logger = EventLogger(sink=FailingSink(UnprintableFailure()), strict_logging=True)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        with pytest.raises(ValueError, match="original"):
            with logger.event("operation"):
                raise ValueError("original")
    assert logger.health.last_error == "UnprintableFailure: [UNPRINTABLE]"


def test_bound_loggers_share_consistent_health_across_threads():
    logger = EventLogger(sink=MemorySink())

    def record(index):
        child = logger.bind(index=index)
        for _ in range(30):
            child.record_event("checkpoint")

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(record, range(8)))
    health = logger.health
    assert health.attempted == health.succeeded == 240
    assert health.failed == 0


@pytest.mark.parametrize("propagate_context", [False, True])
def test_logger_bound_decorators_run_in_threads_with_explicit_parent_propagation(
    propagate_context,
):
    sink = MemorySink()
    logger = EventLogger(sink=sink)
    ready = Barrier(3)

    @logger.logged_event("table.load")
    def load(index):
        ready.wait(timeout=10)
        logger.record_event(f"checkpoint.{index}")
        return f"loaded {index}"

    with logger.event("parent") as parent:
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = [
                pool.submit(copy_context().run, load, index)
                if propagate_context
                else pool.submit(load, index)
                for index in range(3)
            ]
            assert [future.result(timeout=10) for future in futures] == [
                "loaded 0", "loaded 1", "loaded 2",
            ]
        assert logger.record_event("same-thread-child").parent_event_id == parent.event_id
    operations = [event for event in sink.events if event.event_name == "table.load"]
    assert len(operations) == 3
    assert all(event.status == "success" for event in operations)
    expected_parent = parent.event_id if propagate_context else None
    assert all(event.parent_event_id == expected_parent for event in operations)
    checkpoints = [event for event in sink.events if event.event_name.startswith("checkpoint.")]
    assert {event.parent_event_id for event in checkpoints} == {
        event.event_id for event in operations
    }
    assert logger.record_event("after").parent_event_id is None


def test_failure_overrides_edited_status_and_captures_bounded_frames_without_locals():
    sink = MemorySink()
    logger = EventLogger(sink=sink, capture_error_frames=True, error_message_max_chars=5)

    def recurse(depth):
        secret_local = "do-not-include-local-value"
        if depth:
            return recurse(depth - 1)
        raise ValueError("long message" if secret_local else "unused")

    with pytest.raises(ValueError):
        with logger.event("operation", status="skipped", severity="debug"):
            recurse(25)
    (event,) = sink.events
    assert event.status == "failed"
    assert event.severity == "error"
    assert len(event.error_message) <= 5
    assert len(event.stack_trace_hash) == 64
    frames = json.loads(event.error_frames_json)
    assert len(frames) == 20
    assert all(set(frame) == {"file", "function", "line"} for frame in frames)
    assert all("/" not in frame["file"] and "\\" not in frame["file"] for frame in frames)
    assert "do-not-include-local-value" not in event.error_frames_json


def test_error_frame_capture_is_opt_in():
    sink = MemorySink()
    logger = EventLogger(sink=sink)
    with pytest.raises(ValueError):
        with logger.event("operation"):
            raise ValueError("business failure")
    assert sink.events[0].error_frames_json is None
    assert sink.events[0].stack_trace_hash


@pytest.mark.parametrize(
    "method,kwargs",
    [
        ("event", {"event_name": ""}),
        ("event", {"event_name": "bad", "row_count": -1}),
        ("event", {"event_name": "bad", "status": "invalid"}),
        ("event", {"event_name": "bad", "metadata": []}),
        ("logged_event", {"event_name": ""}),
        ("record_event", {"event_name": "bad", "metric_value": True}),
    ],
)
def test_static_invalid_arguments_raise_without_counting_delivery(method, kwargs):
    logger = EventLogger(sink=MemorySink())
    with pytest.raises((TypeError, ValueError)):
        getattr(logger, method)(**kwargs)
    assert logger.health.attempted == 0
    assert logger.record_event("after").parent_event_id is None


def test_invalid_edited_result_is_a_preparation_failure_and_preserves_business_error():
    sink = MemorySink()
    logger = EventLogger(sink=sink)
    with pytest.warns(RuntimeWarning):
        with logger.event("operation") as scope:
            scope.row_count = -1
    assert logger.health.failed == 1
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        with pytest.raises(ValueError, match="business failure"):
            with logger.event("operation") as scope:
                scope.row_count = -1
                raise ValueError("business failure")
    assert logger.health.failed == 2
    assert not sink.events


def test_sync_decorators_and_task_wrapper_share_lifecycle_and_return_values():
    sink = MemorySink()
    logger = EventLogger(sink=sink)

    @logger.logged_event("function", metadata={"label": "one"})
    def operation(value):
        """Function docs survive wrapping."""
        logger.record_event("checkpoint")
        return value * 2

    assert operation.__name__ == "operation"
    assert operation.__doc__ == "Function docs survive wrapping."
    assert logger.run_task("task", operation, 4) == 8
    checkpoint, function, task = sink.events
    assert checkpoint.parent_event_id == function.event_id
    assert function.parent_event_id == task.event_id
    assert task.parent_event_id is None
    assert task.event_type == "task"
    assert json.loads(function.metadata_json) == {"label": "one"}


def test_async_decorator_awaits_actual_work_and_tracks_failure_and_cancellation():
    sink = MemorySink()
    logger = EventLogger(sink=sink)

    @logger.logged_event("async-operation")
    async def operation(error=None):
        await asyncio.sleep(0)
        logger.record_event("checkpoint")
        if error is not None:
            raise error
        return 42

    async def scenario():
        coroutine = operation()
        assert not sink.events
        assert await coroutine == 42
        error = ValueError("async business failure")
        with pytest.raises(ValueError) as captured:
            await operation(error)
        assert captured.value is error
        with pytest.raises(asyncio.CancelledError):
            await logger.run_task("async-task", operation, asyncio.CancelledError())

    asyncio.run(scenario())
    operations = [event for event in sink.events if event.event_name == "async-operation"]
    assert [event.status for event in operations] == ["success", "failed", "failed"]
    assert sink.events[-1].status == "failed"
    assert sink.events[-1].error_class == "CancelledError"
    assert logger.record_event("after").parent_event_id is None


def test_async_concurrent_siblings_keep_their_own_parent_context():
    sink = MemorySink()
    logger = EventLogger(sink=sink)

    async def worker(name, ready, release):
        with logger.event(name) as scope:
            ready.set()
            await release.wait()
            child = logger.bind(worker=name).record_event(f"{name}.child")
            assert child.parent_event_id == scope.event_id

    async def scenario():
        ready_a, ready_b, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        with logger.event("parent") as parent:
            first = asyncio.create_task(worker("a", ready_a, release))
            second = asyncio.create_task(worker("b", ready_b, release))
            await ready_a.wait()
            await ready_b.wait()
            release.set()
            await asyncio.gather(first, second)
            assert logger.record_event("parent.checkpoint").parent_event_id == parent.event_id

    asyncio.run(scenario())
    events = {event.event_name: event for event in sink.events}
    assert events["a"].parent_event_id == events["parent"].event_id
    assert events["b"].parent_event_id == events["parent"].event_id
    assert events["a.child"].parent_event_id != events["b.child"].parent_event_id
    assert logger.health.succeeded == 6


def test_generator_functions_are_rejected_before_lazy_work_is_misreported():
    logger = EventLogger(sink=MemorySink())

    def generator():
        yield 1

    async def async_generator():
        yield 1

    for function in (generator, async_generator):
        with pytest.raises(TypeError, match="scope around the consuming loop"):
            logger.logged_event("generator")(function)
        with pytest.raises(TypeError, match="scope around the consuming loop"):
            logger.run_task("generator", function)
    assert not logger.sink.events


def test_consumer_scope_completes_when_loop_breaks_without_closing_generator():
    logger = EventLogger(sink=MemorySink())

    def rows():
        yield from range(5)

    source = rows()
    seen = []
    with logger.event("positions.read") as scope:
        for value in source:
            seen.append(value)
            scope.row_count = len(seen)
            if value == 1:
                break
    (event,) = logger.sink.events
    assert seen == [0, 1]
    assert event.status == "success"
    assert event.row_count == 2
    # Delivery follows the consumer's block, even while iteration remains suspended.
    assert next(source) == 2
    assert logger.record_event("after").parent_event_id is None
    source.close()
    assert len(logger.sink.events) == 2


def test_consumer_scope_records_iteration_failure_and_preserves_exception():
    logger = EventLogger(sink=MemorySink())
    error = ValueError("source failed")

    def rows():
        yield 1
        raise error

    with pytest.raises(ValueError) as captured:
        with logger.event("positions.read") as scope:
            scope.row_count = 0
            for _ in rows():
                scope.row_count += 1
    assert captured.value is error
    (event,) = logger.sink.events
    assert event.status == "failed"
    assert event.error_class == "ValueError"
    assert event.row_count == 1
    assert logger.record_event("after").parent_event_id is None


@pytest.mark.parametrize(
    "settings",
    [
        {"metadata_max_bytes": 1},
        {"metadata_max_bytes": True},
        {"metadata_string_max_chars": -1},
        {"error_message_max_chars": -1},
        {"error_message_max_chars": True},
        {"sink": object()},
    ],
)
def test_invalid_configuration_is_rejected_at_construction(settings):
    with pytest.raises((TypeError, ValueError)):
        EventLogger(**settings)
