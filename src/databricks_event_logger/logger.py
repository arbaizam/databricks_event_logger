"""
Event logger public API.

``EventLogger`` owns event construction, failure capture, and sink dispatch.
Business code should not need repetitive ``try``/``except`` blocks. Decorators,
context managers, and task wrappers centralize that behavior while preserving
the original exception for Databricks job failure semantics.
"""

from __future__ import annotations

import hashlib
import traceback
import warnings
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime
from functools import wraps
from typing import Any, TypeVar
from uuid import uuid4

from databricks_event_logger.config import EventLoggerConfig
from databricks_event_logger.context import RuntimeContext, resolve_databricks_context
from databricks_event_logger.errors import EventLoggerConfigurationError
from databricks_event_logger.event import EventRecord
from databricks_event_logger.serialization import (
    DEFAULT_METADATA_MAX_BYTES,
    DEFAULT_METADATA_STRING_MAX_CHARS,
    DEFAULT_REDACT_KEYS,
    serialize_metadata,
)
from databricks_event_logger.sinks.base import EventSink
from databricks_event_logger.sinks.console import ConsoleSink
from databricks_event_logger.timing import elapsed_ms, monotonic_ms, utc_now

F = TypeVar("F", bound=Callable[..., Any])
T = TypeVar("T")
MetadataFactory = Callable[..., Mapping[str, Any] | None]

_default_logger: ContextVar[EventLogger | None] = ContextVar(
    "databricks_event_logger_default",
    default=None,
)
_current_event_id: ContextVar[str | None] = ContextVar(
    "databricks_event_logger_current_event_id",
    default=None,
)


class EventLogger:
    """
    Lightweight structured event logger.

    Parameters
    ----------
    app_name : str | None, default None
        Application name stamped onto emitted events.
    component : str | None, default None
        Component name stamped onto emitted events.
    environment : str | None, default None
        Environment or bundle target stamped onto emitted events.
    sink : EventSink | None, default None
        Event sink. When omitted, events are written as JSON lines through
        ``ConsoleSink``.
    context : RuntimeContext | None, default None
        Runtime context to attach to events.
    correlation_id : str | None, default None
        Correlation identifier shared by emitted events. When omitted, the
        logger derives a stable value from Databricks task/run context when
        available and falls back to a UUID outside Databricks.
    default_metadata : dict[str, Any] | None, default None
        Metadata merged into every event emitted by this logger. Event-level
        metadata wins when the same key appears in both places.
    metadata_max_bytes : int | None, default 4000
        Maximum serialized ``metadata_json`` size. ``None`` disables the cap.
    metadata_string_max_chars : int | None, default 2000
        Maximum string value length inside metadata. ``None`` disables string
        truncation.
    error_message_max_chars : int | None, default 2000
        Maximum stored exception message length. ``None`` disables truncation.
    redact_keys : tuple[str, ...], default DEFAULT_REDACT_KEYS
        Case-insensitive metadata key fragments whose values are redacted.
    strict_logging : bool, default False
        When True, sink failures from successful business paths are raised
        instead of warned. Failure-event logging still preserves the original
        business exception.
    max_events_warning_threshold : int | None, default 100
        Warn once when a logger emits more than this many events. ``None``
        disables the guardrail.
    """

    def __init__(
        self,
        *,
        app_name: str | None = None,
        component: str | None = None,
        environment: str | None = None,
        sink: EventSink | None = None,
        context: RuntimeContext | None = None,
        correlation_id: str | None = None,
        default_metadata: dict[str, Any] | None = None,
        metadata_max_bytes: int | None = DEFAULT_METADATA_MAX_BYTES,
        metadata_string_max_chars: int | None = DEFAULT_METADATA_STRING_MAX_CHARS,
        error_message_max_chars: int | None = 2000,
        redact_keys: tuple[str, ...] = DEFAULT_REDACT_KEYS,
        strict_logging: bool = False,
        max_events_warning_threshold: int | None = 100,
    ) -> None:
        """
        Create an event logger.
        """
        self.config = EventLoggerConfig(
            app_name=app_name,
            component=component,
            environment=environment,
        )
        self.context = context or RuntimeContext()
        self.correlation_id = (
            correlation_id or _correlation_id_from_context(self.context) or str(uuid4())
        )
        self.sink: EventSink = sink if sink is not None else ConsoleSink()
        self.default_metadata = dict(default_metadata or {})
        self.metadata_max_bytes = metadata_max_bytes
        self.metadata_string_max_chars = metadata_string_max_chars
        self.error_message_max_chars = error_message_max_chars
        self.redact_keys = redact_keys
        self.strict_logging = strict_logging
        self.max_events_warning_threshold = max_events_warning_threshold
        self._event_count = 0
        self._event_threshold_warned = False

    @property
    def job_url(self) -> str | None:
        """
        Return the Databricks job UI URL when workspace and job ids are known.

        Returns
        -------
        str | None
            URL shaped as ``https://<workspace>/jobs/<job_id>``, or ``None``
            when the current context is not a Databricks job run.
        """
        return _job_url(self.context)

    @property
    def job_run_url(self) -> str | None:
        """
        Return the Databricks job-run UI URL when run identifiers are known.

        Returns
        -------
        str | None
            URL shaped as ``https://<workspace>/jobs/<job_id>/runs/<run_id>``,
            or ``None`` when any required field is unavailable.
        """
        return _job_run_url(self.context)

    def record_event(
        self,
        event_name: str,
        *,
        event_type: str = "custom",
        status: str = "success",
        severity: str | None = None,
        metadata: dict[str, Any] | None = None,
        event_id: str | None = None,
        source_table: str | None = None,
        target_table: str | None = None,
        row_count: int | None = None,
        metric_name: str | None = None,
        metric_value: float | None = None,
        start_ts: datetime | None = None,
        end_ts: datetime | None = None,
        duration_ms: int | None = None,
        error_class: str | None = None,
        error_message: str | None = None,
        stack_trace_hash: str | None = None,
        parent_event_id: str | None = None,
        use_current_parent: bool = True,
    ) -> EventRecord:
        """
        Create and emit one event.

        Parameters
        ----------
        event_name : str
            Stable event name.
        event_type : str, default "custom"
            Event category.
        status : str, default "success"
            Event status.
        severity : str | None, default None
            Optional severity label.
        metadata : dict[str, Any] | None, default None
            Caller-controlled metadata.
        event_id : str | None, default None
            Optional preallocated event id. Decorators and context managers use
            this so nested events can point at the operation being observed.
        source_table : str | None, default None
            Source table for I/O events.
        target_table : str | None, default None
            Target table for I/O events.
        row_count : int | None, default None
            Known row count. The logger never computes this automatically.
        metric_name : str | None, default None
            Metric name for metric events.
        metric_value : float | None, default None
            Metric value for metric events.
        start_ts, end_ts : datetime | None, default None
            Optional operation timestamps.
        duration_ms : int | None, default None
            Optional measured operation duration.
        error_class : str | None, default None
            Exception class name for failed events.
        error_message : str | None, default None
            Exception message for failed events.
        stack_trace_hash : str | None, default None
            Stable hash of the captured stack trace.
        parent_event_id : str | None, default None
            Parent event id. Defaults to the current active event, if any.
        use_current_parent : bool, default True
            When True, attach the current active event as parent if
            ``parent_event_id`` is omitted. Observed wrapper events set this to
            False so they do not parent to themselves.

        Returns
        -------
        EventRecord
            Event object that was handed to the sink.
        """
        metadata_json = serialize_metadata(
            self._merge_metadata(metadata),
            redact_keys=self.redact_keys,
            string_max_chars=self.metadata_string_max_chars,
            max_bytes=self.metadata_max_bytes,
        )
        event = EventRecord(
            event_name=event_name,
            event_type=event_type,
            status=status,
            event_id=event_id,
            correlation_id=self.correlation_id,
            parent_event_id=(
                parent_event_id
                if parent_event_id is not None
                else (_current_event_id.get() if use_current_parent else None)
            ),
            start_ts=start_ts,
            end_ts=end_ts,
            duration_ms=duration_ms,
            severity=severity,
            app_name=self.config.app_name,
            component=self.config.component,
            environment=self.config.environment,
            context=self.context,
            source_table=source_table,
            target_table=target_table,
            row_count=row_count,
            metric_name=metric_name,
            metric_value=metric_value,
            error_class=error_class,
            error_message=self._bounded_text(error_message, self.error_message_max_chars),
            stack_trace_hash=stack_trace_hash,
            metadata_json=metadata_json,
        )
        self._emit(event)
        return event

    def record_metric(
        self,
        metric_name: str,
        metric_value: float,
        *,
        event_name: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> EventRecord:
        """
        Record one explicit metric event.

        Parameters
        ----------
        metric_name : str
            Metric identifier.
        metric_value : float
            Numeric metric value.
        event_name : str | None, default None
            Optional event name. Defaults to ``metric.<metric_name>``.
        metadata : dict[str, Any] | None, default None
            Caller-controlled metadata.

        Returns
        -------
        EventRecord
            Emitted metric event.
        """
        return self.record_event(
            event_name or f"metric.{metric_name}",
            event_type="metric",
            status="success",
            metric_name=metric_name,
            metric_value=metric_value,
            metadata=metadata,
        )

    def logged_event(
        self,
        event_name: str,
        *,
        event_type: str = "function",
        metadata: Mapping[str, Any] | None = None,
        metadata_factory: MetadataFactory | None = None,
    ) -> Callable[[F], F]:
        """
        Decorate a function so success and failure are logged automatically.

        Parameters
        ----------
        event_name : str
            Event emitted when the function finishes.
        event_type : str, default "function"
            Event category.
        metadata : Mapping[str, Any] | None, default None
            Static metadata to attach to the event.
        metadata_factory : Callable[..., Mapping[str, Any] | None] | None, default None
            Optional callable evaluated at function-call time with the wrapped
            function's positional and keyword arguments. Returned keys are
            merged into ``metadata`` and win on key conflicts.

        Returns
        -------
        Callable[[F], F]
            Decorator that preserves the wrapped function's return value and
            original exception behavior.
        """

        def decorator(func: F) -> F:
            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                return self._run_observed(
                    event_name,
                    lambda: func(*args, **kwargs),
                    event_type=event_type,
                    metadata=metadata,
                    metadata_factory=metadata_factory,
                    factory_args=args,
                    factory_kwargs=kwargs,
                )

            return wrapper  # type: ignore[return-value]

        return decorator

    @contextmanager
    def event(
        self,
        event_name: str,
        *,
        event_type: str = "custom",
        metadata: dict[str, Any] | None = None,
        source_table: str | None = None,
        target_table: str | None = None,
        row_count: int | None = None,
    ):
        """
        Log success or failure for a custom block of code.

        Parameters
        ----------
        event_name : str
            Event emitted when the block exits.
        event_type : str, default "custom"
            Event category.
        metadata : dict[str, Any] | None, default None
            Caller-controlled metadata.
        source_table : str | None, default None
            Source table for I/O events.
        target_table : str | None, default None
            Target table for I/O events.
        row_count : int | None, default None
            Known row count for the block.
        """
        start_ts = utc_now()
        start_ms = monotonic_ms()
        event_id = str(uuid4())
        parent_event_id = _current_event_id.get()
        token = _current_event_id.set(event_id)
        try:
            yield
        except Exception as exc:
            end_ts = utc_now()
            self._record_failure(
                event_name,
                exc,
                event_type=event_type,
                metadata=metadata,
                start_ts=start_ts,
                end_ts=end_ts,
                duration_ms=elapsed_ms(start_ms),
                event_id=event_id,
                parent_event_id=parent_event_id,
                source_table=source_table,
                target_table=target_table,
                row_count=row_count,
            )
            raise
        else:
            end_ts = utc_now()
            self.record_event(
                event_name,
                event_type=event_type,
                status="success",
                metadata=metadata,
                start_ts=start_ts,
                end_ts=end_ts,
                duration_ms=elapsed_ms(start_ms),
                event_id=event_id,
                parent_event_id=parent_event_id,
                use_current_parent=False,
                source_table=source_table,
                target_table=target_table,
                row_count=row_count,
            )
        finally:
            _current_event_id.reset(token)

    def run_task(
        self,
        event_name: str,
        func: Callable[..., T],
        *args: Any,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> T:
        """
        Run a task entry point with SDK-level lifecycle logging.

        Parameters
        ----------
        event_name : str
            Event emitted when the task callable finishes.
        func : Callable[..., T]
            Task entry-point callable.
        *args : Any
            Positional arguments passed to ``func``.
        metadata : dict[str, Any] | None, default None
            Caller-controlled metadata.
        **kwargs : Any
            Keyword arguments passed to ``func``.

        Returns
        -------
        T
            Return value from ``func``.
        """
        return self._run_observed(
            event_name,
            lambda: func(*args, **kwargs),
            event_type="task",
            metadata=metadata,
        )

    def _run_observed(
        self,
        event_name: str,
        func: Callable[[], T],
        *,
        event_type: str,
        metadata: Mapping[str, Any] | None,
        metadata_factory: MetadataFactory | None = None,
        factory_args: tuple[Any, ...] = (),
        factory_kwargs: Mapping[str, Any] | None = None,
    ) -> T:
        """
        Execute one callable and emit success or failure.
        """
        start_ts = utc_now()
        start_ms = monotonic_ms()
        event_id = str(uuid4())
        parent_event_id = _current_event_id.get()
        token = _current_event_id.set(event_id)
        resolved_metadata = _resolve_observed_metadata(
            metadata,
            metadata_factory,
            event_name=event_name,
            factory_args=factory_args,
            factory_kwargs=factory_kwargs,
        )
        try:
            result = func()
        except Exception as exc:
            end_ts = utc_now()
            self._record_failure(
                event_name,
                exc,
                event_type=event_type,
                metadata=resolved_metadata,
                start_ts=start_ts,
                end_ts=end_ts,
                duration_ms=elapsed_ms(start_ms),
                event_id=event_id,
                parent_event_id=parent_event_id,
            )
            raise
        else:
            end_ts = utc_now()
            self.record_event(
                event_name,
                event_type=event_type,
                status="success",
                metadata=resolved_metadata,
                start_ts=start_ts,
                end_ts=end_ts,
                duration_ms=elapsed_ms(start_ms),
                event_id=event_id,
                parent_event_id=parent_event_id,
                use_current_parent=False,
            )
            return result
        finally:
            _current_event_id.reset(token)

    def _record_failure(
        self,
        event_name: str,
        exc: BaseException,
        *,
        event_type: str,
        metadata: dict[str, Any] | None,
        start_ts: datetime,
        end_ts: datetime,
        duration_ms: int,
        event_id: str,
        parent_event_id: str | None,
        source_table: str | None = None,
        target_table: str | None = None,
        row_count: int | None = None,
    ) -> None:
        """
        Emit a failure event without masking the original exception.
        """
        try:
            self.record_event(
                event_name,
                event_type=event_type,
                status="failed",
                severity="error",
                metadata=metadata,
                start_ts=start_ts,
                end_ts=end_ts,
                duration_ms=duration_ms,
                error_class=exc.__class__.__name__,
                error_message=str(exc),
                stack_trace_hash=_stack_trace_hash(exc),
                event_id=event_id,
                parent_event_id=parent_event_id,
                use_current_parent=False,
                source_table=source_table,
                target_table=target_table,
                row_count=row_count,
            )
        except Exception as logging_exc:
            # The caller is already handling a business exception. Never replace
            # it with a secondary logging failure.
            warnings.warn(
                f"Failed to emit failure event {event_name!r}: {logging_exc}",
                RuntimeWarning,
                stacklevel=1,
            )

    def _emit(self, event: EventRecord) -> None:
        """
        Hand an event to the configured sink.
        """
        self._event_count += 1
        self._warn_on_event_volume()
        try:
            self.sink.emit(event)
        except Exception as exc:
            if self.strict_logging:
                raise
            warnings.warn(
                f"Failed to emit event {event.event_name!r}: {exc}",
                RuntimeWarning,
                stacklevel=3,
            )

    def _merge_metadata(self, metadata: dict[str, Any] | None) -> dict[str, Any] | None:
        """
        Merge logger defaults with event-level metadata.
        """
        if not self.default_metadata and not metadata:
            return None
        merged = dict(self.default_metadata)
        if metadata:
            merged.update(metadata)
        return merged

    def _bounded_text(self, value: str | None, max_chars: int | None) -> str | None:
        """
        Return a bounded text value for high-volume error fields.
        """
        if value is None or max_chars is None or max_chars < 0 or len(value) <= max_chars:
            return value
        return f"{value[:max_chars]}...[TRUNCATED]"

    def _warn_on_event_volume(self) -> None:
        """
        Warn once when a logger emits more events than expected for one task.
        """
        threshold = self.max_events_warning_threshold
        if threshold is None or self._event_threshold_warned or self._event_count <= threshold:
            return
        self._event_threshold_warned = True
        warnings.warn(
            "EventLogger has emitted more than "
            f"{threshold} events in this logger instance. Consider aggregating "
            "very chatty loops into summary events when possible.",
            RuntimeWarning,
            stacklevel=3,
        )

def set_default_logger(logger: EventLogger) -> None:
    """
    Set the default logger for decorators and helper functions.

    Parameters
    ----------
    logger : EventLogger
        Logger to use as the current default.
    """
    _default_logger.set(logger)


def get_default_logger() -> EventLogger:
    """
    Return the configured default logger.

    Returns
    -------
    EventLogger
        Current default logger.

    Raises
    ------
    EventLoggerConfigurationError
        If ``observe_notebook`` or ``set_default_logger`` has not configured a
        default logger.
    """
    logger = _default_logger.get()
    if logger is None:
        raise EventLoggerConfigurationError(
            "No default EventLogger is configured. Call observe_notebook(...) "
            "or set_default_logger(...) before using default logger helpers."
        )
    return logger


def observe_notebook(
    *,
    spark: Any,
    dbutils: Any,
    app_name: str | None = None,
    component: str | None = None,
    environment: str | None = None,
    sink: EventSink | None = None,
    correlation_id: str | None = None,
    default_metadata: dict[str, Any] | None = None,
    strict_logging: bool = False,
) -> EventLogger:
    """
    Initialize the default Databricks notebook logger and emit ``notebook.started``.

    ``spark`` and ``dbutils`` are explicit required dependencies. The default
    sink is ``ConsoleSink``; pass ``DeltaSink(spark, table_name)`` when events
    must be persisted.
    """
    logger = EventLogger(
        app_name=app_name,
        component=component,
        environment=environment,
        sink=sink,
        context=resolve_databricks_context(dbutils=dbutils, spark=spark),
        correlation_id=correlation_id,
        default_metadata=default_metadata,
        strict_logging=strict_logging,
    )
    set_default_logger(logger)
    logger.record_event(
        "notebook.started",
        event_type="notebook",
        status="started",
        metadata={
            "sink_type": type(logger.sink).__name__,
            "event_table": getattr(logger.sink, "table_name", None),
            "strict_logging": strict_logging,
            "job_url": logger.job_url,
            "job_run_url": logger.job_run_url,
        },
    )
    return logger


def _stack_trace_hash(exc: BaseException) -> str:
    """
    Return a hash identifying one exception instance.

    The hash includes the traceback, so it varies between call sites and across
    code changes.
    """
    trace_text = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    return hashlib.sha256(trace_text.encode("utf-8")).hexdigest()


def _resolve_observed_metadata(
    metadata: Mapping[str, Any] | None,
    metadata_factory: MetadataFactory | None,
    *,
    event_name: str,
    factory_args: tuple[Any, ...],
    factory_kwargs: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    """
    Resolve static and call-time metadata for observed functions.
    """
    resolved = _copy_metadata(metadata)
    if metadata_factory is None:
        return resolved
    try:
        factory_metadata = metadata_factory(*factory_args, **dict(factory_kwargs or {}))
        factory_metadata = dict(factory_metadata or {})
    except Exception as exc:
        resolved = _metadata_with_factory_error(resolved, exc)
        warnings.warn(
            f"metadata_factory for {event_name!r} raised "
            f"{type(exc).__name__}: {exc}. Static metadata will be used.",
            RuntimeWarning,
            stacklevel=3,
        )
        return resolved
    if factory_metadata:
        if resolved is None:
            resolved = {}
        resolved.update(factory_metadata)
    return resolved


def _copy_metadata(metadata: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """
    Return a mutable metadata copy while preserving empty metadata as ``None``.
    """
    if not metadata:
        return None
    return dict(metadata)


def _metadata_with_factory_error(
    metadata: dict[str, Any] | None,
    exc: BaseException,
) -> dict[str, Any]:
    """
    Return metadata annotated with a non-blocking metadata factory failure.
    """
    resolved = dict(metadata or {})
    resolved.update(
        {
            "metadata_factory_error": True,
            "metadata_factory_error_class": type(exc).__name__,
            "metadata_factory_error_message": str(exc),
        }
    )
    return resolved


def _correlation_id_from_context(context: RuntimeContext) -> str | None:
    """
    Return a stable task/run correlation id from Databricks context.

    The default is task-run correlation: ``task_run_id`` when Databricks
    supplies it, otherwise ``run_id:task_key[:attempt]`` or ``run_id``. Pass an
    explicit ``correlation_id`` when a workflow needs a single ID shared across
    multiple tasks or retries.
    """
    if context.task_run_id:
        return context.task_run_id
    if context.run_id and context.task_key:
        parts = [context.run_id, context.task_key]
        if context.task_attempt_number:
            parts.append(context.task_attempt_number)
        return ":".join(parts)
    if context.run_id:
        return context.run_id
    return None


def _job_url(context: RuntimeContext) -> str | None:
    """
    Build the Databricks job URL from normalized runtime context.
    """
    workspace_url = _workspace_url_with_scheme(context.workspace_url)
    if not workspace_url or not context.job_id:
        return None
    return f"{workspace_url}/jobs/{context.job_id}"


def _job_run_url(context: RuntimeContext) -> str | None:
    """
    Build the Databricks job-run URL from normalized runtime context.
    """
    job_url = _job_url(context)
    if not job_url or not context.run_id:
        return None
    return f"{job_url}/runs/{context.run_id}"


def _workspace_url_with_scheme(workspace_url: str | None) -> str | None:
    """
    Return a browser-ready workspace URL.
    """
    if not workspace_url:
        return None
    normalized = workspace_url.strip().rstrip("/").lower()
    if normalized.startswith("http://") or normalized.startswith("https://"):
        return normalized
    return f"https://{normalized}"
