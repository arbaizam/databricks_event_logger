"""
Event logger public API.

``EventLogger`` owns event construction, failure capture, and sink dispatch.
Business code should not need repetitive ``try``/``except`` blocks. Decorators,
context managers, and task wrappers centralize that behavior while preserving
the original exception for Databricks job failure semantics.
"""

from __future__ import annotations

import hashlib
import inspect
import traceback
import warnings
from collections.abc import Callable
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
from databricks_event_logger.sinks.base import EventSink
from databricks_event_logger.sinks.delta import DeltaSink
from databricks_event_logger.sinks.memory import MemorySink
from databricks_event_logger.timing import elapsed_ms, monotonic_ms, utc_now

F = TypeVar("F", bound=Callable[..., Any])
T = TypeVar("T")

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
        Event sink. When omitted, events are stored in an in-memory sink. This
        avoids accidental Delta writes in tests and still gives callers a real
        emitted event object.
    context : RuntimeContext | None, default None
        Runtime context to attach to events.
    correlation_id : str | None, default None
        Correlation identifier shared by emitted events. A UUID is generated
        when omitted.
    event_table : str | None, default None
        Fully qualified event table for Delta-backed usage. Stored in config
        but not used unless a Delta sink is configured.
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
        event_table: str | None = None,
    ) -> None:
        """
        Create an event logger.
        """
        self.config = EventLoggerConfig(
            app_name=app_name,
            component=component,
            environment=environment,
            event_table=event_table,
        )
        self.context = context or RuntimeContext()
        self.correlation_id = correlation_id or str(uuid4())
        self.sink: EventSink = sink or MemorySink()

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
            error_message=error_message,
            stack_trace_hash=stack_trace_hash,
            metadata=metadata,
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
        metadata: dict[str, Any] | None = None,
    ) -> Callable[[F], F]:
        """
        Decorate a function so success and failure are logged automatically.

        Parameters
        ----------
        event_name : str
            Event emitted when the function finishes.
        event_type : str, default "function"
            Event category.
        metadata : dict[str, Any] | None, default None
            Static metadata to attach to the event.

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

    def flush(self) -> None:
        """
        Flush the configured sink.
        """
        self._call_sink("flush")

    def close(self) -> None:
        """
        Close the configured sink.
        """
        self._call_sink("close")

    def _run_observed(
        self,
        event_name: str,
        func: Callable[[], T],
        *,
        event_type: str,
        metadata: dict[str, Any] | None,
    ) -> T:
        """
        Execute one callable and emit success or failure.
        """
        start_ts = utc_now()
        start_ms = monotonic_ms()
        event_id = str(uuid4())
        parent_event_id = _current_event_id.get()
        token = _current_event_id.set(event_id)
        try:
            result = func()
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
        try:
            self.sink.emit(event)
        except Exception as exc:
            warnings.warn(
                f"Failed to emit event {event.event_name!r}: {exc}",
                RuntimeWarning,
                stacklevel=3,
            )

    def _call_sink(self, method_name: str) -> None:
        """
        Call a sink lifecycle method and warn on failure.
        """
        try:
            getattr(self.sink, method_name)()
        except Exception as exc:
            warnings.warn(
                f"Failed to {method_name} event sink: {exc}",
                RuntimeWarning,
                stacklevel=2,
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


class NotebookObserver:
    """
    Callable notebook bootstrap helper.
    """

    def __call__(
        self,
        *,
        app_name: str | None = None,
        component: str | None = None,
        environment: str | None = None,
        event_table: str | None = None,
        sink: EventSink | None = None,
        spark: Any | None = None,
        dbutils: Any | None = None,
        context: RuntimeContext | None = None,
    ) -> EventLogger:
        """
        Initialize the default logger and emit ``notebook.started``.

        ``observe_notebook`` records notebook startup only. Use
        ``logger.run_task(...)`` around the notebook's main callable when a
        terminal success or failure event is required.

        Parameters
        ----------
        app_name, component, environment : str | None
            Event identity fields passed by the job or bundle.
        event_table : str | None, default None
            Fully qualified event table used when constructing a Delta sink.
        sink : EventSink | None, default None
            Explicit sink. When omitted and both ``spark`` and ``event_table``
            are supplied, a ``DeltaSink`` is used. Otherwise a ``MemorySink`` is
            used. Passing ``event_table`` without ``spark`` does not create
            Delta writes.
        spark : Any | None, default None
            Spark session for context capture and optional Delta sink creation.
        dbutils : Any | None, default None
            Databricks dbutils object for context capture.
        context : RuntimeContext | None, default None
            Explicit runtime context.

        Returns
        -------
        EventLogger
            Configured default logger.
        """
        resolved_context = context or resolve_databricks_context(dbutils=dbutils, spark=spark)
        resolved_sink = sink
        if resolved_sink is None and spark is not None and event_table:
            resolved_sink = DeltaSink(spark=spark, table_name=event_table)
        logger = EventLogger(
            app_name=app_name,
            component=component,
            environment=environment,
            sink=resolved_sink,
            context=resolved_context,
            event_table=event_table,
        )
        set_default_logger(logger)
        logger.record_event("notebook.started", event_type="notebook", status="started")
        return logger

    def from_widgets(
        self,
        *,
        dbutils: Any | None = None,
        spark: Any | None = None,
        sink: EventSink | None = None,
    ) -> EventLogger:
        """
        Initialize a logger from standard Databricks widgets.

        The expected widget names are ``app_name``, ``component``,
        ``environment``, and ``observability_event_table``. When ``dbutils`` or
        ``spark`` are omitted, this method performs a bounded lookup through
        caller frames for notebook globals with those names. Passing them
        explicitly is preferred for package code and tests. Optional widgets
        named ``workspace_id``, ``workspace_url``, ``cluster_id``, ``job_id``,
        ``run_id``, ``task_key``, ``task_run_id``,
        ``task_attempt_number``, ``job_start_time``, ``job_trigger_type``,
        ``notebook_path``, ``user_name``, and ``run_as_user_name`` are used as
        context fallbacks when the Databricks runtime context does not expose
        those fields directly.

        Parameters
        ----------
        dbutils : Any | None, default None
            Optional dbutils object. When omitted, the caller's notebook globals
            are inspected for ``dbutils``.
        spark : Any | None, default None
            Optional Spark session. When omitted, the caller's notebook globals
            are inspected for ``spark``.
        sink : EventSink | None, default None
            Explicit sink for tests or custom persistence.

        Returns
        -------
        EventLogger
            Configured default logger.
        """
        resolved_dbutils = dbutils or _caller_global("dbutils")
        resolved_spark = spark or _caller_global("spark")
        widget_values = _widget_values(resolved_dbutils)
        context = resolve_databricks_context(
            dbutils=resolved_dbutils,
            spark=resolved_spark,
            fallback=_context_from_widgets(widget_values),
        )
        return self(
            app_name=_widget_value(widget_values, "app_name"),
            component=_widget_value(widget_values, "component"),
            environment=_widget_value(widget_values, "environment"),
            event_table=_widget_value(widget_values, "observability_event_table"),
            sink=sink,
            spark=resolved_spark,
            dbutils=resolved_dbutils,
            context=context,
        )


observe_notebook = NotebookObserver()


def _stack_trace_hash(exc: BaseException) -> str:
    """
    Return a hash identifying one exception instance.

    The hash includes the traceback, so it varies between call sites and across
    code changes.
    """
    trace_text = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    return hashlib.sha256(trace_text.encode("utf-8")).hexdigest()


def _widget_values(dbutils: Any | None) -> dict[str, str]:
    """
    Return all visible Databricks widget values.
    """
    if dbutils is None:
        return {}
    values: dict[str, str] = {}
    try:
        raw_values = dbutils.widgets.getAll()
    except Exception:
        raw_values = {}
    try:
        items = raw_values.items()
    except Exception:
        items = ()
    for key, value in items:
        if text := _string_or_none(value):
            values[str(key)] = text
    for name in _KNOWN_WIDGET_NAMES:
        if name not in values and (value := _legacy_widget_value(dbutils, name)):
            values[name] = value
    return values


def _widget_value(values: dict[str, str], name: str) -> str | None:
    """
    Read one Databricks widget value from a captured widget mapping.
    """
    return values.get(name)


def _context_from_widgets(values: dict[str, str]) -> dict[str, str | None]:
    """
    Return optional runtime context fallback values from Databricks widgets.
    """
    context_values: dict[str, str] = {}
    for field, widget_names in _WIDGET_CONTEXT_KEY_MAP.items():
        for widget_name in widget_names:
            if value := values.get(widget_name):
                context_values[field] = value
                break
    return context_values


def _string_or_none(value: Any) -> str | None:
    """
    Convert a widget value to ``str`` while preserving missing values.
    """
    if value is None:
        return None
    text = str(value).strip()
    return text or None


_WIDGET_CONTEXT_KEY_MAP = {
    "workspace_id": ("workspace_id",),
    "workspace_url": ("workspace_url",),
    "cluster_id": ("cluster_id",),
    "job_id": ("job_id",),
    "run_id": ("run_id", "job_run_id"),
    "task_key": ("task_key", "task_name"),
    "task_run_id": ("task_run_id",),
    "task_attempt_number": ("task_attempt_number", "task_execution_count"),
    "job_start_time": ("job_start_time",),
    "job_trigger_type": ("job_trigger_type",),
    "notebook_path": ("notebook_path",),
    "user_name": ("user_name",),
    "run_as_user_name": ("run_as_user_name",),
}

_KNOWN_WIDGET_NAMES = (
    "app_name",
    "component",
    "environment",
    "observability_event_table",
    *tuple(name for names in _WIDGET_CONTEXT_KEY_MAP.values() for name in names),
)


def _legacy_widget_value(dbutils: Any | None, name: str) -> str | None:
    """
    Read one Databricks widget value using the direct widget API.
    """
    if dbutils is None:
        return None
    try:
        value = dbutils.widgets.get(name)
    except Exception:
        return None
    text = str(value).strip()
    return text or None


def _caller_global(name: str, *, max_depth: int = 10) -> Any | None:
    """
    Return a value from a nearby caller frame that defines ``name``.

    This keeps ``observe_notebook.from_widgets()`` low-burden in Databricks
    notebooks while avoiding a hard dependency on notebook-only globals.
    """
    frame = inspect.currentframe()
    frame = frame.f_back if frame is not None else None
    for _ in range(max_depth):
        if frame is None:
            break
        local_value = frame.f_locals.get(name)
        if local_value is not None:
            return local_value
        global_value = frame.f_globals.get(name)
        if global_value is not None:
            return global_value
        frame = frame.f_back
    return None
