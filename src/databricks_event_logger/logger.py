"""Record events and observe operations through one lifecycle."""

from __future__ import annotations

import hashlib
import inspect
import json
import warnings
from collections import deque
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from copy import copy
from dataclasses import dataclass, field, replace
from datetime import datetime
from pathlib import PurePath
from threading import Lock
from typing import Any, TypeVar, cast
from uuid import uuid4

from databricks_event_logger.context import RuntimeContext
from databricks_event_logger.errors import EventLoggerConfigurationError
from databricks_event_logger.event import EventRecord
from databricks_event_logger.serialization import (
    DEFAULT_METADATA_MAX_BYTES,
    DEFAULT_METADATA_STRING_MAX_CHARS,
    DEFAULT_REDACT_KEYS,
    safe_text,
    serialize_metadata,
)
from databricks_event_logger.sinks.base import EventSink
from databricks_event_logger.sinks.console import ConsoleSink
from databricks_event_logger.timing import elapsed_ms, monotonic_ms, utc_now

F = TypeVar("F", bound=Callable[..., Any])
T = TypeVar("T")
_default_logger: ContextVar[EventLogger | None] = ContextVar("event_logger_default", default=None)


@dataclass(frozen=True)
class DeliveryHealth:
    """A snapshot of delivery attempts, including event-preparation failures."""

    attempted: int = 0
    succeeded: int = 0
    failed: int = 0
    last_error: str | None = None


@dataclass
class _DeliveryState:
    health: DeliveryHealth = field(default_factory=DeliveryHealth)
    lock: Any = field(default_factory=Lock)


@dataclass
class EventScope:
    """Editable result fields for one running operation; emitted when the block exits."""

    _event: EventRecord = field(repr=False)
    metadata: dict[str, Any] = field(default_factory=dict)
    row_count: int | None = None
    status: str = "success"
    severity: str | None = None
    source_table: str | None = None
    target_table: str | None = None

    @property
    def event_id(self) -> str:
        """The operation ID, available before its final event is emitted."""
        return self._event.event_id


class EventLogger:
    """Structured events with explicit identity, metadata, and a synchronous sink.

    Ordinary preparation/delivery failures warn and return ``None``. Strict mode
    raises them after successful work; an active business exception always wins.
    Bound loggers share delivery health and operation lineage. Independent
    loggers have independent lineage, even when using the same sink.
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
        default_metadata: Mapping[str, Any] | None = None,
        metadata_max_bytes: int | None = DEFAULT_METADATA_MAX_BYTES,
        metadata_string_max_chars: int | None = DEFAULT_METADATA_STRING_MAX_CHARS,
        error_message_max_chars: int | None = 2000,
        redact_keys: tuple[str, ...] = DEFAULT_REDACT_KEYS,
        strict_logging: bool = False,
        capture_error_frames: bool = False,
    ) -> None:
        # Configuration errors are detected before any business operation starts.
        serialize_metadata(
            {},
            redact_keys=redact_keys,
            string_max_chars=metadata_string_max_chars,
            max_bytes=metadata_max_bytes,
        )
        if error_message_max_chars is not None and (
            type(error_message_max_chars) is not int or error_message_max_chars < 0
        ):
            raise ValueError("error_message_max_chars must be a nonnegative integer or None.")
        self.app_name = app_name
        self.component = component
        self.environment = environment
        self.context = context if context is not None else RuntimeContext()
        self.correlation_id = correlation_id if correlation_id is not None else str(uuid4())
        self.sink = sink if sink is not None else ConsoleSink()
        if not callable(getattr(self.sink, "emit", None)):
            raise TypeError("sink must provide emit(event).")
        self._default_metadata = _copy_metadata(default_metadata)
        self.metadata_max_bytes = metadata_max_bytes
        self.metadata_string_max_chars = metadata_string_max_chars
        self.error_message_max_chars = error_message_max_chars
        self.redact_keys = tuple(redact_keys)
        self.strict_logging = strict_logging
        self.capture_error_frames = capture_error_frames
        self._delivery = _DeliveryState()
        self._current_event_id: ContextVar[str | None] = ContextVar("event_parent", default=None)
        self._new_record("logger.configuration")

    @property
    def health(self) -> DeliveryHealth:
        """Return a consistent, immutable snapshot shared by this logger's bindings."""
        with self._delivery.lock:
            return self._delivery.health

    @property
    def default_metadata(self) -> dict[str, Any]:
        """Return a shallow copy; nested values remain owned by the caller."""
        return dict(self._default_metadata)

    def bind(self, **metadata: Any) -> EventLogger:
        """Bind a shallow metadata snapshot while sharing delivery and lineage."""
        child = copy(self)
        child._default_metadata = {**self._default_metadata, **metadata}
        return child

    def record_event(
        self,
        event_name: str,
        *,
        event_type: str = "custom",
        status: str = "success",
        severity: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        event_id: str | None = None,
        parent_event_id: str | None = None,
        source_table: str | None = None,
        target_table: str | None = None,
        row_count: int | None = None,
        metric_name: str | None = None,
        metric_value: float | int | None = None,
        start_ts: datetime | None = None,
        end_ts: datetime | None = None,
        duration_ms: int | None = None,
    ) -> EventRecord | None:
        """Validate and deliver one event; return None if non-strict delivery fails."""
        _validate_metadata(metadata)
        event = self._new_record(
            event_name,
            event_type=event_type,
            status=status,
            severity=severity,
            event_id=event_id if event_id is not None else str(uuid4()),
            parent_event_id=(
                parent_event_id if parent_event_id is not None else self._current_event_id.get()
            ),
            source_table=source_table,
            target_table=target_table,
            row_count=row_count,
            metric_name=metric_name,
            metric_value=metric_value,
            start_ts=start_ts,
            end_ts=end_ts,
            duration_ms=duration_ms,
        )
        return self._deliver(event, metadata)

    def record_metric(
        self,
        metric_name: str,
        metric_value: int | float,
        *,
        event_name: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> EventRecord | None:
        """Record an explicit numeric metric; no computation is performed."""
        return self.record_event(
            event_name if event_name is not None else f"metric.{metric_name}",
            event_type="metric",
            metric_name=metric_name,
            metric_value=metric_value,
            metadata=metadata,
        )

    def event(
        self,
        event_name: str,
        *,
        event_type: str = "custom",
        metadata: Mapping[str, Any] | None = None,
        status: str = "success",
        severity: str | None = None,
        source_table: str | None = None,
        target_table: str | None = None,
        row_count: int | None = None,
    ):
        """Observe a block and yield editable results. Also usable inside async functions."""
        event = self._new_record(
            event_name,
            event_type=event_type,
            status=status,
            severity=severity,
            source_table=source_table,
            target_table=target_table,
            row_count=row_count,
        )
        scope = EventScope(
            event, _copy_metadata(metadata), row_count, status, severity, source_table, target_table
        )
        return self._event_scope(scope)

    @contextmanager
    def _event_scope(self, scope: EventScope) -> Iterator[EventScope]:
        event = replace(
            scope._event, parent_event_id=self._current_event_id.get(), start_ts=utc_now()
        )
        started = monotonic_ms()
        token = self._current_event_id.set(event.event_id)
        error: BaseException | None = None
        try:
            yield scope
        except BaseException as exc:
            error = exc
            raise
        finally:
            self._current_event_id.reset(token)
            ended = utc_now()
            self._deliver(
                event,
                scope.metadata,
                error=error,
                changes={
                    "row_count": scope.row_count,
                    "status": scope.status,
                    "severity": scope.severity,
                    "source_table": scope.source_table,
                    "target_table": scope.target_table,
                    "end_ts": ended,
                    "event_ts": ended,
                    "duration_ms": elapsed_ms(started),
                },
            )

    def logged_event(
        self,
        event_name: str,
        *,
        event_type: str = "function",
        metadata: Mapping[str, Any] | None = None,
    ) -> Callable[[F], F]:
        """Observe a sync/async function; generators must be scoped by their consumer."""
        self._new_record(event_name, event_type=event_type)
        snapshot = _copy_metadata(metadata)
        return lambda func: _decorate(
            func,
            lambda: self.event(event_name, event_type=event_type, metadata=snapshot),
        )

    def run_task(
        self,
        event_name: str,
        func: Callable[..., T],
        *args: Any,
        metadata: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> T:
        """Observe a task entry point, preserving its return value (await async tasks)."""
        return self.logged_event(event_name, event_type="task", metadata=metadata)(func)(
            *args,
            **kwargs,
        )

    def _new_record(self, event_name: str, **fields: Any) -> EventRecord:
        return EventRecord(
            event_name=event_name,
            app_name=self.app_name,
            component=self.component,
            environment=self.environment,
            context=self.context,
            correlation_id=self.correlation_id,
            **fields,
        )

    def _deliver(
        self,
        event: EventRecord,
        metadata: Mapping[str, Any] | None,
        *,
        changes: dict[str, Any] | None = None,
        error: BaseException | None = None,
    ) -> EventRecord | None:
        self._update_health("attempted")
        try:
            fields = dict(changes or {})
            if error is not None:
                fields.update(
                    _error_fields(error, self.capture_error_frames, self.error_message_max_chars)
                )
            event = replace(
                event,
                **fields,
                metadata_json=serialize_metadata(
                    {**self._default_metadata, **(metadata if metadata is not None else {})},
                    redact_keys=self.redact_keys,
                    string_max_chars=self.metadata_string_max_chars,
                    max_bytes=self.metadata_max_bytes,
                ),
            )
            self.sink.emit(event)
        except BaseException as exc:
            self._update_health("failed", f"{type(exc).__name__}: {safe_text(exc, max_chars=500)}")
            if error is None and (self.strict_logging or not isinstance(exc, Exception)):
                raise
            _warn_delivery(type(exc).__name__)
            return None
        self._update_health("succeeded")
        return event

    def _update_health(self, counter: str, last_error: str | None = None) -> None:
        with self._delivery.lock:
            health = self._delivery.health
            self._delivery.health = replace(
                health,
                **{counter: getattr(health, counter) + 1},
                last_error=last_error if last_error is not None else health.last_error,
            )


def _validate_metadata(metadata: Mapping[str, Any] | None) -> None:
    if metadata is not None and not isinstance(metadata, Mapping):
        raise TypeError("metadata must be a mapping or None.")


def _copy_metadata(metadata: Mapping[str, Any] | None) -> dict[str, Any]:
    _validate_metadata(metadata)
    return dict(metadata) if metadata is not None else {}


def _error_fields(
    exc: BaseException, capture_frames: bool, max_chars: int | None
) -> dict[str, Any]:
    # Only file basenames, function names and line numbers: never source or locals.
    frames: deque[dict[str, Any]] = deque(maxlen=20)
    trace = exc.__traceback__
    while trace is not None:
        code = trace.tb_frame.f_code
        frames.append(
            {
                "file": PurePath(code.co_filename).name[:200],
                "function": code.co_name[:200],
                "line": trace.tb_lineno,
            }
        )
        trace = trace.tb_next
    encoded = json.dumps(list(frames), separators=(",", ":"))
    fingerprint = hashlib.sha256(f"{type(exc).__name__}:{encoded}".encode()).hexdigest()
    return {
        "status": "failed",
        "severity": "error",
        "error_class": type(exc).__name__,
        "error_message": safe_text(exc, max_chars=max_chars),
        "stack_trace_hash": fingerprint,
        "error_frames_json": encoded if capture_frames else None,
    }


def _warn_delivery(error_class: str) -> None:
    try:
        warnings.warn(
            f"Event delivery failed ({error_class}); inspect logger.health.",
            RuntimeWarning,
            stacklevel=3,
        )
    except BaseException:
        # Warning filters and custom warning handlers must not alter business control flow.
        pass


def _decorate(func: F, scope_factory: Callable[..., Any]) -> F:
    from functools import wraps

    if inspect.isgeneratorfunction(func) or inspect.isasyncgenfunction(func):
        raise TypeError(
            "Generator functions are not supported; observe their iteration with event()."
        )
    if inspect.iscoroutinefunction(func):

        @wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            with scope_factory():
                return await func(*args, **kwargs)

        return cast(F, async_wrapper)

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        with scope_factory():
            return func(*args, **kwargs)

    return cast(F, wrapper)


@contextmanager
def use_logger(logger: EventLogger) -> Iterator[EventLogger]:
    """Install a context-local default logger and restore its predecessor on exit."""
    if not isinstance(logger, EventLogger):
        raise TypeError("logger must be an EventLogger.")
    token = _default_logger.set(logger)
    try:
        yield logger
    finally:
        _default_logger.reset(token)


def get_default_logger() -> EventLogger:
    logger = _default_logger.get()
    if logger is None:
        raise EventLoggerConfigurationError(
            "No default logger; configure one with use_logger(logger)."
        )
    return logger


def observed(
    event_name: str,
    *,
    event_type: str = "function",
    metadata: Mapping[str, Any] | None = None,
) -> Callable[[F], F]:
    """Observe a function using the default logger active when it is called."""
    EventRecord(event_name=event_name, event_type=event_type)
    snapshot = _copy_metadata(metadata)
    return lambda func: _decorate(
        func,
        lambda: get_default_logger().event(
            event_name,
            event_type=event_type,
            metadata=snapshot,
        ),
    )
