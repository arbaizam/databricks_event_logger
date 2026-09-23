"""``EventLogger``: the main entry point for recording events.

There are three ways to record an event. All of them end in ``_deliver``:

- ``record_event`` / ``record_metric``: record something that already happened.
- ``event(...)``: a ``with`` block that times an operation and emits one
  event when the block exits.
- ``logged_event`` / ``run_task``: the same as ``event(...)``, wrapped
  around a function call.

Every event goes through the same steps (see ``_deliver``):

1. Build the final ``EventRecord``: identity, timing, results, error details,
   and metadata JSON.
2. Pass it to ``sink.emit(record)``.
3. Update the delivery health counters.

**Failure rule:** logging must never break the application. If step 1 or 2
fails, the logger warns and returns ``None``. There are two exceptions:
with ``strict_logging=True`` the error is raised, and interrupts such as
``KeyboardInterrupt`` are always raised. Even then, if the application's own
code already raised an exception, that original exception wins.

**Parent tracking:** while an ``event(...)`` block is open, any event
recorded inside it gets the block's ``event_id`` as its ``parent_event_id``.
This uses a ``ContextVar``, so it works correctly with threads and
``asyncio``.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from contextvars import ContextVar
from copy import copy
from dataclasses import replace
from datetime import datetime
from typing import Any, TypeVar
from uuid import uuid4

from databricks_event_logger.context import RuntimeContext
from databricks_event_logger.decorators import wrap_in_scope
from databricks_event_logger.diagnostics import warn_safely
from databricks_event_logger.failures import describe_exception
from databricks_event_logger.health import DeliveryHealth, DeliveryTracker
from databricks_event_logger.metadata import (
    DEFAULT_METADATA_MAX_BYTES,
    DEFAULT_METADATA_STRING_MAX_CHARS,
    DEFAULT_REDACT_KEYS,
    check_is_mapping,
    copy_metadata,
    serialize_metadata,
)
from databricks_event_logger.record import EventRecord
from databricks_event_logger.scope import EventScope
from databricks_event_logger.sinks.base import EventSink
from databricks_event_logger.sinks.console import ConsoleSink
from databricks_event_logger.timing import elapsed_ms, monotonic_ms, utc_now

F = TypeVar("F", bound=Callable[..., Any])
T = TypeVar("T")

DEFAULT_ERROR_MESSAGE_MAX_CHARS = 2000


class EventLogger:
    """Records structured events and sends each one to a sink right away.

    Example::

        logger = EventLogger(app_name="positions", sink=MemorySink())

        with logger.event("positions.validate") as scope:
            scope.row_count = len(rows)

        logger.record_metric("positions.total", 150.0)

    Args:
        app_name: The application name, copied onto every event.
        component: The part of the application, such as ``"publish"``.
        environment: The deployment environment, such as ``"dev"`` or ``"prod"``.
        sink: Where events go. Defaults to ``ConsoleSink`` (prints JSON lines).
        context: Where the code runs (job, task, workspace). Defaults to empty.
        correlation_id: Groups related events. Defaults to a new UUID. Pass
            the same value to several tasks to link them together.
        default_metadata: Metadata added to every event. Per-event metadata
            wins when the same key appears in both.
        metadata_max_bytes: The maximum size of ``metadata_json``, in bytes.
        metadata_string_max_chars: The maximum length of each metadata string.
        error_message_max_chars: The maximum length of ``error_message``.
        redact_keys: Hide metadata values whose key contains any of these words.
        strict_logging: If ``True``, raise logging failures instead of warning.
        capture_error_frames: If ``True``, store the file, function, and line
            of each failure in ``error_frames_json``.

    Raises:
        TypeError: If ``sink`` has no ``emit`` method, or metadata isn't a mapping.
        ValueError: If a setting or identity value is invalid.
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
        error_message_max_chars: int | None = DEFAULT_ERROR_MESSAGE_MAX_CHARS,
        redact_keys: tuple[str, ...] = DEFAULT_REDACT_KEYS,
        strict_logging: bool = False,
        capture_error_frames: bool = False,
    ) -> None:
        # Check the settings first, so a bad setup fails before any work starts.
        _check_error_message_limit(error_message_max_chars)
        sink = sink if sink is not None else ConsoleSink()
        if not callable(getattr(sink, "emit", None)):
            raise TypeError("sink must provide emit(event).")
        default_metadata = copy_metadata(default_metadata)
        # Serializing the defaults once checks both the metadata and its settings.
        serialize_metadata(
            default_metadata,
            redact_keys=redact_keys,
            string_max_chars=metadata_string_max_chars,
            max_bytes=metadata_max_bytes,
        )

        # Who is logging. These are copied onto every event.
        self.app_name = app_name
        self.component = component
        self.environment = environment
        self.context = context if context is not None else RuntimeContext()
        self.correlation_id = correlation_id if correlation_id is not None else str(uuid4())

        # Where events go, and how metadata and errors are written.
        self.sink = sink
        self._default_metadata = default_metadata
        self.metadata_max_bytes = metadata_max_bytes
        self.metadata_string_max_chars = metadata_string_max_chars
        self.error_message_max_chars = error_message_max_chars
        self.redact_keys = tuple(redact_keys)
        self.strict_logging = strict_logging
        self.capture_error_frames = capture_error_frames

        # State shared with every logger created by bind().
        self._health = DeliveryTracker()
        self._active_event_id: ContextVar[str | None] = ContextVar("event_parent", default=None)

        # Check the identity fields above using the same rules as events.
        self._new_record("logger.configuration")

    # ------------------------------------------------------------------
    # Read-only views
    # ------------------------------------------------------------------

    @property
    def health(self) -> DeliveryHealth:
        """Delivery counts for this logger and its bound loggers. See ``DeliveryHealth``."""
        return self._health.snapshot()

    @property
    def default_metadata(self) -> dict[str, Any]:
        """A copy of the metadata added to every event.

        Only the top level is copied. Changing a nested value changes it for
        the logger too.
        """
        return dict(self._default_metadata)

    # ------------------------------------------------------------------
    # Creating related loggers
    # ------------------------------------------------------------------

    def bind(self, **metadata: Any) -> EventLogger:
        """Return a new logger that adds ``metadata`` to every event.

        The new logger shares this logger's sink, identity, correlation ID,
        delivery health, and parent tracking. Only its default metadata is
        different. The original logger is not changed.

        Example::

            batch_logger = logger.bind(batch_id="2026-09-04")

        Raises:
            TypeError: If a metadata key (including a nested key) isn't a string.
        """
        merged = {**self._default_metadata, **metadata}
        self._serialize_metadata(merged)  # Raises if the merged metadata is invalid.
        # A shallow copy shares _health and _active_event_id with this logger.
        child = copy(self)
        child._default_metadata = merged
        return child

    # ------------------------------------------------------------------
    # Recording events
    # ------------------------------------------------------------------

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
        """Record something that has already happened, and deliver it now.

        ``parent_event_id`` defaults to the innermost open ``event(...)``
        block. Other arguments match the ``EventRecord`` fields.

        Returns:
            The delivered ``EventRecord``, or ``None`` if delivery failed
            and ``strict_logging`` is off.

        Raises:
            TypeError, ValueError: If an argument is invalid. Nothing is
                delivered, and health isn't counted.
        """
        check_is_mapping(metadata)
        event = self._new_record(
            event_name,
            event_type=event_type,
            status=status,
            severity=severity,
            event_id=event_id if event_id is not None else str(uuid4()),
            parent_event_id=(
                parent_event_id if parent_event_id is not None else self._active_event_id.get()
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
        """Record a number you already have, as an event with type ``"metric"``.

        The logger only stores the value. It never calculates anything.

        Args:
            metric_name: The metric name, such as ``"rows_loaded"``.
            metric_value: A finite number.
            event_name: Defaults to ``"metric.<metric_name>"``.
            metadata: Extra key/value data for this event.

        Returns:
            The same as ``record_event``.
        """
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
    ) -> AbstractContextManager[EventScope]:
        """Time a block of code and emit one event when it ends.

        The block receives an ``EventScope``, where you can set results such
        as ``row_count``, ``status``, and ``metadata``. When the block exits:

        - Normal exit: the event has the status you set (default ``"success"``).
        - Exception: the event has status ``"failed"`` and error details, and
          the original exception propagates unchanged.

        Events recorded inside the block get this event as their parent.

        Example::

            with logger.event("positions.load", target_table="main.sales.positions") as scope:
                df.write.saveAsTable("main.sales.positions")
                scope.metadata["mode"] = "append"

        Rules:
            - Use each returned object in only one ``with`` statement.
            - Enter and exit the block in the same thread or async task.
              ``await`` inside the block is fine.
            - Never keep a block open across a generator's ``yield``. Put the
              block around the loop that consumes the generator instead.

        Raises:
            TypeError, ValueError: Right away, before the block runs, if an
                argument is invalid.
        """
        # Build the record now, so invalid arguments fail before the block runs.
        template = self._new_record(
            event_name,
            event_type=event_type,
            status=status,
            severity=severity,
            source_table=source_table,
            target_table=target_table,
            row_count=row_count,
        )
        scope = EventScope(
            _template=template,
            # A snapshot, so the caller's later changes to their dict don't leak in.
            metadata=copy_metadata(metadata),
            row_count=row_count,
            status=status,
            severity=severity,
            source_table=source_table,
            target_table=target_table,
        )
        return self._run_scope(scope)

    def logged_event(
        self,
        event_name: str,
        *,
        event_type: str = "function",
        metadata: Mapping[str, Any] | None = None,
    ) -> Callable[[F], F]:
        """Return a decorator that records one event for every call of a function.

        Works on both normal and ``async`` functions. The return value and
        any exception pass through unchanged.

        Example::

            @logger.logged_event("positions.total")
            def total_amount(rows):
                return sum(row["amount"] for row in rows)

        Raises:
            TypeError, ValueError: Right away if an argument is invalid.
            TypeError: When decorating a generator function. See ``event()``.
        """
        # Check the arguments now, before the decorated function can ever run.
        self._new_record(event_name, event_type=event_type)
        snapshot = copy_metadata(metadata)

        def decorate(func: F) -> F:
            # event() copies the snapshot on each call, so calls don't share edits.
            return wrap_in_scope(
                func, lambda: self.event(event_name, event_type=event_type, metadata=snapshot)
            )

        return decorate

    def run_task(
        self,
        event_name: str,
        func: Callable[..., T],
        *args: Any,
        metadata: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> T:
        """Call ``func(*args, **kwargs)`` and record it as an event with type ``"task"``.

        Returns whatever ``func`` returns. For an ``async`` function this is a
        coroutine, so ``await`` it.
        """
        decorator = self.logged_event(event_name, event_type="task", metadata=metadata)
        return decorator(func)(*args, **kwargs)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @contextmanager
    def _run_scope(self, scope: EventScope) -> Iterator[EventScope]:
        """Run one ``event(...)`` block: track the parent, time it, and deliver on exit."""
        event = replace(
            scope._template, parent_event_id=self._active_event_id.get(), start_ts=utc_now()
        )
        started_ms = monotonic_ms()
        # Make this scope the parent of anything recorded inside the block.
        token = self._active_event_id.set(event.event_id)
        business_error: BaseException | None = None
        try:
            yield scope
        except BaseException as error:
            business_error = error
            raise
        finally:
            # Restore the outer parent first, so it's correct even if delivery raises.
            self._active_event_id.reset(token)
            ended_at = utc_now()
            self._deliver(
                event,
                scope.metadata,
                business_error=business_error,
                updates={
                    **scope._result_fields(),
                    "end_ts": ended_at,
                    "event_ts": ended_at,
                    "duration_ms": elapsed_ms(started_ms),
                },
            )

    def _new_record(self, event_name: str, **fields: Any) -> EventRecord:
        """Build an ``EventRecord`` with this logger's identity. Raises if a field is invalid."""
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
        updates: dict[str, Any] | None = None,
        business_error: BaseException | None = None,
    ) -> EventRecord | None:
        """Finish ``event``, send it to the sink, and apply the failure rule.

        Args:
            event: The event so far.
            metadata: Per-event metadata, merged over the default metadata.
            updates: Final field values, such as timing and scope results.
            business_error: The exception the application raised, if any. If
                set, the event is marked failed and delivery never raises.

        Returns:
            The delivered record, or ``None`` if delivery failed without raising.
        """
        self._health.record_attempt()
        try:
            final_event = self._finish_record(event, metadata, updates, business_error)
            self.sink.emit(final_event)
        except BaseException as delivery_error:
            self._health.record_failure(delivery_error)
            if self._should_raise(delivery_error, business_error):
                raise
            # stacklevel=2 points the warning at the code that called _deliver.
            warn_safely(
                f"Event delivery failed ({type(delivery_error).__name__}); inspect logger.health.",
                stacklevel=2,
            )
            return None
        self._health.record_success()
        return final_event

    def _finish_record(
        self,
        event: EventRecord,
        metadata: Mapping[str, Any] | None,
        updates: dict[str, Any] | None,
        business_error: BaseException | None,
    ) -> EventRecord:
        """Apply updates, error details, and metadata. Raises if anything is invalid."""
        fields = dict(updates or {})
        if business_error is not None:
            # Error details come last, so a failure always overrides an edited status.
            fields.update(describe_exception(
                business_error,
                capture_frames=self.capture_error_frames,
                message_max_chars=self.error_message_max_chars,
            ))
        merged_metadata = {**self._default_metadata, **(metadata if metadata is not None else {})}
        return replace(event, **fields, metadata_json=self._serialize_metadata(merged_metadata))

    def _should_raise(
        self, delivery_error: BaseException, business_error: BaseException | None
    ) -> bool:
        """Decide whether a delivery failure should be raised to the caller."""
        if business_error is not None:
            # The application's exception must win. The scope re-raises it after this.
            return False
        if not isinstance(delivery_error, Exception):
            return True  # KeyboardInterrupt, SystemExit, and similar always propagate.
        return self.strict_logging

    def _serialize_metadata(self, metadata: Mapping[str, Any]) -> str | None:
        """Serialize metadata using this logger's settings."""
        return serialize_metadata(
            metadata,
            redact_keys=self.redact_keys,
            string_max_chars=self.metadata_string_max_chars,
            max_bytes=self.metadata_max_bytes,
        )


def _check_error_message_limit(value: Any) -> None:
    """Raise ``ValueError`` unless ``value`` is ``None`` or an ``int`` of 0 or more."""
    if value is not None and (type(value) is not int or value < 0):
        raise ValueError("error_message_max_chars must be a nonnegative integer or None.")
