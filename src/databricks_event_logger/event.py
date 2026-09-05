"""Validated, immutable event snapshots."""

from __future__ import annotations

import math
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timezone
from enum import Enum
from numbers import Integral, Real
from typing import Any
from uuid import uuid4

from databricks_event_logger.context import RuntimeContext
from databricks_event_logger.events import EventSeverity, EventStatus
from databricks_event_logger.timing import utc_now
from databricks_event_logger.version import __version__

VALID_STATUSES = frozenset(item.value for item in EventStatus)
VALID_SEVERITIES = frozenset(item.value for item in EventSeverity)
MAX_EVENT_NAME_CHARS = 255
MAX_EVENT_TYPE_CHARS = 100
MAX_INT64 = (1 << 63) - 1


@dataclass(frozen=True)
class EventRecord:
    """One event, ready for delivery; metadata is already normalized JSON.

    Timestamps must include a timezone and are stored in UTC. ``event_date``
    is derived from that UTC timestamp. ``as_dict()`` flattens runtime context
    for storage while Python callers access it through ``event.context``.
    """

    event_name: str
    event_type: str = "custom"
    status: str = "success"
    event_id: str = field(default_factory=lambda: str(uuid4()))
    correlation_id: str | None = None
    parent_event_id: str | None = None
    event_ts: datetime = field(default_factory=utc_now)
    event_date: date = field(init=False)
    start_ts: datetime | None = None
    end_ts: datetime | None = None
    duration_ms: int | None = None
    severity: str | None = None
    app_name: str | None = None
    component: str | None = None
    environment: str | None = None
    sdk_version: str = __version__
    context: RuntimeContext = field(default_factory=RuntimeContext)
    source_table: str | None = None
    target_table: str | None = None
    row_count: int | None = None
    metric_name: str | None = None
    metric_value: float | None = None
    error_class: str | None = None
    error_message: str | None = None
    stack_trace_hash: str | None = None
    error_frames_json: str | None = None
    metadata_json: str | None = None
    created_at: datetime = field(default_factory=utc_now)

    def __post_init__(self) -> None:
        for name in ("event_name", "event_type", "status", "severity"):
            value = getattr(self, name)
            if isinstance(value, Enum):
                object.__setattr__(self, name, value.value)

        for name, maximum in (
            ("event_name", MAX_EVENT_NAME_CHARS),
            ("event_type", MAX_EVENT_TYPE_CHARS),
            ("event_id", None),
            ("sdk_version", None),
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty string.")
            if maximum is not None and len(value) > maximum:
                raise ValueError(f"{name} must be {maximum} characters or fewer.")
        for name, choices in (("status", VALID_STATUSES), ("severity", VALID_SEVERITIES)):
            value = getattr(self, name)
            if name == "severity" and value is None:
                continue
            if not isinstance(value, str) or value not in choices:
                raise ValueError(f"{name} must be one of: {', '.join(sorted(choices))}.")

        for name in (
            "correlation_id", "parent_event_id", "app_name", "component", "environment",
            "source_table", "target_table", "metric_name", "error_class", "error_message",
            "stack_trace_hash", "error_frames_json", "metadata_json",
        ):
            value = getattr(self, name)
            if value is not None and not isinstance(value, str):
                raise ValueError(f"{name} must be a string or None.")
        if not isinstance(self.context, RuntimeContext):
            raise ValueError("context must be a RuntimeContext.")

        for name in ("event_ts", "start_ts", "end_ts", "created_at"):
            value = getattr(self, name)
            if value is None and name in {"start_ts", "end_ts"}:
                continue
            if not isinstance(value, datetime) or value.utcoffset() is None:
                raise ValueError(f"{name} must be a timezone-aware datetime.")
            object.__setattr__(self, name, value.astimezone(timezone.utc))
        object.__setattr__(self, "event_date", self.event_ts.date())

        for name in ("row_count", "duration_ms"):
            value = getattr(self, name)
            if value is None:
                continue
            if isinstance(value, bool) or not isinstance(value, Integral):
                raise ValueError(f"{name} must be an integer from 0 through {MAX_INT64}.")
            value = int(value)
            if not 0 <= value <= MAX_INT64:
                raise ValueError(f"{name} must be an integer from 0 through {MAX_INT64}.")
            object.__setattr__(self, name, value)
        if self.metric_value is not None:
            if isinstance(self.metric_value, bool) or not isinstance(self.metric_value, Real):
                raise ValueError("metric_value must be a finite number.")
            try:
                metric = float(self.metric_value)
            except OverflowError as exc:
                raise ValueError("metric_value must be a finite number.") from exc
            if not math.isfinite(metric):
                raise ValueError("metric_value must be a finite number.")
            object.__setattr__(self, "metric_value", metric)

    def as_dict(self) -> dict[str, Any]:
        """Return the flat persistence row without copying arbitrary objects."""
        row = {
            item.name: getattr(self, item.name) for item in fields(self) if item.name != "context"
        }
        row.update(self.context.as_dict())
        return row

    def as_json_dict(self) -> dict[str, Any]:
        """Return the flat row with ISO-formatted dates and timestamps."""
        return {
            key: value.isoformat() if isinstance(value, datetime | date) else value
            for key, value in self.as_dict().items()
        }
