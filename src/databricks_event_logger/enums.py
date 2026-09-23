"""The allowed values for an event's ``status`` and ``severity``.

Using these enums instead of plain strings helps avoid typos. Plain strings
are also accepted, as long as they match one of the values below. Event names
and event types are free-form strings chosen by the caller.

To add a new value, add a member here. ``EventRecord`` accepts it right away.
"""

from __future__ import annotations

from enum import Enum


class EventStatus(str, Enum):
    """The outcome of the operation an event describes.

    ``WARNING`` means the operation finished but found a problem that did not
    stop it, for example a validation that found a few bad rows.
    """

    STARTED = "started"
    SUCCESS = "success"
    FAILED = "failed"
    WARNING = "warning"
    SKIPPED = "skipped"


class EventSeverity(str, Enum):
    """How important an event is when someone reads the log.

    Severity is separate from status. For example, a ``success`` event can
    still have ``warning`` severity if it needs attention.
    """

    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


STATUS_VALUES = frozenset(item.value for item in EventStatus)
SEVERITY_VALUES = frozenset(item.value for item in EventSeverity)
