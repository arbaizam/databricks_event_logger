"""
Suggested event types and supported statuses and severities.

The package still accepts plain strings so teams can define domain-specific
event names and event types. These constants cover common SDK values and reduce
typos at notebook and package call sites.
"""

from __future__ import annotations

from enum import Enum


class EventStatus(str, Enum):
    """
    Supported event outcome values.

    ``WARNING`` means the operation outcome itself should be treated as a
    warning, for example a validation that found a non-blocking issue.
    """

    STARTED = "started"
    SUCCESS = "success"
    FAILED = "failed"
    WARNING = "warning"
    SKIPPED = "skipped"


class EventSeverity(str, Enum):
    """
    Supported event severity values.

    ``WARNING`` means the event should be displayed at warning severity, even
    when the status/outcome uses a different value.
    """

    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class EventType(str, Enum):
    """
    Common event type values.

    Event types remain caller-configurable; these constants are the recommended
    shared vocabulary for common SDK and notebook operations.
    """

    TASK = "task"
    FUNCTION = "function"
    VALIDATION = "validation"
    METRIC = "metric"
    CUSTOM = "custom"
