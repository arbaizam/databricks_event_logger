"""
Supported statuses and severities.

These constants reduce typos at notebook and package call sites. Event names
and event types are caller-defined strings.
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
