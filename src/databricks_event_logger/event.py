"""Compatibility imports for event records.

Existing imports and pickles keep working. New code can use the package-level
``EventRecord`` import; validation lives in ``record.py``.
"""

from databricks_event_logger.enums import SEVERITY_VALUES as VALID_SEVERITIES
from databricks_event_logger.enums import STATUS_VALUES as VALID_STATUSES
from databricks_event_logger.enums import EventSeverity, EventStatus
from databricks_event_logger.record import (
    MAX_EVENT_NAME_CHARS,
    MAX_EVENT_TYPE_CHARS,
    MAX_INT64,
    EventRecord,
)

__all__ = [
    "EventRecord",
    "EventSeverity",
    "EventStatus",
    "MAX_EVENT_NAME_CHARS",
    "MAX_EVENT_TYPE_CHARS",
    "MAX_INT64",
    "VALID_SEVERITIES",
    "VALID_STATUSES",
]
