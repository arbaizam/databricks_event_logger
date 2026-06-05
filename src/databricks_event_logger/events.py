"""
Common event names, types, statuses, and severities.

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

    NOTEBOOK = "notebook"
    TASK = "task"
    FUNCTION = "function"
    DELTA_READ = "delta_read"
    DELTA_WRITE = "delta_write"
    SPARK_ACTION = "spark_action"
    SQL = "sql"
    VALIDATION = "validation"
    BUSINESS_PROCESS = "business_process"
    METRIC = "metric"
    CUSTOM = "custom"


class CommonEvent(str, Enum):
    """
    Common SDK-level event names.

    These are stable names callers can pass to ``record_event``. They are not
    all emitted automatically; for example, ``NOTEBOOK_COMPLETED`` is available
    for explicit caller use while ``observe_notebook`` emits startup only.
    """

    NOTEBOOK_STARTED = "notebook.started"
    NOTEBOOK_COMPLETED = "notebook.completed"
    DELTA_READ = "delta.read"
    DELTA_WRITE = "delta.write"
    SQL_EXECUTE = "sql.execute"
    VALIDATION_ROW_COUNT = "validation.row_count"
    VALIDATION_TABLE_EXISTS = "validation.table_exists"
    SPARK_COUNT = "spark.count"
