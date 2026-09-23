"""Record structured events from Python code and Databricks notebooks.

Start with ``EventLogger``. Everything most users need can be imported from
here::

    from databricks_event_logger import EventLogger, MemorySink

Package layout (each module does one job):

======================  ====================================================
``logger.py``           ``EventLogger``: the public API and delivery rules.
``scope.py``            ``EventScope``: editable results inside ``event()``.
``decorators.py``       Wraps functions for ``logged_event`` / ``run_task``.
``record.py``           ``EventRecord``: one checked, immutable event.
``enums.py``            ``EventStatus`` and ``EventSeverity`` values.
``context.py``          ``RuntimeContext``: job, task, and workspace IDs.
``schema.py``           The storage columns, the single source of truth.
``metadata.py``         Turns metadata into small, redacted JSON.
``failures.py``         Describes an exception as event fields.
``health.py``           ``DeliveryHealth`` counters.
``diagnostics.py``      Safe text and warnings that never raise.
``timing.py``           UTC timestamps and monotonic durations.
``errors.py``           Package exceptions.
``sinks/``              Where events go: console, memory, Delta.
``databricks/``         Notebook-only helpers, such as ``resolve_context``.
======================  ====================================================
"""

from databricks_event_logger.context import RuntimeContext
from databricks_event_logger.enums import EventSeverity, EventStatus
from databricks_event_logger.health import DeliveryHealth
from databricks_event_logger.logger import EventLogger
from databricks_event_logger.record import EventRecord
from databricks_event_logger.scope import EventScope
from databricks_event_logger.sinks.base import EventSink
from databricks_event_logger.sinks.console import ConsoleSink
from databricks_event_logger.sinks.delta import DeltaSink, create_table_sql
from databricks_event_logger.sinks.memory import MemorySink
from databricks_event_logger.version import __version__

__all__ = [
    "ConsoleSink",
    "DeliveryHealth",
    "DeltaSink",
    "EventLogger",
    "EventRecord",
    "EventScope",
    "EventSeverity",
    "EventSink",
    "EventStatus",
    "MemorySink",
    "RuntimeContext",
    "__version__",
    "create_table_sql",
]
