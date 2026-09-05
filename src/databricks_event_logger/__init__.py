"""Structured events and editable operation scopes with explicit delivery."""

from databricks_event_logger.context import RuntimeContext
from databricks_event_logger.event import EventRecord
from databricks_event_logger.events import (
    EventSeverity,
    EventStatus,
    EventType,
)
from databricks_event_logger.logger import (
    DeliveryHealth,
    EventLogger,
    EventScope,
    get_default_logger,
    observed,
    use_logger,
)
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
    "EventType",
    "MemorySink",
    "RuntimeContext",
    "__version__",
    "create_table_sql",
    "get_default_logger",
    "observed",
    "use_logger",
]
