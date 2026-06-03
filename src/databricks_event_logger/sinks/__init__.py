"""Event sink implementations."""

from databricks_event_logger.sinks.base import EventSink
from databricks_event_logger.sinks.console import ConsoleSink
from databricks_event_logger.sinks.delta import DeltaSink
from databricks_event_logger.sinks.memory import MemorySink

__all__ = ["ConsoleSink", "DeltaSink", "EventSink", "MemorySink"]
