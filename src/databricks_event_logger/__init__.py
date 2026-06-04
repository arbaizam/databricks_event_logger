"""Structured event logging helpers for Databricks workloads."""

from databricks_event_logger.decorators import observed
from databricks_event_logger.logger import (
    EventLogger,
    get_default_logger,
    observe_notebook,
    set_default_logger,
)
from databricks_event_logger.sinks.console import ConsoleSink
from databricks_event_logger.sinks.delta import DeltaSink
from databricks_event_logger.sinks.memory import MemorySink
from databricks_event_logger.version import __version__

__all__ = [
    "ConsoleSink",
    "DeltaSink",
    "EventLogger",
    "MemorySink",
    "__version__",
    "get_default_logger",
    "observe_notebook",
    "observed",
    "set_default_logger",
]
