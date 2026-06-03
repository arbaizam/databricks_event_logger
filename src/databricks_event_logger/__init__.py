"""Structured event logging helpers for Databricks workloads."""

from databricks_event_logger.decorators import observed
from databricks_event_logger.logger import (
    EventLogger,
    get_default_logger,
    observe_notebook,
    set_default_logger,
)
from databricks_event_logger.version import __version__

__all__ = [
    "EventLogger",
    "__version__",
    "get_default_logger",
    "observe_notebook",
    "observed",
    "set_default_logger",
]
