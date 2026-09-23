"""Sinks: where events go after the logger builds them.

- ``ConsoleSink``: prints each event as a JSON line. This is the default.
- ``MemorySink``: keeps events in a list. Useful for tests.
- ``DeltaSink``: inserts each event into an existing Delta table.

To add your own sink, see ``EventSink`` in ``base.py``.
"""

from databricks_event_logger.sinks.base import EventSink
from databricks_event_logger.sinks.console import ConsoleSink
from databricks_event_logger.sinks.delta import DeltaSink, create_table_sql
from databricks_event_logger.sinks.memory import MemorySink

__all__ = ["ConsoleSink", "DeltaSink", "EventSink", "MemorySink", "create_table_sql"]
