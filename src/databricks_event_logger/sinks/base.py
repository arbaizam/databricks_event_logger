"""
Base sink protocol.

Sinks keep output concerns out of ``EventLogger``. The protocol has one job:
emit an event.
"""

from __future__ import annotations

from typing import Protocol

from databricks_event_logger.event import EventRecord


class EventSink(Protocol):
    """
    Persistence interface for event sinks.
    """

    def emit(self, event: EventRecord) -> None:
        """
        Persist one event.

        Parameters
        ----------
        event : EventRecord
            Event record to persist.
        """
