"""
Base sink protocol.

Sinks keep persistence concerns out of ``EventLogger``. The protocol is small:
emit one event, optionally flush, optionally close. This leaves room for future
buffering without changing the logger API, while v1 sinks remain immediate and
simple.
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

    def flush(self) -> None:
        """
        Flush pending events.

        Notes
        -----
        Immediate-write sinks may implement this as a no-op.
        """

    def close(self) -> None:
        """
        Release sink resources.

        Notes
        -----
        Most v1 sinks do not own external resources, so this may be a no-op.
        """
