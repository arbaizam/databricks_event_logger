"""``MemorySink``: keep events in a list, for tests."""

from __future__ import annotations

from dataclasses import dataclass, field

from databricks_event_logger.record import EventRecord


@dataclass
class MemorySink:
    """Store each event in ``events``, in the order it was emitted.

    Attributes:
        events: Mutable list of received records. A fresh list is created by
            default. Records are stored by reference, without copying.

    Example::

        sink = MemorySink()
        logger = EventLogger(sink=sink)
        logger.record_event("done")
        assert sink.events[0].event_name == "done"
    """

    events: list[EventRecord] = field(default_factory=list)

    def emit(self, event: EventRecord) -> None:
        """Append the supplied EventRecord to events; return None."""
        self.events.append(event)
