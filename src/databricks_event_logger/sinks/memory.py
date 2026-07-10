"""
In-memory sink for Databricks-hosted unit tests.

The memory sink is intentionally tiny. Tests can assert against emitted
``EventRecord`` objects without needing a real Delta table.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from databricks_event_logger.event import EventRecord


@dataclass
class MemorySink:
    """
    Store emitted events in a list.

    Attributes
    ----------
    events : list[EventRecord]
        Events emitted through this sink, in emission order.
    """

    events: list[EventRecord] = field(default_factory=list)

    def emit(self, event: EventRecord) -> None:
        """
        Store one event in memory.

        Parameters
        ----------
        event : EventRecord
            Event to append.
        """
        self.events.append(event)
