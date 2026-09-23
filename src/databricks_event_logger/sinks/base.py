"""``EventSink``: the interface every sink implements.

A sink is any object with an ``emit(event)`` method. It doesn't need to
inherit from ``EventSink``. The class below only documents the interface
for type checkers.

To write a new sink:

1. Create a class with ``emit(self, event: EventRecord) -> None``.
2. In ``emit``, store or send the event. Use ``event.as_dict()`` for a flat
   row with Python values, or ``event.as_json_dict()`` for JSON-ready values.
3. Raise an exception if delivery fails. Don't catch it yourself. The logger
   counts the failure in ``logger.health`` and applies its failure rule.
4. Keep ``emit`` synchronous. The logger calls it once per event and waits
   for it to return.
"""

from __future__ import annotations

from typing import Protocol

from databricks_event_logger.record import EventRecord


class EventSink(Protocol):
    """Anything with an ``emit(event)`` method can be used as a sink."""

    def emit(self, event: EventRecord) -> None:
        """Deliver one record synchronously; return None.

        Args:
            event: Finished EventRecord; use as_dict or as_json_dict for storage.

        Raises:
            Exception: The sink's own delivery error. EventLogger counts it
                and applies its failure policy; the sink should not retry.
        """
