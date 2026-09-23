"""The mutable state deliberately shared by a logger and its bindings.

Delivery counts are protected by their own lock. The active parent is local
to each execution context; sharing the ContextVar does not share its value
between independent threads. Configuration values stay on EventLogger.
"""

from contextvars import ContextVar
from dataclasses import dataclass, field

from databricks_event_logger.health import DeliveryTracker


@dataclass
class _LoggerState:
    """One logger family's delivery tracker and execution-local parent slot."""

    health: DeliveryTracker = field(default_factory=DeliveryTracker)
    active_event_id: ContextVar[str | None] = field(
        default_factory=lambda: ContextVar("event_parent", default=None)
    )
