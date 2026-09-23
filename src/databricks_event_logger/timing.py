"""Clock helpers.

Events use two different clocks:

- Wall-clock UTC time (``utc_now``) for timestamps that people and queries read.
- A monotonic timer (``monotonic_ms``) for durations. The system clock can
  jump, for example after a time sync, but this timer only moves forward, so
  durations are always correct.
"""

from __future__ import annotations

from datetime import datetime, timezone
from time import perf_counter


def utc_now() -> datetime:
    """Return the current time as a timezone-aware UTC ``datetime``."""
    return datetime.now(timezone.utc)


def monotonic_ms() -> float:
    """Return a monotonic timer reading in milliseconds.

    The value only makes sense when compared with another reading. Pass it to
    ``elapsed_ms`` to get a duration.
    """
    return perf_counter() * 1000.0


def elapsed_ms(start_ms: float, end_ms: float | None = None) -> int:
    """Return the whole milliseconds between two ``monotonic_ms`` readings.

    Args:
        start_ms: The start reading.
        end_ms: The end reading. Defaults to now.

    Returns:
        The rounded duration. It is never negative.
    """
    if end_ms is None:
        end_ms = monotonic_ms()
    return max(0, int(round(end_ms - start_ms)))
