"""
Timing helpers.

The package records wall-clock timestamps for persisted event records and uses
``time.perf_counter`` for duration measurement. Wall-clock time is useful for
dashboard filtering. Monotonic timing is safer for duration because it is not
affected by system clock adjustments.
"""

from __future__ import annotations

from datetime import datetime, timezone
from time import perf_counter


def utc_now() -> datetime:
    """
    Return the current timezone-aware UTC timestamp.

    Returns
    -------
    datetime
        Current UTC time with ``tzinfo`` populated.
    """
    return datetime.now(timezone.utc)


def monotonic_ms() -> float:
    """
    Return a monotonic timestamp in milliseconds.

    Returns
    -------
    float
        Monotonic process timer value in milliseconds.
    """
    return perf_counter() * 1000.0


def elapsed_ms(start_ms: float, end_ms: float | None = None) -> int:
    """
    Return elapsed milliseconds from a monotonic start value.

    Parameters
    ----------
    start_ms : float
        Start timestamp from ``monotonic_ms``.
    end_ms : float | None, default None
        Optional end timestamp. When omitted, the current monotonic timestamp is
        used.

    Returns
    -------
    int
        Non-negative elapsed milliseconds rounded to the nearest integer.
    """
    measured_end_ms = monotonic_ms() if end_ms is None else end_ms
    return max(0, int(round(measured_end_ms - start_ms)))
