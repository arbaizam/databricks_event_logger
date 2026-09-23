"""Counters that show whether events are reaching the sink.

``EventLogger.health`` returns a ``DeliveryHealth`` snapshot. A logger and all
loggers created from it with ``bind()`` share one ``DeliveryTracker``, so the
counts cover all of them.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from threading import Lock

from databricks_event_logger.diagnostics import safe_text

MAX_LAST_ERROR_CHARS = 500


@dataclass(frozen=True)
class DeliveryHealth:
    """A read-only snapshot of delivery counts.

    Each delivery counts as one attempt; setup validation does not. An attempt
    then either succeeds or fails. During delivery, it fails if the final record
    couldn't be built (for example, because of
    invalid metadata) or if the sink raised an error.

    Attributes:
        attempted: Events the logger tried to deliver.
        succeeded: Events the sink accepted.
        failed: Events that could not be built or delivered.
        last_error: Text of the most recent failure, such as
            ``"RuntimeError: table not found"``. Later successes don't clear it.
    """

    attempted: int = 0
    succeeded: int = 0
    failed: int = 0
    last_error: str | None = None


class DeliveryTracker:
    """Internal counter owner shared by bound loggers; starts with zero counts.

    Call record_attempt before delivery, then record_success or record_failure.
    Each update is locked separately, so a snapshot can include in-flight attempts.
    Consumers should read EventLogger.health rather than update this tracker.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._health = DeliveryHealth()

    def snapshot(self) -> DeliveryHealth:
        """Return the current counts. The returned object never changes."""
        with self._lock:
            return self._health

    def record_attempt(self) -> None:
        """Increment attempted before preparing a final event; return None."""
        with self._lock:
            self._health = replace(self._health, attempted=self._health.attempted + 1)

    def record_success(self) -> None:
        """Increment succeeded after the sink returns; leave last_error unchanged."""
        with self._lock:
            self._health = replace(self._health, succeeded=self._health.succeeded + 1)

    def record_failure(self, error: BaseException) -> None:
        """Increment failed and retain diagnostic text for error; return None.

        Args:
            error: Preparation or sink failure. Its message is bounded to 500
                characters, preceded by the exception class name. A broken
                __str__ is replaced by an unprintable marker.
        """
        # Build the text before taking the lock, because str(error) runs user code.
        message = f"{type(error).__name__}: {safe_text(error, max_chars=MAX_LAST_ERROR_CHARS)}"
        with self._lock:
            self._health = replace(
                self._health, failed=self._health.failed + 1, last_error=message
            )
