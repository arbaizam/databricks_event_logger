"""``EventScope``: the results of a running operation, which the caller can edit.

``logger.event(...)`` gives you an ``EventScope``. Inside the ``with`` block,
you set fields such as ``row_count`` or ``metadata`` as results come in.
When the block exits, the logger reads these fields once and emits a single
event. The timing and lifecycle code lives in ``EventLogger._run_scope``.

To make another event field editable, add it here and to ``_result_fields()``.
Then add a matching parameter to ``EventLogger.event()``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from databricks_event_logger.record import EventRecord


@dataclass
class EventScope:
    """The editable results of one operation. Emitted as one event when the block exits.

    Attributes:
        metadata: Extra key/value data. Add keys while the block runs.
        row_count: The number of rows the operation handled, if known.
        status: The outcome. Defaults to ``"success"``. If the block raises an
            exception, the status is always ``"failed"``.
        severity: How important the event is, or ``None``.
        source_table: The table the operation read from.
        target_table: The table the operation wrote to.

    Example::

        with logger.event("positions.validate") as scope:
            scope.row_count = len(rows)
            if not rows:
                scope.status = "warning"
    """

    # The fixed parts of the event, checked when the scope was created.
    _template: EventRecord = field(repr=False)
    metadata: dict[str, Any] = field(default_factory=dict)
    row_count: int | None = None
    status: str = "success"
    severity: str | None = None
    source_table: str | None = None
    target_table: str | None = None

    @property
    def event_id(self) -> str:
        """The ID the final event will have. It's available before the event is emitted."""
        return self._template.event_id

    def _result_fields(self) -> dict[str, Any]:
        """Return the editable ``EventRecord`` fields. The logger reads these on exit.

        ``metadata`` isn't included, because it's merged and serialized separately.
        """
        return {
            "row_count": self.row_count,
            "status": self.status,
            "severity": self.severity,
            "source_table": self.source_table,
            "target_table": self.target_table,
        }
