"""``EventScope``: the results of a running operation, which the caller can edit.

``logger.event(...)`` gives you an ``EventScope``. Inside the ``with`` block,
you set fields such as ``row_count`` or ``metadata`` as results come in.
When the block exits, the logger reads these fields once and emits a single
event. The timing and lifecycle code lives in ``EventLogger._run_scope``.

All public fields except ``metadata`` are event results. ``_result_fields``
reads that list from this dataclass, so there is no second list to maintain.
See CONTRIBUTING.md for adding a field and wiring its initial value.
"""

from __future__ import annotations

from dataclasses import dataclass, field, fields
from typing import Any

from databricks_event_logger.record import EventRecord


@dataclass
class EventScope:
    """The editable results of one operation. Emitted as one event when the block exits.

    Obtain scopes from EventLogger.event(). Construction only stores values;
    the logger validates initial values before entry and edited values at
    delivery. The internal _event argument is the validated fixed record.

    Attributes:
        metadata: Extra key/value data. Add keys while the block runs.
        row_count: The number of rows the operation handled, if known.
        status: The outcome. Defaults to ``"success"``. If the block raises an
            exception, the status is always ``"failed"``.
        severity: How important the event is, or ``None``.
        source_table: The table the operation read from.
        target_table: The table the operation wrote to.
        event_id: Read-only ID taken from the fixed record.

    Example::

        with logger.event("positions.validate") as scope:
            scope.row_count = len(rows)
            if not rows:
                scope.status = "warning"
    """

    # The fixed parts of the event, checked when the scope was created.
    # Keep the original name for existing constructors and serialized scopes.
    _event: EventRecord = field(repr=False)
    metadata: dict[str, Any] = field(default_factory=dict)
    row_count: int | None = None
    status: str = "success"
    severity: str | None = None
    source_table: str | None = None
    target_table: str | None = None

    @property
    def event_id(self) -> str:
        """The ID the final event will have. It's available before the event is emitted."""
        return self._event.event_id

    def __setstate__(self, state: dict[str, Any]) -> None:
        """Load scopes saved with either internal template-field name."""
        state = dict(state)
        if "_template" in state:
            state["_event"] = state.pop("_template")
        self.__dict__.update(state)

    def _result_fields(self) -> dict[str, Any]:
        """Return the editable ``EventRecord`` fields. The logger reads these on exit.

        ``metadata`` isn't included, because it's merged and serialized separately.
        """
        return {
            item.name: getattr(self, item.name)
            for item in fields(self)
            if not item.name.startswith("_") and item.name != "metadata"
        }
