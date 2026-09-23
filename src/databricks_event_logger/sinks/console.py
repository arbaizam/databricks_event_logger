"""``ConsoleSink``: print events as JSON, for debugging and notebooks."""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from typing import TextIO

from databricks_event_logger.record import EventRecord


@dataclass
class ConsoleSink:
    """Print each event as one line of JSON.

    Attributes:
        stream: Where to write. Defaults to ``sys.stdout``, looked up at each
            write, so output still goes to the right place if the notebook or
            a test replaces ``sys.stdout``.
    """

    stream: TextIO | None = None

    def emit(self, event: EventRecord) -> None:
        """Print one JSON line with sorted keys; return None.

        Args:
            event: Record whose dates/timestamps are converted to ISO strings.

        Raises:
            Exception: Serialization or stream-write errors propagate to the logger.
        """
        target = self.stream or sys.stdout
        print(json.dumps(event.as_json_dict(), sort_keys=True), file=target)
