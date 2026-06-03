"""
Console sink for debugging.

Console output is useful in notebooks and during Databricks-hosted tests when a
developer wants to inspect emitted events without writing to Delta.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from typing import TextIO

from databricks_event_logger.event import EventRecord


@dataclass
class ConsoleSink:
    """
    Print each event as one JSON line.

    Parameters
    ----------
    stream : TextIO | None, default None
        Output stream. Defaults to ``sys.stdout`` at emit time.
    """

    stream: TextIO | None = None

    def emit(self, event: EventRecord) -> None:
        """
        Print one event as JSON.

        Parameters
        ----------
        event : EventRecord
            Event to print.
        """
        target = self.stream or sys.stdout
        print(json.dumps(event.as_json_dict(), sort_keys=True), file=target)

    def flush(self) -> None:
        """
        Flush the configured stream.
        """
        target = self.stream or sys.stdout
        target.flush()

    def close(self) -> None:
        """
        Close the sink.

        Notes
        -----
        The console sink does not close caller-owned streams.
        """
