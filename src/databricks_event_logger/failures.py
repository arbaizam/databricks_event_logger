"""Describe an exception using its message and a bounded list of code locations.

When an operation fails, its event gets:

- ``error_class``: the exception type name, such as ``"ValueError"``.
- ``error_message``: ``str(exception)``, shortened to a limit.
- ``stack_trace_hash``: a SHA-256 fingerprint of the exception type and the
  code locations it passed through. Failures from the same place share a
  hash, even when their messages differ.
- ``error_frames_json`` (optional): those code locations as JSON.

Only file names (not full paths), function names, and line numbers are
recorded. Source code and local variables are never captured.
Messages are free text, not redacted. Keep secrets out of exception messages.
Scope/decorator locations retain their legacy storage labels; see ``_trace_compat``.
"""

from __future__ import annotations

import hashlib
import json
from collections import deque
from pathlib import PurePath
from typing import Any

from databricks_event_logger._trace_compat import _legacy_frame
from databricks_event_logger.diagnostics import safe_text
from databricks_event_logger.enums import EventSeverity, EventStatus

MAX_FRAMES = 20  # Keep only the innermost frames, closest to where the error was raised.
MAX_FRAME_TEXT_CHARS = 200


def describe_exception(
    error: BaseException,
    *,
    capture_frames: bool,
    message_max_chars: int | None,
) -> dict[str, Any]:
    """Return the ``EventRecord`` fields that describe a failed operation.

    Args:
        error: The exception raised by the operation.
        capture_frames: If ``True``, also store the frame list in
            ``error_frames_json``.
        message_max_chars: The maximum length of ``error_message``, or
            ``None`` for no limit.

    Returns:
        A dict that sets ``status`` to ``"failed"``, ``severity`` to
        ``"error"``, and fills in the error fields.
    """
    error_class = type(error).__name__
    frames_json = json.dumps(_traceback_frames(error), separators=(",", ":"))
    fingerprint = hashlib.sha256(f"{error_class}:{frames_json}".encode()).hexdigest()
    return {
        "status": EventStatus.FAILED.value,
        "severity": EventSeverity.ERROR.value,
        "error_class": error_class,
        "error_message": safe_text(error, max_chars=message_max_chars),
        "stack_trace_hash": fingerprint,
        "error_frames_json": frames_json if capture_frames else None,
    }


def _traceback_frames(error: BaseException) -> list[dict[str, Any]]:
    """List the innermost ``MAX_FRAMES`` traceback frames as small dicts."""
    frames: deque[dict[str, Any]] = deque(maxlen=MAX_FRAMES)  # Drops the oldest frames.
    trace = error.__traceback__
    while trace is not None:
        code = trace.tb_frame.f_code
        frames.append(_legacy_frame(trace) or {
            "file": PurePath(code.co_filename).name[:MAX_FRAME_TEXT_CHARS],
            "function": code.co_name[:MAX_FRAME_TEXT_CHARS],
            "line": trace.tb_lineno,
        })
        trace = trace.tb_next
    return list(frames)
