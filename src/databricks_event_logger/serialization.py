"""Normalize and redact metadata once before JSON encoding."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from numbers import Integral, Real
from pathlib import Path
from typing import Any

DEFAULT_REDACT_KEYS = (
    "password", "token", "secret", "credential", "api_key", "access_key", "private_key",
)
DEFAULT_METADATA_MAX_BYTES = 4000
DEFAULT_METADATA_STRING_MAX_CHARS = 2000
TRUNCATED_MARKER = "...[TRUNCATED]"
REDACTED_VALUE = "[REDACTED]"
DEPTH_LIMIT_VALUE = "[DEPTH_LIMIT]"
UNSUPPORTED_VALUE = "[UNSUPPORTED]"


def safe_text(value: Any, *, max_chars: int | None = 2000) -> str:
    """Get bounded diagnostic text, tolerating a broken ``__str__`` method.

    This is for error reporting, not metadata conversion or secret redaction.
    It never calls ``repr``. Metadata uses a fixed marker for unknown objects.
    """
    try:
        result = str(value)
    except BaseException:
        result = "[UNPRINTABLE]"
    return _truncate_string(result, max_chars)


def sanitize_metadata(
    metadata: Mapping[str, Any] | None,
    *,
    redact_keys: tuple[str, ...] = DEFAULT_REDACT_KEYS,
    string_max_chars: int | None = DEFAULT_METADATA_STRING_MAX_CHARS,
    max_depth: int = 8,
) -> dict[str, Any] | None:
    """Convert supported values while recursively applying key redaction.

    Supported extensions to JSON are dates, decimals, paths, enums, dataclass
    instances, named records, tuples, and real numeric scalars. Unknown objects,
    sets, malformed named records, and excessive depth become
    fixed markers. Mapping keys must be strings. Key redaction is a heuristic,
    so callers remain responsible for excluding secrets from free text.
    """
    if string_max_chars is not None and (
        isinstance(string_max_chars, bool)
        or not isinstance(string_max_chars, int)
        or string_max_chars < 0
    ):
        raise ValueError("string_max_chars must be a nonnegative integer or None.")
    if isinstance(max_depth, bool) or not isinstance(max_depth, int) or max_depth < 1:
        raise ValueError("max_depth must be a positive integer.")
    if not isinstance(redact_keys, tuple) or any(not isinstance(key, str) for key in redact_keys):
        raise ValueError("redact_keys must be a tuple of strings.")
    if metadata is None:
        return None
    if not isinstance(metadata, Mapping):
        raise TypeError("metadata must be a mapping with string keys.")
    redaction_terms = tuple(key.lower() for key in redact_keys if key)

    def normalize(value: Any, depth: int) -> Any:
        if depth >= max_depth:
            return DEPTH_LIMIT_VALUE
        # Enums and dataclasses can introduce new dictionaries containing
        # sensitive keys. Convert them before descending through the result.
        if isinstance(value, Enum):
            return normalize(value.value, depth + 1)
        if is_dataclass(value) and not isinstance(value, type):
            value = {item.name: getattr(value, item.name) for item in fields(value)}
        if isinstance(value, tuple) and (hasattr(value, "_fields") or hasattr(value, "__fields__")):
            # Named tuples and PySpark Rows carry keys that must survive redaction.
            names = getattr(value, "_fields", getattr(value, "__fields__", None))
            if (
                not isinstance(names, list | tuple)
                or len(names) != len(value)
                or any(not isinstance(name, str) for name in names)
                or len(set(names)) != len(names)
            ):
                return UNSUPPORTED_VALUE
            value = dict(zip(names, value, strict=True))
        if isinstance(value, Mapping):
            result = {}
            for key, child in value.items():
                if not isinstance(key, str):
                    raise TypeError("metadata keys must be strings.")
                result[key] = (
                    REDACTED_VALUE
                    if any(term in key.lower() for term in redaction_terms)
                    else normalize(child, depth + 1)
                )
            return result
        if isinstance(value, list | tuple):
            return [normalize(child, depth + 1) for child in value]
        if isinstance(value, datetime | date):
            value = value.isoformat()
        elif isinstance(value, Decimal | Path):
            value = str(value)
        if isinstance(value, str):
            return _truncate_string(value, string_max_chars)
        if value is None or isinstance(value, bool):
            return value
        if isinstance(value, Integral):
            return int(value)
        if isinstance(value, Real):
            value = float(value)
            return value if math.isfinite(value) else "[NONFINITE]"
        return UNSUPPORTED_VALUE

    return normalize(metadata, 0) or None


def serialize_metadata(
    metadata: Mapping[str, Any] | None,
    *,
    redact_keys: tuple[str, ...] = DEFAULT_REDACT_KEYS,
    string_max_chars: int | None = DEFAULT_METADATA_STRING_MAX_CHARS,
    max_bytes: int | None = DEFAULT_METADATA_MAX_BYTES,
    max_depth: int = 8,
) -> str | None:
    """Return deterministic sanitized JSON within the UTF-8 byte budget.

    Oversized payloads become a truncation summary, retaining a bounded preview
    when space permits. Budgets too small for a marker return ``{}``; the
    minimum supported budget is two bytes. Empty metadata returns ``None``.
    """
    if max_bytes is not None and (
        isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes < 2
    ):
        raise ValueError("max_bytes must be an integer of at least 2 or None.")
    normalized = sanitize_metadata(
        metadata, redact_keys=redact_keys, string_max_chars=string_max_chars, max_depth=max_depth,
    )
    if normalized is None:
        return None
    serialized = _encode(normalized)
    if max_bytes is None or len(serialized.encode("utf-8")) <= max_bytes:
        return serialized
    summary = {"_truncated": True, "_original_size_bytes": len(serialized.encode("utf-8"))}
    if len(_encode(summary).encode("utf-8")) > max_bytes:
        marker = _encode({"_truncated": True})
        return marker if len(marker.encode("utf-8")) <= max_bytes else "{}"
    # Binary search the preview length because JSON escaping changes its size.
    left, right = 0, min(len(serialized), max_bytes)
    bounded = _encode(summary)
    while left <= right:
        middle = (left + right) // 2
        candidate = _encode({**summary, "_preview": serialized[:middle]})
        if len(candidate.encode("utf-8")) <= max_bytes:
            bounded = candidate
            left = middle + 1
        else:
            right = middle - 1
    return bounded


def _encode(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=True, allow_nan=False, sort_keys=True, separators=(",", ":"),
    )


def _truncate_string(value: str, max_chars: int | None) -> str:
    if max_chars is None or len(value) <= max_chars:
        return value
    if max_chars <= len(TRUNCATED_MARKER):
        return TRUNCATED_MARKER[:max_chars]
    return value[:max_chars - len(TRUNCATED_MARKER)] + TRUNCATED_MARKER
