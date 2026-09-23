"""Turn caller metadata into small, safe JSON.

Metadata is the free-form ``dict`` that callers attach to events. Before
it's stored, it goes through two steps:

1. **Sanitize** (``sanitize_metadata``): walk the data and turn every value into a
   JSON-friendly one. Values under sensitive keys are replaced with
   ``REDACTED_VALUE``.
2. **Encode** (``serialize_metadata``): write compact JSON with sorted keys.
   If the result is bigger than the byte limit, a short summary is stored
   instead.

Supported values:

- Built-ins: ``dict``, ``list``, ``tuple``, ``str``, ``int``, ``float``,
  ``bool``, and ``None``.
- Dates and datetimes become ISO 8601 strings.
- ``Decimal`` and ``Path`` become strings.
- An enum becomes its value.
- Dataclasses, named tuples, and PySpark ``Row`` objects become dicts, so
  their field names can be redacted.
- NumPy integer and real-number scalars become ``int`` or ``float``.
  NumPy booleans and complex numbers are unsupported.

Anything else becomes ``UNSUPPORTED_VALUE``. Non-finite real numbers become
``NONFINITE_VALUE``. Data nested too deeply becomes ``DEPTH_LIMIT_VALUE``.
``repr()`` is never called, so object contents can't leak by accident.

To support a new type, add a branch to ``_Sanitizer.clean`` (for containers)
or ``_Sanitizer._clean_scalar`` (for single values).

Redaction only looks at key names. It can't find secrets hidden inside free
text, so callers should still keep secrets out of metadata.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass, fields, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from numbers import Integral, Real
from pathlib import Path
from typing import Any

from databricks_event_logger.diagnostics import truncate_text

DEFAULT_REDACT_KEYS = (
    "password", "token", "secret", "credential", "api_key", "access_key", "private_key",
)
DEFAULT_METADATA_MAX_BYTES = 4000
DEFAULT_METADATA_STRING_MAX_CHARS = 2000
DEFAULT_MAX_DEPTH = 8

REDACTED_VALUE = "[REDACTED]"
DEPTH_LIMIT_VALUE = "[DEPTH_LIMIT]"
UNSUPPORTED_VALUE = "[UNSUPPORTED]"
NONFINITE_VALUE = "[NONFINITE]"


def copy_metadata(metadata: Mapping[str, Any] | None) -> dict[str, Any]:
    """Return a shallow copy of ``metadata`` as a new ``dict``.

    Only the top level is copied. Nested lists and dicts are still shared
    with the caller.

    Raises:
        TypeError: If ``metadata`` isn't a mapping or ``None``.
    """
    check_is_mapping(metadata)
    return dict(metadata) if metadata is not None else {}


def check_is_mapping(metadata: Any) -> None:
    """Raise ``TypeError`` unless ``metadata`` is a mapping or ``None``."""
    if metadata is not None and not isinstance(metadata, Mapping):
        raise TypeError("metadata must be a mapping or None.")


def serialize_metadata(
    metadata: Mapping[str, Any] | None,
    *,
    redact_keys: tuple[str, ...] = DEFAULT_REDACT_KEYS,
    string_max_chars: int | None = DEFAULT_METADATA_STRING_MAX_CHARS,
    max_bytes: int | None = DEFAULT_METADATA_MAX_BYTES,
    max_depth: int = DEFAULT_MAX_DEPTH,
) -> str | None:
    """Clean ``metadata`` and encode it as compact JSON of at most ``max_bytes``.

    The output is always the same for the same input: keys are sorted, and
    non-ASCII characters are escaped, so the text is always valid UTF-8.

    If the JSON is too big, this returns a summary instead:
    ``{"_truncated": true, "_original_size_bytes": N, "_preview": "..."}``.
    The preview is as long as will fit. When even the summary is too big,
    it returns ``{"_truncated":true}``, or ``{}`` as a last resort.

    Args:
        metadata: The data to encode, or ``None``.
        redact_keys: Hide the value of any key that contains one of these
            words. Matching ignores case.
        string_max_chars: The maximum length of each string value, or
            ``None`` for no limit.
        max_bytes: The maximum size of the result in UTF-8 bytes. Must be at
            least 2. ``None`` means no limit.
        max_depth: How many levels of nesting to keep.

    Returns:
        A JSON string, or ``None`` when there is no metadata.

    Raises:
        TypeError: If ``metadata`` isn't a mapping, or a key isn't a string.
        ValueError: If one of the settings is invalid.
    """
    if max_bytes is not None and (
        isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes < 2
    ):
        raise ValueError("max_bytes must be an integer of at least 2 or None.")
    cleaned = sanitize_metadata(
        metadata, redact_keys=redact_keys, string_max_chars=string_max_chars, max_depth=max_depth,
    )
    if cleaned is None:
        return None
    encoded = _to_json(cleaned)
    if max_bytes is None or _utf8_size(encoded) <= max_bytes:
        return encoded
    return _truncation_summary(encoded, max_bytes)


def sanitize_metadata(
    metadata: Mapping[str, Any] | None,
    *,
    redact_keys: tuple[str, ...] = DEFAULT_REDACT_KEYS,
    string_max_chars: int | None = DEFAULT_METADATA_STRING_MAX_CHARS,
    max_depth: int = DEFAULT_MAX_DEPTH,
) -> dict[str, Any] | None:
    """Return a JSON-friendly copy of ``metadata`` with sensitive values hidden.

    See the module docstring for how each type is converted. The settings
    have the same meaning as in ``serialize_metadata``.

    Returns:
        A new ``dict``, or ``None`` when ``metadata`` is ``None`` or empty.

    Raises:
        TypeError: If ``metadata`` isn't a mapping, or a key isn't a string.
        ValueError: If one of the settings is invalid.
    """
    sanitizer = _Sanitizer.from_settings(redact_keys, string_max_chars, max_depth)
    if metadata is None:
        return None
    if not isinstance(metadata, Mapping):
        raise TypeError("metadata must be a mapping with string keys.")
    return sanitizer.clean(metadata) or None


@dataclass(frozen=True)
class _Sanitizer:
    """Walks nested metadata and converts each value. See ``sanitize_metadata``."""

    redaction_terms: tuple[str, ...]  # Lowercase words that mark a key as sensitive.
    string_max_chars: int | None
    max_depth: int

    @classmethod
    def from_settings(cls, redact_keys: Any, string_max_chars: Any, max_depth: Any) -> _Sanitizer:
        """Check the settings and build a sanitizer. Raises ``ValueError`` if invalid."""
        if string_max_chars is not None and (
            isinstance(string_max_chars, bool)
            or not isinstance(string_max_chars, int)
            or string_max_chars < 0
        ):
            raise ValueError("string_max_chars must be a nonnegative integer or None.")
        if isinstance(max_depth, bool) or not isinstance(max_depth, int) or max_depth < 1:
            raise ValueError("max_depth must be a positive integer.")
        if not isinstance(redact_keys, tuple) or any(not isinstance(k, str) for k in redact_keys):
            raise ValueError("redact_keys must be a tuple of strings.")
        terms = tuple(key.lower() for key in redact_keys if key)
        return cls(terms, string_max_chars, max_depth)

    def clean(self, value: Any, depth: int = 0) -> Any:
        """Return a JSON-friendly version of ``value``, nested ``depth`` levels deep."""
        if depth >= self.max_depth:
            return DEPTH_LIMIT_VALUE
        if isinstance(value, Enum):
            # An enum's value can itself be a dict, so clean it like any other value.
            return self.clean(value.value, depth + 1)

        # Turn objects with named fields into dicts *before* redaction, so a
        # field called "password" is hidden just like a dict key would be.
        if is_dataclass(value) and not isinstance(value, type):
            value = {item.name: getattr(value, item.name) for item in fields(value)}
        elif _is_named_record(value):
            names = _record_field_names(value)
            if names is None:
                # Without trustworthy names, redaction can't work, so hide everything.
                return UNSUPPORTED_VALUE
            value = dict(zip(names, value, strict=True))

        if isinstance(value, Mapping):
            return {key: self._clean_entry(key, child, depth) for key, child in value.items()}
        if isinstance(value, list | tuple):
            return [self.clean(child, depth + 1) for child in value]
        return self._clean_scalar(value)

    def _clean_entry(self, key: Any, value: Any, depth: int) -> Any:
        """Clean one ``key: value`` pair, hiding the value if the key is sensitive."""
        if not isinstance(key, str):
            raise TypeError("metadata keys must be strings.")
        lowered = key.lower()
        if any(term in lowered for term in self.redaction_terms):
            return REDACTED_VALUE
        return self.clean(value, depth + 1)

    def _clean_scalar(self, value: Any) -> Any:
        """Convert a single (non-container) value to a JSON-friendly one."""
        if isinstance(value, datetime | date):
            value = value.isoformat()
        elif isinstance(value, Decimal | Path):
            value = str(value)
        if isinstance(value, str):
            return truncate_text(value, self.string_max_chars)
        # bool is checked before Integral because bool is a subclass of int.
        if value is None or isinstance(value, bool):
            return value
        if isinstance(value, Integral):
            return int(value)
        if isinstance(value, Real):
            number = float(value)
            return number if math.isfinite(number) else NONFINITE_VALUE
        return UNSUPPORTED_VALUE


def _is_named_record(value: Any) -> bool:
    """True for tuples with field names: named tuples (``_fields``) and PySpark Rows
    (``__fields__``)."""
    return isinstance(value, tuple) and (hasattr(value, "_fields") or hasattr(value, "__fields__"))


def _record_field_names(record: tuple) -> list[str] | tuple[str, ...] | None:
    """Return the record's field names, or ``None`` if they are missing or malformed.

    Valid names are a list or tuple of unique strings, one per value.
    """
    names = getattr(record, "_fields", getattr(record, "__fields__", None))
    if (
        isinstance(names, list | tuple)
        and len(names) == len(record)
        and all(isinstance(name, str) for name in names)
        and len(set(names)) == len(names)
    ):
        return names
    return None


def _truncation_summary(encoded: str, max_bytes: int) -> str:
    """Return a JSON summary of ``encoded`` that fits in ``max_bytes``."""
    summary = {"_truncated": True, "_original_size_bytes": _utf8_size(encoded)}
    best = _to_json(summary)
    if _utf8_size(best) > max_bytes:
        marker = _to_json({"_truncated": True})
        return marker if _utf8_size(marker) <= max_bytes else "{}"

    # Find the longest preview that fits. Escaping changes the size in ways
    # that are hard to predict, so use a binary search over the preview length.
    # Adding a character never shrinks its JSON encoding, so a failing length
    # rules out every longer prefix. best keeps the last fitting candidate;
    # it stays a summary without a preview when even an empty preview won't fit.
    shortest, longest = 0, min(len(encoded), max_bytes)
    while shortest <= longest:
        length = (shortest + longest) // 2
        candidate = _to_json({**summary, "_preview": encoded[:length]})
        if _utf8_size(candidate) <= max_bytes:
            best = candidate
            shortest = length + 1
        else:
            longest = length - 1
    return best


def _to_json(value: Any) -> str:
    """Encode compact, ASCII-only JSON with sorted keys."""
    return json.dumps(
        value, ensure_ascii=True, allow_nan=False, sort_keys=True, separators=(",", ":"),
    )


def _utf8_size(text: str) -> int:
    return len(text.encode("utf-8"))
