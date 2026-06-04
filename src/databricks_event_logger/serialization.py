"""
Metadata serialization helpers.

Metadata remains caller-configurable in v1: there are no key allowlists and the
logger does not reject arbitrary diagnostic fields. The serializer still applies
production-safe hygiene by default: sensitive-looking keys are redacted, long
strings can be truncated, and the final JSON payload can be capped to a maximum
byte size.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any

DEFAULT_REDACT_KEYS = (
    "password",
    "token",
    "secret",
    "credential",
    "api_key",
    "access_key",
    "private_key",
)
DEFAULT_METADATA_MAX_BYTES = 4000
DEFAULT_METADATA_STRING_MAX_CHARS = 2000
TRUNCATED_MARKER = "...[TRUNCATED]"
REDACTED_VALUE = "[REDACTED]"


def json_default(value: Any) -> Any:
    """
    Convert common non-JSON values into JSON-compatible values.

    Parameters
    ----------
    value : Any
        Value passed by ``json.dumps`` when the default encoder cannot handle
        it.

    Returns
    -------
    Any
        JSON-compatible representation.

    Notes
    -----
    ``repr`` is the final fallback because observability metadata should never
    fail business processing solely due to a non-serializable diagnostic value.
    """
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if isinstance(value, set | frozenset):
        return sorted(value, key=repr)
    return repr(value)


def sanitize_metadata(
    metadata: Mapping[str, Any] | None,
    *,
    redact_keys: tuple[str, ...] = DEFAULT_REDACT_KEYS,
    string_max_chars: int | None = DEFAULT_METADATA_STRING_MAX_CHARS,
    max_depth: int = 8,
) -> dict[str, Any] | None:
    """
    Return metadata with redaction and bounded scalar strings applied.

    Parameters
    ----------
    metadata : Mapping[str, Any] | None
        Caller-supplied metadata.
    redact_keys : tuple[str, ...], default DEFAULT_REDACT_KEYS
        Case-insensitive key fragments whose values should be replaced with a
        redaction marker. This is not an allowlist; all other keys are retained.
    string_max_chars : int | None, default DEFAULT_METADATA_STRING_MAX_CHARS
        Maximum length for string values. ``None`` disables string truncation.
    max_depth : int, default 8
        Maximum recursion depth for nested dictionaries and sequences.

    Returns
    -------
    dict[str, Any] | None
        Sanitized metadata, or ``None`` when no metadata is supplied.
    """
    if not metadata:
        return None
    redaction_terms = tuple(term.lower() for term in redact_keys if term)
    return {
        str(key): _sanitize_value(
            key=str(key),
            value=value,
            redact_keys=redaction_terms,
            string_max_chars=string_max_chars,
            depth=0,
            max_depth=max_depth,
        )
        for key, value in metadata.items()
    }


def serialize_metadata(
    metadata: Mapping[str, Any] | None,
    *,
    redact_keys: tuple[str, ...] = DEFAULT_REDACT_KEYS,
    string_max_chars: int | None = DEFAULT_METADATA_STRING_MAX_CHARS,
    max_bytes: int | None = DEFAULT_METADATA_MAX_BYTES,
) -> str | None:
    """
    Serialize event metadata to deterministic JSON.

    Parameters
    ----------
    metadata : Mapping[str, Any] | None
        Caller-supplied metadata. ``None`` and empty mappings return ``None`` so
        empty event metadata does not become noisy ``"{}"`` strings.
    redact_keys : tuple[str, ...], default DEFAULT_REDACT_KEYS
        Case-insensitive key fragments whose values should be redacted before
        serialization.
    string_max_chars : int | None, default DEFAULT_METADATA_STRING_MAX_CHARS
        Maximum length for string values. ``None`` disables string truncation.
    max_bytes : int | None, default DEFAULT_METADATA_MAX_BYTES
        Maximum UTF-8 byte size for the serialized JSON. ``None`` disables the
        final payload cap.

    Returns
    -------
    str | None
        Serialized JSON string, or ``None`` when no metadata is supplied.
    """
    sanitized = sanitize_metadata(
        metadata,
        redact_keys=redact_keys,
        string_max_chars=string_max_chars,
    )
    if not sanitized:
        return None
    serialized = json.dumps(
        sanitized,
        default=json_default,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    if max_bytes is None or len(serialized.encode("utf-8")) <= max_bytes:
        return serialized
    return _bounded_metadata_json(serialized, max_bytes=max_bytes)


def deserialize_metadata(metadata_json: str | None) -> dict[str, Any]:
    """
    Deserialize metadata JSON into a dictionary.

    Parameters
    ----------
    metadata_json : str | None
        JSON string stored in an event record.

    Returns
    -------
    dict[str, Any]
        Parsed metadata dictionary. Empty input returns an empty dictionary.
    """
    if not metadata_json:
        return {}
    parsed = json.loads(metadata_json)
    if isinstance(parsed, dict):
        return parsed
    return {"value": parsed}


def _sanitize_value(
    *,
    key: str,
    value: Any,
    redact_keys: tuple[str, ...],
    string_max_chars: int | None,
    depth: int,
    max_depth: int,
) -> Any:
    """
    Sanitize one metadata value while preserving JSON-friendly structure.
    """
    if _is_redacted_key(key, redact_keys):
        return REDACTED_VALUE
    if depth >= max_depth:
        return _truncate_string(repr(value), string_max_chars)
    if isinstance(value, str):
        return _truncate_string(value, string_max_chars)
    if isinstance(value, Mapping):
        return {
            str(child_key): _sanitize_value(
                key=str(child_key),
                value=child_value,
                redact_keys=redact_keys,
                string_max_chars=string_max_chars,
                depth=depth + 1,
                max_depth=max_depth,
            )
            for child_key, child_value in value.items()
        }
    if isinstance(value, list | tuple):
        return [
            _sanitize_value(
                key=key,
                value=child_value,
                redact_keys=redact_keys,
                string_max_chars=string_max_chars,
                depth=depth + 1,
                max_depth=max_depth,
            )
            for child_value in value
        ]
    if isinstance(value, set | frozenset):
        return [
            _sanitize_value(
                key=key,
                value=child_value,
                redact_keys=redact_keys,
                string_max_chars=string_max_chars,
                depth=depth + 1,
                max_depth=max_depth,
            )
            for child_value in sorted(value, key=repr)
        ]
    return value


def _is_redacted_key(key: str, redact_keys: tuple[str, ...]) -> bool:
    """
    Return whether a metadata key should have its value redacted.
    """
    lowered_key = key.lower()
    return any(term in lowered_key for term in redact_keys)


def _truncate_string(value: str, max_chars: int | None) -> str:
    """
    Truncate one string value when a maximum length is configured.
    """
    if max_chars is None or max_chars < 0 or len(value) <= max_chars:
        return value
    return f"{value[:max_chars]}{TRUNCATED_MARKER}"


def _bounded_metadata_json(serialized: str, *, max_bytes: int) -> str:
    """
    Return a deterministic small replacement payload for oversized metadata.
    """
    original_size = len(serialized.encode("utf-8"))
    preview_budget = max(0, max_bytes - 120)
    preview = serialized.encode("utf-8")[:preview_budget].decode("utf-8", errors="ignore")
    payload = {
        "_truncated": True,
        "_original_size_bytes": original_size,
        "_preview": f"{preview}{TRUNCATED_MARKER}",
    }
    bounded = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    while len(bounded.encode("utf-8")) > max_bytes and payload["_preview"]:
        payload["_preview"] = payload["_preview"][:-1]
        bounded = json.dumps(
            payload,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
    return bounded
