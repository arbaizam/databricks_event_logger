"""
Metadata serialization helpers.

Metadata is intentionally caller-configurable in v1. The serializer therefore
does not enforce key allowlists or hard size limits. Its responsibility is to
produce deterministic JSON for values that are valid JSON and a stable fallback
for common Python objects that JSON cannot encode directly.
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


def serialize_metadata(metadata: Mapping[str, Any] | None) -> str | None:
    """
    Serialize event metadata to deterministic JSON.

    Parameters
    ----------
    metadata : Mapping[str, Any] | None
        Caller-supplied metadata. ``None`` and empty mappings return ``None`` so
        empty event metadata does not become noisy ``"{}"`` strings.

    Returns
    -------
    str | None
        Serialized JSON string, or ``None`` when no metadata is supplied.
    """
    if not metadata:
        return None
    return json.dumps(
        dict(metadata),
        default=json_default,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )


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
