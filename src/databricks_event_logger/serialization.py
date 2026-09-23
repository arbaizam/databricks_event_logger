"""Compatibility imports for metadata conversion and diagnostic text.

The function arguments, return values and exceptions are documented on the
original function objects in ``metadata.py`` and ``diagnostics.py``. These
aliases do not issue warnings or change conversion behavior.
"""

from databricks_event_logger.diagnostics import TRUNCATED_MARKER, safe_text
from databricks_event_logger.metadata import (
    DEFAULT_METADATA_MAX_BYTES,
    DEFAULT_METADATA_STRING_MAX_CHARS,
    DEFAULT_REDACT_KEYS,
    DEPTH_LIMIT_VALUE,
    REDACTED_VALUE,
    UNSUPPORTED_VALUE,
    sanitize_metadata,
    serialize_metadata,
)

__all__ = [
    "DEFAULT_METADATA_MAX_BYTES", "DEFAULT_METADATA_STRING_MAX_CHARS",
    "DEFAULT_REDACT_KEYS", "DEPTH_LIMIT_VALUE", "REDACTED_VALUE", "TRUNCATED_MARKER",
    "UNSUPPORTED_VALUE", "safe_text", "sanitize_metadata", "serialize_metadata",
]
