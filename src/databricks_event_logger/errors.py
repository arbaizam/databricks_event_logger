"""
Package-specific exceptions.

The logger is intentionally conservative about where it raises package errors.
Business failures should remain visible to Databricks and to callers. Package
exceptions are therefore used for configuration and direct API misuse, not for
masking exceptions raised by user code.
"""

from __future__ import annotations


class EventLoggerError(Exception):
    """
    Base exception for package-owned failures.

    Notes
    -----
    The logger should not raise this exception while handling an unrelated
    business exception. In those paths the original business exception must be
    preserved and re-raised.
    """


class EventLoggerConfigurationError(EventLoggerError):
    """
    Raised when logger configuration is invalid.
    """
