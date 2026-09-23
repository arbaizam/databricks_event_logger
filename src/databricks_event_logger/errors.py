"""Exceptions raised by this package.

The package raises its own exceptions only for setup mistakes, like a bad
table name. It never uses them to wrap or replace an exception raised by
application code. The application's original exception always propagates.
"""

from __future__ import annotations


class EventLoggerError(Exception):
    """Base class for package-specific exceptions.

    Field and metadata validation also use built-in ValueError and TypeError;
    those are not subclasses of this class. Sink exceptions are not wrapped.
    """


class EventLoggerConfigurationError(EventLoggerError):
    """Raised when the logger or a sink is set up incorrectly."""
