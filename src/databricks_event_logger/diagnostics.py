"""Helpers for reporting problems without causing new ones.

The logger often runs while the application is already handling an error.
safe_text tolerates a failing __str__; callers supply a valid text limit.
warn_safely suppresses failures from warning filters and handlers. Text
truncation requires a string and a nonnegative integer limit (or None).
"""

from __future__ import annotations

import warnings

TRUNCATED_MARKER = "...[TRUNCATED]"
UNPRINTABLE_VALUE = "[UNPRINTABLE]"


def truncate_text(text: str, max_chars: int | None) -> str:
    """Shorten text to at most ``max_chars`` characters.

    When text is cut, it ends with ``TRUNCATED_MARKER`` so readers can tell.
    The marker counts toward the limit. If the limit is shorter than the
    marker, only its prefix fits; a zero limit returns an empty string.

    Args:
        text: The text to shorten.
        max_chars: Nonnegative integer length, or ``None`` for no limit.
            This low-level helper assumes the caller has validated it.

    Returns:
        The original text if it fits, otherwise a shortened copy.
    """
    if max_chars is None or len(text) <= max_chars:
        return text
    if max_chars <= len(TRUNCATED_MARKER):
        return TRUNCATED_MARKER[:max_chars]
    return text[: max_chars - len(TRUNCATED_MARKER)] + TRUNCATED_MARKER


def safe_text(value: object, *, max_chars: int | None = 2000) -> str:
    """Convert any value, usually an exception, into short text.

    Use this for error messages only. It is not a secret filter, and it is not
    used for metadata (metadata has its own rules in ``metadata.py``).

    It calls ``str(value)`` but never ``repr(value)``. If ``str`` raises
    anything, even ``KeyboardInterrupt``, it uses ``UNPRINTABLE_VALUE`` before
    applying the same length limit (which may shorten that marker).

    Args:
        value: The value to describe.
        max_chars: Validated nonnegative integer length, or ``None`` for no limit.

    Returns:
        Text that is at most ``max_chars`` characters long.
    """
    try:
        text = str(value)
    except BaseException:
        text = UNPRINTABLE_VALUE
    return truncate_text(text, max_chars)


def warn_safely(message: str, *, stacklevel: int = 1) -> None:
    """Emit a ``RuntimeWarning`` that can never raise.

    If warnings are set to "error", or a custom warning handler fails, the
    problem is ignored. Warnings exist to inform. They must never change how
    the application runs.

    Args:
        message: The warning text.
        stacklevel: The same meaning as in ``warnings.warn``, counted from the
            function that calls ``warn_safely``.
    """
    try:
        # +1 skips this helper's own frame.
        warnings.warn(message, RuntimeWarning, stacklevel=stacklevel + 1)
    except BaseException:
        pass
