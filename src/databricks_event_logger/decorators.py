"""Wrap a function so that every call runs inside an event scope.

This module is used by ``EventLogger.logged_event`` and ``EventLogger.run_task``.
Both normal functions and ``async`` functions are supported.

Generator functions are rejected. A generator's code runs later, bit by bit,
as the caller loops over it. So a scope around the call would measure almost
nothing and could report the wrong outcome. Put the scope around the loop
that consumes the generator instead.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from contextlib import AbstractContextManager
from functools import wraps
from typing import Any, TypeVar, cast

from databricks_event_logger._trace_compat import _legacy_wrapper

F = TypeVar("F", bound=Callable[..., Any])


def wrap_in_scope(func: F, open_scope: Callable[[], AbstractContextManager[Any]]) -> F:
    """Return a wrapper that runs ``func`` inside ``open_scope()`` on every call.

    The wrapper keeps ``func``'s name, docstring, and return value.
    ``open_scope`` is called once per call, so each call gets its own event.

    Args:
        func: A normal function or an ``async def`` function.
        open_scope: A function that returns a new context manager, such as
            ``lambda: logger.event("name")``.

    Returns:
        A function with the original signature available through __wrapped__.
        For async functions its result must be awaited.

    Raises:
        TypeError: If ``func`` is a generator or async generator function.
        BaseException: Calling the wrapper propagates errors according to the
            supplied context manager. EventLogger supplies its failure policy.
    """
    if inspect.isgeneratorfunction(func) or inspect.isasyncgenfunction(func):
        raise TypeError(
            "Generator functions are not supported; put an event() scope around the "
            "consuming loop, never across a yield."
        )

    if inspect.iscoroutinefunction(func):

        @wraps(func)
        @_legacy_wrapper
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            # The scope stays open until the awaited work has finished.
            with open_scope():
                return await func(*args, **kwargs)

        return cast(F, async_wrapper)

    @wraps(func)
    @_legacy_wrapper
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        with open_scope():
            return func(*args, **kwargs)

    return cast(F, wrapper)
