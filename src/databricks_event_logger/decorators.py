"""
Decorator helpers for observed functions.

The module-level ``observed`` decorator resolves the default logger at call time
so import order does not matter. Notebooks can initialize the logger once with
``observe_notebook`` and application functions can use ``@observed`` normally.
"""

from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from typing import Any, TypeVar

from databricks_event_logger.logger import get_default_logger

F = TypeVar("F", bound=Callable[..., Any])


def observed(
    event_name: str,
    *,
    event_type: str = "function",
    metadata: dict[str, Any] | None = None,
) -> Callable[[F], F]:
    """
    Decorate a function using the current default logger.

    Parameters
    ----------
    event_name : str
        Event emitted when the function finishes.
    event_type : str, default "function"
        Event category.
    metadata : dict[str, Any] | None, default None
        Static metadata attached to the event.

    Returns
    -------
    Callable[[F], F]
        Decorated function.
    """

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            logger = get_default_logger()
            return logger._run_observed(  # noqa: SLF001 - shared implementation for default decorator
                event_name,
                lambda: func(*args, **kwargs),
                event_type=event_type,
                metadata=metadata,
            )

        return wrapper  # type: ignore[return-value]

    return decorator
