"""Keep the original scope/decorator identities in stored failure frames.

Release 0.1.3 included logger implementation locations in its failure hashes.
Moving those functions must not split existing application-failure groups.
These compatibility identities are storage labels from commit 77f33cf,
not navigation links into the current source. Application frames are untouched.

Registration uses code objects, so a user's function with the same name is
never mistaken for an SDK frame. Bytecode supplies the yield/call line;
adding comments or moving the function therefore does not change its label.
This module does not change Python tracebacks or exception objects.
"""

from collections.abc import Callable
from dis import get_instructions
from inspect import CO_COROUTINE
from types import CodeType, TracebackType
from typing import Any, TypeVar

F = TypeVar("F", bound=Callable[..., Any])
_LOCATIONS: dict[CodeType, tuple[str, int | None, dict[int, int]]] = {}


def _instruction_lines(code: CodeType, operation: str) -> list[int]:
    """Find source lines for an instruction on Python 3.10 and later."""
    line = code.co_firstlineno
    result = []
    for instruction in get_instructions(code):
        if instruction.starts_line is not None:
            line = instruction.starts_line
        if instruction.opname == operation:
            result.append(line)
    return result


def _legacy_scope(func: F) -> F:
    """Register the original scope's yield and exit locations; return func unchanged."""
    yield_lines = _instruction_lines(func.__code__, "YIELD_VALUE")
    _LOCATIONS[func.__code__] = ("_event_scope", 258, dict.fromkeys(yield_lines, 251))
    return func


def _legacy_wrapper(func: F) -> F:
    """Register the original synchronous or asynchronous decorator locations."""
    async_function = func.__code__.co_flags & CO_COROUTINE
    call_line = 423 if async_function else 430
    # func(*args, **kwargs) uses CALL_FUNCTION_EX; the scope factory does not.
    call_lines = _instruction_lines(func.__code__, "CALL_FUNCTION_EX")
    if async_function:
        call_lines += _instruction_lines(func.__code__, "YIELD_VALUE")
    _LOCATIONS[func.__code__] = (
        "async_wrapper" if async_function else "wrapper",
        call_line - 1,
        dict.fromkeys(call_lines, call_line),
    )
    return func


def _legacy_delivery(func: F) -> F:
    """Preserve the sink-call location when strict delivery interrupts an outer scope."""
    line = func.__code__.co_firstlineno
    calls = {}
    for instruction in get_instructions(func):
        if instruction.starts_line is not None:
            line = instruction.starts_line
        if instruction.opname in {"LOAD_METHOD", "LOAD_ATTR"} and instruction.argval == "emit":
            calls[line] = 340
    _LOCATIONS[func.__code__] = ("_deliver", None, calls)
    return func


def _legacy_frame(trace: TracebackType) -> dict[str, Any] | None:
    """Return the stored legacy location for a registered frame, if any."""
    location = _LOCATIONS.get(trace.tb_frame.f_code)
    if location is None:
        return None
    name, default_line, lines = location
    line = lines.get(trace.tb_lineno, default_line)
    if line is None:
        return None
    return {"file": "logger.py", "function": name, "line": line}
