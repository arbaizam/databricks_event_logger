"""``RuntimeContext``: where the code is running (workspace, job, task, user).

This module only stores and cleans values that it is given. It never looks up
anything by itself. To read values from a Databricks notebook, use
``databricks_event_logger.databricks.resolve_context``.

To add a new identifier:

1. Add a ``str | None`` field to ``RuntimeContext``.
2. Add a matching nullable ``STRING`` column in ``schema.py``.
3. Optionally, map it to a notebook JSON key in ``databricks/context.py``.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass, fields
from typing import Any
from urllib.parse import urlsplit


@dataclass(frozen=True)
class RuntimeContext:
    """Identifiers that say where an event came from. All are optional.

    Every value is stored as a trimmed string, or ``None``. Integers are
    accepted and turned into strings, and blank strings become ``None``.
    ``workspace_url`` is reduced to a lowercase hostname, so
    ``"https://ADB-1.example.net/path"`` becomes ``"adb-1.example.net"``.

    Raises:
        TypeError: If a value isn't a ``str``, an ``int``, or ``None``.
        ValueError: If ``workspace_url`` isn't a hostname or HTTP(S) URL.
    """

    workspace_id: str | None = None
    workspace_url: str | None = None
    cluster_id: str | None = None
    job_id: str | None = None
    run_id: str | None = None
    task_key: str | None = None
    task_run_id: str | None = None
    task_attempt_number: str | None = None
    job_start_time: str | None = None
    job_trigger_type: str | None = None
    notebook_path: str | None = None
    user_name: str | None = None
    run_as_user_name: str | None = None

    def __post_init__(self) -> None:
        for item in fields(self):
            value = _clean_identifier(item.name, getattr(self, item.name))
            if value is not None and item.name == "workspace_url":
                value = _workspace_hostname(value)
            # The dataclass is frozen, so fields must be set this way.
            object.__setattr__(self, item.name, value)

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any] | None) -> RuntimeContext:
        """Build a context from a dict that uses the field names above.

        Raises:
            TypeError: If ``values`` isn't a mapping, or if it contains an
                unknown key. Unknown keys are rejected so that typos like
                ``"jobid"`` are caught instead of silently ignored.
        """
        if values is not None and not isinstance(values, Mapping):
            raise TypeError("values must be a mapping or None.")
        return cls(**dict(values or {}))

    def as_dict(self) -> dict[str, str | None]:
        """Return every field as a plain dict, including the ``None`` values."""
        return asdict(self)

    @property
    def job_url(self) -> str | None:
        """A link to the job in the workspace, or ``None`` if unknown."""
        if self.workspace_url and self.job_id:
            return f"https://{self.workspace_url}/jobs/{self.job_id}"
        return None

    @property
    def job_run_url(self) -> str | None:
        """A link to this job run in the workspace, or ``None`` if unknown."""
        if self.job_url and self.run_id:
            return f"{self.job_url}/runs/{self.run_id}"
        return None


def _clean_identifier(name: str, value: Any) -> str | None:
    """Return ``value`` as a trimmed string, or ``None`` if it's empty."""
    if value is None:
        return None
    # Exact type check on purpose: bool and other int subclasses are rejected.
    if type(value) not in (str, int):
        raise TypeError(f"{name} must be a string, integer, or None.")
    return str(value).strip() or None


def _workspace_hostname(value: str) -> str:
    """Return the lowercase hostname from a workspace URL or bare hostname."""
    parsed = urlsplit(value if "://" in value else f"https://{value}")
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ValueError("workspace_url must be a workspace hostname or HTTP(S) URL.")
    return parsed.hostname.lower()
