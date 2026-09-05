"""Explicit runtime identity; collecting it belongs to the Databricks adapter."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass, fields
from typing import Any
from urllib.parse import urlsplit


@dataclass(frozen=True)
class RuntimeContext:
    """Optional execution identifiers, normalized to strings for storage.

    Supply job/task values explicitly when reliable attribution is required.
    Constructing context never discovers runtime state or imports Spark.
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
        for field in fields(self):
            value = getattr(self, field.name)
            if value is not None:
                if type(value) not in (str, int):
                    raise TypeError(f"{field.name} must be a string, integer, or None.")
                value = str(value).strip() or None
            if value and field.name == "workspace_url":
                parsed = urlsplit(value if "://" in value else f"https://{value}")
                if parsed.scheme not in ("http", "https") or not parsed.hostname:
                    raise ValueError("workspace_url must be a workspace hostname or HTTP(S) URL.")
                value = parsed.hostname.lower()
            object.__setattr__(self, field.name, value)

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any] | None) -> RuntimeContext:
        """Construct from canonical field names; reject unknown names and bad values."""
        if values is not None and not isinstance(values, Mapping):
            raise TypeError("values must be a mapping or None.")
        return cls(**dict(values or {}))

    def as_dict(self) -> dict[str, str | None]:
        return asdict(self)

    @property
    def job_url(self) -> str | None:
        if self.workspace_url and self.job_id:
            return f"https://{self.workspace_url}/jobs/{self.job_id}"
        return None

    @property
    def job_run_url(self) -> str | None:
        if self.job_url and self.run_id:
            return f"{self.job_url}/runs/{self.run_id}"
        return None
