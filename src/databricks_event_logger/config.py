"""
Configuration models and helpers.

Configuration is intentionally small. Unity Catalog ownership, grants,
retention, and bundle deployment policy are handled outside the package. The
logger only needs enough runtime configuration to stamp events consistently.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class EventLoggerConfig:
    """
    Runtime configuration stamped onto emitted events.

    Parameters
    ----------
    app_name : str | None, default None
        Application name for emitted events.
    component : str | None, default None
        Component or job/task area for emitted events.
    environment : str | None, default None
        Deployment environment, usually the bundle target.
    event_table : str | None, default None
        Fully qualified target event table used by Delta-backed sinks.
    """

    app_name: str | None = None
    component: str | None = None
    environment: str | None = None
    event_table: str | None = None

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any] | None) -> EventLoggerConfig:
        """
        Build config from a mapping.

        Parameters
        ----------
        values : Mapping[str, Any] | None
            Mapping containing any subset of config fields.

        Returns
        -------
        EventLoggerConfig
            Config object with unknown fields ignored.
        """
        if not values:
            return cls()
        allowed = cls.__dataclass_fields__.keys()
        return cls(**{key: _string_or_none(values.get(key)) for key in allowed})


def _string_or_none(value: Any) -> str | None:
    """
    Convert a config value to ``str`` while preserving missing values.
    """
    if value is None:
        return None
    text = str(value).strip()
    return text or None
