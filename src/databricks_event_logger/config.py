"""
Configuration models and helpers.

Configuration is intentionally small. Unity Catalog ownership, grants,
retention, and bundle deployment policy are handled outside the package. The
logger only needs enough runtime configuration to stamp events consistently.
"""

from __future__ import annotations

from dataclasses import dataclass


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
    """

    app_name: str | None = None
    component: str | None = None
    environment: str | None = None
