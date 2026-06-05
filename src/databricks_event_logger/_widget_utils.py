"""
Shared Databricks widget parsing helpers.

These functions are private package utilities used by both notebook bootstrap
and readiness diagnostics. Keeping them in one module prevents widget-name
drift between the normal startup path and the preflight path.
"""

from __future__ import annotations

from typing import Any


def widget_values(dbutils: Any | None) -> dict[str, str]:
    """
    Return all visible Databricks widget values.
    """
    if dbutils is None:
        return {}
    values: dict[str, str] = {}
    try:
        raw_values = dbutils.widgets.getAll()
    except Exception:
        raw_values = {}
    try:
        items = raw_values.items()
    except Exception:
        items = ()
    for key, value in items:
        if text := string_or_none(value):
            values[str(key)] = text
    for name in KNOWN_WIDGET_NAMES:
        if name not in values and (value := legacy_widget_value(dbutils, name)):
            values[name] = value
    return values


def widget_value(values: dict[str, str], name: str) -> str | None:
    """
    Read one Databricks widget value from a captured widget mapping.
    """
    return values.get(name)


def context_from_widgets(values: dict[str, str]) -> dict[str, str | None]:
    """
    Return optional runtime context fallback values from Databricks widgets.
    """
    context_values: dict[str, str] = {}
    for field, widget_names in WIDGET_CONTEXT_KEY_MAP.items():
        for widget_name in widget_names:
            if value := values.get(widget_name):
                context_values[field] = value
                break
    return context_values


def legacy_widget_value(dbutils: Any | None, name: str) -> str | None:
    """
    Read one Databricks widget value using the direct widget API.
    """
    if dbutils is None:
        return None
    try:
        value = dbutils.widgets.get(name)
    except Exception:
        return None
    return string_or_none(value)


def string_or_none(value: Any) -> str | None:
    """
    Convert a widget value to ``str`` while preserving missing values.
    """
    if value is None:
        return None
    text = str(value).strip()
    return text or None


WIDGET_CONTEXT_KEY_MAP = {
    "workspace_id": ("workspace_id",),
    "workspace_url": ("workspace_url",),
    "cluster_id": ("cluster_id",),
    "job_id": ("job_id",),
    "run_id": ("run_id", "job_run_id"),
    "task_key": ("task_key", "task_name"),
    "task_run_id": ("task_run_id",),
    "task_attempt_number": ("task_attempt_number", "task_execution_count"),
    "job_start_time": ("job_start_time",),
    "job_trigger_type": ("job_trigger_type",),
    "notebook_path": ("notebook_path",),
    "user_name": ("user_name",),
    "run_as_user_name": ("run_as_user_name",),
}

KNOWN_WIDGET_NAMES = (
    "app_name",
    "component",
    "environment",
    "observability_event_table",
    *tuple(name for names in WIDGET_CONTEXT_KEY_MAP.values() for name in names),
)
