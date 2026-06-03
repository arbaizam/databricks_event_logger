"""
Databricks runtime context resolution.

Context capture is deliberately best-effort. The same package code may run from
a Databricks job, an interactive notebook, an integration-test task, or a simple
Python interpreter inside a Databricks cluster. Missing fields should produce
``None`` values rather than failed event logging.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RuntimeContext:
    """
    Databricks runtime fields attached to each event.

    Parameters
    ----------
    workspace_id : str | None
        Databricks workspace/account organization identifier when available.
    workspace_url : str | None
        Browser/workspace host URL when available.
    cluster_id : str | None
        Cluster identifier for the current execution.
    job_id : str | None
        Databricks job identifier.
    run_id : str | None
        Databricks job run identifier.
    task_key : str | None
        Databricks task key within the job.
    task_attempt_number : str | None
        Databricks task attempt number when available.
    notebook_path : str | None
        Notebook path for notebook-backed tasks.
    user_name : str | None
        User name associated with the current run when available.
    """

    workspace_id: str | None = None
    workspace_url: str | None = None
    cluster_id: str | None = None
    job_id: str | None = None
    run_id: str | None = None
    task_key: str | None = None
    task_attempt_number: str | None = None
    notebook_path: str | None = None
    user_name: str | None = None

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any] | None) -> RuntimeContext:
        """
        Build context from a generic mapping.

        Parameters
        ----------
        values : Mapping[str, Any] | None
            Mapping containing any subset of context fields.

        Returns
        -------
        RuntimeContext
            Context object with unknown fields ignored.
        """
        if not values:
            return cls()
        allowed = cls.__dataclass_fields__.keys()
        return cls(**{key: _string_or_none(values.get(key)) for key in allowed})

    def as_dict(self) -> dict[str, str | None]:
        """
        Return context fields as a dictionary.

        Returns
        -------
        dict[str, str | None]
            Field names and values suitable for merging into event records.
        """
        return {
            "workspace_id": self.workspace_id,
            "workspace_url": self.workspace_url,
            "cluster_id": self.cluster_id,
            "job_id": self.job_id,
            "run_id": self.run_id,
            "task_key": self.task_key,
            "task_attempt_number": self.task_attempt_number,
            "notebook_path": self.notebook_path,
            "user_name": self.user_name,
        }


def resolve_databricks_context(
    *,
    dbutils: Any | None = None,
    spark: Any | None = None,
    fallback: Mapping[str, Any] | None = None,
) -> RuntimeContext:
    """
    Resolve Databricks runtime context without requiring Databricks imports.

    Parameters
    ----------
    dbutils : Any | None, default None
        Optional Databricks ``dbutils`` object. Supplying it is the most reliable
        way to read notebook context in Databricks notebooks.
    spark : Any | None, default None
        Optional Spark session. When supplied, selected Spark configuration
        values may be used as fallbacks.
    fallback : Mapping[str, Any] | None, default None
        Explicit fallback values. These are useful in tests and for code paths
        where a caller already resolved context externally.

    Returns
    -------
    RuntimeContext
        Best-effort context. Missing or inaccessible values are ``None``.
    """
    values: dict[str, Any] = dict(fallback or {})
    values.update(_context_from_environment())
    values.update(_context_from_spark(spark))
    values.update(_context_from_dbutils(dbutils))
    return RuntimeContext.from_mapping(values)


def _context_from_environment() -> dict[str, str]:
    """
    Return context-like values from process environment variables.

    Environment variables vary by Databricks runtime and execution mode, so this
    is intentionally opportunistic.
    """
    env_map = {
        "workspace_url": "DATABRICKS_HOST",
        "cluster_id": "DATABRICKS_CLUSTER_ID",
        "job_id": "DATABRICKS_JOB_ID",
        "run_id": "DATABRICKS_RUN_ID",
        "task_key": "DATABRICKS_TASK_KEY",
        "task_attempt_number": "DATABRICKS_TASK_ATTEMPT_NUMBER",
        "user_name": "DATABRICKS_USER",
    }
    return {
        field: value
        for field, env_name in env_map.items()
        if (value := os.environ.get(env_name))
    }


def _context_from_spark(spark: Any | None) -> dict[str, str]:
    """
    Return selected Spark configuration values when available.
    """
    if spark is None:
        return {}
    conf = getattr(spark, "conf", None)
    if conf is None:
        return {}
    spark_map = {
        "cluster_id": "spark.databricks.clusterUsageTags.clusterId",
        "workspace_id": "spark.databricks.clusterUsageTags.clusterOwnerOrgId",
    }
    values: dict[str, str] = {}
    for field, key in spark_map.items():
        try:
            value = conf.get(key)
        except Exception:
            value = None
        if value:
            values[field] = str(value)
    return values


def _context_from_dbutils(dbutils: Any | None) -> dict[str, str]:
    """
    Return notebook context tags from ``dbutils`` when available.
    """
    if dbutils is None:
        return {}
    try:
        context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    except Exception:
        return {}

    tag_map = {
        "workspace_id": ("orgId", "workspaceId"),
        "workspace_url": ("browserHostName", "apiUrl"),
        "cluster_id": ("clusterId",),
        "job_id": ("jobId",),
        "run_id": ("jobRunId", "runId"),
        "task_key": ("taskKey",),
        "task_attempt_number": ("taskAttemptNumber",),
        "notebook_path": ("notebook_path", "notebookPath"),
        "user_name": ("user", "userName"),
    }
    values: dict[str, str] = {}
    for field, tag_names in tag_map.items():
        for tag_name in tag_names:
            if value := _context_tag(context, tag_name):
                values[field] = value
                break
    return values


def _context_tag(context: Any, tag_name: str) -> str | None:
    """
    Read one tag value from Databricks notebook context.
    """
    try:
        tags = context.tags()
        option_value = tags.get(tag_name)
        if hasattr(option_value, "isDefined") and not option_value.isDefined():
            return None
        if hasattr(option_value, "get"):
            return _string_or_none(option_value.get())
        return _string_or_none(option_value)
    except Exception:
        return None


def _string_or_none(value: Any) -> str | None:
    """
    Convert a context value to ``str`` while preserving missing values.
    """
    if value is None:
        return None
    text = str(value).strip()
    return text or None
