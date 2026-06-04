"""
Databricks runtime context resolution.

Context capture is deliberately best-effort. The same package code may run from
a Databricks job, an interactive notebook, an integration-test task, or a simple
Python interpreter inside a Databricks cluster. Missing fields should produce
``None`` values rather than failed event logging.
"""

from __future__ import annotations

import json
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
    task_run_id : str | None
        Databricks task run identifier when available.
    task_attempt_number : str | None
        Databricks task attempt number when available.
    job_start_time : str | None
        Databricks job start time when supplied by dynamic task parameters.
    job_trigger_type : str | None
        Databricks job trigger type such as ``one_time`` or ``periodic``.
    notebook_path : str | None
        Notebook path for notebook-backed tasks.
    user_name : str | None
        User name associated with the current run when available.
    run_as_user_name : str | None
        Principal the job runs as when supplied by callers.
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
        return cls(**{key: _context_value(key, values.get(key)) for key in allowed})

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
            "task_run_id": self.task_run_id,
            "task_attempt_number": self.task_attempt_number,
            "job_start_time": self.job_start_time,
            "job_trigger_type": self.job_trigger_type,
            "notebook_path": self.notebook_path,
            "user_name": self.user_name,
            "run_as_user_name": self.run_as_user_name,
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
        "task_run_id": "DATABRICKS_TASK_RUN_ID",
        "task_attempt_number": "DATABRICKS_TASK_ATTEMPT_NUMBER",
        "job_start_time": "DATABRICKS_JOB_START_TIME",
        "job_trigger_type": "DATABRICKS_JOB_TRIGGER_TYPE",
        "user_name": "DATABRICKS_USERNAME",
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

    values: dict[str, str] = {}
    sources = [
        _context_from_json(context),
        _context_from_tags(context),
        _context_from_methods(context),
    ]
    for source in sources:
        for field, value in source.items():
            if value and field not in values:
                values[field] = value
    return values


def _context_from_json(context: Any) -> dict[str, str]:
    """
    Return context fields from Databricks context JSON.

    Databricks runtimes expose slightly different tag maps across interactive
    notebooks, job tasks, and runtime versions. ``toJson()`` is often the most
    complete view because it includes nested ``tags`` and ``extraContext``.
    """
    try:
        raw_json = context.toJson()
    except Exception:
        return {}
    try:
        payload = json.loads(str(raw_json))
    except (TypeError, ValueError):
        return {}
    flattened = _flatten_context_payload(payload)
    return _context_fields_from_mapping(flattened)


def _context_from_tags(context: Any) -> dict[str, str]:
    """
    Return context fields from Databricks context tags.
    """
    values: dict[str, str] = {}
    for field, tag_names in _CONTEXT_KEY_MAP.items():
        for tag_name in tag_names:
            if value := _context_tag(context, tag_name):
                values[field] = value
                break
    return values


def _context_from_methods(context: Any) -> dict[str, str]:
    """
    Return context fields from direct notebook context methods when available.
    """
    method_map = {
        "workspace_id": ("orgId", "workspaceId"),
        "workspace_url": ("browserHostName", "apiUrl"),
        "cluster_id": ("clusterId",),
        "job_id": ("jobId",),
        "run_id": ("currentRunId", "rootRunId", "runId", "jobRunId"),
        "task_key": ("taskKey",),
        "task_run_id": ("taskRunId", "taskRunID"),
        "task_attempt_number": ("taskAttemptNumber",),
        "job_start_time": ("jobStartTime", "jobStartTimestamp", "startTime"),
        "job_trigger_type": ("jobTriggerType", "triggerType"),
        "notebook_path": ("notebookPath",),
        "user_name": ("userName", "user"),
        "run_as_user_name": ("runAsUserName", "runAsUser", "runAs"),
    }
    values: dict[str, str] = {}
    for field, method_names in method_map.items():
        for method_name in method_names:
            if value := _context_method_value(context, method_name):
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


def _context_method_value(context: Any, method_name: str) -> str | None:
    """
    Read one optional value from a Databricks notebook context method.
    """
    try:
        method = getattr(context, method_name)
    except Exception:
        return None
    if not callable(method):
        return _extract_context_value(method)
    try:
        return _extract_context_value(method())
    except Exception:
        return None


def _context_fields_from_mapping(values: Mapping[str, Any]) -> dict[str, str]:
    """
    Map Databricks context key variants to package context fields.
    """
    output: dict[str, str] = {}
    for field, key_names in _CONTEXT_KEY_MAP.items():
        for key_name in key_names:
            if value := _extract_context_value(values.get(key_name)):
                output[field] = value
                break
    return output


def _flatten_context_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """
    Flatten the Databricks context JSON sections that contain runtime fields.
    """
    flattened: dict[str, Any] = dict(payload)
    for section_name in ("tags", "extraContext"):
        section = payload.get(section_name)
        if isinstance(section, Mapping):
            flattened.update(section)
    return flattened


def _extract_context_value(value: Any) -> str | None:
    """
    Return a simple string from Databricks Option, run id, or primitive values.
    """
    if value is None:
        return None
    try:
        if isinstance(value, Mapping):
            for key in ("id", "value", "name"):
                if extracted := _extract_context_value(value.get(key)):
                    return extracted
            return None
        if hasattr(value, "isDefined") and not value.isDefined():
            return None
        if hasattr(value, "get"):
            return _extract_context_value(value.get())
    except Exception:
        return None
    return _string_or_none(value)


def _string_or_none(value: Any) -> str | None:
    """
    Convert a context value to ``str`` while preserving missing values.
    """
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _context_value(field_name: str, value: Any) -> str | None:
    """
    Return a normalized string value for one context field.
    """
    if field_name == "workspace_url":
        return _normalize_workspace_url(value)
    return _string_or_none(value)


def _normalize_workspace_url(value: Any) -> str | None:
    """
    Normalize workspace URLs to a bare lowercase hostname.
    """
    text = _string_or_none(value)
    if text is None:
        return None
    lowered = text.lower()
    if lowered.startswith("https://"):
        text = text[8:]
    elif lowered.startswith("http://"):
        text = text[7:]
    return text.rstrip("/").split("/", 1)[0].lower()


_CONTEXT_KEY_MAP = {
    "workspace_id": ("orgId", "workspaceId", "workspace_id"),
    "workspace_url": ("browserHostName", "apiUrl", "workspaceUrl", "workspace_url"),
    "cluster_id": ("clusterId", "cluster_id"),
    "job_id": ("jobId", "job_id"),
    "run_id": (
        "jobRunId",
        "runId",
        "currentRunId",
        "rootRunId",
        "job_run_id",
        "run_id",
    ),
    "task_key": ("taskKey", "task_key"),
    "task_run_id": ("taskRunId", "task_run_id", "task.run_id"),
    "task_attempt_number": (
        "taskAttemptNumber",
        "taskAttempt",
        "jobRunAttempt",
        "jobRunOriginalAttempt",
        "task_attempt_number",
        "taskExecutionCount",
        "task_execution_count",
        "task.execution_count",
    ),
    "job_start_time": (
        "jobStartTime",
        "job_start_time",
        "job.start_time.iso_datetime",
        "startTime",
    ),
    "job_trigger_type": (
        "jobTriggerType",
        "job_trigger_type",
        "job.trigger.type",
        "triggerType",
    ),
    "notebook_path": ("notebook_path", "notebookPath", "notebookPathInWorkspace"),
    "user_name": ("user", "userName", "userEmail", "email"),
    "run_as_user_name": ("runAsUserName", "run_as_user_name", "runAsUser", "run_as"),
}
