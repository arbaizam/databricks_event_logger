"""One best-effort notebook JSON lookup plus explicit job/task parameters."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from databricks_event_logger.context import RuntimeContext

_JSON_NAMES = {
    "workspace_id": "orgId",
    "workspace_url": "browserHostName",
    "cluster_id": "clusterId",
    "job_id": "jobId",
    "run_id": "jobRunId",
    "task_key": "taskKey",
    "task_run_id": "taskRunId",
    "task_attempt_number": "taskAttemptNumber",
    "job_start_time": "jobStartTime",
    "job_trigger_type": "jobTriggerType",
    "notebook_path": "notebook_path",
    "user_name": "user",
    "run_as_user_name": "runAsUserName",
}


def resolve_context(
    *, dbutils: Any = None, values: Mapping[str, Any] | None = None
) -> RuntimeContext:
    """Enrich explicit context with supported notebook JSON fields.

    Explicit canonical fields win, including None. Unknown explicit fields raise
    rather than silently hiding typos. Missing, inaccessible, or malformed runtime
    JSON contributes nothing. No environment, Spark-conf, tag-method, or caller
    inspection fallbacks are attempted. Pass job/task parameters for guaranteed
    identity on runtimes where notebook context JSON is restricted.
    """
    if values is not None and not isinstance(values, Mapping):
        raise TypeError("values must be a mapping or None.")
    discovered: dict[str, Any] = {}
    if dbutils is not None:
        try:
            context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
            payload = json.loads(context.toJson())
            if isinstance(payload, dict):
                flattened = dict(payload)
                for section in ("tags", "extraContext"):
                    if isinstance(payload.get(section), dict):
                        flattened.update(payload[section])
                for name, json_name in _JSON_NAMES.items():
                    value = flattened.get(name, flattened.get(json_name))
                    if type(value) in (str, int) and str(value).strip():
                        discovered[name] = value
                discovered = RuntimeContext.from_mapping(discovered).as_dict()
        except Exception:
            discovered = {}
    discovered.update(values or {})
    return RuntimeContext.from_mapping(discovered)
