"""Build a ``RuntimeContext`` from a Databricks notebook plus explicit values.

``resolve_context`` combines two sources. Explicit values always win:

1. **Discovered values:** one attempt to read the notebook's context JSON
   through ``dbutils``. An ordinary exception makes it contribute nothing;
   interrupts such as KeyboardInterrupt still propagate.
   This is common on restricted or serverless compute.
2. **Explicit values:** values you pass in, usually job task parameters such
   as ``{{job.run_id}}``. Valid explicit values bypass discovery, so use them for
   reliable job and task IDs.

No other sources are checked. That includes environment variables, the Spark
config, and stack inspection.

To discover a new field, add it to ``RuntimeContext`` first, then map it to
its notebook JSON key in ``_NOTEBOOK_JSON_KEYS`` below.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from databricks_event_logger.context import RuntimeContext

# RuntimeContext field name -> key in the notebook context JSON.
_NOTEBOOK_JSON_KEYS = {
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

# Sections of the notebook JSON whose keys are merged into the top level.
_NESTED_SECTIONS = ("tags", "extraContext")


def resolve_context(
    *, dbutils: Any = None, values: Mapping[str, Any] | None = None
) -> RuntimeContext:
    """Return a ``RuntimeContext`` from notebook discovery plus explicit values.

    Example::

        context = resolve_context(
            dbutils=dbutils,
            values={"job_id": dbutils.widgets.get("job_id")},
        )

    Args:
        dbutils: The notebook's ``dbutils`` object, or ``None`` to skip discovery.
        values: Explicit values keyed by ``RuntimeContext`` field name. They
            override discovered values, and an explicit ``None`` clears one.

    Raises:
        TypeError: Non-mapping values, unknown explicit keys or invalid explicit
            identifier types, as described by RuntimeContext.
        ValueError: An invalid explicit workspace hostname/URL.
        BaseException: Discovery interrupts such as KeyboardInterrupt.

    Returns:
        A RuntimeContext. Wrong-typed/blank discovered values are skipped.
        If normalization of accepted discovered values fails (for example an
        invalid workspace URL), all discovery is discarded before explicit
        values are applied. Explicit None clears a discovered value.
    """
    if values is not None and not isinstance(values, Mapping):
        raise TypeError("values must be a mapping or None.")
    merged = _discover(dbutils) if dbutils is not None else {}
    merged.update(values or {})
    return RuntimeContext.from_mapping(merged)


def _discover(dbutils: Any) -> dict[str, Any]:
    """Read ``RuntimeContext`` fields from the notebook context JSON.

    Returns an empty dict for ordinary lookup/normalization errors, such as
    no access, bad JSON or an invalid workspace URL. Interrupts propagate.
    """
    try:
        notebook_context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        payload = json.loads(notebook_context.toJson())
        if not isinstance(payload, dict):
            return {}

        flat = dict(payload)
        for section in _NESTED_SECTIONS:
            if isinstance(payload.get(section), dict):
                flat.update(payload[section])

        found = {}
        for field_name, json_key in _NOTEBOOK_JSON_KEYS.items():
            value = flat.get(field_name, flat.get(json_key))
            if type(value) in (str, int) and str(value).strip():
                found[field_name] = value
        # Check the discovered values now, so a bad one is dropped here
        # instead of breaking the final RuntimeContext.
        return RuntimeContext.from_mapping(found).as_dict()
    except Exception:
        return {}
