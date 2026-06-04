"""
Event model definitions.

The package uses one flat event record because the target Delta table is a
single append-only event log. Optional fields stay on the record instead of
being split into separate metric/error/context types so caller code remains
simple and dashboard views can evolve independently.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any
from uuid import uuid4

from databricks_event_logger.context import RuntimeContext
from databricks_event_logger.serialization import serialize_metadata
from databricks_event_logger.timing import utc_now
from databricks_event_logger.version import __version__


@dataclass(init=False)
class EventRecord:
    """
    Structured event row written by sinks.

    Parameters
    ----------
    event_name : str
        Stable event name such as ``delta.write`` or
        ``reporting.positions_daily``.
    event_type : str
        Coarse event category used for dashboards and filtering.
    status : str
        Event status. Common values are ``started``, ``success``, ``failed``,
        ``warning``, and ``skipped``.
    app_name : str | None, default None
        Application name supplied by the job/notebook.
    component : str | None, default None
        Component name supplied by the job/notebook.
    environment : str | None, default None
        Environment or bundle target supplied by the job/notebook.
    metadata : Mapping[str, Any] | None, default None
        Caller-controlled structured metadata. It is serialized to
        ``metadata_json`` during initialization.
    context : RuntimeContext | None, default None
        Runtime context fields captured from Databricks or supplied by tests.

    Notes
    -----
    ``metadata`` is not retained as a dataclass field because the persisted
    table stores JSON. Callers that need parsed metadata can deserialize
    ``metadata_json``.
    """

    event_name: str
    event_type: str
    status: str

    event_id: str
    correlation_id: str | None
    parent_event_id: str | None

    event_ts: datetime
    event_date: date
    start_ts: datetime | None
    end_ts: datetime | None
    duration_ms: int | None

    severity: str | None

    app_name: str | None
    component: str | None
    environment: str | None
    sdk_version: str

    workspace_id: str | None
    workspace_url: str | None
    cluster_id: str | None
    job_id: str | None
    run_id: str | None
    task_key: str | None
    task_run_id: str | None
    task_attempt_number: str | None
    job_start_time: str | None
    job_trigger_type: str | None
    notebook_path: str | None
    user_name: str | None
    run_as_user_name: str | None

    source_table: str | None
    target_table: str | None
    row_count: int | None

    metric_name: str | None
    metric_value: float | None

    error_class: str | None
    error_message: str | None
    stack_trace_hash: str | None

    metadata_json: str | None
    created_at: datetime

    def __init__(
        self,
        event_name: str,
        event_type: str = "custom",
        status: str = "success",
        *,
        event_id: str | None = None,
        correlation_id: str | None = None,
        parent_event_id: str | None = None,
        event_ts: datetime | None = None,
        event_date: date | None = None,
        start_ts: datetime | None = None,
        end_ts: datetime | None = None,
        duration_ms: int | None = None,
        severity: str | None = None,
        app_name: str | None = None,
        component: str | None = None,
        environment: str | None = None,
        sdk_version: str = __version__,
        context: RuntimeContext | None = None,
        source_table: str | None = None,
        target_table: str | None = None,
        row_count: int | None = None,
        metric_name: str | None = None,
        metric_value: float | None = None,
        error_class: str | None = None,
        error_message: str | None = None,
        stack_trace_hash: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        metadata_json: str | None = None,
        created_at: datetime | None = None,
        **context_overrides: Any,
    ) -> None:
        """
        Create an event record.
        """
        # Keep defaulting here so metadata serialization and runtime context
        # flattening remain explicit and happen exactly once per event.
        resolved_event_ts = event_ts or utc_now()
        context_values = (context or RuntimeContext()).as_dict()
        context_values.update(
            {
                key: value
                for key, value in context_overrides.items()
                if key in RuntimeContext.__dataclass_fields__
            }
        )

        self.event_name = event_name
        self.event_type = event_type
        self.status = status
        self.event_id = event_id or str(uuid4())
        self.correlation_id = correlation_id
        self.parent_event_id = parent_event_id
        self.event_ts = resolved_event_ts
        self.event_date = event_date or resolved_event_ts.date()
        self.start_ts = start_ts
        self.end_ts = end_ts
        self.duration_ms = duration_ms
        self.severity = severity
        self.app_name = app_name
        self.component = component
        self.environment = environment
        self.sdk_version = sdk_version
        self.workspace_id = context_values.get("workspace_id")
        self.workspace_url = context_values.get("workspace_url")
        self.cluster_id = context_values.get("cluster_id")
        self.job_id = context_values.get("job_id")
        self.run_id = context_values.get("run_id")
        self.task_key = context_values.get("task_key")
        self.task_run_id = context_values.get("task_run_id")
        self.task_attempt_number = context_values.get("task_attempt_number")
        self.job_start_time = context_values.get("job_start_time")
        self.job_trigger_type = context_values.get("job_trigger_type")
        self.notebook_path = context_values.get("notebook_path")
        self.user_name = context_values.get("user_name")
        self.run_as_user_name = context_values.get("run_as_user_name")
        self.source_table = source_table
        self.target_table = target_table
        self.row_count = row_count
        self.metric_name = metric_name
        self.metric_value = metric_value
        self.error_class = error_class
        self.error_message = error_message
        self.stack_trace_hash = stack_trace_hash
        self.metadata_json = (
            metadata_json if metadata_json is not None else serialize_metadata(metadata)
        )
        self.created_at = created_at or utc_now()

    def as_dict(self) -> dict[str, Any]:
        """
        Return the event as a flat dictionary.

        Returns
        -------
        dict[str, Any]
            Dictionary whose keys match the event table columns.
        """
        return asdict(self)

    def as_json_dict(self) -> dict[str, Any]:
        """
        Return a JSON-friendly dictionary.

        Returns
        -------
        dict[str, Any]
            Event dictionary with dates rendered as ISO strings.
        """
        output: dict[str, Any] = {}
        for key, value in self.as_dict().items():
            if isinstance(value, datetime | date):
                output[key] = value.isoformat()
            else:
                output[key] = value
        return output
