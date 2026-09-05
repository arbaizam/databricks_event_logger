import json
from types import SimpleNamespace

import pytest

from databricks_event_logger.context import RuntimeContext
from databricks_event_logger.databricks import resolve_context


def notebook_runtime(raw_json):
    context = SimpleNamespace(toJson=lambda: raw_json)
    notebook = SimpleNamespace(getContext=lambda: context)
    proxy = SimpleNamespace(notebook=lambda: notebook)
    return SimpleNamespace(
        notebook=SimpleNamespace(entry_point=SimpleNamespace(getDbutils=lambda: proxy))
    )


def test_explicit_context_normalizes_values_and_builds_links():
    context = RuntimeContext.from_mapping(
        {"job_id": 123, "run_id": 456, "workspace_url": " https://ADB-123.example.net/path "}
    )
    assert context.job_id == "123"
    assert context.workspace_url == "adb-123.example.net"
    assert context.job_run_url == "https://adb-123.example.net/jobs/123/runs/456"
    assert context.as_dict()["task_key"] is None
    assert RuntimeContext().job_url is None
    assert RuntimeContext(job_id="123").job_run_url is None


def test_context_rejects_typo_and_complex_identity_values():
    with pytest.raises(TypeError):
        RuntimeContext.from_mapping({"jobid": "123"})
    with pytest.raises(TypeError, match="job_id"):
        RuntimeContext.from_mapping({"job_id": {"id": 123}})
    with pytest.raises(TypeError, match="mapping"):
        RuntimeContext.from_mapping([])


def test_discovery_enriches_explicit_values_without_overriding_them():
    runtime = notebook_runtime(json.dumps({
        "tags": {"jobId": "123", "jobRunId": "456", "orgId": "9"},
        "extraContext": {"notebook_path": "/Workspace/task", "user": "engineer"},
    }))
    context = resolve_context(dbutils=runtime, values={"job_id": "override", "user_name": None})
    assert context.job_id == "override"
    assert context.run_id == "456"
    assert context.workspace_id == "9"
    assert context.notebook_path == "/Workspace/task"
    assert context.user_name is None


@pytest.mark.parametrize("raw_json", ["null", "[]", '"text"', "0", "invalid", "{}"])
def test_malformed_or_missing_runtime_context_preserves_explicit_values(raw_json):
    context = resolve_context(dbutils=notebook_runtime(raw_json), values={"run_id": "123"})
    assert context == RuntimeContext(run_id="123")


def test_restricted_runtime_does_not_require_spark_or_fallback_probing():
    assert resolve_context(dbutils=object()) == RuntimeContext()
    assert resolve_context(values={"task_key": "load"}) == RuntimeContext(task_key="load")
    assert resolve_context() == RuntimeContext()


def test_discovery_does_not_confuse_current_task_run_with_job_run():
    runtime = notebook_runtime('{"currentRunId":{"id":456},"tags":{"taskRunId":"789"}}')
    context = resolve_context(dbutils=runtime)
    assert context.run_id is None
    assert context.task_run_id == "789"


def test_invalid_explicit_values_are_not_swallowed_as_discovery_failures():
    with pytest.raises(TypeError):
        resolve_context(dbutils=object(), values={"runid": "123"})
    with pytest.raises(ValueError, match="workspace_url"):
        resolve_context(values={"workspace_url": "ftp://example.net"})
    with pytest.raises(TypeError, match="mapping"):
        resolve_context(values=[])
