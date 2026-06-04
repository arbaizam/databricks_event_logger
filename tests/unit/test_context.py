import json

from databricks_event_logger.context import RuntimeContext, resolve_databricks_context


class FakeSparkConf:
    def __init__(self, values):
        self._values = values

    def get(self, key):
        return self._values[key]


class FakeSpark:
    def __init__(self, values):
        self.conf = FakeSparkConf(values)


def test_runtime_context_from_mapping_ignores_unknown_fields():
    """
    What: Builds context from partial mappings and ignores unknown keys.
    Why: Callers and tests may supply context dictionaries with extra fields.
    Fails when: Unknown context fields break logger initialization.
    """
    context = RuntimeContext.from_mapping({"job_id": 123, "unknown": "ignored"})

    assert context.job_id == "123"
    assert context.run_id is None


def test_context_resolver_uses_spark_config_when_available():
    """
    What: Resolves Spark configuration fields without Databricks imports.
    Why: The package should capture available context while remaining lightweight.
    Fails when: Spark-backed context resolution requires PySpark type imports.
    """
    spark = FakeSpark(
        {
            "spark.databricks.clusterUsageTags.clusterId": "cluster-1",
            "spark.databricks.clusterUsageTags.clusterOwnerOrgId": "workspace-1",
        }
    )

    context = resolve_databricks_context(spark=spark)

    assert context.cluster_id == "cluster-1"
    assert context.workspace_id == "workspace-1"


def test_context_resolver_uses_dbutils_context_json_for_job_fields():
    """
    What: Resolves job/task fields from Databricks context JSON.
    Why: Job-task metadata may appear in toJson() even when tag access is sparse.
    Fails when: Job runs emit null job_id/run_id/task_key/notebook_path fields.
    """
    payload = {
        "currentRunId": {"id": 456},
        "tags": {
            "jobId": "123",
            "taskKey": "smoke_task",
            "taskRunId": "789",
            "taskAttemptNumber": "2",
            "jobTriggerType": "one_time",
            "browserHostName": "adb-123.azuredatabricks.net",
        },
        "extraContext": {
            "job_start_time": "2026-06-04T13:00:00Z",
            "notebook_path": "/Repos/team/event_logger_smoke",
            "user": "user@example.com",
            "run_as_user_name": "svc@example.com",
        },
    }
    dbutils = FakeDbutils(FakeNotebookContext(json_payload=payload))

    context = resolve_databricks_context(dbutils=dbutils)

    assert context.job_id == "123"
    assert context.run_id == "456"
    assert context.task_key == "smoke_task"
    assert context.task_run_id == "789"
    assert context.task_attempt_number == "2"
    assert context.job_start_time == "2026-06-04T13:00:00Z"
    assert context.job_trigger_type == "one_time"
    assert context.notebook_path == "/Repos/team/event_logger_smoke"
    assert context.workspace_url == "adb-123.azuredatabricks.net"
    assert context.user_name == "user@example.com"
    assert context.run_as_user_name == "svc@example.com"


def test_context_resolver_uses_dbutils_tags_when_json_is_unavailable():
    """
    What: Resolves job/task fields from Databricks context tags.
    Why: Some runtime contexts expose tags but not parseable JSON.
    Fails when: Tag-only contexts stop populating job metadata.
    """
    dbutils = FakeDbutils(
        FakeNotebookContext(
            tags={
                "jobId": "123",
                "jobRunId": "456",
                "taskKey": "smoke_task",
                "taskRunId": "789",
                "taskAttemptNumber": "1",
                "notebookPath": "/Workspace/smoke",
            }
        )
    )

    context = resolve_databricks_context(dbutils=dbutils)

    assert context.job_id == "123"
    assert context.run_id == "456"
    assert context.task_key == "smoke_task"
    assert context.task_run_id == "789"
    assert context.task_attempt_number == "1"
    assert context.notebook_path == "/Workspace/smoke"


class FakeDbutils:
    def __init__(self, context):
        self.notebook = FakeNotebook(context)


class FakeNotebook:
    def __init__(self, context):
        self.entry_point = FakeEntryPoint(context)

    def __call__(self):
        return self

    def getContext(self):
        return self.entry_point.getDbutils().notebook().getContext()


class FakeEntryPoint:
    def __init__(self, context):
        self._context = context

    def getDbutils(self):
        return FakeDbutilsProxy(self._context)


class FakeDbutilsProxy:
    def __init__(self, context):
        self._context = context

    def notebook(self):
        return FakeNotebookProxy(self._context)


class FakeNotebookProxy:
    def __init__(self, context):
        self._context = context

    def getContext(self):
        return self._context


class FakeNotebookContext:
    def __init__(self, *, json_payload=None, tags=None):
        self._json_payload = json_payload
        self._tags = tags or {}

    def toJson(self):
        if self._json_payload is None:
            raise RuntimeError("no json")
        return json.dumps(self._json_payload)

    def tags(self):
        return FakeTags(self._tags)


class FakeTags:
    def __init__(self, values):
        self._values = values

    def get(self, key):
        if key not in self._values:
            return FakeOption(None)
        return FakeOption(self._values[key])


class FakeOption:
    def __init__(self, value):
        self._value = value

    def isDefined(self):
        return self._value is not None

    def get(self):
        return self._value
