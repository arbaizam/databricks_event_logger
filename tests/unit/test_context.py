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
