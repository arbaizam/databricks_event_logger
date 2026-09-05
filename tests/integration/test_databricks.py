"""Opt-in tests against generated Delta tables in an explicitly supplied test schema.

Run on a Databricks driver with an active Spark session and
EVENT_LOGGER_TEST_SCHEMA=catalog.disposable_schema. No existing tables are modified.
"""

import json
import os
from uuid import uuid4

import pytest

from databricks_event_logger import DeltaSink, EventLogger, RuntimeContext, create_table_sql
from databricks_event_logger.errors import EventLoggerConfigurationError

pytestmark = pytest.mark.integration


@pytest.fixture
def spark_session():
    if not os.environ.get("EVENT_LOGGER_TEST_SCHEMA"):
        pytest.skip("Set EVENT_LOGGER_TEST_SCHEMA to opt into live Delta tests.")
    from pyspark.sql import SparkSession

    session = SparkSession.getActiveSession()
    if session is None:
        pytest.skip("Run these tests on a Databricks driver with an active Spark session.")
    return session


@pytest.fixture
def event_table(spark_session):
    test_schema = os.environ["EVENT_LOGGER_TEST_SCHEMA"].strip()
    table = f"{test_schema}.event_logger_test_{uuid4().hex}"
    # The production identifier validator rejects anything except catalog.schema.table.
    ddl = create_table_sql(table)
    try:
        spark_session.sql(ddl)
        yield table
    finally:
        spark_session.sql(f"DROP TABLE IF EXISTS {table}")


def test_real_delta_delivery_nulls_metrics_scopes_and_failures(spark_session, event_table):
    sink = DeltaSink(spark_session, event_table)
    sink.validate()
    logger = EventLogger(
        sink=sink, strict_logging=True, capture_error_frames=True,
        context=RuntimeContext(job_id="test-job"),
    )
    logger.record_event("simple")
    logger.record_metric("count", 7)
    with logger.event("count.rows") as event:
        event.row_count = spark_session.range(3).count()
        event.metadata["password"] = "test-only-sensitive-value"
    original = ValueError("expected test failure")
    with pytest.raises(ValueError) as raised:
        with logger.event("failed.operation"):
            raise original
    assert raised.value is original

    rows = {row.event_name: row.asDict() for row in spark_session.table(event_table).collect()}
    assert rows["simple"]["row_count"] is None
    assert rows["simple"]["job_id"] == "test-job"
    assert rows["metric.count"]["metric_value"] == 7.0
    assert rows["count.rows"]["row_count"] == 3
    assert "test-only-sensitive-value" not in rows["count.rows"]["metadata_json"]
    assert rows["failed.operation"]["status"] == "failed"
    assert rows["failed.operation"]["error_frames_json"]
    assert logger.health.succeeded == 4


def test_added_nullable_column_and_reordered_columns_are_supported(spark_session, event_table):
    spark_session.sql(f"ALTER TABLE {event_table} ADD COLUMNS (team_note STRING)")
    spark_session.sql(f"ALTER TABLE {event_table} ALTER COLUMN status FIRST")
    sink = DeltaSink(spark_session, event_table)
    sink.validate()
    logger = EventLogger(sink=sink, strict_logging=True)
    logger.record_event("schema.order", status="warning")
    row = spark_session.table(event_table).first()
    assert row.event_name == "schema.order"
    assert row.status == "warning"
    assert row.team_note is None


def test_incompatible_schema_is_detected_before_delivery(spark_session, event_table):
    # Replace this test-owned empty table with a deliberately incomplete schema.
    spark_session.sql(f"DROP TABLE {event_table}")
    spark_session.sql(f"CREATE TABLE {event_table} (event_name STRING) USING DELTA")
    with pytest.raises(EventLoggerConfigurationError):
        DeltaSink(spark_session, event_table).validate()


def test_sink_never_creates_a_missing_table(spark_session):
    from pyspark.errors import AnalysisException

    table = f"{os.environ['EVENT_LOGGER_TEST_SCHEMA'].strip()}.event_logger_test_{uuid4().hex}"
    create_table_sql(table)  # Validate the identifier without provisioning anything.
    try:
        logger = EventLogger(sink=DeltaSink(spark_session, table), strict_logging=True)
        with pytest.raises(AnalysisException):
            logger.record_event("missing.destination")
        assert logger.health.failed == 1
        assert not spark_session.catalog.tableExists(table)
    finally:
        # Clean up a regression that accidentally created this generated test table.
        spark_session.sql(f"DROP TABLE IF EXISTS {table}")


def test_context_reads_runtime_json_when_accessible(spark_session):
    from databricks_event_logger.databricks import resolve_context

    dbutils = pytest.importorskip("pyspark.dbutils").DBUtils(spark_session)
    try:
        runtime = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        payload = json.loads(runtime.toJson())
    except Exception:
        pytest.skip("This runtime restricts notebook context JSON; use explicit job parameters.")
    tags = payload.get("tags", {})
    expected_workspace = tags.get("orgId")
    if expected_workspace is None:
        pytest.skip("Runtime JSON does not expose workspace identity.")
    context = resolve_context(dbutils=dbutils, values={"task_key": "integration-test"})
    assert context.task_key == "integration-test"
    assert context.workspace_id == str(expected_workspace).strip()
