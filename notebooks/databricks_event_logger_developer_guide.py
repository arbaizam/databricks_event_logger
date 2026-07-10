# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Event Logger Developer Guide
# MAGIC
# MAGIC This notebook is an instructional guide for `databricks-event-logger`.
# MAGIC
# MAGIC It demonstrates:
# MAGIC
# MAGIC - explicit notebook bootstrap
# MAGIC - console versus Delta sink behavior
# MAGIC - explicit events
# MAGIC - decorators
# MAGIC - context-manager logging
# MAGIC - Spark helper logging
# MAGIC - task-wrapper logging
# MAGIC - metrics
# MAGIC - conditional severity
# MAGIC - parent-child event IDs
# MAGIC - Spark action/materialization logging
# MAGIC - optional rules engine wrapping at the notebook layer
# MAGIC - event table queries
# MAGIC
# MAGIC The notebook is intentionally verbose. It is meant to teach the event logger
# MAGIC contract and serve as a Databricks smoke-test checklist.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Widget Contract
# MAGIC
# MAGIC Jobs should normally pass these values as task parameters. The widgets below
# MAGIC make the notebook runnable interactively as well.
# MAGIC
# MAGIC The four primary widgets are:
# MAGIC
# MAGIC - `app_name`
# MAGIC - `component`
# MAGIC - `environment`
# MAGIC - `observability_event_table`
# MAGIC
# MAGIC If `observability_event_table` is blank, this guide uses `ConsoleSink`.
# MAGIC Otherwise, it explicitly constructs and validates a `DeltaSink`.

# COMMAND ----------

dbutils.widgets.text("app_name", "databricks_event_logger_demo")
dbutils.widgets.text("component", "developer_guide")
dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("observability_event_table", "")
dbutils.widgets.text("correlation_id", "")

dbutils.widgets.dropdown("run_failure_demos", "false", ["false", "true"])
dbutils.widgets.dropdown("run_rules_engine_demo", "false", ["false", "true"])
dbutils.widgets.dropdown("strict_logging", "false", ["false", "true"])
dbutils.widgets.text("demo_target_table", "")
dbutils.widgets.text("rules_engine_schema", "")
dbutils.widgets.text("ruleset_name", "")
dbutils.widgets.text("ruleset_version", "")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bootstrap The Logger
# MAGIC
# MAGIC This is the standard notebook entry point:
# MAGIC
# MAGIC ```python
# MAGIC event_logger = observe_notebook(spark=spark, dbutils=dbutils)
# MAGIC ```
# MAGIC
# MAGIC Inputs are explicit. The function resolves Databricks runtime context, sets
# MAGIC the default logger, and emits `notebook.started`.

# COMMAND ----------

from databricks_event_logger import (
    ConsoleSink,
    DeltaSink,
    get_default_logger,
    observe_notebook,
    observed,
)
from databricks_event_logger.spark import count_rows, run_sql, validate_row_count, write_delta

strict_logging = dbutils.widgets.get("strict_logging").strip().lower() == "true"
event_table = dbutils.widgets.get("observability_event_table").strip()
sink = DeltaSink(spark=spark, table_name=event_table) if event_table else ConsoleSink()
if isinstance(sink, DeltaSink):
    sink.validate()

event_logger = observe_notebook(
    spark=spark,
    dbutils=dbutils,
    app_name=dbutils.widgets.get("app_name"),
    component=dbutils.widgets.get("component"),
    environment=dbutils.widgets.get("environment"),
    correlation_id=dbutils.widgets.get("correlation_id").strip() or None,
    sink=sink,
    strict_logging=strict_logging,
    default_metadata={
        "guide": "databricks_event_logger_developer_guide",
    },
)

print(f"sink: {type(event_logger.sink).__name__}")
print(f"event table: {getattr(event_logger.sink, 'table_name', None)}")
print(f"app_name: {event_logger.config.app_name}")
print(f"component: {event_logger.config.component}")
print(f"environment: {event_logger.config.environment}")
print(f"correlation_id: {event_logger.correlation_id}")
print(f"job_id: {event_logger.context.job_id}")
print(f"run_id: {event_logger.context.run_id}")
print(f"task_key: {event_logger.context.task_key}")
print(f"task_run_id: {event_logger.context.task_run_id}")
print(f"task_attempt_number: {event_logger.context.task_attempt_number}")
print(f"job_run_url: {event_logger.job_run_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verify Default Logger
# MAGIC
# MAGIC The module-level `@observed(...)` decorator uses the default logger. Calling
# MAGIC `observe_notebook(...)` configured it.

# COMMAND ----------

assert get_default_logger() is event_logger

event_logger.record_event(
    "developer_guide.bootstrap_verified",
    event_type="custom",
    status="success",
    severity="info",
    metadata={
        "sink": type(event_logger.sink).__name__,
        "event_table": getattr(event_logger.sink, "table_name", None),
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Explicit Event
# MAGIC
# MAGIC Use `record_event(...)` when you want full control over status, severity,
# MAGIC source/target table fields, row count, or metadata.

# COMMAND ----------

explicit_event = event_logger.record_event(
    "developer_guide.explicit_event",
    event_type="custom",
    status="success",
    severity="info",
    source_table="demo.source_table",
    target_table="demo.target_table",
    row_count=3,
    metadata={
        "purpose": "show explicit event fields",
        "metadata_is_json": True,
    },
)

print(explicit_event.event_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Decorator Event
# MAGIC
# MAGIC Use `@observed(...)` for functions that have a clear success/failure boundary.
# MAGIC
# MAGIC The decorator:
# MAGIC
# MAGIC - records duration
# MAGIC - records success on return
# MAGIC - records failure on exception
# MAGIC - re-raises the original exception
# MAGIC
# MAGIC Metadata on the decorator is static for that call definition. If metadata
# MAGIC needs to be computed dynamically after the function runs, use
# MAGIC `record_event(...)` after the call.

# COMMAND ----------

@observed(
    "developer_guide.decorated_transform",
    event_type="function",
    metadata={
        "input_shape": "small_demo_rows",
    },
)
def decorated_transform(rows):
    return [
        {
            "id": row["id"],
            "amount": row["amount"],
            "amount_bucket": "large" if row["amount"] >= 100 else "small",
        }
        for row in rows
    ]


demo_rows = [
    {"id": 1, "amount": 50},
    {"id": 2, "amount": 125},
    {"id": 3, "amount": 250},
]

transformed_rows = decorated_transform(demo_rows)
transformed_rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Context Manager Event
# MAGIC
# MAGIC Use `event_logger.event(...)` for code blocks, especially Spark actions and
# MAGIC writes.
# MAGIC
# MAGIC The block below creates a small DataFrame and counts it. The `count()` is the
# MAGIC Spark action. The logger does not compute row counts automatically.

# COMMAND ----------

from pyspark.sql import functions as F

demo_df = spark.createDataFrame(transformed_rows)

demo_row_count = count_rows(
    demo_df,
    spark=spark,
    event_name="developer_guide.demo_dataframe_counted",
    table_name="developer_guide.demo_df",
    action="count",
    reason="demonstrate explicit Spark action helper",
)

print(f"demo_row_count={demo_row_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Metric Event
# MAGIC
# MAGIC `metric_name` and `metric_value` are populated by `record_metric(...)`.
# MAGIC They are normally `NULL` for decorator and context-manager events.

# COMMAND ----------

event_logger.record_metric(
    "developer_guide_demo_rows",
    float(demo_row_count),
    event_name="developer_guide.metric.demo_rows",
    metadata={
        "dataframe": "demo_df",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Conditional Severity
# MAGIC
# MAGIC Use `record_event(...)` directly when severity depends on a runtime condition.
# MAGIC
# MAGIC This example marks the event as `warning` if there are any records in the
# MAGIC `large` bucket.

# COMMAND ----------

large_count = count_rows(
    demo_df.where(F.col("amount_bucket") == "large"),
    spark=spark,
    event_name="developer_guide.large_bucket_counted",
)

event_logger.record_event(
    "developer_guide.large_bucket_checked",
    event_type="validation",
    status="success",
    severity="warning" if large_count > 0 else "info",
    row_count=large_count,
    metadata={
        "condition": "amount_bucket = large",
        "warning_threshold": 0,
    },
)

print(f"large_count={large_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Parent-Child Events
# MAGIC
# MAGIC Direct events recorded inside an active context manager or decorator are
# MAGIC automatically linked to the active parent event through `parent_event_id`.

# COMMAND ----------

with event_logger.event(
    "developer_guide.parent_operation",
    event_type="custom",
    metadata={"purpose": "show parent-child event linking"},
):
    child_event = event_logger.record_event(
        "developer_guide.child_checkpoint",
        event_type="custom",
        status="success",
        metadata={"child": True},
    )

print(f"child_event_id={child_event.event_id}")
print(f"child_parent_event_id={child_event.parent_event_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Optional Delta Write Demo
# MAGIC
# MAGIC Set `demo_target_table` to a fully qualified table to test a real write.
# MAGIC Leave it blank to skip this section safely.

# COMMAND ----------

demo_target_table = dbutils.widgets.get("demo_target_table").strip()

if demo_target_table:
    lowered_target = demo_target_table.lower()
    if "sandbox" not in lowered_target and "_demo" not in lowered_target:
        raise ValueError(
            "demo_target_table must contain 'sandbox' or '_demo' to protect "
            "non-demo tables from overwrite."
        )

    write_delta(
        demo_df,
        demo_target_table,
        mode="overwrite",
        row_count=demo_row_count,
        options={"overwriteSchema": "true"},
        metadata={
            "source": "developer guide demo_df",
            "safety_check": "target name contains sandbox or _demo",
        },
    )

    event_logger.record_event(
        "developer_guide.demo_table_write_confirmed",
        event_type="validation",
        status="success",
        target_table=demo_target_table,
        metadata={"confirmation": "write call returned"},
    )

    run_sql(
        f"REFRESH TABLE {demo_target_table}",
        spark=spark,
        metadata={"reason": "developer guide write verification"},
    )
    confirmed_row_count = validate_row_count(
        demo_target_table,
        spark=spark,
        expected_exact=demo_row_count,
        metadata={"validation_name": "demo_target_row_count"},
    )
    print(f"confirmed_row_count={confirmed_row_count}")
else:
    event_logger.record_event(
        "developer_guide.demo_table_write_skipped",
        event_type="custom",
        status="skipped",
        severity="info",
        metadata={
            "reason": "demo_target_table widget is blank",
        },
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Task Wrapper
# MAGIC
# MAGIC `run_task(...)` logs a success/failure boundary around a callable. This is
# MAGIC useful when a production notebook is structured around a `main()` function.

# COMMAND ----------

def demo_main():
    event_logger.record_event(
        "developer_guide.main_checkpoint",
        event_type="custom",
        status="success",
        metadata={"inside": "demo_main"},
    )
    return "main complete"


main_result = event_logger.run_task(
    "developer_guide.main_task",
    demo_main,
    metadata={"wrapper": "run_task"},
)

print(main_result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Guarded Failure Demos
# MAGIC
# MAGIC Set `run_failure_demos=true` to intentionally emit failed events. The demo
# MAGIC catches the exception after logging so the notebook can continue.
# MAGIC
# MAGIC In production, do not catch the exception unless the workflow really can
# MAGIC continue. Letting the exception escape is what makes the Databricks task fail.

# COMMAND ----------

run_failure_demos = dbutils.widgets.get("run_failure_demos").strip().lower() == "true"

if run_failure_demos:

    @observed("developer_guide.decorated_failure", event_type="function")
    def fail_decorated():
        raise ValueError("intentional decorated failure demo")

    try:
        fail_decorated()
    except ValueError as exc:
        event_logger.record_event(
            "developer_guide.decorated_failure_caught",
            event_type="custom",
            status="success",
            severity="info",
            metadata={"caught_error": str(exc)},
        )

    try:
        with event_logger.event("developer_guide.context_failure", event_type="custom"):
            raise RuntimeError("intentional context manager failure demo")
    except RuntimeError as exc:
        event_logger.record_event(
            "developer_guide.context_failure_caught",
            event_type="custom",
            status="success",
            severity="info",
            metadata={"caught_error": str(exc)},
        )
else:
    event_logger.record_event(
        "developer_guide.failure_demos_skipped",
        event_type="custom",
        status="skipped",
        metadata={"run_failure_demos": False},
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Optional Rules Engine Pattern
# MAGIC
# MAGIC This section is disabled by default because it requires `rules_engine` and
# MAGIC configured rules engine metadata tables.
# MAGIC
# MAGIC The key point is architectural: keep `rules_engine` unchanged and decorate the
# MAGIC notebook-layer wrapper around `service.evaluate_dataframe(...)`.

# COMMAND ----------

run_rules_engine_demo = dbutils.widgets.get("run_rules_engine_demo").strip().lower() == "true"

if run_rules_engine_demo:
    from rules_engine.service import RulesEngineService

    rules_schema = dbutils.widgets.get("rules_engine_schema")
    ruleset_name = dbutils.widgets.get("ruleset_name")
    ruleset_version = dbutils.widgets.get("ruleset_version") or None
    column_prefix = "rules_engine"

    if not rules_schema or not ruleset_name:
        event_logger.record_event(
            "developer_guide.rules_engine_demo_skipped",
            event_type="custom",
            status="skipped",
            severity="info",
            metadata={
                "reason": "rules_engine_schema and ruleset_name widgets are required",
            },
        )
    else:
        service = RulesEngineService.from_schema(spark, rules_schema)

        @observed(
            "rules_engine.evaluate_dataframe",
            event_type="rules_engine",
            metadata={
                "ruleset_name": ruleset_name,
                "version": ruleset_version,
                "column_prefix": column_prefix,
                "fail_on_error": True,
            },
        )
        def evaluate_rules(input_df):
            return service.evaluate_dataframe(
                input_df,
                ruleset_name=ruleset_name,
                version=ruleset_version,
                column_prefix=column_prefix,
                fail_on_error=True,
            )

        evaluated_df = evaluate_rules(demo_df)

        with event_logger.event(
            "rules_engine.evaluate_dataframe.counted",
            event_type="rules_engine",
            metadata={
                "ruleset_name": ruleset_name,
                "version": ruleset_version,
            },
        ):
            rules_output_rows = evaluated_df.count()

        event_logger.record_metric(
            "rules_engine_output_rows",
            float(rules_output_rows),
            event_name="rules_engine.metric.output_rows",
            metadata={
                "ruleset_name": ruleset_name,
                "version": ruleset_version,
            },
        )
else:
    event_logger.record_event(
        "developer_guide.rules_engine_demo_skipped",
        event_type="custom",
        status="skipped",
        metadata={
            "run_rules_engine_demo": False,
            "guidance": "decorate notebook wrapper around service.evaluate_dataframe",
        },
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Understand Console Output
# MAGIC
# MAGIC `ConsoleSink` prints one JSON event per line in notebook output. It is the
# MAGIC library default and requires no storage configuration. Use `MemorySink`
# MAGIC explicitly in unit tests when assertions need access to emitted events.

# COMMAND ----------

print(f"Current sink: {type(event_logger.sink).__name__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Query Persisted Events
# MAGIC
# MAGIC If the sink is `DeltaSink`, query the configured event table.

# COMMAND ----------

if event_table and type(event_logger.sink).__name__ == "DeltaSink":
    display(
        spark.table(event_table)
        .where(F.col("correlation_id") == event_logger.correlation_id)
        .select(
            "event_ts",
            "event_name",
            "event_type",
            "status",
            "severity",
            "duration_ms",
            "row_count",
            "metric_name",
            "metric_value",
            "parent_event_id",
            "event_id",
            "error_class",
            "error_message",
            "metadata_json",
            "job_id",
            "run_id",
            "task_key",
            "task_run_id",
            "task_attempt_number",
        )
        .orderBy("event_ts")
    )
else:
    print("No persisted event table is configured for this run.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 16. Query Recent Failures

# COMMAND ----------

if event_table and type(event_logger.sink).__name__ == "DeltaSink":
    display(
        spark.table(event_table)
        .where(F.col("status") == "failed")
        .select(
            "event_ts",
            "event_name",
            "event_type",
            "severity",
            "error_class",
            "error_message",
            "stack_trace_hash",
            "metadata_json",
            "job_id",
            "run_id",
            "task_key",
        )
        .orderBy(F.col("event_ts").desc())
        .limit(50)
    )
else:
    print("No persisted event table is configured for this run.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 17. Query Recent Metrics

# COMMAND ----------

if event_table and type(event_logger.sink).__name__ == "DeltaSink":
    display(
        spark.table(event_table)
        .where(F.col("event_type") == "metric")
        .select(
            "event_ts",
            "event_name",
            "metric_name",
            "metric_value",
            "metadata_json",
            "job_id",
            "run_id",
            "task_key",
        )
        .orderBy(F.col("event_ts").desc())
        .limit(50)
    )
else:
    print("No persisted event table is configured for this run.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 18. End Of Guide
# MAGIC
# MAGIC Review checklist:
# MAGIC
# MAGIC - `notebook.started` exists.
# MAGIC - `developer_guide.bootstrap_verified` exists.
# MAGIC - Decorator events exist.
# MAGIC - Context manager events exist.
# MAGIC - Metric events populate `metric_name` and `metric_value`.
# MAGIC - Conditional severity event is `warning` when large rows exist.
# MAGIC - Child events have `parent_event_id`.
# MAGIC - If `DeltaSink` was used, events are visible in the Delta table.
# MAGIC - If `ConsoleSink` was used, events appear as JSON in notebook output.
