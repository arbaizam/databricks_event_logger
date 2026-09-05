# Databricks notebook source
# MAGIC %md
# MAGIC # Publish and validate one business date
# MAGIC
# MAGIC Install the package wheel and provision the event table before this task.
# MAGIC Set the table parameters and `as_of_date`. In the job configuration, map
# MAGIC `job_id` to `{{job.id}}`, `run_id` to `{{job.run_id}}`, `task_key` to
# MAGIC `{{task.name}}`, and `task_run_id` to `{{task.run_id}}`.
# MAGIC Set `workspace_id` to `{{workspace.id}}` and `workspace_url` to `{{workspace.url}}`.
# MAGIC This example expects source columns `id`, `amount`, and date column `AsOfDate`.

# COMMAND ----------

for name, default in {
    "app_name": "positions",
    "component": "publish",
    "environment": "dev",
    "observability_event_table": "",
    "source_table": "",
    "target_table": "",
    "as_of_date": "",
    "correlation_id": "",
    "job_id": "",
    "run_id": "",
    "task_key": "",
    "task_run_id": "",
    "workspace_id": "",
    "workspace_url": "",
}.items():
    dbutils.widgets.text(name, default)

# COMMAND ----------

from datetime import date

from pyspark.sql import functions as F

from databricks_event_logger import DeltaSink, EventLogger
from databricks_event_logger.databricks import resolve_context

parameters = dbutils.widgets.getAll()
for name in ("observability_event_table", "source_table", "target_table", "as_of_date"):
    if not parameters[name].strip():
        raise ValueError(f"{name} is required")

as_of_date = date.fromisoformat(parameters["as_of_date"])
source_table = parameters["source_table"].strip()
target_table = parameters["target_table"].strip()
event_table = parameters["observability_event_table"].strip()

context = resolve_context(
    dbutils=dbutils,
    values={
        name: parameters[name]
        for name in ("workspace_id", "workspace_url", "job_id", "run_id", "task_key", "task_run_id")
        if parameters[name].strip()
    },
)
sink = DeltaSink(spark=spark, table_name=event_table)
sink.validate()

logger = EventLogger(
    app_name=parameters["app_name"],
    component=parameters["component"],
    environment=parameters["environment"],
    context=context,
    correlation_id=parameters["correlation_id"].strip() or context.run_id,
    sink=sink,
    capture_error_frames=True,
).bind(as_of_date=as_of_date.isoformat())

# COMMAND ----------

with logger.event("positions.process"):
    with logger.event(
        "positions.publish",
        source_table=source_table,
        target_table=target_table,
    ):
        output = (
            spark.table(source_table)
            .where(F.col("AsOfDate") == F.lit(as_of_date))
            .select("id", "amount", "AsOfDate")
        )
        (
            output.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"AsOfDate = DATE '{as_of_date.isoformat()}'")
            .saveAsTable(target_table)
        )

    with logger.event(
        "positions.validate",
        source_table=target_table,
        metadata={"expected_min": 1},
    ) as event:
        # Count the date just written. Rows from other dates cannot satisfy this check.
        event.row_count = (
            spark.table(target_table)
            .where(F.col("AsOfDate") == F.lit(as_of_date))
            .count()
        )
        if event.row_count < 1:
            raise ValueError("No output rows for the requested business date")

# COMMAND ----------

print(logger.health)
display(
    spark.table(event_table)
    .where(F.col("correlation_id") == logger.correlation_id)
    .orderBy("event_ts")
)
