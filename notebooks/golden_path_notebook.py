# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Event Logger Golden Path
# MAGIC
# MAGIC Minimal production-oriented notebook pattern:
# MAGIC bootstrap once, use Spark helpers for observable I/O, and validate output.
# MAGIC Set `observability_event_table` before running. Persistence is explicit:
# MAGIC this notebook constructs and validates its Delta sink before bootstrap.

# COMMAND ----------

dbutils.widgets.text("app_name", "example_app")
dbutils.widgets.text("component", "example_task")
dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("observability_event_table", "")
dbutils.widgets.text("correlation_id", "")
dbutils.widgets.text("as_of_date", "2026-06-30")

# COMMAND ----------

from pyspark.sql import functions as F

from databricks_event_logger import DeltaSink, observe_notebook
from databricks_event_logger.spark import read_table, validate_row_count, write_delta

event_table = dbutils.widgets.get("observability_event_table").strip()
if not event_table:
    raise ValueError("observability_event_table is required")

sink = DeltaSink(spark=spark, table_name=event_table)
sink.validate()

event_logger = observe_notebook(
    spark=spark,
    dbutils=dbutils,
    app_name=dbutils.widgets.get("app_name"),
    component=dbutils.widgets.get("component"),
    environment=dbutils.widgets.get("environment"),
    correlation_id=dbutils.widgets.get("correlation_id").strip() or None,
    sink=sink,
    strict_logging=True,
    default_metadata={
        "notebook_pattern": "golden_path",
    },
)

as_of_date = dbutils.widgets.get("as_of_date")

# COMMAND ----------

source_df = read_table(
    "catalog.schema.source",
    spark=spark,
    as_of_date=as_of_date,
)

output_df = source_df.select("id", "amount", "AsOfDate")

write_delta(
    output_df,
    table="catalog.schema.output",
    mode="overwrite",
    replace_where=f"AsOfDate = DATE '{as_of_date}'",
    as_of_date=as_of_date,
)

validate_row_count(
    table="catalog.schema.output",
    spark=spark,
    expected_min=1,
    validation_name="output_not_empty",
    as_of_date=as_of_date,
)

# COMMAND ----------

display(
    spark.table(event_table)
    .where(F.col("correlation_id") == event_logger.correlation_id)
    .orderBy("event_ts")
)
