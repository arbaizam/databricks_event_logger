# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Event Logger Golden Path
# MAGIC
# MAGIC Minimal production-oriented notebook pattern:
# MAGIC bootstrap once, use Spark helpers for observable I/O, and validate output.
# MAGIC Set `observability_event_table` before running; the production preset
# MAGIC fails fast when persistence is not configured.

# COMMAND ----------

dbutils.widgets.text("app_name", "example_app")
dbutils.widgets.text("component", "example_task")
dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("observability_event_table", "")
dbutils.widgets.text("as_of_date", "2026-06-30")

# COMMAND ----------

from databricks_event_logger import observe_notebook
from databricks_event_logger.spark import read_table, validate_row_count, write_delta
from pyspark.sql import functions as F

event_logger = observe_notebook.production_from_widgets(
    dbutils=dbutils,
    spark=spark,
    default_metadata={
        "notebook_pattern": "golden_path",
    },
)

as_of_date = dbutils.widgets.get("as_of_date")

# COMMAND ----------

source_df = read_table(
    "catalog.schema.source",
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
    expected_min=1,
    validation_name="output_not_empty",
    as_of_date=as_of_date,
)

# COMMAND ----------

display(
    spark.table(dbutils.widgets.get("observability_event_table"))
    .where(F.col("correlation_id") == event_logger.correlation_id)
    .orderBy("event_ts")
)
