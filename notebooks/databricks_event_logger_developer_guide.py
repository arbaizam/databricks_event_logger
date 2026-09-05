# Databricks notebook source
# MAGIC %md
# MAGIC # Event logger in a few operations
# MAGIC
# MAGIC This notebook uses memory and runs as ordinary Python too. It does not need
# MAGIC Spark, dbutils, a table, or network access. See `golden_path_notebook.py` for
# MAGIC a production Databricks task.

# COMMAND ----------

import json

from databricks_event_logger import EventLogger, MemorySink, observed, use_logger

sink = MemorySink()
logger = EventLogger(
    app_name="positions",
    component="tutorial",
    environment="local",
    sink=sink,
    capture_error_frames=True,
)
batch_logger = logger.bind(batch_id="close-2026-09-04")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Attach results and inspect parent relationships
# MAGIC
# MAGIC Each scope emits one outcome on exit. Counts and metadata can be filled in
# MAGIC after the business operation produces its result.

# COMMAND ----------

with batch_logger.event("positions.process") as process:
    with batch_logger.event("positions.validate") as validation:
        positions = [{"id": 1, "amount": 125.0}, {"id": 2, "amount": 25.0}]
        validation.row_count = len(positions)
        validation.metadata["total_amount"] = sum(row["amount"] for row in positions)

    batch_logger.record_event("positions.ready", row_count=len(positions))

assert sink.events[0].parent_event_id == process.event_id
assert sink.events[1].parent_event_id == process.event_id
assert sink.events[2].parent_event_id is None
assert json.loads(sink.events[0].metadata_json)["batch_id"] == "close-2026-09-04"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Choose an outcome without throwing an exception

# COMMAND ----------

with batch_logger.event("positions.optional_enrichment") as event:
    event.status = "skipped"
    event.metadata["reason"] = "No enrichment source configured"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Observe a function
# MAGIC
# MAGIC Use an instance decorator when a logger is available. For imported code,
# MAGIC an optional scoped default resolves at invocation and is restored on exit.

# COMMAND ----------


@batch_logger.logged_event("positions.total")
def total_amount(rows):
    return sum(row["amount"] for row in rows)


@observed("positions.format")
def format_total(total):
    return f"Total: {total:.2f}"


with use_logger(batch_logger):
    summary = format_total(total_amount(positions))

print(summary)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Failures retain the original exception
# MAGIC
# MAGIC Exception messages are bounded free text. The optional captured frames
# MAGIC contain file basename, function, and line number, with no source or locals.

# COMMAND ----------

try:
    with batch_logger.event("positions.required_field"):
        raise ValueError("Required business date is missing")
except ValueError as error:
    print(f"Business code received: {error}")

failure = sink.events[-1]
assert failure.status == "failed"
assert failure.error_class == "ValueError"
print(failure.error_frames_json)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Inspect delivery health and serialized events
# MAGIC
# MAGIC Health counts delivery, so a successfully recorded business failure counts
# MAGIC as a successful delivery. Bound loggers share the same health state.

# COMMAND ----------

print(logger.health)
assert logger.health.succeeded == len(sink.events)
assert logger.health.failed == 0
for record in sink.events:
    print(json.dumps(record.as_dict(), default=str))
