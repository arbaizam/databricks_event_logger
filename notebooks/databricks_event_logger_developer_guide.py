# Databricks notebook source
# MAGIC %md
# MAGIC # Event logger in a few operations
# MAGIC
# MAGIC This notebook uses memory and runs as ordinary Python too. It does not need
# MAGIC Spark, dbutils, a table, or network access. See `golden_path_notebook.py` for
# MAGIC a production Databricks task.

# COMMAND ----------

import json

from databricks_event_logger import EventLogger, MemorySink

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
# MAGIC Use a logger-bound decorator. Pass the logger to reusable application code,
# MAGIC or wrap an imported function with `logger.logged_event(name)(function)`.

# COMMAND ----------


@batch_logger.logged_event("positions.total")
def total_amount(rows):
    return sum(row["amount"] for row in rows)


@batch_logger.logged_event("positions.format")
def format_total(total):
    return f"Total: {total:.2f}"


summary = format_total(total_amount(positions))

print(summary)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Observe iteration from the consumer
# MAGIC
# MAGIC Enter and exit scopes in the same execution context. A scope must not span
# MAGIC a generator's `yield`; put it around consumer iteration instead. Stopping
# MAGIC early then closes the scope promptly without recording a false failure.

# COMMAND ----------

with batch_logger.event("positions.read") as event:
    event.row_count = 0
    for position in (row for row in positions):
        event.row_count += 1
        if position["id"] == 1:
            break

assert sink.events[-1].status == "success"
assert sink.events[-1].row_count == 1

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
