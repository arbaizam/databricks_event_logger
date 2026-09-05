# Databricks Event Logger

Record what an operation did, how long it took, and whether it succeeded. Use the
same small API in Python code and Databricks notebooks; choose console, memory,
or an existing Delta table for delivery.

## Quickstart

```bash
python -m pip install .
```

```python
from databricks_event_logger import EventLogger

logger = EventLogger(app_name="positions")  # Prints JSON to the console.
batch_logger = logger.bind(batch_id="close-2026-09-04")

with batch_logger.event("positions.validate") as event:
    positions = [{"id": 1, "amount": 125.0}]
    event.row_count = len(positions)
    event.metadata["total_amount"] = sum(row["amount"] for row in positions)

batch_logger.record_event("positions.ready", row_count=len(positions))
```

The scope emits one event when it exits. A normal exit records `success`; an
exception records `failed` and propagates the original exception. Nested scopes
and direct events record the active scope's ID in `parent_event_id`.

## Enrich an operation as it runs

Set the result once the operation has produced it. Counting rows is always an
explicit application operation; the logger never triggers a Spark count.

```python
with logger.event("positions.check", metadata={"expected_min": 1}) as event:
    event.row_count = len(positions)
    if event.row_count == 0:
        event.status = "warning"
        event.severity = "warning"
        event.metadata["reason"] = "No positions received"
```

The editable fields are `metadata`, `row_count`, `status`, `severity`,
`source_table`, and `target_table`. `event_id` is available during the block for
correlating other records. An exception always produces a failed outcome.
Use a fresh scope for each operation.

`logger.bind(**metadata)` returns a logger with additional business context. It
shares the sink, correlation ID, and delivery health. Top-level metadata changes
are isolated; nested mutable values remain shared. Event metadata overrides bound
values. Pass the same explicit `correlation_id` to separate tasks that belong to
one workflow. An omitted correlation ID becomes a new UUID; it is independent of Databricks run
identity.

## Observe functions

```python
@logger.logged_event("positions.total")
def total_amount(positions):
    return sum(row["amount"] for row in positions)


@logger.logged_event("source.fetch")
async def fetch_record(client, record_id):
    return await client.fetch(record_id)
```

Async decorators await the function before recording its outcome. You can also
use an ordinary `with logger.event(...)` block inside an async function across
`await` calls. Generator and async-generator functions are rejected because
their execution continues during iteration.

For imported application functions that cannot receive a logger directly,
`@observed("event.name")` resolves the logger installed by `with
use_logger(logger):` at call time. The previous logger is restored on exit. This
is optional; explicit logger instances work everywhere.

## Use Delta in Databricks

Provision the event table once, using SQL generated from the package's storage
schema:

```python
from databricks_event_logger import create_table_sql

ddl = create_table_sql("main.observability.event_log")
print(ddl)  # Review or put this SQL in your deployment process.
# spark.sql(ddl) executes the creation when you choose to provision it.
```

The catalog and schema must already exist. At task startup, choose the existing
table and supply runtime identity:

```python
from databricks_event_logger import DeltaSink, EventLogger
from databricks_event_logger.databricks import resolve_context

sink = DeltaSink(spark=spark, table_name="main.observability.event_log")
sink.validate()

context = resolve_context(
    dbutils=dbutils,
    values={
        "workspace_id": dbutils.widgets.get("workspace_id"),
        "workspace_url": dbutils.widgets.get("workspace_url"),
        "job_id": dbutils.widgets.get("job_id"),
        "run_id": dbutils.widgets.get("run_id"),
        "task_key": dbutils.widgets.get("task_key"),
        "task_run_id": dbutils.widgets.get("task_run_id"),
    },
)
logger = EventLogger(
    sink=sink,
    context=context,
    app_name="positions",
    component="publish",
    environment="prod",
    correlation_id=context.run_id,
)
```

Configure these notebook task parameters in the job configuration. Databricks
substitutes them before the task starts; the expressions do not belong in Python
code. See [Databricks dynamic value references](https://docs.databricks.com/aws/en/jobs/dynamic-value-references).

| Parameter | Job configuration value |
| --- | --- |
| `job_id` | `{{job.id}}` |
| `run_id` | `{{job.run_id}}` |
| `task_key` | `{{task.name}}` |
| `task_run_id` | `{{task.run_id}}` |
| `workspace_id` | `{{workspace.id}}` |
| `workspace_url` | `{{workspace.url}}` |

The resolver makes one best-effort attempt to read notebook context JSON, then
applies explicit canonical field values. Unknown explicit fields are errors;
missing discovered fields stay empty. It does not inspect Spark configuration or
environment variables. Use explicit task parameters when notebook discovery is
unavailable. Navigation URLs are available as `context.job_url` and
`context.job_run_url`.

Write ordinary Spark code inside a scope:

```python
with logger.event("positions.publish", target_table=target_table):
    output.write.format("delta").mode("append").saveAsTable(target_table)
```

Spark transformations are lazy: a scope around `spark.table(...)` measures plan
construction. Put the scope around the write, count, or other action whose
execution you want to observe.

Delta delivery is immediate and synchronous, with one insert per event. The sink
never creates its target table automatically. `validate()` checks column names,
types, and nullability compatibility; it does not establish insert permission,
table constraints, or the storage provider.

## Delivery and failure behavior

`logger.health` is an immutable snapshot with `attempted`, `succeeded`, `failed`,
and `last_error` fields. The last error remains available after later successes.
`record_event()` returns the emitted `EventRecord`, or `None` after a tolerated
preparation or delivery failure.

With the default `strict_logging=False`, internal logging failures are recorded
in health and do not fail business work. Invalid setup and call arguments raise
before work starts. Editable scope fields are checked on exit; invalid edits
follow the same preparation-failure policy as other internal logging failures.
With `strict_logging=True`, a logging failure after successful business work
raises. An exception already raised by business code is always preserved.

Metadata is bounded JSON with sensitive-key redaction. Exception messages are
bounded free text, so avoid putting secrets in exception messages. For useful
failure locations, enable `capture_error_frames=True` when constructing the
logger. This stores up to 20 selected frames with file basename, function, and
line number in `error_frames_json`; it captures neither source text nor local
variables. `stack_trace_hash` groups the exception type and frame locations.

## Examples and development

- [Local developer notebook](notebooks/databricks_event_logger_developer_guide.py):
  editable scopes, nested events, decorators, memory sink, and delivery health.
- [Production notebook](notebooks/golden_path_notebook.py): explicit job context,
  native Spark write, and validation of the current date's output.
- [Event queries](docs/event_queries.sql): a correlation timeline, recent failures,
  and duration summaries.
- [API and architecture](docs/databricks-event-logger-design-spec.md): the compact
  implementation contract.

```bash
python -m pip install -e ".[dev]"
python -m pytest tests/unit -q
python -m ruff check .
python -m build --wheel
```

Install `.[dev,spark-test]` to include local PySpark schema tests. The integration
suite requires an active Databricks Spark session and an explicitly chosen test
schema:

From a Databricks Python notebook with this repository as its working directory:

```python
import os

import pytest

os.environ["EVENT_LOGGER_TEST_SCHEMA"] = "main.event_logger_tests"
result = pytest.main(["tests/integration", "-q"])
assert result == 0
```

Run pytest in the notebook's Python process so it can use the active Spark
session. Integration tests create uniquely named test tables within the chosen
schema and remove only those generated tables afterward. Without the variable
or an active session, those tests are skipped.
