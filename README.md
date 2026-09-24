# Databricks Event Logger

Record what an operation did, how long it took, and whether it worked. The
same small API works in Python code and in Databricks notebooks. Events can
go to the console, to memory (for tests), or to an existing Delta table.

## Quickstart

```bash
python -m pip install .
```

```python
from databricks_event_logger import EventLogger

logger = EventLogger(app_name="positions")  # Prints each event as JSON.
batch_logger = logger.bind(batch_id="close-2026-09-04")

with batch_logger.event("positions.validate") as event:
    positions = [{"id": 1, "amount": 125.0}]
    event.row_count = len(positions)
    event.metadata["total_amount"] = sum(row["amount"] for row in positions)

batch_logger.record_event("positions.ready", row_count=len(positions))
```

A `with logger.event(...)` block is called a **scope**. It emits one event when
it ends:

- If the block finishes normally, it keeps the status you set (default `success`).
- If the block raises an exception, the status is `failed`, and the exception
  is raised again exactly as it was.

Events recorded inside a scope get the scope's ID as their `parent_event_id`.
This works for nested scopes and for bound loggers too, as long as they run in
the same thread or async task.

## Add results while an operation runs

Set results on the scope as soon as you have them. The logger never counts
rows by itself, so it never starts a Spark job you didn't ask for.

```python
with logger.event("positions.check", metadata={"expected_min": 1}) as event:
    event.row_count = len(positions)
    if event.row_count == 0:
        event.status = "warning"
        event.severity = "warning"
        event.metadata["reason"] = "No positions received"
```

You can edit these fields: `metadata`, `row_count`, `status`, `severity`,
`source_table`, and `target_table`. You can read `event.event_id` inside the
block, for example to link other records to this operation. If the block
raises an exception, the status is always `failed`, whatever you set.

Rules for scopes:

- Use a new scope for each operation.
- Enter and exit a scope in the same thread or async task.
- Never keep a scope open across a generator's `yield`. See
  [Observe functions](#observe-functions) for what to do instead.

### Record data validations

Use `event_type="validation"` to record whether a calculation agrees with an
expected result. Your code performs the calculation and comparison, then sets
the event's status and stores the actual value, expected value, and rule in
metadata. Setting `status="failed"` records a mismatch; it does not raise an
exception or stop the job.

See the [data validation recipes](docs/data_validation_recipes.md) for row
counts, exact decimal totals, comparisons with a tolerance, and several checks
from one Spark aggregation. They use the existing API and storage schema.

### Shared metadata with `bind()`

`logger.bind(**metadata)` returns a new logger that adds extra metadata to
every event. The new logger shares the sink, the correlation ID, and the
delivery health with the original.

- Metadata is checked when you call `bind()` or create the logger. Traversed
  keys must be strings. Redacted values and branches beyond the depth limit
  are not traversed, so their nested keys are not checked.
- Adding or replacing top-level keys never changes the original logger.
  Nested lists and dicts are still shared, though. If you change them later,
  they're checked again when an event is written.
- If an event's own metadata uses the same key as the bound metadata, the
  event's value wins.

### Correlation IDs

A **correlation ID** groups the events of one workflow. If you don't pass
one, each logger gets a new random UUID, which is unrelated to the Databricks
run ID. To link separate tasks, pass the same `correlation_id` to each
task's logger.

## Observe functions

```python
@logger.logged_event("positions.total")
def total_amount(positions):
    return sum(row["amount"] for row in positions)


@logger.logged_event("source.fetch")
async def fetch_record(client, record_id):
    return await client.fetch(record_id)
```

For `async` functions, the event is recorded after the function has finished.
You can also use a normal `with logger.event(...)` block inside an `async`
function, including around `await` calls.

Generator functions can't be decorated, because their code runs later, bit by
bit, while the caller loops over them. Put the scope around the loop instead.
The scope then ends when the loop finishes or stops early:

```python
with logger.event("positions.read") as event:
    event.row_count = 0
    for position in read_positions():
        process_position(position)
        event.row_count += 1
```

To use a logger in reusable code, pass the logger in as an argument. To wrap
a function you imported, call `logger.logged_event("event.name")(function)`.
There is no global default logger.

## Observe work in threads

A decorated function works in worker threads. But thread pools don't pass
the current scope to the new thread by default. To keep the parent link,
make a **new context copy for every task you submit**:

```python
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context


@logger.logged_event("table.load")
def load_table(name):
    return name  # Replace with application work.


with logger.event("tables.load"):
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = [
            pool.submit(copy_context().run, load_table, name)
            for name in ["positions", "prices", "accounts"]
        ]
        results = [future.result() for future in futures]
```

Without `copy_context()`, events are still delivered, but they have no
parent. Never share one copied context between tasks that run at the same
time. For a single event, you can set the parent yourself instead:
`logger.record_event("table.loaded", parent_event_id=parent_id)`.

## Use Delta in Databricks

### 1. Create the table once

The package generates the table SQL for you:

```python
from databricks_event_logger import create_table_sql

ddl = create_table_sql("main.observability.event_log")
print(ddl)  # Review it, or add it to your deployment process.
# spark.sql(ddl) creates the table when you choose to.
```

The catalog and schema must already exist.

### 2. Set up the logger when the task starts

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

Add these notebook task parameters in the **job configuration**, not in Python
code. Databricks fills in the values before the task starts. See
[Databricks dynamic value references](https://docs.databricks.com/aws/en/jobs/dynamic-value-references).

| Parameter | Job configuration value |
| --- | --- |
| `job_id` | `{{job.id}}` |
| `run_id` | `{{job.run_id}}` |
| `task_key` | `{{task.name}}` |
| `task_run_id` | `{{task.run_id}}` |
| `workspace_id` | `{{workspace.id}}` |
| `workspace_url` | `{{workspace.url}}` |

`resolve_context` works in two steps:

1. It tries once to read the notebook's context JSON. Whatever it can't
   find stays empty.
2. It applies the values you pass in. Your values always win.

It doesn't read Spark settings or environment variables. An unknown field
name raises an error, so typos are caught. If notebook discovery is blocked
on your compute, pass the task parameters above. For links to the job and the
run, use `context.job_url` and `context.job_run_url`.

### 3. Wrap your Spark code in scopes

```python
with logger.event("positions.publish", target_table=target_table):
    output.write.format("delta").mode("append").saveAsTable(target_table)
```

Spark transformations are lazy: `spark.table(...)` only builds a plan, and
the real work happens later. So put the scope around the **action** you want
to measure, such as a write or a count.

### How Delta delivery works

- Each event is written right away, with one insert per event. The logger
  waits for the insert to finish.
- The sink never creates the table.
- `validate()` checks column names, types, and whether columns allow nulls.
  It doesn't check insert permission, table constraints, or the storage
  format.

## When logging fails

### Delivery health

`logger.health` is an immutable snapshot of delivery counts shared by a
logger and its bindings. Read it again to see later updates:

| Field | Meaning |
| --- | --- |
| `attempted` | Events the logger tried to deliver. |
| `succeeded` | Events the sink accepted. |
| `failed` | Events that couldn't be built or delivered. |
| `last_error` | The most recent failure. It stays set after later successes. |

`record_event()` returns the `EventRecord` it delivered, or `None` if delivery
failed and the failure was tolerated.

### The failure rules

Logging should never break your job.

- **Setup mistakes fail right away.** Invalid logger settings, invalid event
  fields, and metadata that isn't a mapping all raise before your work starts.
  Mappings include `dict` and `collections.UserDict`.
- **Delivery problems are tolerated by default.** Some problems can only be
  checked when the event is written: a bad metadata value, an invalid value
  set on a scope, or an ordinary sink exception. With `strict_logging=False`,
  these are counted in `health` and a warning is issued. Your code keeps
  running.
- **Interrupts propagate.** After successful work, `KeyboardInterrupt`,
  `SystemExit`, and similar delivery interrupts propagate in either mode.
- **Strict mode raises them.** With `strict_logging=True`, a logging failure
  raises after your work succeeds. In nested scopes, this stops the outer
  operation too, so the outer event is recorded as `failed`.
- **Your exception always wins.** If your code already raised an exception,
  that exception is the one you get, whatever happens during logging.

### What metadata can contain

Metadata is stored as JSON, with a size limit.

- Keys that look sensitive, such as `password`, `token`, or `secret`, have
  their values replaced with `[REDACTED]`. Fields of dataclasses, named
  tuples, and Spark `Row` objects are checked the same way.
- Dates, decimals, paths, enums, and NumPy numbers are converted
  automatically. NumPy isn't required to install the package.
- A NumPy boolean in metadata is stored as `[UNSUPPORTED]`.
- Non-ASCII text is escaped, including broken characters (lone surrogates),
  so the JSON is always valid UTF-8.

Counts must be whole numbers from 0 up to the 64-bit integer limit. Metrics
must be finite numbers. `True` and `False` are not accepted for either.

### Error details

- Exception messages are stored as shortened free text. Don't put secrets in
  exception messages.
- To see where failures happen, create the logger with
  `capture_error_frames=True`. This stores up to 20 frames in
  `error_frames_json`, each with the file name, function name, and line
  number. Source code and local variables are never stored.
- `stack_trace_hash` groups the exception type and captured locations, ignoring
  the message. The same captured stack has the same hash. Scope/decorator
  frames retain the SDK's legacy storage labels so moving those functions
  does not split existing application-failure groups. Application frames
  use actual locations; moving application code can change the hash.

### Dates and time zones

`event_date` is always the UTC date of `event_ts`. If your query session uses
another time zone, `to_date(event_ts)` can show a different date near
midnight. Filter partitions using UTC dates. The logger never changes your
Spark session's time zone.

## Examples and documentation

- [Data validation recipes](docs/data_validation_recipes.md): compare DataFrame
  calculations with expected results, record mismatches, and optionally stop work.
- [Local developer notebook](notebooks/databricks_event_logger_developer_guide.py):
  scopes, nested events, decorators, the memory sink, and delivery health.
- [Production notebook](notebooks/golden_path_notebook.py): job context, a
  Spark write, and a check of today's output.
- [Event queries](docs/event_queries.sql): a workflow timeline, recent
  failures, and duration summaries.
- [API and architecture](docs/databricks-event-logger-design-spec.md): how
  the package works inside, and which module does what.
- [Contributing](CONTRIBUTING.md): how to set up, test, and extend the code.
- [Databricks validation](docs/databricks_validation.md): how to run the live
  tests and measure how long delivery takes.

## Development

```bash
python -m pip install -e ".[dev]"
python -m pytest tests/unit -q
python -m ruff check .
python -m build --wheel
```

- The installed package has no runtime dependencies. The development extras
  include NumPy, for testing NumPy number handling.
- Install `.[dev,spark-test]` to also run the local PySpark schema tests.
- Live integration tests need an active Databricks Spark session and a test
  schema that you choose. Follow the [validation guide](docs/databricks_validation.md).
- Local tests can't check Unity Catalog permissions, Delta transactions, or
  serverless compatibility.
