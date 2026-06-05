# Databricks Event Logger

`databricks-event-logger` is a small Python package for structured runtime event
logging in Databricks notebooks, jobs, and tasks.

It is intentionally lightweight. The package does not manage Unity Catalog
permissions, retention policies, job definitions, or bundle deployment. Those
remain platform and Asset Bundle concerns. The package owns runtime event
construction, timing, failure capture, parent-child relationships, and sink
dispatch.

The package import name is:

```python
import databricks_event_logger
```

The distribution name is:

```text
databricks-event-logger
```

The v1 design background is in
[docs/databricks-event-logger-design-spec.md](docs/databricks-event-logger-design-spec.md).

## Table Of Contents

- [Core Idea](#core-idea)
- [What This Package Does](#what-this-package-does)
- [What This Package Does Not Do](#what-this-package-does-not-do)
- [Installation](#installation)
- [Event Table Setup](#event-table-setup)
- [Required Notebook Widgets And Task Parameters](#required-notebook-widgets-and-task-parameters)
- [Quickstart](#quickstart)
- [How Sink Selection Works](#how-sink-selection-works)
- [Production Controls](#production-controls)
- [Observability Readiness Check](#observability-readiness-check)
- [Public API](#public-api)
- [Spark Helper API](#spark-helper-api)
- [Instrumentation Decision Tree](#instrumentation-decision-tree)
- [Notebook Patterns](#notebook-patterns)
- [Job And Task Patterns](#job-and-task-patterns)
- [Package-Level Decorator Pattern](#package-level-decorator-pattern)
- [Rules Engine Integration Pattern](#rules-engine-integration-pattern)
- [Metrics, Row Counts, And Spark Laziness](#metrics-row-counts-and-spark-laziness)
- [Historical AsOfDate Reload Pattern](#historical-asofdate-reload-pattern)
- [Conditional Severity](#conditional-severity)
- [Parent And Correlation IDs](#parent-and-correlation-ids)
- [Failure Behavior](#failure-behavior)
- [Metadata Guidance](#metadata-guidance)
- [Querying The Event Table](#querying-the-event-table)
- [Troubleshooting](#troubleshooting)
- [Development And Testing](#development-and-testing)

## Core Idea

Databricks system tables tell you what job or task ran. This package tells you
what the code did inside that run.

Examples of events this package can emit:

- `notebook.started`
- `rules_engine.evaluate_dataframe`
- `rules_engine.evaluate_dataframe.materialized`
- `rules_engine.metric.output_rows`
- `validation.row_errors_checked`
- `positions.write_delta`
- `positions.publish_snapshot`

Each event is a structured row with:

- stable event identity fields
- Databricks job/task context when available
- timing fields
- optional table fields
- optional metric fields
- optional error fields
- caller-controlled JSON metadata

The common runtime shape is:

```python
from databricks_event_logger import observe_notebook

event_logger = observe_notebook.from_widgets(dbutils=dbutils, spark=spark)
```

After that call, notebook code can use:

```python
from databricks_event_logger import observed

@observed("my.operation")
def work():
    ...
```

or:

```python
with event_logger.event("my.spark.write"):
    df.write.saveAsTable("catalog.schema.table")
```

or:

```python
event_logger.record_event("my.checkpoint", metadata={"step": "done"})
event_logger.record_metric("output_rows", 123)
```

## What This Package Does

The package provides:

- `EventLogger`: the explicit logger object.
- `observe_notebook`: notebook bootstrap helper.
- `observe_notebook.from_widgets(...)`: Databricks widget-driven bootstrap.
- `observed(...)`: module-level decorator using the current default logger.
- `EventLogger.logged_event(...)`: logger-bound decorator.
- `EventLogger.event(...)`: context manager for custom code blocks.
- `EventLogger.run_task(...)`: task/main wrapper.
- `EventLogger.record_event(...)`: explicit event logging.
- `EventLogger.record_metric(...)`: explicit metric logging.
- `databricks_event_logger.spark.read_table(...)`: logged Spark table read helper.
- `databricks_event_logger.spark.write_delta(...)`: logged Delta write helper.
- `databricks_event_logger.spark.run_sql(...)`: logged Spark SQL helper.
- `databricks_event_logger.spark.validate_row_count(...)`: logged row-count validation.
- `databricks_event_logger.spark.count_rows(...)`: explicit logged Spark count helper.
- `databricks_event_logger.spark.table_exists(...)`: logged table-existence smoke check.
- `MemorySink`: in-memory events for tests and dry runs.
- `ConsoleSink`: JSON-line event output for debugging.
- `DeltaSink`: immediate Delta writes for Databricks persistence.

## What This Package Does Not Do

The package does not:

- create catalogs or schemas
- grant table permissions
- configure retention
- deploy Asset Bundles
- publish the wheel to a package feed
- automatically count Spark DataFrame rows
- automatically infer full data lineage
- monkey-patch Spark
- force internal logging into domain packages such as `rules_engine`
- make logging failures fail successful business code

That separation is deliberate:

```text
Asset Bundles and platform setup configure the environment.
The Python package emits runtime events.
```

## Installation

In Databricks, install the wheel using your Asset Bundle or cluster/job library
configuration. A typical development build is:

```powershell
python -m build --wheel
```

Then attach the resulting wheel to the Databricks job/task or configure it as an
Asset Bundle artifact.

The package has no required third-party runtime dependencies. `pyspark` is only
needed when using `DeltaSink`, and that import happens inside the Delta sink path
in Databricks.

Minimum runtime: use Python 3.10 or later. Confirm that the target Databricks
Runtime uses Python 3.10+ before attaching the wheel.

## Event Table Setup

Create the event table before using `DeltaSink`.

The template DDL lives in
[resources/sql/create_event_log.sql](resources/sql/create_event_log.sql).
Optional dashboard view templates live in
[resources/sql/create_event_log_views.sql](resources/sql/create_event_log_views.sql).

Migration setup order:

1. Choose simple Unity Catalog object names made from letters, numbers, and
   underscores.
2. Create the target catalog.
3. Create the target schema.
4. Substitute `${observability_event_table}` with the fully qualified table name.
5. Run [resources/sql/create_event_log.sql](resources/sql/create_event_log.sql).
6. Grant the job run-as principal table write permission.
7. Grant read/describe permission when using `validate_sink=True` or dashboards.
8. Pass the table name as the `observability_event_table` task parameter.

Replace `${observability_event_table}` with your fully qualified table name, for
example:

```text
dev_observability.observability.event_log
```

For V1, `DeltaSink` accepts only simple three-part Unity Catalog identifiers
made from letters, numbers, and underscores:

```text
catalog.schema.table
```

Names requiring backticks or other special characters are intentionally rejected
because the target table identifier is used in SQL `INSERT` statements. Use
plain three-part UC object names such as
`dev_observability.observability.event_log`.

The SQL resource files are templates. `${observability_event_table}` and the
view-name placeholders must be substituted by Asset Bundle deployment or
manually replaced before running the SQL in a notebook or SQL editor.

The table columns are:

```sql
CREATE TABLE IF NOT EXISTS <catalog>.<schema>.event_log (
  event_name STRING NOT NULL,
  event_type STRING NOT NULL,
  status STRING NOT NULL,
  event_id STRING NOT NULL,
  correlation_id STRING,
  parent_event_id STRING,
  event_ts TIMESTAMP NOT NULL,
  event_date DATE NOT NULL,
  start_ts TIMESTAMP,
  end_ts TIMESTAMP,
  duration_ms BIGINT,
  severity STRING,
  app_name STRING,
  component STRING,
  environment STRING,
  sdk_version STRING NOT NULL,
  workspace_id STRING,
  workspace_url STRING,
  cluster_id STRING,
  job_id STRING,
  run_id STRING,
  task_key STRING,
  task_run_id STRING,
  task_attempt_number STRING,
  job_start_time STRING,
  job_trigger_type STRING,
  notebook_path STRING,
  user_name STRING,
  run_as_user_name STRING,
  source_table STRING,
  target_table STRING,
  row_count BIGINT,
  metric_name STRING,
  metric_value DOUBLE,
  error_class STRING,
  error_message STRING,
  stack_trace_hash STRING,
  metadata_json STRING,
  created_at TIMESTAMP NOT NULL
)
USING DELTA;
```

Permissions are handled outside the package. At minimum, the job's run-as
principal needs permission to append rows to the event table. With Unity
Catalog, that normally means `USE CATALOG`, `USE SCHEMA`, and table write
permission such as `MODIFY`. When using `production_from_widgets(...)` or
`validate_sink=True`, the run-as principal must also be able to run
`DESCRIBE TABLE` against the event table, which commonly means `SELECT` or
ownership depending on workspace policy. Dashboard and query readers also need
read permission.

## Required Notebook Widgets And Task Parameters

`observe_notebook.from_widgets(...)` reads standard Databricks widgets. The
first four identify the event stream and choose the sink:

| Widget | Required for Delta persistence | Purpose |
|---|---:|---|
| `app_name` | Recommended | Application or product name stamped on every event. |
| `component` | Recommended | Job, task, notebook, or component name. |
| `environment` | Recommended | Environment such as `dev`, `test`, `prod`, or bundle target. |
| `observability_event_table` | Yes | Fully qualified Delta table for persisted event writes. |

The remaining widgets are optional context fallbacks. They are useful because
some Databricks context values are not consistently available through notebook
context APIs in every execution mode.

| Widget | Recommended dynamic value |
|---|---|
| `workspace_id` | `{{workspace.id}}` |
| `workspace_url` | `{{workspace.url}}` |
| `job_id` | `{{job.id}}` |
| `run_id` | `{{job.run_id}}` |
| `task_key` | `{{task.name}}` |
| `task_run_id` | `{{task.run_id}}` |
| `task_attempt_number` | `{{task.execution_count}}` |
| `job_start_time` | `{{job.start_time.iso_datetime}}` |
| `job_trigger_type` | `{{job.trigger.type}}` |
| `notebook_path` | `{{task.notebook_path}}` |
| `run_as_user_name` | Supply explicitly if useful for your platform. |

Databricks dynamic value references must be passed into the task as parameters.
They are not directly evaluated inside notebook code. Databricks documents the
supported values here:
[Databricks dynamic value references](https://docs.databricks.com/aws/en/jobs/dynamic-value-references).
Verify `{{task.run_id}}` for the task types you plan to use; if a dynamic value
does not resolve, the logger falls back to the other available run context.

When both widgets/task parameters and heuristic runtime context are available,
the explicit widget/task-parameter values win. This keeps the job contract
stable across Databricks runtime modes.

`task_attempt_number` is stored as a string in the event table for consistency
with other Databricks context fields. Cast it when you need numeric comparisons:

```sql
CAST(task_attempt_number AS INT) > 1
```

### Notebook Widget Cell

Use this in an interactive notebook or as documentation for expected widgets:

```python
dbutils.widgets.text("app_name", "example_app")
dbutils.widgets.text("component", "example_task")
dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("observability_event_table", "catalog.schema.event_log")

dbutils.widgets.text("workspace_id", "")
dbutils.widgets.text("workspace_url", "")
dbutils.widgets.text("job_id", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("task_key", "")
dbutils.widgets.text("task_run_id", "")
dbutils.widgets.text("task_attempt_number", "")
dbutils.widgets.text("job_start_time", "")
dbutils.widgets.text("job_trigger_type", "")
dbutils.widgets.text("notebook_path", "")
dbutils.widgets.text("run_as_user_name", "")
```

### Asset Bundle Task Parameters

A notebook task can pass the same values through `base_parameters`:

```yaml
base_parameters:
  app_name: example_app
  component: example_task
  environment: ${bundle.target}
  observability_event_table: ${var.observability_event_table}

  workspace_id: "{{workspace.id}}"
  workspace_url: "{{workspace.url}}"
  job_id: "{{job.id}}"
  run_id: "{{job.run_id}}"
  task_key: "{{task.name}}"
  task_run_id: "{{task.run_id}}"
  task_attempt_number: "{{task.execution_count}}"
  job_start_time: "{{job.start_time.iso_datetime}}"
  job_trigger_type: "{{job.trigger.type}}"
  notebook_path: "{{task.notebook_path}}"
```

Reusable bundle snippets live in:

- [resources/observability.yml](resources/observability.yml) for standard
  observability variables and view names.
- [resources/asset_bundle_observability_parameters.yml](resources/asset_bundle_observability_parameters.yml)
  for a copy/paste notebook task `base_parameters` block.

## Quickstart

The shortest copy/paste notebook is
[notebooks/golden_path_notebook.py](notebooks/golden_path_notebook.py).

Use this as the starting shape for interactive development or a Databricks
notebook task:

```python
from databricks_event_logger import observe_notebook
from databricks_event_logger.spark import read_table, validate_row_count, write_delta

event_logger = observe_notebook.from_widgets(
    dbutils=dbutils,
    spark=spark,
)

as_of_date = dbutils.widgets.get("as_of_date")

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
```

The bootstrap call does four things:

1. Reads the standard widgets.
2. Resolves best-effort Databricks runtime context.
3. Creates and stores the default logger.
4. Emits `notebook.started`.

The Spark helpers then emit standard read, write, and validation events without
requiring hand-written `try`/`except` blocks or context managers.

For scheduled production jobs with a deployed event table, prefer the
production preset:

```python
event_logger = observe_notebook.production_from_widgets(
    dbutils=dbutils,
    spark=spark,
)
```

That preset requires Delta persistence, validates the event table before
`notebook.started`, and enables strict logging for successful business paths.
It is intentionally fail-fast; use `from_widgets(...)` while iterating
interactively if the event table is not configured yet.

Inspect the emitted events. When the sink is `DeltaSink`, query the event table.
When the sink is `MemorySink`, inspect the in-memory events for this Python
process:

```python
if type(event_logger.sink).__name__ == "DeltaSink":
    from pyspark.sql import functions as F

    display(
        spark.table(dbutils.widgets.get("observability_event_table"))
        .where(F.col("correlation_id") == event_logger.correlation_id)
        .orderBy("event_ts", ascending=False)
    )
else:
    display(
        spark.createDataFrame(
            [(event.event_name, event.status) for event in event_logger.sink.events],
            ["event_name", "status"],
        )
    )
```

## How Sink Selection Works

The sink determines where events go.

| Bootstrap inputs | Sink used | Persistence |
|---|---|---|
| `sink=<explicit sink>` | Explicit sink | Whatever that sink does. |
| `spark` supplied and `event_table` supplied | `DeltaSink` | Writes to Delta immediately. |
| Missing `spark` or missing/blank `event_table` | `MemorySink` | Stored only in Python memory. |

The most common persisted path is:

```python
event_logger = observe_notebook.from_widgets(dbutils=dbutils, spark=spark)
```

This becomes a `DeltaSink` only when the `observability_event_table` widget is
present and non-empty.

Check the current sink:

```python
type(event_logger.sink).__name__
```

Expected persisted sink:

```text
DeltaSink
```

Expected non-persisted sink:

```text
MemorySink
```

Check the configured event table:

```python
event_logger.config.event_table
```

If the sink is `MemorySink`, events were not written to Delta. This is useful in
tests and interactive experimentation, but it is not persistent.

When no explicit sink is supplied, the bootstrap helper warns if persistence
looks misconfigured:

- `observability_event_table` is present but no `spark` session was supplied.
- `spark` is present but `observability_event_table` is missing or blank.

## Production Controls

Development notebooks can rely on the default best-effort behavior. Production
jobs should usually use the production preset:

```python
event_logger = observe_notebook.production_from_widgets(
    dbutils=dbutils,
    spark=spark,
    default_metadata={
        "workflow": "daily_positions",
        "owner": "finance_data",
    },
)
```

This preset requires Delta persistence, validates the sink before emitting
`notebook.started`, enables strict logging for successful business paths, and
keeps the event-volume warning threshold at `100`.

The equivalent explicit setup is:

```python
event_logger = observe_notebook.from_widgets(
    dbutils=dbutils,
    spark=spark,
    require_persistence=True,
    validate_sink=True,
    strict_logging=True,
    default_metadata={
        "workflow": "daily_positions",
        "owner": "finance_data",
    },
)
```

`require_persistence=True` raises during bootstrap unless the resolved sink is a
`DeltaSink`. This prevents a misconfigured job from silently using `MemorySink`.

`validate_sink=True` calls `DeltaSink.validate()` before `notebook.started` is
emitted. Validation checks that the table can be described and that the required
event columns exist.

`strict_logging=True` raises sink errors for successful business paths. Failure
logging still preserves the original business exception, so a broken sink cannot
hide the exception that should fail the Databricks task.

Metadata and error payloads are bounded by default:

```python
event_logger = observe_notebook.from_widgets(
    dbutils=dbutils,
    spark=spark,
    metadata_max_bytes=4000,
    metadata_string_max_chars=2000,
    error_message_max_chars=2000,
    max_events_warning_threshold=100,
)
```

Sensitive-looking metadata keys such as `password`, `token`, `secret`,
`credential`, `api_key`, `access_key`, and `private_key` are redacted. This is a
safety net, not a substitute for keeping credentials and row-level records out of
event metadata.

The startup event `notebook.started` also includes bootstrap diagnostics in
`metadata_json`, including `sink_type`, `event_table`, `require_persistence`,
`validate_sink`, `strict_logging`, `job_url`, and `job_run_url`. That makes the
first event in a run useful for debugging configuration and quick navigation.

## Observability Readiness Check

Use `assert_observability_ready(...)` when you want a preflight check before any
events are emitted:

```python
from databricks_event_logger import assert_observability_ready

report = assert_observability_ready(
    dbutils=dbutils,
    spark=spark,
    require_persistence=True,
    validate_sink=True,
)

print(report.summary())
```

The helper verifies widget/table configuration, determines whether the runtime
would use `DeltaSink` or `MemorySink`, resolves available Databricks context
fields, and optionally validates the event table schema. It returns an
`ObservabilityReadinessReport` on success and raises
`EventLoggerConfigurationError` with the report summary on failure.

For non-raising diagnostics:

```python
from databricks_event_logger import check_observability_ready

report = check_observability_ready(
    dbutils=dbutils,
    spark=spark,
    require_persistence=True,
    validate_sink=False,
)

display(report.as_dict())
```

## Public API

### `observe_notebook(...)`

Direct bootstrap:

```python
from databricks_event_logger import observe_notebook

event_logger = observe_notebook(
    app_name="example_app",
    component="example_task",
    environment="dev",
    event_table="catalog.schema.event_log",
    spark=spark,
    dbutils=dbutils,
)
```

Use this when you want to pass values explicitly instead of widgets.

If `spark` and `event_table` are supplied, the default sink is `DeltaSink`.
Otherwise the default sink is `MemorySink`.

### `observe_notebook.from_widgets(...)`

Widget-driven bootstrap:

```python
from databricks_event_logger import observe_notebook

event_logger = observe_notebook.from_widgets(dbutils=dbutils, spark=spark)
```

This is the preferred Databricks notebook task pattern. It keeps notebook
boilerplate low and lets Asset Bundles own environment-specific configuration.

### `observe_notebook.production_from_widgets(...)`

Production widget-driven bootstrap:

```python
from databricks_event_logger import observe_notebook

event_logger = observe_notebook.production_from_widgets(
    dbutils=dbutils,
    spark=spark,
)
```

Use this for normal production job tasks. It is the lowest-friction way to enable
the recommended production controls without repeating three flags in every
notebook. It requires a configured Delta event table; use
`observe_notebook.from_widgets(...)` for local tests and interactive notebooks
that should tolerate `MemorySink`.

### Event Constants

The package accepts plain strings, but common values are available as string
enums:

```python
from databricks_event_logger import (
    CommonEvent,
    EventSeverity,
    EventStatus,
    EventType,
)

event_logger.record_event(
    CommonEvent.DELTA_WRITE,
    event_type=EventType.DELTA_WRITE,
    status=EventStatus.SUCCESS,
    severity=EventSeverity.INFO,
)
```

Constants reduce typos while preserving the persisted string values such as
`delta.write`, `delta_write`, and `success`. `CommonEvent.NOTEBOOK_COMPLETED`
is available for caller-emitted events, but `observe_notebook(...)` does not
emit it automatically. Use `EventStatus.WARNING` when the event outcome is a
warning, and `EventSeverity.WARNING` when the event should be displayed at
warning severity.

### `observed(...)`

Module-level decorator:

```python
from databricks_event_logger import observed

@observed(
    "example.transform_inputs",
    event_type="business_process",
    metadata={"as_of_date": as_of_date},
)
def transform_inputs(df):
    return df.select("id", "amount")
```

For metadata that depends on function arguments, use `metadata_factory`:

```python
@observed(
    "example.transform_inputs",
    event_type=EventType.BUSINESS_PROCESS,
    metadata_factory=lambda df, *, as_of_date, ruleset: {
        "as_of_date": as_of_date,
        "ruleset": ruleset,
    },
)
def transform_inputs(df, *, as_of_date: str, ruleset: str):
    return df.select("id", "amount")
```

`observed(...)` resolves the default logger at call time. The notebook must call
`observe_notebook(...)` or `set_default_logger(...)` before the decorated
function is called.

Decorator events emit:

- `status="success"` when the function returns
- `status="failed"` and `severity="error"` when the function raises
- `duration_ms`
- `start_ts`
- `end_ts`
- static metadata supplied to the decorator
- call-time metadata returned by `metadata_factory`

If `metadata_factory` raises, the wrapped function still runs. The package emits
a Python `RuntimeWarning`, uses static metadata, and annotates the emitted event
metadata with the factory error class and message.

### `EventLogger.logged_event(...)`

Logger-bound decorator:

```python
@event_logger.logged_event(
    "example.prepare_snapshot",
    event_type="business_process",
    metadata={"snapshot_name": "daily_positions"},
    metadata_factory=lambda *, as_of_date: {"as_of_date": as_of_date},
)
def prepare_snapshot(*, as_of_date: str):
    return build_snapshot()
```

Use this when you already have the `event_logger` object and want to avoid the
module-level default logger lookup.

### `EventLogger.event(...)`

Context manager:

```python
with event_logger.event(
    "example.write_snapshot",
    event_type="delta_write",
    target_table="catalog.schema.snapshot",
    metadata={"mode": "overwrite"},
):
    (
        snapshot_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("catalog.schema.snapshot")
    )
```

Use this for custom blocks, especially Spark actions and writes. The block logs
success or failure and preserves the original exception behavior.

### `EventLogger.run_task(...)`

Task/main wrapper:

```python
def main():
    source_df = spark.table("catalog.schema.source")
    output_df = transform_inputs(source_df)
    output_df.write.mode("overwrite").saveAsTable("catalog.schema.output")


event_logger.run_task(
    "example.task.main",
    main,
    metadata={"task_contract": "daily_snapshot"},
)
```

Use `run_task(...)` when the notebook has a clear `main()` function and you want
one explicit SDK-level success/failure event around the entire task.

`observe_notebook(...)` emits `notebook.started`; it does not emit an automatic
`notebook.completed` event. Databricks system tables remain authoritative for
overall task completion. Use `run_task(...)` when you want an SDK-level
success/failure boundary for normal Python return and exception paths.

### `EventLogger.record_event(...)`

Explicit event:

```python
event_logger.record_event(
    "example.source_table_checked",
    event_type="validation",
    status="success",
    severity="info",
    source_table="catalog.schema.source",
    row_count=source_count,
    metadata={
        "threshold": 1,
        "check_name": "source_not_empty",
    },
)
```

Use this when you need full control over `status`, `severity`, `row_count`,
`source_table`, `target_table`, or metadata.

### `EventLogger.record_metric(...)`

Explicit metric:

```python
event_logger.record_metric(
    "output_rows",
    float(output_count),
    event_name="example.metric.output_rows",
    metadata={
        "target_table": "catalog.schema.output",
    },
)
```

Metric fields are only populated by `record_metric(...)` or by explicitly
passing `metric_name` and `metric_value` to `record_event(...)`.

Decorator and context manager events normally have `metric_name` and
`metric_value` as `NULL`.

## Spark Helper API

For common notebook operations, prefer the helper API over hand-written context
manager blocks. The helpers keep call sites short while still logging
success/failure, timing, source/target tables, and relevant metadata.

```python
from databricks_event_logger.spark import (
    count_rows,
    read_table,
    run_sql,
    table_exists,
    validate_row_count,
    write_delta,
)
```

### `read_table(...)`

```python
source_df = read_table(
    "catalog.schema.source",
    as_of_date="2026-06-30",
)
```

This emits `event_name="delta.read"` and returns `spark.table(...)`. Spark reads
are usually lazy, so this event means the DataFrame was requested. It does not
prove rows were materialized.

If the source table is missing or the caller lacks permission, the error may
surface at the first downstream Spark action, such as a write, count, or
validation, rather than at `read_table(...)`.

### `write_delta(...)`

```python
write_delta(
    output_df,
    table="catalog.schema.output",
    mode="overwrite",
    row_count=known_output_rows,
    overwrite_schema=True,
    replace_where="AsOfDate = DATE '2026-06-30'",
    as_of_date="2026-06-30",
)
```

This wraps `df.write.format("delta").mode(mode).saveAsTable(table_name)` and
emits `event_name="delta.write"`. The helper does not call `count()`; pass
`row_count` only when the workflow already has it.

The default Spark write mode is `append`. Use `mode="overwrite"`,
`replace_where=...`, or a workflow-specific merge/upsert pattern when reruns
must avoid duplicate rows.

### `run_sql(...)`

```python
run_sql(
    "OPTIMIZE catalog.schema.output",
    maintenance_action="optimize",
)
```

The helper logs a SHA-256 hash of the SQL text by default. It does not store a
SQL preview unless you explicitly opt in:

```python
run_sql(
    "OPTIMIZE catalog.schema.output",
    include_sql_preview=True,
    maintenance_action="optimize",
)
```

When preview is enabled, quoted strings and obvious numeric literals are
redacted before the preview is truncated. Do not enable SQL previews for
statements that may contain sensitive predicates, tokens, account identifiers,
customer identifiers, or row-level values.

### `validate_row_count(...)`

```python
output_rows = validate_row_count(
    table="catalog.schema.output",
    expected_min=1,
    validation_name="output_not_empty",
)
```

This helper intentionally materializes a Spark action by calling `count()`. It
logs `status="success"` with the observed row count, or logs
`status="failed"` and re-raises when the count does not satisfy the expectation.

### `count_rows(...)`

```python
output_rows = count_rows(
    output_df,
    table_name="catalog.schema.output",
    metric_context="post_transform",
)
```

This helper intentionally performs `DataFrame.count()` and logs the resulting
row count. It is useful for explicit metrics and smoke checks, but should not be
called automatically inside read/write helpers.

### `table_exists(...)`

```python
if not table_exists(table="catalog.schema.source", check_context="startup"):
    raise RuntimeError("Required source table is missing.")
```

This helper uses Spark catalog metadata and logs a validation event with
`status="success"` when the table exists or `status="warning"` when it does not.

## Instrumentation Decision Tree

Use this as the default standard for new notebooks and packages:

```text
Use observe_notebook.from_widgets(...)
  Every Databricks notebook or job task.

Use observe_notebook.production_from_widgets(...)
  Production notebook tasks where event persistence is required.

Use Spark helpers
  Standard reads, writes, SQL calls, table-existence checks, explicit counts,
  and row-count validations.

Use @observed(...)
  Public business functions and public APIs in packages your team owns.
  Use metadata_factory when metadata comes from function arguments.

Use EventLogger.event(...)
  Custom blocks that do not fit a helper or decorator.

Use record_event(...)
  Explicit checkpoints, decisions, warnings, or business milestones.

Use record_metric(...)
  Numeric values already known to the workflow.

Do not log
  Row-level operations, inner loops, every rule condition, raw sensitive SQL,
  credentials, row samples, large payloads, or low-level utility functions.
```

## Notebook Patterns

### Minimal Notebook

```python
from databricks_event_logger import observe_notebook

event_logger = observe_notebook.from_widgets(dbutils=dbutils, spark=spark)

event_logger.record_event(
    "example.notebook.ready",
    metadata={"message": "logger initialized"},
)
```

### Notebook With Explicit Main Function

```python
from databricks_event_logger import observe_notebook
from databricks_event_logger.spark import read_table, write_delta

event_logger = observe_notebook.from_widgets(dbutils=dbutils, spark=spark)


def main():
    source_table = "catalog.schema.source"
    target_table = "catalog.schema.target"

    source_df = read_table(source_table)
    write_delta(source_df, target_table, mode="overwrite")


event_logger.run_task("example.task", main)
```

### Notebook With Decorated Functions

```python
from databricks_event_logger import observe_notebook, observed

event_logger = observe_notebook.from_widgets(dbutils=dbutils, spark=spark)


@observed(
    "example.prepare_dataframe",
    event_type="business_process",
    metadata={"stage": "prepare"},
)
def prepare_dataframe():
    return spark.table("catalog.schema.source").where("active_flag = true")


df = prepare_dataframe()
```

This logs that the DataFrame expression was prepared. It does not necessarily
mean Spark has executed the transformation, because Spark DataFrames are lazy.
Wrap a downstream action or write to log execution.

## Job And Task Patterns

### Recommended Job Task Contract

For persisted logging, each notebook task should have:

- the package wheel installed
- an existing event table
- task parameters or widgets for app/component/environment/table
- `spark` and `dbutils` passed to `observe_notebook.from_widgets(...)`
- table write permission, plus describe/read permission when validating the sink

### Python Script Tasks

Notebook tasks provide `spark` and `dbutils` globals. Python script tasks do
not always inject those globals, so pass them explicitly:

```python
from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession

from databricks_event_logger import observe_notebook

spark = SparkSession.builder.getOrCreate()
event_logger = observe_notebook.from_widgets(dbutils=dbutils, spark=spark)
```

If `databricks.sdk.runtime` is not available in your runtime, create `dbutils`
from Spark:

```python
from pyspark.dbutils import DBUtils

dbutils = DBUtils(spark)
```

Also pass `spark=...` to Spark helpers when the script does not expose a module
or local variable named `spark` near the helper call.

### Standard Task Bootstrap

```python
from databricks_event_logger import observe_notebook

event_logger = observe_notebook.from_widgets(dbutils=dbutils, spark=spark)

print(f"event sink: {type(event_logger.sink).__name__}")
print(f"event table: {event_logger.config.event_table}")
print(f"job_id: {event_logger.context.job_id}")
print(f"run_id: {event_logger.context.run_id}")
print(f"task_key: {event_logger.context.task_key}")
print(f"task_run_id: {event_logger.context.task_run_id}")
print(f"job_run_url: {event_logger.job_run_url}")
```

### Task Wrapper Pattern

```python
def main():
    ...


event_logger.run_task(
    "example.workflow_task.main",
    main,
    metadata={
        "notebook_contract": "one main call owns task success/failure logging",
    },
)
```

If `main()` raises, the package emits a failed event and re-raises the original
exception so the Databricks task still fails normally.

## Package-Level Decorator Pattern

For packages your team owns, prefer decorating public business APIs directly.
That makes instrumentation part of the package contract and keeps notebooks
focused on orchestration:

```python
from databricks_event_logger import EventType, observed


@observed(
    "rules_engine.evaluate_dataframe",
    event_type=EventType.BUSINESS_PROCESS,
    metadata_factory=lambda input_df, *, ruleset_name, version=None, **kwargs: {
        "ruleset_name": ruleset_name,
        "version": version,
    },
)
def evaluate_dataframe(input_df, *, ruleset_name: str, version: str | None = None, **kwargs):
    return _evaluate_dataframe_internal(
        input_df,
        ruleset_name=ruleset_name,
        version=version,
        **kwargs,
    )
```

Notebook usage stays clean:

```python
evaluated_df = evaluate_dataframe(
    source_df,
    ruleset_name="MVE_DOE_POSITION_MAPPING",
    version="2026.06.01",
)
```

For third-party, legacy, or pre-existing packages that you do not want to modify,
keep the package implementation unchanged and wrap the public method at the
notebook or application boundary. This still gives dashboards a stable event
around the business operation.

```python
from databricks_event_logger import observed


def evaluate_dataframe_with_logging(
    service,
    input_df,
    *,
    ruleset_name: str,
    version: str | None,
    column_prefix: str,
    fail_on_error: bool,
):
    metadata = {
        "ruleset_name": ruleset_name,
        "version": version,
        "column_prefix": column_prefix,
        "fail_on_error": fail_on_error,
    }

    @observed(
        "rules_engine.evaluate_dataframe",
        event_type="rules_engine",
        metadata=metadata,
    )
    def _evaluate():
        return service.evaluate_dataframe(
            input_df,
            ruleset_name=ruleset_name,
            version=version,
            column_prefix=column_prefix,
            fail_on_error=fail_on_error,
        )

    return _evaluate()
```

Define the decorated inner function after runtime values are known. Avoid module
import-time decorators when metadata depends on notebook widgets, task
parameters, `AsOfDate`, ruleset versions, or other runtime values.

## Rules Engine Integration Pattern

Keep `rules_engine` unchanged. Instrument the notebook or application wrapper
that calls it.

`event_type` is a free-form string in V1. Values such as `rules_engine` and
`rules_engine_quality` are recommended examples for this integration, not
database-enforced enum values.

```python
from databricks_event_logger import observed

ruleset_name = "Example Ruleset"
ruleset_version = "1"
column_prefix = "rules_engine"


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


evaluated_df = evaluate_rules(source_df)
```

That event means the rules engine returned a transformed DataFrame. It does not
mean Spark has evaluated every row yet.

Log the materialization separately:

```python
from databricks_event_logger.spark import write_delta

target_table = "catalog.schema.rules_output"

write_delta(
    evaluated_df,
    table=target_table,
    mode="overwrite",
    ruleset_name=ruleset_name,
    version=ruleset_version,
    materialization="saveAsTable",
)
```

Log metrics separately:

```python
from databricks_event_logger.spark import count_rows

output_rows = count_rows(
    evaluated_df,
    event_name="rules_engine.evaluate_dataframe.counted",
    table_name=target_table,
    ruleset_name=ruleset_name,
    version=ruleset_version,
)

event_logger.record_metric(
    "rules_engine_output_rows",
    float(output_rows),
    event_name="rules_engine.metric.output_rows",
    metadata={
        "ruleset_name": ruleset_name,
        "version": ruleset_version,
    },
)
```

Log row-level evaluator errors if the rules engine output contains an error
column:

```python
error_column = f"{column_prefix}_error"

error_count = count_rows(
    evaluated_df.where(f"{error_column} is not null"),
    event_name="rules_engine.row_errors.counted",
    table_name=target_table,
    ruleset_name=ruleset_name,
    version=ruleset_version,
    error_column=error_column,
)

event_logger.record_event(
    "rules_engine.row_errors_checked",
    event_type="rules_engine_quality",
    status="success",
    severity="warning" if error_count > 0 else "info",
    row_count=error_count,
    metadata={
        "ruleset_name": ruleset_name,
        "version": ruleset_version,
        "error_column": error_column,
        "threshold": 0,
    },
)
```

If row errors should fail the job, raise after logging:

```python
if error_count > 0:
    raise RuntimeError(f"Rules engine produced {error_count} row errors.")
```

The enclosing `run_task(...)`, `observed(...)`, or `event(...)` wrapper will log
the failure and preserve Databricks task failure behavior.

## Metrics, Row Counts, And Spark Laziness

Spark transformations are lazy. This matters for logging.

This code usually does not execute a Spark job:

```python
output_df = service.evaluate_dataframe(input_df, ruleset_name="Ruleset")
```

This code does execute a Spark job:

```python
row_count = output_df.count()
```

This code also executes a Spark job:

```python
output_df.write.saveAsTable("catalog.schema.output")
```

The package never automatically calls `count()`. Row counts and metrics should
be logged only when the workflow already knows the value or explicitly accepts
the cost of computing it.

Use `row_count` on operation events when a count describes the operation:

```python
event_logger.record_event(
    "example.output_validated",
    event_type="validation",
    row_count=output_rows,
)
```

Use `record_metric(...)` when the value should be treated as a metric:

```python
event_logger.record_metric("output_rows", float(output_rows))
```

## Historical AsOfDate Reload Pattern

When an upstream IT-owned table can be reloaded historically, do not rely only
on the latest table-update trigger timestamp. Track which business dates changed
since the last processed watermark, log all affected dates, and then decide
which dates the current workflow will process.

Recommended workflow-owned control table fields:

- `source_table`
- `business_key_hash`
- `as_of_date`
- `last_seen_mod_ts`
- `last_seen_deleted_flag`
- `last_detected_at`

High-level flow:

```python
from databricks_event_logger.spark import read_table

source_table = "catalog.schema.it_owned_source"
source_df = read_table(source_table)

changed_dates_df = (
    source_df
    .where("M_ModDate > '<last successful watermark>'")
    .selectExpr("CAST(AsOfDate AS DATE) AS as_of_date")
    .distinct()
)

changed_dates = [row.as_of_date.isoformat() for row in changed_dates_df.collect()]

for as_of_date in changed_dates:
    event_logger.record_event(
        "source_table.as_of_date_changed",
        event_type="source_table_change",
        status="success",
        severity="info",
        source_table=source_table,
        metadata={
            "as_of_date": as_of_date,
            "change_detection": "M_ModDate watermark",
        },
    )
```

For v1, log every changed `AsOfDate` but process only the latest date if that is
the current business requirement. If deletes matter, include delete indicators
or anti-join detection in the control-table comparison so historical deletes
also emit `source_table.as_of_date_changed`.

## Conditional Severity

Use `record_event(...)` directly when severity depends on runtime values.

```python
from databricks_event_logger.spark import count_rows

warning_threshold = 0
error_count = count_rows(
    evaluated_df.where("rules_engine_error is not null"),
    event_name="rules_engine.row_errors.counted",
    table_name="catalog.schema.rules_output",
)

event_logger.record_event(
    "rules_engine.row_errors_checked",
    event_type="rules_engine_quality",
    status="success",
    severity="warning" if error_count > warning_threshold else "info",
    row_count=error_count,
    metadata={
        "warning_threshold": warning_threshold,
    },
)
```

Use `status="failed"` only when the event itself represents a failure. If the
workflow should fail, raise an exception after logging the condition.

## Parent And Correlation IDs

Every `EventLogger` has one `correlation_id`. All events from that logger share
it. When no explicit `correlation_id` is supplied, the logger derives a
task-run correlation ID from Databricks context:

- `task_run_id` when available
- `run_id:task_key:task_attempt_number` when task and attempt fields are known
- `run_id:task_key` when task is known but attempt is not
- `run_id` when only the job run is known
- a random UUID outside Databricks job context

Pass an explicit `correlation_id` when several tasks, notebooks, or retries
should be grouped under one ID. For cross-task dashboards, `run_id` is usually
the better grouping key.

Decorators, `run_task(...)`, and context managers create an active parent event
while the wrapped function/block is running. Direct `record_event(...)` calls
inside that active scope automatically receive `parent_event_id`.

Example:

```python
with event_logger.event("example.parent"):
    child = event_logger.record_event("example.child")
```

`example.child.parent_event_id` points to the event ID that will be used for
`example.parent`.

This is useful for dashboards:

```sql
SELECT
  parent.event_name AS parent_event,
  child.event_name AS child_event,
  child.status,
  child.duration_ms
FROM catalog.schema.event_log AS child
LEFT JOIN catalog.schema.event_log AS parent
  ON child.parent_event_id = parent.event_id
WHERE child.correlation_id = '<correlation id>';
```

## Failure Behavior

For decorated functions and context managers:

- success emits `status="success"`
- failure emits `status="failed"` and `severity="error"`
- `error_class` is the exception class name
- `error_message` is `str(exception)`
- `stack_trace_hash` is a stable hash of the traceback
- the original exception is re-raised

Logging failures do not mask business failures.

If business code succeeds but event emission fails, the package emits a Python
warning and lets business code continue.

If business code fails and failure logging also fails, the package warns and
re-raises the original business exception.

When `strict_logging=True`, sink failures can fail an otherwise successful
business path. Use this only when event persistence is itself a production
control. Even in strict mode, failure-event logging does not replace the original
business exception.

## Metadata Guidance

Metadata is caller-controlled JSON.

Recommended `event_type` values:

- `notebook`
- `task`
- `function`
- `delta_read`
- `delta_write`
- `spark_action`
- `sql`
- `validation`
- `business_process`
- `metric`
- `custom`

Domain-specific values such as `rules_engine` are allowed when they make
dashboard filtering clearer.

Good metadata:

```python
metadata={
    "ruleset_name": "Position Classification",
    "version": "2026.06.01",
    "as_of_date": "2026-06-30",
    "source_table": "catalog.schema.source",
    "validation_name": "row_errors_checked",
}
```

Avoid metadata that contains:

- credentials
- tokens
- secrets
- row-level records
- customer-level sensitive values
- large unbounded payloads
- full unrestricted SQL text

The serializer handles common non-JSON objects such as dates, datetimes,
Decimals, enums, Paths, dataclasses, and sets. It does not enforce a key
allowlist, but it does apply production-safe hygiene by default:

- redacts sensitive-looking keys such as `password`, `token`, `secret`,
  `credential`, `api_key`, `access_key`, and `private_key`
- truncates long string values
- replaces oversized serialized metadata with a bounded preview payload

Tune these defaults with `metadata_max_bytes`, `metadata_string_max_chars`, and
`redact_keys` when creating the logger.

## Querying The Event Table

The optional view template
[resources/sql/create_event_log_views.sql](resources/sql/create_event_log_views.sql)
adds dashboard-friendly views for:

- recent events with `job_url` and `job_run_url`
- failure-first events with direct job-run navigation
- run summaries with event counts, failure counts, warning counts, and first/last
  event timestamps
- slow or long-running events
- Delta read/write operations
- validation results
- metrics
- business-process events

### Recent Events

```python
event_table = dbutils.widgets.get("observability_event_table")

display(
    spark.table(event_table)
    .orderBy("event_ts", ascending=False)
    .limit(100)
)
```

### Current Job Run

```python
from pyspark.sql import functions as F

event_table = dbutils.widgets.get("observability_event_table")
run_id = dbutils.widgets.get("run_id")

display(
    spark.table(event_table)
    .where(F.col("run_id") == run_id)
    .orderBy("event_ts")
)
```

### Rules Engine Events

```python
display(
    spark.table(dbutils.widgets.get("observability_event_table"))
    .where("event_name like 'rules_engine.%'")
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
        "error_class",
        "error_message",
        "metadata_json",
        "job_id",
        "run_id",
        "task_key",
        "task_run_id",
        "task_attempt_number",
    )
    .orderBy("event_ts", ascending=False)
)
```

### Failures

```python
display(
    spark.table(dbutils.widgets.get("observability_event_table"))
    .where("status = 'failed'")
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
    .orderBy("event_ts", ascending=False)
)
```

### Metrics

```python
display(
    spark.table(dbutils.widgets.get("observability_event_table"))
    .where("event_type = 'metric'")
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
    .orderBy("event_ts", ascending=False)
)
```

### Current Run Enrichment

Databricks system tables remain authoritative for job/task run state. Build
views that join this event table to system tables in your workspace after
validating the available system-table schema and column names.

Example shape:

```sql
CREATE OR REPLACE VIEW catalog.schema.event_log_with_run_context AS
SELECT
  e.*,
  r.run_name,
  r.result_state,
  r.trigger_type AS system_trigger_type
FROM catalog.schema.event_log AS e
LEFT JOIN system.lakeflow.job_run_timeline AS r
  ON e.run_id = CAST(r.run_id AS STRING);
```

Treat this as a workspace-owned dashboard/view pattern, not package-owned DDL.
If your workspace exposes different system table names or columns, adjust the
view outside the package.

## Troubleshooting

### Events Are Not In Delta

Check the sink:

```python
type(event_logger.sink).__name__
```

If it is `MemorySink`, events are not persisted.

Common causes:

- `observability_event_table` widget is missing
- `observability_event_table` widget is blank
- `spark` was not passed to `observe_notebook.from_widgets(...)`
- an explicit `MemorySink` was passed

### `job_id` Or `task_key` Is Null

Check the task run Parameters panel in Databricks. The parameter must exist and
must resolve to a concrete value.

Use these parameter keys:

```text
job_id
run_id
task_key
task_run_id
task_attempt_number
job_start_time
job_trigger_type
notebook_path
```

Use these dynamic values:

```text
{{job.id}}
{{job.run_id}}
{{task.name}}
{{task.run_id}}
{{task.execution_count}}
{{job.start_time.iso_datetime}}
{{job.trigger.type}}
{{task.notebook_path}}
```

### `metric_name` And `metric_value` Are Null

That is expected for decorator and context manager events.

Metric fields are populated by:

```python
event_logger.record_metric("output_rows", 123.0)
```

or direct `record_event(...)` calls that pass `metric_name` and `metric_value`.

### The Decorator Raises A Configuration Error

`@observed(...)` uses the default logger. Call this first:

```python
event_logger = observe_notebook.from_widgets(dbutils=dbutils, spark=spark)
```

If you do not want to use the default logger, use `event_logger.logged_event(...)`
instead.

### The Event Table Insert Fails

Typical causes:

- table does not exist
- wrong fully qualified table name
- missing insert permission
- schema does not match `resources/sql/create_event_log.sql`
- `databricks_event_logger` wheel version and table schema are out of sync

### There Are More Delta Writes Than Expected

`DeltaSink` writes immediately. Each event is inserted when emitted. This is
simple and resilient for v1, but it means instrumentation should stay coarse
grained. Do not log inside row-level loops or high-volume inner loops.

## Development And Testing

The project is designed for Databricks-hosted testing.

Local syntax/unit checks can still run when the environment has the required
tools:

```powershell
$env:PYTHONPATH='src'; python -m pytest tests/unit
python -m ruff check . --no-cache
python -m build --wheel
```

Databricks smoke tests should verify:

- `observe_notebook.from_widgets(...)` emits `notebook.started`
- `DeltaSink` writes to the configured table
- `MemorySink` is used when no event table is configured
- `@observed(...)` logs success and failure
- `event_logger.event(...)` logs success and failure
- `event_logger.run_task(...)` logs success and failure
- `record_metric(...)` populates metric fields
- context widgets populate job/task fields
- nested events populate `parent_event_id`
- rules engine wrapper logs DataFrame construction and materialization

## Bundle

Asset Bundle configuration lives outside the committed package source. The local
`databricks.yaml` file is intentionally ignored so environment-specific bundle
settings do not enter the package repository.
