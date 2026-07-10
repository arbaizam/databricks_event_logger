# Databricks Event Logger

`databricks-event-logger` records structured lifecycle, validation, metric, and
Spark-operation events from Databricks notebooks and jobs.

The package assumes it is running on Databricks. Bootstrap is deliberately
explicit: pass both `spark` and `dbutils`, choose a sink, and pass notebook/job
parameters yourself. There is no widget-aware bootstrap or caller-frame lookup.

## Install

Install the wheel built by the asset bundle, or install a local checkout:

```bash
python -m pip install .
```

The project uses setuptools through `pyproject.toml`. Build a wheel with:

```bash
python -m build --wheel
```

The wheel is written to `dist/`.

## Bootstrap

`spark` and `dbutils` are required. `ConsoleSink` is the default, so this is the
smallest valid setup:

```python
from databricks_event_logger import observe_notebook

logger = observe_notebook(
    spark=spark,
    dbutils=dbutils,
    app_name="risk_platform",
    component="daily_positions",
    environment="prod",
)
```

Bootstrap resolves Databricks runtime context, makes the logger available to
module-level decorators and helpers, and emits `notebook.started`.

### Custom correlation ID

Pass a workflow or business correlation ID directly. If omitted, a UUID is
generated.

```python
logger = observe_notebook(
    spark=spark,
    dbutils=dbutils,
    app_name="risk_platform",
    correlation_id="close-2026-07-10",
)
```

### Persist events to Delta

Persistence is explicit. Construct and optionally validate a `DeltaSink`, then
pass it to bootstrap:

```python
from databricks_event_logger import DeltaSink, observe_notebook

event_table = dbutils.widgets.get("observability_event_table").strip()
if not event_table:
    raise ValueError("observability_event_table is required")

sink = DeltaSink(spark=spark, table_name=event_table)
sink.validate()

logger = observe_notebook(
    spark=spark,
    dbutils=dbutils,
    app_name=dbutils.widgets.get("app_name"),
    component=dbutils.widgets.get("component"),
    environment=dbutils.widgets.get("environment"),
    correlation_id=dbutils.widgets.get("correlation_id").strip() or None,
    sink=sink,
    strict_logging=True,
)
```

Use `MemorySink` explicitly in unit tests when assertions need access to the
emitted events.

## Record events

### Direct event

```python
logger.record_event(
    "positions.validated",
    event_type="validation",
    status="success",
    source_table="finance.positions",
    row_count=12_450,
    metadata={"as_of_date": "2026-07-10"},
)
```

### Function decorator

Use a logger-bound decorator:

```python
@logger.logged_event("positions.transformed", event_type="transformation")
def transform(df):
    return df.select("account_id", "position_value")
```

Or use the module-level decorator after `observe_notebook(...)` has configured
the default logger:

```python
from databricks_event_logger import observed

@observed("positions.transformed", event_type="transformation")
def transform(df):
    return df.select("account_id", "position_value")
```

`metadata_factory` can compute metadata from call arguments without adding an
inner wrapper:

```python
@observed(
    "positions.loaded",
    metadata_factory=lambda table_name: {"table_name": table_name},
)
def load(table_name):
    return spark.table(table_name)
```

### Context manager

```python
with logger.event("positions.write", event_type="write"):
    output_df.write.mode("overwrite").saveAsTable("finance.positions_output")
```

### Metric

```python
logger.record_metric(
    "invalid_position_count",
    7,
    event_name="positions.invalid_count",
)
```

### Task boundary

```python
def main():
    ...

logger.run_task("daily_positions", main)
```

This emits an explicit task success or failure event. `observe_notebook(...)`
only emits startup; Databricks remains authoritative for the final task state.

## Spark helpers

Spark helpers log the operation and preserve the original Spark exception. All
helpers that need a session require `spark` explicitly.

```python
from databricks_event_logger.spark import (
    count_rows,
    read_table,
    run_sql,
    table_exists,
    validate_row_count,
    write_delta,
)

source_df = read_table(
    "finance.positions",
    spark=spark,
    as_of_date="2026-07-10",
)

row_count = count_rows(
    source_df,
    spark=spark,
    table_name="finance.positions",
)

write_delta(
    source_df,
    table="finance.positions_output",
    mode="overwrite",
    row_count=row_count,
)

validate_row_count(
    "finance.positions_output",
    spark=spark,
    expected_min=1,
)

run_sql("REFRESH TABLE finance.positions_output", spark=spark)
exists = table_exists("finance.positions_output", spark=spark)
```

`read_table` does not trigger a Spark action. `count_rows` and
`validate_row_count` do, and therefore log the materialized row count.

Helpers use the default logger configured by `observe_notebook(...)`. Pass
`logger=logger` to override it.

## Readiness check

Readiness checks use the same explicit runtime and sink contract:

```python
from databricks_event_logger import DeltaSink, assert_observability_ready

sink = DeltaSink(spark=spark, table_name="platform.observability.event_log")

report = assert_observability_ready(
    spark=spark,
    dbutils=dbutils,
    sink=sink,
)
```

Omit `sink` to validate the default `ConsoleSink` configuration.

## Configuration and context

Logger configuration contains only user-supplied identity:

```python
logger.config.app_name
logger.config.component
logger.config.environment
```

Runtime identity is available through `logger.context`, including fields such
as `workspace_id`, `workspace_url`, `job_id`, `run_id`, `task_key`,
`task_run_id`, `task_attempt_number`, and `notebook_path` when Databricks
provides them.

Sink-specific configuration belongs to the sink:

```python
event_table = getattr(logger.sink, "table_name", None)
```

Useful navigation links are derived when enough context is present:

```python
logger.job_url
logger.job_run_url
```

## Failure behavior

By default, a sink failure warns but does not replace a successful business
result. Set `strict_logging=True` when event emission must succeed. If business
code already failed, its original exception is preserved even if failure-event
emission also fails.

Metadata is JSON-normalized and size-limited. Error messages and stack traces
are sanitized before emission.

## Event fields

Every event includes a stable schema covering:

- event identity and timing: `event_id`, `parent_event_id`, `correlation_id`,
  `event_ts`, `duration_ms`
- classification: `event_name`, `event_type`, `status`, `severity`
- application identity: `app_name`, `component`, `environment`
- Databricks context: workspace, job, run, task, notebook, and user fields
- data-operation context: `source_table`, `target_table`, `row_count`
- metrics: `metric_name`, `metric_value`
- failures: `error_class`, `error_message`, `stack_trace`, `stack_trace_hash`
- extensibility: `metadata_json`

## Development

```bash
python -m pip install -e ".[dev]"
python -m pytest
python -m ruff check .
python -m build --wheel
```

The production-oriented example is
[`notebooks/golden_path_notebook.py`](notebooks/golden_path_notebook.py). The
long-form walkthrough is
[`notebooks/databricks_event_logger_developer_guide.py`](notebooks/databricks_event_logger_developer_guide.py).
