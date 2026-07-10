# Databricks Event Logger Design Spec

Status: Draft  
Last updated: 2026-06-03  


## 1. Purpose

This project defines a lightweight observability and event logging framework for Databricks workloads.

The framework combines:

- Databricks-native job and task telemetry for execution metadata.
- A small Python SDK for application, business-process, performance, validation, and failure events.
- A Delta event table and dashboard-ready views.
- Databricks Asset Bundle configuration for standard deployment-time settings.
- Asset Bundle based wheel build/deployment for the SDK package.

The guiding principle is:

```text
Databricks system telemetry tells us what ran.
The custom SDK tells us what the code actually did.
```

The SDK should make the correct observability behavior the easiest behavior. Developers should initialize observability once, use approved helper APIs, and rely on decorators inside shared packages. They should not need to write repetitive `try`/`except` blocks around normal business logic.

## 2. Scope

### 2.1 In Scope

- Runtime event logging from Databricks jobs, tasks, notebooks, and internal Python packages.
- Structured business/process events such as rules engine execution, hierarchy publication, reconciliation checks, and validation events.
- Structured I/O events for approved Spark helpers such as Delta reads, Delta writes, and SQL execution.
- Automatic success and failure logging for decorated functions and helper calls.
- Context capture from Databricks where available.
- Databricks-hosted unit and integration testing.
- Delta persistence and dashboard views.
- Asset Bundle configuration patterns.
- Asset Bundle based package build/deploy guidance for the SDK.
- Secondary context-manager and task-wrapper APIs for cases where helpers or decorators are not enough.

### 2.2 Out of Scope for Version 1

- OpenTelemetry exporter.
- Datadog, Splunk, or Azure Log Analytics exporters.
- Distributed tracing.
- Spark listener integration.
- Monkey-patching Spark or DataFrame APIs.
- Notebook cell magic.
- Row-level audit logging.
- Automatic full SQL lineage.
- Secrets detection engine beyond basic sanitization and truncation.
- Complex normalized telemetry schema.
- Alerting platform.
- Large multi-sink observability platform.
- Full implementation of an Azure Artifacts feed or enterprise package repository.
- Committing dependency wheel files into application source repositories as a long-term pattern.
- Async batching.
- Retry framework.

## 3. Goals

The framework must:

- Provide a generic enterprise event logging pattern for Databricks workloads.
- Support jobs, tasks, notebooks, reusable packages, and custom user-defined events.
- Capture durations, row counts when available, source/target tables, operation metadata, validation outcomes, and failures.
- Capture failures through SDK wrappers and decorators without forcing users to write manual `try`/`except`.
- Keep notebook boilerplate to one initialization call for normal usage.
- Support decorator-based instrumentation inside reusable packages.
- Support helper-based instrumentation in notebooks.
- Store events in a governed Delta table suitable for dashboards.
- Integrate cleanly with Asset Bundle variables, job tags, and task parameters.
- Support repeatable package deployment through Asset Bundle wheel builds.
- Run meaningful unit and integration tests in Databricks.
- Keep the first implementation small and maintainable.

## 4. Architecture

```text
Databricks Asset Bundle
  - Defines jobs, tasks, targets, variables, and parameters
  - Applies standard tags
  - Passes observability config to notebooks/tasks
  - Deploys or references SDK/application wheels
  - Deploys SQL objects for event tables and views

CI / Asset Bundle Build
  - Builds and tests the SDK/application package
  - Produces deployable wheel artifacts
  - Keeps artifact construction outside runtime code

Databricks Job / Task / Notebook Runtime
  - Calls observe_notebook(spark=spark, dbutils=dbutils, ...) once
  - Uses Spark helper APIs for common operations
  - Uses decorators, context managers, and task wrappers where appropriate

databricks_event_logger SDK
  - Creates runtime context
  - Resolves Databricks metadata where available
  - Emits structured events
  - Handles success/failure timing
  - Serializes/sanitizes metadata
  - Sends events to configured sinks

Sinks
  - MemorySink for tests
  - ConsoleSink as the zero-configuration default
  - DeltaSink for Databricks persistence

Delta Observability Table
  - dev_observability.observability.event_log
  - Append-only event records

Dashboard Views
  - Curated views over event_log
  - Optional enrichment with Databricks system tables
```

Databricks system tables remain the authoritative source for job and task run state. The custom event table provides richer application semantics inside those jobs and tasks.

Asset Bundles are the control plane: they standardize deployment, parameters, tags, library references, SQL objects, and environment selection. The SDK is the runtime data plane: it captures what the code actually did while the task was running.

### 4.1 Evaluated Solution Patterns

The full design conversation evaluated three broad patterns:

| Pattern | Summary | Decision |
|---|---|---|
| Databricks-native event observability layer | Use system tables for job/task telemetry and a custom Delta event table for application events. | Adopt as the foundation. |
| Internal Python event-logging SDK backed by Delta | Build a small SDK with decorators, helpers, context, failure capture, and Delta persistence. | Adopt for business/runtime events. |
| External observability platform | Send events to Azure Log Analytics, Datadog, Splunk, or OpenTelemetry infrastructure. | Defer; possible future sink/exporter. |

The recommended architecture is therefore a Databricks-first hybrid: native system metadata plus a small runtime SDK writing governed Delta events.

## 5. Core Concepts

### 5.1 Event

An event is a structured record describing something meaningful that happened during runtime.

Examples:

- `notebook.started`
- `notebook.completed`
- `delta.read`
- `delta.write`
- `sql.execute`
- `validation.row_count`
- `reporting.rules_engine.apply_rules`
- `reporting.hierarchy.publish`
- `reporting.reconciliation.run_check`

### 5.2 Event Status

Supported statuses:

- `started`
- `success`
- `failed`
- `warning`
- `skipped`

### 5.3 Event Type

Supported event types:

- `notebook`
- `task`
- `function`
- `delta_read`
- `delta_write`
- `sql`
- `validation`
- `business_process`
- `custom`

### 5.4 Correlation ID

`correlation_id` groups all events emitted by one logger instance. By default,
the SDK uses task-run correlation in Databricks jobs.

Recommended behavior:

- In a Databricks task run, prefer `task_run_id` when available.
- If `task_run_id` is unavailable, use a stable value derived from job run ID,
  task key, and attempt number.
- Callers can pass an explicit `correlation_id` when several tasks, notebooks,
  or retries should share one correlation key.
- Outside Databricks, generate a UUID during logger initialization.
- Preserve the generated ID for the lifetime of the logger.

`event_id` is unique per event.

`parent_event_id` is optional and should be populated for nested decorated operations when the SDK can track a current parent event.

### 5.5 Metadata

`metadata_json` contains flexible event-specific JSON.

Allowed examples:

```json
{
  "ruleset": "MVE_DOE_POSITION_MAPPING",
  "ruleset_version": "2026.06.01",
  "as_of_date": "2026-06-30",
  "source": "positions_transform"
}
```

Metadata must not contain credentials, tokens, secrets, customer-level sensitive values, row-level data, unbounded payloads, or unrestricted SQL text.

## 6. Python Package Design

Recommended package name:

```text
databricks_event_logger
```

Recommended structure:

```text
src/
  databricks_event_logger/
    __init__.py
    config.py
    context.py
    event.py
    logger.py
    decorators.py
    serialization.py
    timing.py
    errors.py

    sinks/
      __init__.py
      base.py
      memory.py
      console.py
      delta.py

    spark/
      __init__.py
      io.py
      sql.py
      validation.py

tests/
  unit/
  integration/
```

### 6.1 Public API

Recommended imports:

```python
from databricks_event_logger import (
    EventLogger,
    observe_notebook,
    get_default_logger,
    set_default_logger,
    observed,
)
```

Recommended Spark helper imports:

```python
from databricks_event_logger.spark import (
    read_table,
    write_delta,
    run_sql,
    validate_row_count,
    count_rows,
    table_exists,
)
```

### 6.2 Logger Initialization

Notebook or job entry point:

```python
from databricks_event_logger import DeltaSink, observe_notebook

event_table = dbutils.widgets.get("observability_event_table").strip()
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
)
```

`observe_notebook` must:

- Require explicit `spark` and `dbutils` dependencies.
- Create an `EventLogger`.
- Resolve Databricks context where available.
- Set the logger as the default logger.
- Emit `notebook.started`.

Important lifecycle decision:

- Databricks system tables are authoritative for task and job completion.
- V1 does not emit automatic SDK-level `notebook.completed` events from `observe_notebook()` alone.
- Use `logger.run_task(..., main)` when an explicit SDK-level success/failure boundary is required.
- Operation-level decorators and helpers must reliably emit success/failure for the code they wrap.

### 6.3 Default Logger

The default logger lets notebooks initialize observability once and lets application-level code emit events without hardcoding configuration.

Application-level functions can do this:

```python
from databricks_event_logger import get_default_logger


def apply_rules(df, ruleset: str, **metadata):
    logger = get_default_logger()

    @logger.logged_event(
        event_name="reporting.rules_engine.apply_rules",
        event_type="business_process",
        metadata={
            "ruleset": ruleset,
            **metadata,
        },
    )
    def _run():
        return _apply_rules_internal(df, ruleset=ruleset)

    return _run()
```

Application code should not hardcode event table names, environment names, catalog names, or workspace-specific configuration.

### 6.4 Generic Decorator

Application code may use:

```python
from databricks_event_logger import observed


@observed("reporting.model_inputs.build_positions", event_type="business_process")
def build_positions(as_of_date: str):
    ...
```

The `observed` decorator must resolve the default logger at call time, not import time. This prevents package import order from breaking observability initialization.

### 6.5 Custom Events

The SDK must support explicit custom events:

```python
logger = get_default_logger()

logger.record_event(
    event_name="reporting.custom_checkpoint",
    event_type="custom",
    status="success",
    metadata={
        "as_of_date": as_of_date,
        "checkpoint": "post_rules_pre_publish",
    },
)
```

### 6.6 Context Manager

Context managers are useful when a notebook or package needs to instrument a block that does not fit a helper function.

```python
logger = get_default_logger()

with logger.event(
    "reporting.positions.write_delta",
    event_type="delta_write",
    target_table="silver.positions_daily",
    metadata={
        "mode": "overwrite",
        "as_of_date": as_of_date,
    },
):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("silver.positions_daily")
    )
```

Required behavior:

- On success, log `status=success` with duration.
- On failure, log `status=failed` with error metadata and re-raise the original exception.
- Do not make context managers the main notebook pattern. Prefer helpers such as `write_delta`, `read_table`, `run_sql`, and `validate_row_count` for standard operations.

### 6.7 Task Wrapper

A task wrapper is useful when a job/task has a clear `main()` function and the team wants SDK-level task lifecycle events in addition to Databricks system table status.

```python
from databricks_event_logger import observe_notebook


def main():
    positions = read_table("catalog.bronze.co_positions", spark=spark)
    mapped = apply_rules(positions, ruleset="MVE_DOE_POSITION_MAPPING")
    write_delta(mapped, "catalog.silver.positions_daily", mode="overwrite")


logger = observe_notebook(spark=spark, dbutils=dbutils)
logger.run_task("reporting.positions_daily", main)
```

Required behavior:

- Emit task/notebook start, success, and failure events for the wrapped callable.
- Preserve and re-raise the original exception.
- Avoid relying on global exception hooks as the only failure mechanism.

### 6.8 Explicit Metrics

The SDK should support simple explicit metric events for values already known to the pipeline.

```python
logger.record_metric(
    metric_name="input_row_count",
    metric_value=input_count,
    event_name="reporting.rules_engine.apply_rules.metric",
    metadata={
        "ruleset": ruleset,
        "as_of_date": as_of_date,
    },
)
```

The SDK should not automatically compute expensive metrics such as `DataFrame.count()` unless the caller explicitly opts in.

## 7. Spark Helper Design

Spark helpers are intended to make standard operations observable without extra notebook boilerplate.

### 7.1 Delta Read

```python
from databricks_event_logger.spark import read_table

positions = read_table(
    "catalog.bronze.co_positions",
    spark=spark,
    as_of_date=as_of_date,
)
```

Expected event:

```text
event_name: delta.read
event_type: delta_read
status: success or failed
source_table: catalog.bronze.co_positions
duration_ms: captured
metadata_json: includes as_of_date
```

### 7.2 Delta Write

```python
from databricks_event_logger.spark import write_delta

write_delta(
    mapped_positions,
    table="catalog.silver.positions_daily",
    mode="overwrite",
    overwrite_schema=True,
    replace_where=f"AsOfDate = DATE '{as_of_date}'",
    as_of_date=as_of_date,
    row_count=known_row_count,
)
```

Expected event:

```text
event_name: delta.write
event_type: delta_write
status: success or failed
target_table: catalog.silver.positions_daily
duration_ms: captured
row_count: known_row_count if supplied
metadata_json: includes mode, options, overwrite_schema, replace_where, as_of_date
```

Row counts should not be computed automatically by default because `DataFrame.count()` introduces an extra Spark action. The helper accepts an explicit `row_count` when the workflow already knows it; use `validate_row_count(...)` when a materialized count is intentionally required.

### 7.3 SQL Execution

```python
from databricks_event_logger.spark import run_sql

run_sql(
    "OPTIMIZE catalog.silver.positions_daily",
    spark=spark,
    maintenance_action="optimize",
)
```

Expected event:

```text
event_name: sql.execute
event_type: sql
status: success or failed
duration_ms: captured
metadata_json: includes sql_hash; sql_preview only when explicitly enabled
```

metadata_json includes `sql_hash` by default. `sql_preview` is opt-in through
`include_sql_preview=True`.

The SDK should not log SQL preview text by default. When preview is enabled, it
should redact quoted strings and obvious numeric literals before truncation.
Do not enable preview for SQL that may contain sensitive predicates, tokens,
account identifiers, customer identifiers, or row-level values.

### 7.4 Validation

```python
from databricks_event_logger.spark import validate_row_count

validate_row_count(
    table="catalog.silver.positions_daily",
    spark=spark,
    expected_min=1,
    as_of_date=as_of_date,
)
```

Expected behavior:

- On pass, log a successful validation event.
- On fail, log a failed validation event and raise a validation exception.
- Include measured row count, expected threshold, table name, duration, and metadata.

### 7.5 Explicit Count

```python
from databricks_event_logger.spark import count_rows

row_count = count_rows(
    mapped_positions,
    spark=spark,
    table_name="catalog.silver.positions_daily",
    as_of_date=as_of_date,
)
```

Expected behavior:

- Perform an explicit Spark `count()` action.
- Log `event_name=spark.count`, `event_type=spark_action`, and `row_count`.
- Re-raise any Spark exception after logging a failed event.

### 7.6 Table Existence Check

```python
from databricks_event_logger.spark import table_exists

if not table_exists(
    table="catalog.bronze.co_positions",
    spark=spark,
    check_context="startup",
):
    raise RuntimeError("Required source table is missing.")
```

Expected behavior:

- Log success when the table exists.
- Log warning and return `False` when the table is missing.
- Log failure and re-raise when Spark catalog inspection itself fails.

## 8. Failure Handling

Failure handling is a core requirement.

If instrumented business code succeeds:

- Log a success event.
- Return the original result.

If instrumented business code fails:

- Log a failed event.
- Capture error metadata.
- Re-raise the original exception.

If business code fails and logging also fails:

- Preserve and re-raise the original business exception.
- Do not mask it with the secondary logging failure.

If business code succeeds and logging fails:

- Warn and continue.
- Do not fail the business process because event logging failed.

Version 1 behavior:

```python
EventLogger(...)
```

`strict_logging=True` means logging failures can fail an otherwise successful business workflow. That is useful only when event logging is itself a required control. V1 supports this as an explicit opt-in while keeping best-effort logging as the default.

Required failure fields:

- `status = failed`
- `error_class`
- `error_message`
- `stack_trace_hash`
- `duration_ms`
- `event_name`
- `event_type`
- `app_name`
- `component`
- `environment`
- `job_id`
- `run_id`
- `task_key`
- `metadata_json`

## 9. Event Table

Version 1 should use one append-only Delta table:

```text
dev_observability.observability.event_log
```

Recommended schema:

```sql
CREATE TABLE IF NOT EXISTS dev_observability.observability.event_log (
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

`event_date` is included to support retention and common dashboard filters. Physical partitioning or clustering should follow platform standards and expected data volume.

The table location must be fully configurable through runtime or Asset Bundle variables. Unity Catalog ownership, grants, environment separation, and retention are deployment/platform responsibilities and are not configured by the Python package.

## 10. Dashboard Views

Recommended v1 views:

- `observability.v_event_failures`
- `observability.v_event_performance`
- `observability.v_delta_operations`
- `observability.v_validation_results`
- `observability.v_rules_engine_events`
- `observability.v_business_process_events`
- `observability.v_job_task_event_summary`

Dashboards should answer:

- Which jobs or tasks failed today?
- Which business event failed inside a failed task?
- What error class and message were captured?
- How long did the failed step run?
- Which events are getting slower?
- Which Delta writes are most expensive?
- Is duration correlated with row count?
- Which rulesets, validations, hierarchies, or reconciliations ran?
- Which `as_of_date` was processed?

Dashboard views may join `event_log` to Databricks system tables for job/task metadata. The SDK should not duplicate all native system telemetry.

## 11. Asset Bundle Integration

Asset Bundles own deployment-time configuration. Runtime packages should not.

Recommended bundle variables:

```yaml
variables:
  environment:
    description: Deployment environment
    default: dev

  observability_catalog:
    description: Catalog for observability objects
    default: ${bundle.target}_observability

  observability_schema:
    description: Schema for observability objects
    default: observability

  observability_event_table:
    description: Fully qualified event log table
    default: ${var.observability_catalog}.${var.observability_schema}.event_log
```

Recommended job parameters:

```yaml
resources:
  jobs:
    positions_daily:
      name: ${bundle.target}_positions_daily

      tags:
        app_name: example_reporting
        component: positions_daily
        environment: ${bundle.target}
        managed_by: asset_bundle

      tasks:
        - task_key: publish_positions
          base_parameters:
            app_name: example_reporting
            component: positions_daily
            environment: ${bundle.target}
            observability_event_table: ${var.observability_event_table}
```

The runtime pattern is:

```text
Bundle passes configuration.
Runtime SDK captures and writes events.
```

### 11.1 Bundle Versus SDK Decision Boundary

Asset Bundles should standardize deployment and configuration. They should not be treated as a replacement for runtime instrumentation.

| Need | Bundle-level fit |
|---|---|
| Standard job tags | Excellent |
| Standard job parameters | Excellent |
| Per-environment configuration | Excellent |
| Deploying the SDK/application wheel | Excellent |
| Deploying event table/view definitions | Excellent |
| Enforcing `app_name`, `component`, and `environment` | Excellent |
| Capturing job/task start/end/failure | Partial |
| Capturing custom business events | Poor |
| Capturing step-level performance | Poor |
| Capturing rules-engine metadata | Poor |
| Capturing row counts and validation outcomes | Poor |
| Logging failures inside helper functions | Poor |

The design decision is:

| Decision | Recommendation |
|---|---|
| Should observability be bundle-level only? | No |
| Should bundles own deployment/configuration? | Yes |
| Should the SDK own runtime event capture? | Yes |
| Should jobs be required to pass app/component/environment? | Yes |
| Should common helpers use bundle-provided defaults? | Yes |
| Should event tables/views be deployed by bundle? | Yes |
| Should custom step-level events be optional? | Yes, but strongly encouraged for key steps |

The short version:

```text
Bundles enforce the standard.
The SDK captures the truth.
```

## 12. Package Build and Deployment

The package must be buildable as a Python wheel, but v1 does not own artifact publishing, feed authentication, or private package installation. Those concerns are handled by Asset Bundles and the surrounding deployment pipeline.

Recommended separation:

```text
Git repo = source of truth for code
Asset Bundle / CI = source of truth for build and deployment flow
Asset Bundle = source of truth for Databricks deployment
```

### 12.1 V1 Pattern: Asset Bundle Wheel Build

For v1, build this package from the repository in the Asset Bundle flow:

```yaml
artifacts:
  default:
    type: whl
    path: .
    build: |
      python -m pytest tests/unit
      python -m build
```

Benefits:

- Keeps wheel creation outside runtime code.
- Avoids committing built wheels as source.
- Keeps permissions, artifact locations, and deployment policy outside the package.
- Fits the current requirement that no private packages need event logging internally.

Tradeoff:

- This does not create a shared package distribution model for multiple downstream consumers. That can be added later if the package needs to be reused broadly.

### 12.2 Future Pattern: Package Feed or Volume Wheelhouse

If broader reuse is needed later, use one of these deployment-owned patterns:

```text
Preferred long-term: internal PyPI-compatible feed
Databricks-native fallback: governed Unity Catalog Volume wheelhouse
```

Those remain outside the SDK itself. The package should not contain logic for feed authentication, UC Volume permissions, or package publication.

### 12.3 Versioning

Use semantic-ish internal package versions:

```text
MAJOR.MINOR.PATCH
```

Examples:

```text
0.1.0 initial internal release
0.1.1 bug fix
0.2.0 backwards-compatible feature
1.0.0 stable API
2.0.0 breaking change
```

## 13. Context Capture

The SDK should capture these Databricks fields where available:

- `workspace_id`
- `workspace_url`
- `cluster_id`
- `job_id`
- `run_id`
- `task_key`
- `task_run_id`
- `task_attempt_number`
- `job_start_time`
- `job_trigger_type`
- `notebook_path`
- `user_name`
- `run_as_user_name`

When a Databricks context field is unavailable, context resolution must return nulls rather than failing. This keeps tests and non-job execution paths simple.

Context resolution should use a small adapter layer so Databricks-specific lookups are isolated from the rest of the SDK.

## 14. Event Naming

Use this naming convention for business events:

```text
<domain>.<component_or_process>.<action>
```

Examples:

- `reporting.rules_engine.apply_rules`
- `reporting.rules_engine.translate_spec`
- `reporting.hierarchy.publish`
- `reporting.hierarchy.validate_mapping`
- `reporting.reconciliation.run_check`
- `reporting.model_inputs.build_positions`
- `reporting.publish_positions`

Generic SDK events:

- `notebook.started`
- `notebook.completed`
- `delta.read`
- `delta.write`
- `sql.execute`
- `validation.row_count`

Event naming should be a lightweight team convention, not a formal review gate for v1. A central registry can be added later only if naming drift becomes a real problem.

## 15. Metadata Governance

Every event should attempt to include:

- `app_name`
- `component`
- `environment`
- `correlation_id`
- `event_name`
- `event_type`
- `status`
- `event_ts`
- `duration_ms`

Common optional metadata:

- `as_of_date`
- `source_table`
- `target_table`
- `row_count`
- `ruleset`
- `ruleset_version`
- `hierarchy_id`
- `validation_name`
- `check_name`
- `input_count`
- `output_count`

Prohibited metadata:

- Credentials.
- Tokens.
- Secrets.
- Customer-level sensitive values.
- Row-level data.
- Full unrestricted payloads.
- Large data samples.
- Unbounded SQL text with embedded sensitive values.

Version 1 should include:

- JSON serialization with deterministic fallback for non-serializable values.
- Fully caller-configurable metadata.
- No SDK-enforced metadata allowlist.
- Default metadata hygiene for redaction, long string truncation, and bounded serialized payloads.

Callers own metadata content and should keep payloads concise. Large JSON values can increase Delta write cost, make dashboards harder to use, and make event records less useful for troubleshooting. Secrets, credentials, row-level data, and unrestricted sensitive payloads remain prohibited by usage standard; SDK redaction/truncation is a safety net, not a data-governance substitute.

## 16. Sink Behavior

Version 1 sinks:

- `MemorySink` for unit tests.
- `ConsoleSink` for local debugging.
- `DeltaSink` for Databricks persistence.

The sink interface should support:

- `emit(event)`
- `flush()`
- `close()`

Recommended Delta behavior:

- Append events to the configured Delta table.
- Use immediate writes for v1: each event is persisted when emitted.
- Keep `flush()` available on the sink interface even though DeltaSink has no buffered events in v1.
- Do not let sink failures mask business failures.

Immediate writes are simpler and reduce the chance of losing events if the notebook or cluster terminates. The tradeoff is more small Delta writes. Because v1 instrumentation should be coarse-grained, that tradeoff is acceptable.

Buffered writes would collect events in memory and write them in batches. This can reduce write overhead, but it is more complex and can lose buffered events if the process dies before flushing. Buffering is out of scope for v1 but can be added later behind the same sink interface.

## 17. Package Instrumentation Strategy

Recommended logging layers:

```text
Notebook / job entry point          yes
Application-level functions         yes
Custom notebook/code blocks         yes
Owned domain package public API     yes
Third-party/legacy package API      wrap externally
Internal package functions          usually no
Low-level utilities                 usually no
```

Log these:

- Job/task lifecycle wrapper.
- Application-level business step.
- Custom code block with context manager.
- Model input build.
- Delta read/write.
- Validation check.
- Semantic model refresh trigger.

Do not normally log these:

- String utility function.
- Date formatting helper.
- Column name normalization helper.
- Individual row transformation.
- Inner-loop rule condition evaluation.

Owned domain packages should use decorators on stable public business APIs when
the package is already part of the Databricks workload. Third-party, generic, or
legacy packages can still be instrumented at the notebook, job, application
function, helper, context-manager, or task-wrapper layer.

Recommended coupling model:

- Use decorators on owned public business APIs where the package dependency is acceptable.
- Wrap third-party or legacy packages at the application boundary.
- Use no logger dependency for generic low-level utility packages.
- Avoid instrumentation inside internal helpers and row-level loops.

Example application-level dependency:

```toml
[project]
dependencies = [
  "databricks-event-logger>=0.1.0,<0.2.0",
]
```

Avoid optional no-op fallbacks unless there is a real reuse requirement. They add boilerplate and make instrumentation less predictable.

## 18. Testing Requirements

### 18.1 Unit Tests

Unit tests run in Databricks. They should avoid depending on a real observability Delta table unless the test is explicitly marked as integration.

Required areas:

- Event model creation.
- Event serialization.
- Metadata JSON handling.
- Non-serializable metadata handling.
- Metadata truncation and sanitizer behavior.
- Memory sink.
- Console sink.
- `EventLogger.record_event`.
- `EventLogger.record_metric`.
- Decorator success behavior.
- Decorator failure behavior.
- Context manager success behavior.
- Context manager failure behavior.
- Task wrapper success behavior.
- Task wrapper failure behavior.
- Original exception re-raise.
- Logging failure does not mask business failure.
- Context resolver behavior when optional Databricks fields are unavailable.
- Spark helper behavior using mocks.
- Delta write helper behavior using fake writer or mocked Spark session.
- Validation helper behavior.

Minimum Databricks unit-test task commands:

```bash
ruff check .
pytest tests/unit
python -m build
```

### 18.2 Databricks Integration Tests

Integration tests also run in Databricks. They should be marked separately or run in a separate Databricks test task because they touch real Delta persistence and Databricks runtime context.

Required areas:

- Write event to Delta.
- Capture Databricks workspace/job/task context.
- Capture failed event from failed decorated operation.
- Confirm failed task remains failed.
- Validate dashboard view output.

## 19. Implementation Phases

### Phase 1: Core SDK

Build:

- Event model.
- Event logger.
- Default logger registry.
- Memory sink.
- Console sink.
- Sink interface.
- Decorator support.
- Context manager support.
- Task wrapper support.
- Explicit metric support.
- Failure handling.
- Metadata serialization.
- Unit tests.

### Phase 2: Notebook and Spark Helpers

Build:

- `observe_notebook`.
- Databricks context resolver.
- `read_table`.
- `write_delta`.
- `run_sql`.
- `validate_row_count`.
- `count_rows`.
- `table_exists`.
- Mock-based tests.

### Phase 3: Delta Persistence

Build:

- `DeltaSink`.
- Event table DDL.
- Append-only writes.
- Flush behavior.
- Databricks integration tests.

### Phase 4: Asset Bundle Standard

Build:

- Standard observability variables.
- Standard task base parameters.
- Standard job tags.
- Environment-specific configuration pattern.
- Deployment pattern for observability SQL objects.
- Asset Bundle wheel build/deployment standard for this package.

### Phase 5: Application Instrumentation

Instrument application-level code where needed:

- Model input builder.
- Reporting jobs.
- Custom business steps.
- Context-manager wrapped notebook blocks.
- Task-wrapper entry points.

Private/domain package-internal instrumentation is deferred to v2 unless a specific need emerges.

### Phase 6: Dashboards

Build:

- Event failures dashboard.
- Event performance dashboard.
- Rules engine telemetry dashboard.
- Validation dashboard.
- Job/task summary dashboard.

## 20. Functional Q&A

### Does this replace Databricks system tables?

No. Databricks system tables remain authoritative for job and task run metadata. The SDK fills the gap for business-level runtime events Databricks cannot infer.

### How are failures captured without manual `try`/`except`?

The SDK decorators and helper functions contain the `try`/`except` internally. They time the operation, emit success or failure events, and re-raise the original exception when the operation fails.

### Can a single `observe_notebook()` call guarantee `notebook.completed`?

No. In V1, `observe_notebook()` emits `notebook.started` and configures the default logger. It does not emit `notebook.completed`. Databricks system tables should remain authoritative for final task status.

Explicit SDK-level completion means the notebook work is wrapped in a callable such as `logger.run_task("event.name", main)`. In that pattern, the wrapper owns the success/failure boundary and can reliably log completion for normal Python success and exception paths.

### Should the SDK automatically count DataFrame rows?

No, not by default. Counting rows triggers a Spark action and can materially change job cost and runtime. Helpers should accept an explicit `row_count` when the workflow already knows it. Use `validate_row_count(...)` when a workflow intentionally wants a counted validation event.

### What happens if logging fails?

For v1, logging failures warn and continue when business code succeeds. If business code fails, the original business exception is always preserved and re-raised. The package will not fail a business workflow solely because event logging failed.

`strict_logging=True` means logging failure can fail an otherwise successful workflow. It is available in v1 for workflows where event persistence is a required control.

### Can packages use the SDK directly?

Yes, owned domain packages should decorate stable public business APIs when the
package dependency is acceptable. Third-party, generic, or legacy packages can
still be wrapped at the notebook, job, application function, helper,
context-manager, or task-wrapper layer.

### Should low-level utility packages emit events?

Usually no. Instrument public business operations and expensive I/O, not low-level helpers or inner loops.

### Can users create custom business events?

Yes. The SDK should expose `record_event` for explicit custom events, subject to event naming and metadata rules.

### How is SQL handled safely?

Log a SQL hash by default. SQL preview is opt-in, redacted, and truncated. Do not enable SQL preview unless a workflow has been reviewed for sensitive data exposure.

### Where should the event table live?

The event table location should be fully configurable. Unity Catalog catalog/schema/table selection, grants, environment separation, and retention are handled outside the Python package.

### How long should events be retained?

Retention is handled outside the package through Unity Catalog, Delta maintenance, platform jobs, or deployment policy.

### Should metadata keys be centrally governed?

No formal allowlist is required for v1. Metadata is fully configurable by the caller. The usage standard should still prohibit secrets, credentials, row-level data, and unrestricted sensitive payloads.

### How will dashboards connect SDK events to Databricks jobs?

Dashboard views should join `event_log` to Databricks system tables using available workspace, job, run, task, and attempt identifiers.

### Can Asset Bundles replace the SDK?

No. Bundles are excellent for deployment, parameters, tags, libraries, and SQL objects. They cannot see runtime business facts such as which ruleset ran, how many rows were processed, which validation failed, or whether failure occurred during rule translation versus Delta write.

### Should dependency wheel files be committed into application repos?

No. Treat wheels as build outputs. For v1, build/deploy this package through Asset Bundles. If broader reuse is needed later, publish released wheels to an internal package feed or governed Databricks-accessible wheelhouse.

### What is the preferred package distribution model?

For v1, build/deploy the wheel through Asset Bundles. A private feed or Unity Catalog Volume wheelhouse can be introduced later if the package needs broader reuse.

### What is DeltaSink immediate versus buffered writing?

Immediate writing means the sink writes each event to Delta as soon as it is emitted. It is simpler and less likely to lose events, but it can create more small Delta writes.

Buffered writing means the sink holds events in memory and writes batches later. It can reduce write overhead, but it is more complex and can lose events if the process dies before flushing. V1 uses immediate writes.

### Should the SDK support context managers?

Yes, as a secondary API for custom blocks. The preferred notebook pattern remains one bootstrap call plus approved helpers such as `read_table`, `write_delta`, `run_sql`, and `validate_row_count`.

### Should the SDK support a task wrapper?

Yes. A task wrapper is useful when notebooks can be organized around a `main()` function and the team wants SDK-level lifecycle events in addition to Databricks system table status.

## 21. Resolved Decisions

These decisions were resolved during design review:

- Package name is `databricks_event_logger`; do not use project-specific naming for the SDK package.
- Unity Catalog catalog/schema/table location must be fully configurable through variables.
- Table ownership, grants, read/write permissions, system-table permissions, and retention are handled outside the Python package.
- Dev/test/prod event data should be separated through external configuration.
- Best-effort logging remains the default. Production workflows may opt into `strict_logging=True` when logging failure should fail an otherwise successful business path.
- Metadata is fully caller-configurable.
- No SDK-enforced metadata allowlist in v1.
- Metadata remains caller-configurable, with SDK defaults for redaction, truncation, and serialized payload bounds.
- No formal naming review process is required for v1; use lightweight team conventions.
- DeltaSink uses immediate writes in v1. Buffered writes are a future optimization.
- Domain-specific event details are deferred to v2.
- `observe_notebook()` emits startup only. Use `logger.run_task(..., main)` when explicit SDK-level success/failure lifecycle logging is required.
- Wheel build/deploy is handled outside the package through Asset Bundles for now.
- Private packages do not need event logging internally in v1.
- Context manager and task wrapper APIs are required in v1.

Remaining implementation notes:

- Document the expected table schema, but do not create grants or retention policies from Python.
- Keep all Unity Catalog object names configurable.
- Keep the sink interface compatible with future buffering even though v1 writes immediately.

## 22. Acceptance Criteria

Version 1 is acceptable when:

- A notebook can initialize observability from Asset Bundle parameters with one call.
- Decorated functions emit success and failure events.
- Context managers emit success and failure events for custom blocks.
- Task wrappers emit lifecycle success and failure events.
- Explicit metrics can be recorded without forcing expensive Spark actions.
- Spark helpers emit read, write, SQL, and validation events.
- Failed business code re-raises the original exception.
- Logging failures do not mask business failures.
- Events can be written to a Delta table in Databricks.
- The package can be built as a wheel.
- Asset Bundle deployment can build or reference the package wheel.
- The package does not manage permissions, retention, or package-feed authentication.
- Unit tests run in Databricks.
- Integration tests run in Databricks and prove Delta writes and runtime context capture.
- Dashboard views can answer basic failure, performance, I/O, validation, and business process questions.
