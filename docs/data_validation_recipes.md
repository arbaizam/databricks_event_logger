# Record data validation results

These recipes use the existing logger API. The application calculates a value,
decides whether it agrees with an expectation, and records the result. The logger
does not inspect DataFrames or decide whether work should continue.

## Use a consistent event format

| Field | Suggested meaning |
| --- | --- |
| `event_name` | Stable check name, such as `orders.total_matches`. |
| `event_type` | `validation`, an ordinary free-form event type. |
| `status` | `success` when the check passes; `failed` when it does not. |
| `severity` | `info` for a pass; `error` for a failed requirement. |
| `source_table` | Input table, when the DataFrame came from a table. |
| `row_count` | Number of input rows checked, when already known. |
| `metadata.validation` | `passed`, `actual`, `expected`, and `comparison`. |

Add units, tolerances, a rule version, and the input batch or table version when
they are needed to explain or reproduce the check. Keep names stable across runs;
use metadata and `correlation_id` to distinguish batches.

A mismatch is a result, not necessarily an exception. Setting `status="failed"`
does not raise, even with `strict_logging=True`. That setting controls **delivery
errors**, not validation outcomes. A delivered failed validation increments
`logger.health.succeeded`; health measures logging, not data quality. If an
advisory check should appear as `warning`, retain `passed=False` in its metadata.

## Setup

Run these examples in a Databricks notebook with an active `spark` session. This
small DataFrame makes the examples reproducible; substitute your own input and
expected values in application code.

```python
from decimal import Decimal

from pyspark.sql import functions as F

from databricks_event_logger import EventLogger, MemorySink

sink = MemorySink()
logger = EventLogger(sink=sink, app_name="orders")
checks = logger.bind(batch_id="example-batch")

orders = spark.createDataFrame(
    [(1, Decimal("40.00")), (2, Decimal("60.00"))],
    "order_id LONG, amount DECIMAL(18, 2)",
)
```

`MemorySink` lets you inspect the events locally in `sink.events`. For production,
configure a `DeltaSink` as shown in the [README](../README.md#use-delta-in-databricks).
Set `source_table` on the events when the input is a named table; these sample
DataFrames have no source table.

## 1. Compare an exact row count

The scope includes the Spark action, so it records the time spent counting. An
exception from the calculation produces a failed event and propagates unchanged.
Change `expected` to `3` to record a mismatch that allows the notebook to continue.

```python
expected = 2
with checks.event("orders.row_count_matches", event_type="validation") as event:
    actual = orders.count()
    passed = actual == expected
    event.row_count = actual
    event.status = "success" if passed else "failed"
    event.severity = "info" if passed else "error"
    event.metadata["validation"] = {
        "passed": passed,
        "actual": actual,
        "expected": expected,
        "comparison": "equal",
    }
```

`row_count` holds a 64-bit integer. Keep exact counts there or in metadata;
`metric_value` converts numbers to a float and can lose precision for large integers.

## 2. Compare an exact decimal total

Use a decimal input column and a decimal expectation when exact decimal agreement
is required. This rule also requires at least one row and no missing amounts.
Spark ignores nulls in a sum and returns null for an empty or all-null input, so
do not silently substitute zero. See [Spark's aggregate null rules](https://spark.apache.org/docs/latest/sql-ref-null-semantics.html#builtin-aggregate-expressions).

```python
expected = Decimal("100.00")
with checks.event("orders.total_matches", event_type="validation") as event:
    totals = orders.agg(
        F.count("*").alias("rows"),
        F.count("amount").alias("amount_rows"),
        F.sum("amount").alias("total"),
    ).first()
    actual = totals["total"]
    complete = totals["rows"] > 0 and totals["amount_rows"] == totals["rows"]
    passed = (
        complete
        and actual is not None
        and actual.is_finite()
        and actual == expected
    )
    event.row_count = totals["rows"]
    event.status = "success" if passed else "failed"
    event.severity = "info" if passed else "error"
    event.metadata["validation"] = {
        "passed": passed,
        "actual": actual,
        "expected": expected,
        "comparison": "equal",
        "unit": "USD",
        "null_amounts": totals["rows"] - totals["amount_rows"],
        "requires_nonempty": True,
    }
```

The logger serializes `Decimal` metadata as strings, preserving the supplied
decimal values. `metric_value` rejects `Decimal`; converting it to a float first
can lose the precision this check needs. Choose rounding and decimal precision
in the calculation itself, based on your business rule.

## 3. Compare floating-point results with a tolerance

For approximate calculations, specify the allowed difference. This example uses
only an absolute tolerance: `abs(actual - expected) <= 0.000001`. Reject missing
or non-finite results explicitly. Python's [math.isclose](https://docs.python.org/3/library/math.html#math.isclose)
also supports relative tolerance; record both settings so the check is explainable.

```python
import math

measurements = spark.createDataFrame([(0.1,), (0.2,)], "reading DOUBLE")
expected = 0.15
abs_tol = 0.000001
rel_tol = 0.0

with checks.event("readings.mean_matches", event_type="validation") as event:
    totals = measurements.agg(
        F.count("*").alias("rows"),
        F.count("reading").alias("reading_rows"),
        F.avg("reading").alias("mean"),
    ).first()
    actual = totals["mean"]
    complete = totals["rows"] > 0 and totals["reading_rows"] == totals["rows"]
    passed = (
        complete
        and actual is not None
        and math.isfinite(actual)
        and math.isclose(actual, expected, rel_tol=rel_tol, abs_tol=abs_tol)
    )
    event.row_count = totals["rows"]
    event.status = "success" if passed else "failed"
    event.severity = "info" if passed else "error"
    event.metadata["validation"] = {
        "passed": passed,
        "actual": actual,
        "expected": expected,
        "comparison": "isclose",
        "abs_tol": abs_tol,
        "rel_tol": rel_tol,
        "null_readings": totals["rows"] - totals["reading_rows"],
        "requires_nonempty": True,
    }
```

The expectations and tolerances here are valid constants. Validate externally
supplied expectations and tolerances before using them. For pandas/NumPy results,
handle missing values before comparison and convert a scalar NumPy boolean with
`bool(passed)`; the metadata serializer does not support NumPy booleans.

## 4. Run several checks and optionally stop work

Calculate related values in one [DataFrame aggregation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.agg.html)
and bring back only its summary row. Record one event per check. The enclosing
scope supplies their parent ID and records the overall outcome; child failures
do not automatically change their parent's status.

```python
stop_on_failure = False  # Set True when failed requirements must stop the task.

with checks.event("orders.validate", event_type="validation_run") as run:
    totals = orders.agg(
        F.count("*").alias("rows"),
        F.count("amount").alias("amount_rows"),
        F.sum("amount").alias("total"),
    ).first()
    # Each entry is (stable check name, actual result, expected result).
    results = [
        ("orders.row_count_matches", totals["rows"], 2),
        ("orders.null_amounts", totals["rows"] - totals["amount_rows"], 0),
        ("orders.total_matches", totals["total"], Decimal("100.00")),
    ]
    failed = []
    for name, actual, expected in results:
        passed = actual is not None and actual == expected
        if not passed:
            failed.append(name)
        checks.record_event(
            name,
            event_type="validation",
            status="success" if passed else "failed",
            severity="info" if passed else "error",
            row_count=totals["rows"],
            metadata={"validation": {
                "passed": passed,
                "actual": actual,
                "expected": expected,
                "comparison": "equal",
            }},
        )

    run.row_count = totals["rows"]
    run.status = "failed" if failed else "success"
    run.severity = "error" if failed else "info"
    run.metadata.update(checks_run=len(results), checks_failed=len(failed))
    if failed and stop_on_failure:
        raise ValueError(f"Data validation failed: {', '.join(failed)}")
```

This suite has three independent requirements: two rows, no null amounts, and
the expected total. A total can pass while the null check fails; the suite then
fails. Use recipe 2 when the total check itself must require complete inputs.
The explicit `raise` enforces the requirement. Do not use a Python `assert` as a
production control; assertions can be disabled. Individual check events have no
duration here; the parent measures the aggregation and check delivery together.

## Delivery and querying

Each Delta event is a separate synchronous insert. Log checks or small summaries,
not one event for every data row. A validation result does not prove delivery:
`record_event` returns `None` for a tolerated delivery failure. Scopes expose no
delivery return value; inspect `logger.health` or choose strict logging when
missing records must stop otherwise successful work. Existing business exceptions
still take precedence over delivery failures in strict mode.

Metadata is redacted and size-limited (4000 bytes by default); oversized metadata
can become a truncation summary that omits the original fields. Keep the evidence
small. Store large diagnostics separately and record a reference. Strict logging
does not turn metadata truncation into an error or guarantee a complete audit trail.

For events stored in the example Delta table from the README:

```sql
SELECT event_ts, correlation_id, parent_event_id, event_name, status,
       get_json_object(metadata_json, '$.validation.actual') AS actual,
       get_json_object(metadata_json, '$.validation.expected') AS expected,
       get_json_object(metadata_json, '$.validation.comparison') AS comparison
FROM main.observability.event_log
WHERE event_type = 'validation'
ORDER BY event_ts DESC;
```

These extracted values are text; cast them to the appropriate type for numeric
queries. A null extraction can mean missing or truncated evidence, not a passed
check. A failed calculation can produce a failed event without comparison values;
inspect `error_class` and `error_message` to distinguish it from a recorded mismatch.

## Keep future additions small

No package change is required for these recipes. Start by sharing the event
format above across your application. If repeated logging code becomes a burden,
one possible addition is a `record_validation(name, *, passed, actual, expected,
...)` convenience method that delegates to `record_event`.

Such a method should accept an already computed boolean, apply the agreed status
and metadata format, and return the usual `EventRecord | None` delivery result.
It should use the existing sink, parent tracking, and failure rules. It would
not need new columns, dependencies, or statuses. This is a proposal, not an API
available in this version.

Keep DataFrame execution, comparison operators, tolerances, null policy, and
decisions to stop work in application code. An execution engine, rule registry,
or automatic DataFrame inspection would add much more complexity than this
recording use case requires.
