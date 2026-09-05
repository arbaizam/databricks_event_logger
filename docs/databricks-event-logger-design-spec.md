# API and architecture

The package records structured events for one engineering team. The core owns
event construction, correlation, timing, outcome capture, and delivery policy.
Application code owns the work being observed. Databricks discovery and Delta
persistence are adapters.

## Event pipeline

```text
record_event ─────────────────────────┐
                                     v
event scope → construct and normalize event → sink.emit(record)
     ^
logged_event / observed
```

There is one lifecycle implementation. Entering a scope validates static
arguments, allocates its ID, captures its parent, and starts timing. Exiting
restores the enclosing scope and emits one final record. Duration uses a
monotonic clock; timestamps are UTC. Exceptions produce a failed record and are
re-raised unchanged, even when failure-event delivery also fails.

An open scope has no persisted row until it exits. Process termination before
exit can therefore leave no event. There is no buffering, background worker,
retry loop, automatic start event, or implicit Spark action.

## Public API

| API | Contract |
| --- | --- |
| `EventLogger(...)` | Accepts application identity, sink, `RuntimeContext`, correlation ID, default metadata, strict mode, and optional error-frame capture. Console is the default sink; correlation defaults to a fresh UUID. |
| `logger.record_event(name, ...)` | Records an already-known result. Returns the delivered record or `None` after a tolerated internal failure. Metadata is passed as a mapping. |
| `logger.record_metric(name, value, ...)` | Records a known finite numeric value as a metric event. Performs no calculation. |
| `logger.event(name, ...)` | Returns a context manager yielding an `EventScope`. Emits once on exit. |
| `EventScope` | Editable `metadata`, `row_count`, `status`, `severity`, `source_table`, and `target_table`; read-only `event_id`. Use once. |
| `logger.logged_event(name, ...)` | Observes a synchronous or asynchronous function through the same scope. Preserves its return value. Rejects generators and async generators. |
| `logger.run_task(name, func, *args, ...)` | Calls a function through the same decorator, marking the event type as `task`. Await the result for async functions. |
| `logger.bind(**metadata)` | Creates an independently enriched logger sharing sink, identity, correlation, parent tracking, and delivery health. Per-event metadata has precedence. |
| `logger.health` | Immutable attempted/succeeded/failed counters and the last failure text. Counts emission attempts, including internal preparation failures. |
| `use_logger(logger)` | Installs an optional default logger for its context and restores the previous one on exit. |
| `observed(name, ...)` | Resolves that default at invocation; supports the same callable kinds as `logged_event`. |
| `RuntimeContext` | Normalized runtime identity with navigation URL properties. Explicit unknown fields are rejected. |
| `databricks.resolve_context(dbutils=None, values=None)` | Best-effort notebook JSON discovery followed by explicit canonical overrides, including explicit `None`. |
| `ConsoleSink`, `MemorySink`, `DeltaSink` | Implement `emit(EventRecord)`. Memory retains records for application tests. |
| `create_table_sql(table_name)` | Generates Delta table DDL from the storage schema. Executes no SQL. |
| `DeltaSink.validate()` | Checks accessible table schema compatibility. Does not test insert permission, table constraints, or provider. |

Strings are accepted for event names and types. Supported statuses are `started`,
`success`, `failed`, `warning`, and `skipped`; severities are `debug`, `info`,
`warning`, `error`, and `critical`. Scopes default to `success` on normal exit;
exceptions override the outcome to `failed`. Use a direct `started` event only
when the application explicitly needs one.

Counts must be nonnegative signed 64-bit integers. Metric values must be finite
numbers and normalize to floating point. Event identity, timestamps, runtime
fields, table names, metrics, error details, and metadata serialize to one flat
row through `EventRecord.as_dict()`.

## Ownership and failure boundaries

- `logger.py` owns the shared scope lifecycle, context-local parent tracking,
  metadata binding, decorators, and delivery policy.
- `event.py` owns the event dataclass and field validation. Internally it carries
  runtime context as one value; serialized rows flatten it.
- `serialization.py` owns bounded metadata normalization and sensitive-key
  redaction. Supported object conversions happen before recursive redaction;
  unknown objects and depth limits use fixed representations.
- `context.py` owns the plain runtime identity. `databricks/` owns the one
  supported discovery adapter.
- `sinks/` owns delivery. The Delta adapter has one explicit storage schema that
  supplies column ordering, Spark types, validation, and generated DDL.

Invalid setup and call arguments raise a validation error before work starts.
Editable scope fields are validated at exit; invalid edits are preparation
failures. Internal preparation or sink failures increment failed delivery health.
In default mode they do not fail business execution; in strict mode they propagate if execution has not
already failed. Diagnostics must never replace an active business exception or
leave an abandoned scope installed.

Binding copies the top-level metadata mapping; adding or replacing keys does not
mutate the original logger. Nested mutable values remain shared until event
serialization. Parent tracking is context-local. Work observed across `await`
uses the same ordinary scope; async decorators await the complete function.

## Error details and storage

Exception messages are truncated free text, not a guarantee of secret removal.
Metadata redaction applies to sensitive keys and cannot infer every secret in
arbitrary text. Prefer selected operational metadata over raw request bodies.

With `capture_error_frames=True`, the record includes a bounded JSON array of
up to 20 frames containing file basename, function, and line. No source lines or
locals are collected. The exception type and frame locations supply a grouping
hash independently of the exception message.

Delta delivery creates a typed one-row temporary view, inserts into the
pre-existing destination with explicit columns, and removes the view. Required
columns and exact types must match. Nullable record fields must accept nulls;
additional destination columns must be nullable. Table provisioning is explicit.

Unit tests cover the public behavior and failure boundaries. Opt-in Databricks
integration tests exercise actual inserts and schema checks using only generated
tables within `EVENT_LOGGER_TEST_SCHEMA`. Local fake Spark tests cannot establish
runtime permissions or persistence behavior.
