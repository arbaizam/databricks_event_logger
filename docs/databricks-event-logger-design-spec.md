# API and architecture

This package records structured events: what an operation did, how long it
took, and whether it worked. The application owns the work. The logger only
watches it and writes down the result.

For step-by-step recipes to change the code, see [CONTRIBUTING.md](../CONTRIBUTING.md).

## How an event flows

```text
  record_event / record_metric ──────────────┐
                                             │
  event(...) block ──── on exit ────────────►├──► _deliver ──► sink.emit(record)
        ▲                                    │        │
  logged_event / run_task (wrap a function) ─┘        └──► health counters
```

Every path ends in one method, `EventLogger._deliver`. It does three things:

1. **Finish the record.** It adds timing, the scope's results, error details
   (if the block raised), and metadata JSON, then builds the final
   `EventRecord`. Building the record checks every field.
2. **Send it** to `sink.emit(record)`.
3. **Count it** in the delivery health: attempted, then succeeded or failed.

An `event(...)` block works like this:

| When | What happens |
| --- | --- |
| `logger.event(...)` is called | The fixed fields are checked and an `event_id` is assigned. Bad arguments raise right away. |
| The block is entered | The current parent is recorded, the start time is taken, and this scope becomes the parent. |
| The block runs | The caller can edit `EventScope` fields: `metadata`, `row_count`, `status`, `severity`, `source_table`, and `target_table`. |
| The block exits | The previous parent is restored, the duration is measured, and one event is delivered. |

Durations use a monotonic clock. Timestamps are in UTC. An open block has no
stored row until it exits, so if the process is killed mid-block, no event is
written. There is no buffering, background thread, retry, automatic "started"
event, or hidden Spark action.

## Public API

| API | What it does |
| --- | --- |
| `EventLogger(...)` | Takes the app identity, sink, `RuntimeContext`, correlation ID, default metadata, limits, strict mode, and error-frame capture. The default sink is the console. The default correlation ID is a new UUID. |
| `logger.record_event(name, ...)` | Records a result you already have. Returns the delivered record, or `None` if a tolerated failure happened. |
| `logger.record_metric(name, value, ...)` | Records a finite number as a `metric` event. Nothing is calculated. |
| `logger.event(name, ...)` | A context manager that yields an `EventScope` and emits one event on exit. |
| `EventScope` | Editable `metadata`, `row_count`, `status`, `severity`, `source_table`, and `target_table`, plus a read-only `event_id`. Use each one only once. |
| `logger.logged_event(name, ...)` | A decorator for normal or `async` functions. It keeps the return value. Generators are rejected. |
| `logger.run_task(name, func, *args, ...)` | Calls `func` through `logged_event`, with event type `task`. For an async `func`, await the result. |
| `logger.bind(**metadata)` | Returns a logger with extra default metadata. It shares the sink, identity, correlation ID, parent tracking, and health. |
| `logger.health` | A `DeliveryHealth` snapshot: `attempted`, `succeeded`, `failed`, and `last_error`. |
| `RuntimeContext` | Clean runtime IDs (job, run, task, workspace, and more), with `job_url` and `job_run_url`. Unknown fields are rejected. |
| `databricks.resolve_context(dbutils=None, values=None)` | One best-effort read of the notebook context JSON, then explicit values on top. An explicit `None` clears a value. |
| `ConsoleSink`, `MemorySink`, `DeltaSink` | Built-in sinks. Each has `emit(EventRecord)`. |
| `create_table_sql(table_name)` | Returns the Delta `CREATE TABLE` SQL. Runs nothing. |
| `DeltaSink.validate()` | Checks the table's columns, types, and nullability. Doesn't check permissions. |

The allowed statuses are `started`, `success`, `failed`, `warning`, and
`skipped`. The allowed severities are `debug`, `info`, `warning`, `error`,
and `critical`. Event names and types are free-form strings.

Counts must be integers from 0 to the signed 64-bit maximum. Metrics must be
finite numbers. NumPy numbers work for both. Booleans are rejected for both.

## Where each part lives

| Module | Job |
| --- | --- |
| `logger.py` | `EventLogger`: the public methods, the scope lifecycle, parent tracking, and the delivery and failure rules. |
| `scope.py` | `EventScope`: the fields a caller can edit inside `event(...)`. |
| `decorators.py` | Wraps sync and async functions in a scope. Rejects generators. |
| `record.py` | `EventRecord`: one immutable event. Checks and cleans every field. |
| `enums.py` | `EventStatus` and `EventSeverity`. |
| `context.py` | `RuntimeContext`: plain runtime IDs. Never looks anything up by itself. |
| `schema.py` | `EVENT_SCHEMA`: the storage columns. The Delta SQL, the Spark schema, and validation all come from it. |
| `metadata.py` | Sanitizes and redacts metadata, then encodes it as JSON within a byte budget. |
| `failures.py` | Turns an exception into `error_class`, `error_message`, `stack_trace_hash`, and optional frames. |
| `health.py` | `DeliveryHealth` and the thread-safe `DeliveryTracker`. |
| `diagnostics.py` | `safe_text`, `truncate_text`, and `warn_safely`, which never raise. |
| `timing.py` | UTC timestamps and monotonic durations. |
| `errors.py` | Package exceptions, used only for setup mistakes. |
| `sinks/` | Delivery. `base.py` defines the `EventSink` interface. |
| `databricks/` | Databricks-only helpers. The core never imports `dbutils` or Spark. |

The dependencies flow one way. `logger.py` uses everything below it, and the
modules below it never import `logger.py`.

## Failure rules

The most important rule: **logging must never change what the application
does.**

- **Before work starts:** bad logger settings, bad fixed event fields, and
  non-mapping metadata raise right away. `EventLogger(...)` and `bind()`
  also check the actual metadata content, including every key.
- **At delivery:** anything that goes wrong while finishing or sending an
  event counts as a delivery failure. That includes bad metadata content,
  a bad value set on a scope, and a sink error. The failure is counted in
  `health.failed`, and then:
  - by default, a `RuntimeWarning` is issued and `None` is returned;
  - with `strict_logging=True`, the error is raised;
  - `KeyboardInterrupt`, `SystemExit`, and similar interrupts are always raised.
- **If the application already raised:** its original exception always
  propagates unchanged. The delivery failure is only counted, never raised.
- In strict mode, a failure in an inner scope interrupts the outer block, so
  the outer event correctly records `failed`.
- Warnings are sent in a way that can't raise, even with `-W error`.

## Metadata

`bind()` and `event(...)` copy the top level of the metadata dict, so adding
or replacing keys doesn't affect the original. Nested lists and dicts stay
shared until the event is serialized. Changes made to them after that point
are checked at delivery, under the delivery failure rules.

Serialization, in `metadata.py`:

- Converts dates, decimals, paths, enums, dataclasses, named tuples,
  PySpark `Row` objects, and NumPy numbers into JSON-friendly values.
- Hides the value of any key that contains a sensitive word, such as
  `password` or `token`. Matching ignores case. Dataclass and record fields
  are turned into dict keys first, so they are redacted too.
- Replaces unknown objects, too-deep nesting, and NaN or infinity with fixed
  markers. `repr()` is never called.
- Shortens long strings, and replaces JSON that is too big with a summary
  that includes a preview.
- Writes ASCII-only JSON with sorted keys, so the output is always valid UTF-8.

Redaction only checks key names. It can't find a secret written inside free
text.

## Parent tracking

The active scope ID is stored in a `ContextVar`, which a logger shares with
its bound loggers. Separate loggers track parents separately.

- `async` code works automatically. Each task gets its own copy of the context.
- Thread pools don't copy the context. Submit with `copy_context().run` to
  keep the parent. Use a fresh copy for every submission.
- A scope must be entered and exited in the same context, so never keep one
  open across a generator's `yield`. Put it around the loop that consumes
  the generator instead.

## Error details

`error_message` is shortened free text. It is not checked for secrets.
`stack_trace_hash` is a SHA-256 of the exception type and the code locations
it passed through, so failures from the same place share a hash. With
`capture_error_frames=True`, `error_frames_json` stores up to 20 frames. Each
frame has only the file name, function name, and line number. Source code
and local variables are never stored.

## Delta storage

`DeltaSink` writes one event at a time:

1. It builds a typed one-row DataFrame.
2. It registers the DataFrame as a uniquely named temporary view.
3. It runs `INSERT INTO ... (columns) SELECT columns FROM view`.
4. It drops the view.

If the view can't be dropped, the sink only warns. That way a cleanup
problem never hides an insert error, and a successful insert is never
reported as failed. The sink never creates the table.

`event_date` is the UTC date of `event_ts`. In a query session that uses a
different timezone, `to_date(event_ts)` can show a different date near
midnight. The logger never changes the application's Spark timezone.

## Testing

Unit tests cover the public behavior and the failure rules. They don't need
Spark. A few of them use PySpark types when PySpark is installed. The opt-in
integration tests insert real rows into generated tables inside
`EVENT_LOGGER_TEST_SCHEMA`. See the
[Databricks validation guide](databricks_validation.md).
