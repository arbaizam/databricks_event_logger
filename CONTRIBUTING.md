# Contributing

This guide is for people who maintain or extend the package. To learn how
the pieces fit together, read [API and architecture](docs/databricks-event-logger-design-spec.md)
first.

## Set up and check your work

```bash
python -m pip install -e ".[dev]"      # Add ",spark-test" to also run the PySpark schema tests.
python -m pytest tests/unit -q
python -m ruff check .
```

Run both commands before every commit. Live Databricks tests are opt-in.
See [docs/databricks_validation.md](docs/databricks_validation.md).

## Ground rules

1. **Logging must never break the application.** Code that runs while an
   event is delivered must not replace or hide the application's exception.
   Use `diagnostics.safe_text` for exception text and
   `diagnostics.warn_safely` for warnings.
2. **Fail early on setup mistakes.** Bad arguments should raise when a
   logger, scope, or decorator is created, before any business work runs.
3. **One source of truth.** Columns are listed only in `schema.py`. Allowed
   statuses and severities are listed only in `enums.py`. Everything else
   is built from those lists.
4. **No hidden work.** The logger never runs Spark actions, creates tables,
   retries, or starts background threads.
5. **Keep the core free of Databricks.** Only `sinks/delta.py` and
   `databricks/` may touch Spark or `dbutils`, and they import PySpark only
   inside functions.

## Common changes

### Add a new column to every event

Example: a `batch_id` column.

1. `record.py`: add `batch_id: str | None = None` to `EventRecord`. Then add
   `"batch_id"` to the matching field group, which here is
   `_OPTIONAL_TEXT_FIELDS`.
2. `schema.py`: add `Column("batch_id", "STRING", True)`.
3. If callers should set it, add a `batch_id` parameter to
   `EventLogger.record_event` (and to `event`/`EventScope` if it should be
   editable inside a block).
4. Run the tests. `test_deployment_ddl_matches_flat_event_columns` fails if
   `schema.py` and `EventRecord` don't match.
5. Existing Delta tables need `ALTER TABLE ... ADD COLUMNS`. Mention it in
   the release notes.

### Add a runtime identifier (job, cluster, and so on)

1. `context.py`: add a `str | None` field to `RuntimeContext`.
2. `schema.py`: add a nullable `STRING` column with the same name.
3. Optional: to discover it in notebooks, map it to its notebook JSON key in
   `_NOTEBOOK_JSON_KEYS` in `databricks/context.py`.

### Add a new sink

Create a class with an `emit(self, event)` method. There is no need to
inherit from anything. See `sinks/base.py` for the rules. In short: raise on
failure and don't retry. `sinks/memory.py` is the smallest example. Export
the new sink from `sinks/__init__.py` and the package `__init__.py`.

### Support a new metadata type

In `metadata.py`:

- For a single value, like a new date-like type, add a branch in
  `_Sanitizer._clean_scalar`.
- For an object with named fields, convert it to a `dict` in
  `_Sanitizer.clean`, next to the dataclass and named-record handling. It
  must be converted *before* the `Mapping` check, so its keys still get
  redacted.

Add a test in `tests/unit/test_metadata.py`.

### Add a status or severity value

Add a member to `EventStatus` or `EventSeverity` in `enums.py`. Validation
picks it up automatically.

### Make another field editable inside `event(...)`

1. `scope.py`: add the field to `EventScope`, and to `_result_fields()`.
2. `logger.py`: add a matching parameter to `EventLogger.event()`, and pass
   it to both `_new_record` and `EventScope(...)`.

## Where the tests live

| Test file | What it covers |
| --- | --- |
| `tests/unit/test_logger.py` | Logger behavior: scopes, parents, decorators, failure rules, health. |
| `tests/unit/test_record.py` | `EventRecord` field checks and cleanup. |
| `tests/unit/test_metadata.py` | Metadata conversion, redaction, and size limits. |
| `tests/unit/test_context.py` | `RuntimeContext` and `resolve_context`. |
| `tests/unit/test_sinks.py` | Console, memory, and Delta sinks, using a fake Spark. |
| `tests/integration/test_databricks.py` | Real Delta tables. Opt-in. |

## Style

- Write docstrings in plain words, in Google style (`Args:`, `Returns:`,
  `Raises:`). Say what the code does and why. Skip what the code already
  shows.
- Keep functions small, and name them after what they return or do.
- Keep error messages stable. Tests and users match on them.
