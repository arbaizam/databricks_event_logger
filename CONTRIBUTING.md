# Contributing

This guide is for people who maintain or extend the package. To learn how
the pieces fit together, read [API and architecture](docs/databricks-event-logger-design-spec.md)
first.

## Set up and check your work

```bash
python -m pip install -e ".[dev,spark-test]"
python -m pytest tests -q
python -m ruff check .
```

The PySpark tests use local types and Rows; they do not start Spark. Without
`spark-test`, these tests and the saved differential contract test skip.
Run both checks before every commit. Live Databricks tests are opt-in.
See [docs/databricks_validation.md](docs/databricks_validation.md).

For a full comparison with the original source checkout:

```bash
python scripts/compare_baseline.py --baseline ../review-baseline
```

The baseline checkout must be at `77f33cf`. Without `--baseline`, the command
checks the committed observation digests. The same probe runs in separate
interpreters for each version. It compares output, errors, health, metadata,
warning caller functions, parents, SQL and failure hashes; random IDs and clock
readings are excluded. Physical warning line numbers may move with source.
The fixture is regenerated only from `77f33cf` with `--write-baseline`, never
from changed code to make a failure disappear. Intentional future features
need a separately reviewed contract update, not a silent baseline replacement.

## Ground rules

1. **Logging must never break the application.** Code that runs while an
   event is delivered must not replace or hide the application's exception.
   Use `diagnostics.safe_text` for exception text and
   `diagnostics.warn_safely` for warnings.
2. **Preserve validation boundaries.** Invalid settings, fixed event fields
   and non-mapping metadata raise before work. Per-event metadata contents
   and scope edits are checked at delivery, counted in health, and follow the
   strict-mode policy. Redacted/depth-limited branches are not traversed.
3. **One source of truth.** Columns are listed only in `schema.py`. Allowed
   statuses and severities are listed only in `enums.py`. Everything else
   is built from those lists.
4. **No hidden application work.** Never compute a row count or run the
   application's Spark actions. Delta delivery itself runs an insert action.
   The logger never creates tables, retries, or starts background threads.
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
   `EventLogger.record_event`, **and pass `batch_id=batch_id` to `_new_record`**.
   For editable scope support, follow [the editable-field recipe](#make-another-field-editable-inside-event).
4. In `tests/unit/test_extension_contracts.py`, add `"batch_id": "B42"` to
   `FIELD_VALUES`. The direct-forwarding test must assert the delivered value
   is `"B42"`; a test checking only column names cannot catch a dropped value.
   Add record validation tests for accepted and rejected values as appropriate.
   Run the suite. `test_deployment_ddl_matches_flat_event_columns` separately
   checks that the storage schema and flattened record have matching names.
5. Existing Delta tables need `ALTER TABLE ... ADD COLUMNS`. Mention it in
   the release notes.

### Add a runtime identifier (job, cluster, and so on)

1. `context.py`: add a `str | None` field to `RuntimeContext`.
2. `schema.py`: add a nullable `STRING` column with the same name.
3. Optional: to discover it in notebooks, map it to its notebook JSON key in
   `_NOTEBOOK_JSON_KEYS` in `databricks/context.py`.
4. Test integer-to-string normalization, flattened record output and DDL.
   For discovery, test the notebook key, explicit override and explicit None.
5. Migrate existing tables as in the column recipe. Runtime identifiers are
   strings; a structured value needs a separate validation/storage design.

### Add a new sink

Create a class with an `emit(self, event)` method. There is no need to
inherit from anything. See `sinks/base.py` for the rules. In short: raise on
failure and don't retry. `sinks/memory.py` is the smallest example. Export
the new sink from `sinks/__init__.py` and the package `__init__.py`.
Keep `emit` synchronous. Test successful delivery, propagated failures, and
cleanup paths if it owns resources. Test failures through EventLogger too,
including strict mode and an already-active business exception.

### Support a new metadata type

In `metadata.py`:

- For a single value, like a new date-like type, add a branch in
  `_Sanitizer._clean_scalar`.
- For an object with named fields, convert it to a `dict` in
  `_Sanitizer.clean`, next to the dataclass and named-record handling. It
  must be converted *before* the `Mapping` check, so its keys still get
  redacted.

Test conversion, sensitive nested fields, depth limits, malformed values and
byte limits in `tests/unit/test_metadata.py`. Never call recursive conversion
on a child before its containing key has been checked for redaction.

### Add a status or severity value

Add a member to `EventStatus` or `EventSeverity` in `enums.py`. Validation
picks it up automatically.
Test both the enum member and its string, including an edited scope. Update
the list in the architecture document and document what the value means.

### Make another field editable inside `event(...)`

1. Ensure the field exists and is validated in `EventRecord`; if it is new,
   follow the column recipe and plan a table migration.
2. `scope.py`: add the public field and its default to `EventScope`. Its result
   mapping is derived from the dataclass, so there is no second field list.
   Metadata remains separate; identity and timing fields owned by the lifecycle
   are not ordinary editable results.
3. `logger.py`: add the parameter to `EventLogger.event()` and include it in
   the `results` mapping. That mapping initializes both validation and scope.
4. Add a representative value to `FIELD_VALUES` in
   `tests/unit/test_extension_contracts.py`. Tests check the initial value and
   an edit inside the block both reach delivery. Also test invalid edits and
   failure overrides where applicable. Update the scope/API documentation.

### Add mutable logger state

Decide its owner first. State shared by all bindings belongs in `_LoggerState`;
per-binding state must be copied/replaced in `bind()`. Configuration attributes
are shallow-copied, including any subclass attributes, to preserve existing
behavior. Update the ownership test in `test_extension_contracts.py` whenever
an attribute is added, and test whether updates should cross binding boundaries.

### Change error handling

Keep scope parent reset before delivery. New computed scope results must be
prepared inside the delivery failure boundary if they can fail; do not add
fallible computations to the scope's finally block. Test the same exception
object survives with strict mode, sink interrupts, broken __str__, and warning
handlers that raise. Stored scope/decorator locations are compatibility labels
in `_trace_compat.py`; application traceback locations must not be rewritten.

## Where the tests live

| Test file | What it covers |
| --- | --- |
| `tests/unit/test_logger.py` | Logger behavior: scopes, parents, decorators, failure rules, health. |
| `tests/unit/test_record.py` | `EventRecord` field checks and cleanup. |
| `tests/unit/test_metadata.py` | Metadata conversion, redaction, and size limits. |
| `tests/unit/test_context.py` | `RuntimeContext` and `resolve_context`. |
| `tests/unit/test_sinks.py` | Console, memory, and Delta sinks, using a fake Spark. |
| `tests/unit/test_extension_contracts.py` | Value forwarding and explicit binding ownership. |
| `tests/unit/test_compatibility.py` | Legacy imports, saved objects and validation order. |
| `tests/unit/test_baseline_contract.py` | Independent observations captured from 77f33cf. |
| `tests/integration/test_databricks.py` | Real Delta tables. Opt-in. |

## Style

- Write docstrings in plain words, in Google style (`Args:`, `Returns:`,
  `Raises:`). Say what the code does and why. Skip what the code already
  shows.
- Keep functions small, and name them after what they return or do.
- Keep error messages stable. Tests and users match on them.
