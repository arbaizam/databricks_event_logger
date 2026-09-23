# Prompt for Astra: adversarial review of the readability refactor

You are Astra. Do an **adversarial code review** of one refactor in this
repository. Your job is to find reasons this change should *not* be merged
as it is. Don't confirm that it looks fine. Assume the author missed things.
Check every claim below against the code yourself, and don't trust the
author's summary.

## Where to look

- Repository: `arbaizam/databricks_event_logger`
- Branch under review: `claude/refactor-code-readability-c19ls0`
- Baseline (before the refactor): commit `77f33cf`
  ("Harden event metadata and simplify logger binding")
- The diff to review: `git diff 77f33cf..origin/claude/refactor-code-readability-c19ls0`

```bash
git fetch origin claude/refactor-code-readability-c19ls0
git checkout claude/refactor-code-readability-c19ls0
python -m pip install -e ".[dev,spark-test]"   # If PySpark won't build, use a fresh venv with an upgraded setuptools.
python -m pytest tests -q
python -m ruff check .
```

## The owner's objectives (the standard to judge against)

The owner asked for exactly this:

> Refactor this code for readability. A junior developer should be able to
> maintain and enhance it. Ensure the documentation and doc strings are
> written comprehensively, but succinctly using simple terms. Do not add or
> remove features, but re-design it from a first principles perspective to
> ensure future enhancements can be made.

Review against each objective separately:

1. **Readability for a junior developer.** Could someone new to the codebase
   find where a behavior lives, understand it, and change it safely? Point
   out any code that is still clever, dense, or surprising. That includes
   hidden coupling, private attributes reached across modules, `object.__setattr__`
   on frozen dataclasses, `ContextVar` tokens, `copy()`-based sharing in
   `bind()`, and the binary search in `metadata._truncation_summary`.
2. **Documentation quality.** Check that the docs are *comprehensive* (every
   public class, function, and module explains what it does, its arguments,
   return values, and exceptions) and *succinct, in simple terms* (no
   jargon without explanation, no filler, and nothing that just repeats the
   code). Flag docstrings that are wrong, out of date, missing, too long, or
   too vague. Check every factual claim in the docs against the code.
3. **No features added or removed.** This is the most important check.
   Treat any change in observable behavior as a defect unless you can argue
   it doesn't count. Consider return values, exception types and messages,
   warnings (text, category, and `stacklevel`), health counters, event field
   values, metadata JSON output, SQL/DDL text, and import paths.
4. **First-principles design for future enhancements.** Are the module
   boundaries the right ones? Does each concept have exactly one home? Walk
   through the recipes in `CONTRIBUTING.md` (add a column, add a runtime
   identifier, add a sink, add a metadata type, add a status, make a field
   editable). Do they work as written, and are they complete? What
   enhancement would still be awkward or risky with this design?

## What changed (the author's claims, which you should verify)

- `logger.py` (432 lines) was split into: `logger.py` (public API, scope
  lifecycle, delivery and failure rules), `scope.py` (`EventScope`),
  `decorators.py` (`wrap_in_scope`), `failures.py` (`describe_exception`),
  `health.py` (`DeliveryHealth`, `DeliveryTracker`), and `diagnostics.py`
  (`safe_text`, `truncate_text`, `warn_safely`).
- These modules were renamed: `event.py` → `record.py`, `events.py` →
  `enums.py`, and `serialization.py` → `metadata.py`. The test files were
  renamed to match: `test_event_model.py` → `test_record.py` and
  `test_serialization.py` → `test_metadata.py`.
- The storage schema moved from `sinks/delta.py` to a new `schema.py`, as
  `Column` NamedTuples. `sinks/delta.py` still imports `EVENT_SCHEMA` and
  `EVENT_COLUMNS`. The private `_event_schema()` was renamed `_spark_schema()`.
- `EventRecord.__post_init__` and `RuntimeContext.__post_init__` were
  broken into small named helper functions and grouped field lists.
- The nested `normalize` closure in metadata sanitization became the
  `_Sanitizer` class.
- The delivery path was split into `_deliver`, `_finish_record`, and
  `_should_raise`.
- Docstrings were rewritten in Google style. The architecture doc was
  rewritten, and a new `CONTRIBUTING.md` was added.
- The tests only changed their import paths. No assertions were changed.

The author says they ran these checks: 178 unit tests pass (with PySpark),
ruff is clean, and a throwaway differential harness (not committed)
compared the baseline and the refactor on about 6,000 randomized
`serialize_metadata`/`sanitize_metadata` inputs, the `EventRecord` and
`RuntimeContext` error messages, an end-to-end logger scenario, the
`resolve_context` discovery cases, and warning attribution frames.
**Don't rely on that harness.** Build your own comparison against `77f33cf`.

## Known risks and gaps the author admits (confirm or dispute each)

1. **Deep import paths are gone.** There are no compatibility shims for
   `databricks_event_logger.event`, `.events`, or `.serialization`.
   `safe_text` and `TRUNCATED_MARKER` now live in `diagnostics`.
   `VALID_STATUSES`/`VALID_SEVERITIES` became `enums.STATUS_VALUES`/
   `SEVERITY_VALUES`. The top-level `databricks_event_logger` exports didn't
   change. Is this "removing a feature"? Should shims be added?
2. **New public-looking names were added.** These include
   `metadata.copy_metadata`, `metadata.check_is_mapping`, `NONFINITE_VALUE`,
   `DEFAULT_MAX_DEPTH`, `DEFAULT_ERROR_MESSAGE_MAX_CHARS`, the
   `schema.Column` type, and `DeliveryTracker`. Is this "adding features"?
3. **Private names were renamed.** `EventScope._event` became `_template`,
   `EventLogger._delivery` became `_health`, and `_current_event_id` became
   `_active_event_id`. Anything that reached into these private names will
   break.
4. **Error precedence changed.** When several timestamp fields are invalid
   at once, `EventRecord` may now report a different field first, because
   the checks run in a different order.
5. **The README wasn't simplified.** That work was interrupted, so the
   README still has the original dense wording. Judge whether it meets the
   "simple terms" objective.
6. **Live Databricks integration tests weren't run.** The 6 tests in
   `tests/integration/` skipped because no Databricks workspace was available.
7. **Warning `stacklevel`s now go through `warn_safely`**, which adds 1.
   Check that every warning still points at the same caller frame as
   before, both in `EventLogger._deliver` and in `DeltaSink._drop_view_quietly`.

## Attack these areas especially

- **Exception safety:** a business exception must always propagate
  unchanged, even when the sink raises `SystemExit` or `KeyboardInterrupt`,
  when `str(exc)` raises, and when warnings are errors (`-W error`), or when
  `warnings.warn` itself is patched to raise. Check the
  `try/except BaseException` in `_deliver` and the `finally` in `_run_scope`.
- **The strict-mode rules:** check `_should_raise` against the baseline's
  `error is None and (strict or not isinstance(exc, Exception))`.
- **Parent tracking:** check the `ContextVar` token reset order, nested
  scopes, `bind()` sharing, threads with and without `copy_context()`, and
  concurrent `asyncio` tasks.
- **Metadata:** check that redaction happens before descending into
  dataclasses, named tuples, and PySpark Rows. Look at malformed named
  records, enum values that contain dicts, the depth limit, truncation
  markers when `string_max_chars` is smaller than the marker, and the
  truncation summary at byte budgets from 2 to 200. Also check surrogate
  characters and NumPy bool handling.
- **The Delta sink:** check that view cleanup runs on every path, that the
  column list comes only from `schema.py`, and that the DDL text is
  byte-for-byte the same as the baseline.
- **`resolve_context`:** check that an invalid discovered value drops all
  discovered values (baseline behavior), that explicit `None` still
  overrides, and that unknown explicit keys still raise.
- **Documentation accuracy:** check every claim in the docstrings,
  `docs/databricks-event-logger-design-spec.md`, and `CONTRIBUTING.md`
  against the code. For example, check the claim that the modules below
  `logger.py` never import it.

## How to report

Rank your findings from most to least severe. For each one, give:

- **Objective violated:** 1, 2, 3, or 4, from the list above.
- **Location:** `file:line`.
- **Claim:** one sentence.
- **Evidence:** a concrete input, scenario, or quote that shows the problem,
  with the baseline and refactored behavior side by side where it applies.
- **Severity:** blocker, major, or minor.
- **Suggested fix:** the smallest change that addresses it.

End with a clear verdict: *merge*, *merge after fixes*, or *reject*, with a
one-paragraph reason. Leave out praise and style nits that no objective
requires. If you find no real problem in an area, say so in one line and
move on.
