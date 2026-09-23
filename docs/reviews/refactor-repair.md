# Refactor repairs

These changes repair the review of `77f33cf..646736b`. Runtime behavior is
compared with `77f33cf`; the reviewed branch's new module structure is retained.

| Review issue | Repair |
| --- | --- |
| Deleted import paths and unreadable saved objects | Added warning-free `event`, `events`, and `serialization` aliases. Restored the scope's `_event` state and accept `_template` state too. Tests load records, scopes, enums and health actually pickled by the baseline. |
| Changed application-failure hashes | Registered legacy storage locations for scope/decorator frames and the sink call. No Python exception or traceback is mutated. Tests cover ordinary/async failures and a strict inner sink failure recorded by an outer scope. |
| Incomplete column recipe and silent dropped values | Documented every forwarding step. Scope initialization uses one result mapping, and final scope fields are derived from its dataclass. Independent tests check each direct argument, initial scope value and edited result reaches the record. |
| Timestamp error precedence | Restored the original validation order; tests cover multiple invalid timestamps. |
| Incorrect validation, health and exception documentation | Separated setup checks from deferred metadata/result checks and corrected exception types, interrupt behavior and health accounting. |
| README facts lost during simplification | Restored mapping support, immutable health snapshots, ordinary-error qualifications and redaction/depth traversal limits. |
| Overstated Delta cleanup guarantees | Documented the existing registration boundary and ordinary-cleanup-exception policy. Cleanup interrupts are still interrupts, preserving the baseline behavior. |
| Incomplete public documentation | Added argument constraints, return/error descriptions, runtime identifier meanings, schema type limits, helper contracts and partial-marker behavior. |
| Implicit binding ownership | Grouped shared health/parent state in `_LoggerState`; an ownership test requires a decision whenever a logger attribute is added. Existing shallow sharing of nested metadata and subclass attributes remains supported. |

## Verification

- 193 tests passed, including all original assertions; six live Databricks
  integration tests skipped because no workspace was available.
- Ruff passed; the wheel built and contains the compatibility modules and typing marker.
- The committed differential probe matches 7,872 observations against `77f33cf`.
  This includes 7,000 seeded metadata comparisons, every byte budget from 2
  through 200, 150 exception-policy combinations, record/context validation,
  discovery, parent tracking, warnings, event fields, DDL and Spark schemas.
- A disposable column experiment confirmed that omitting value forwarding
  fails the new guard. Completing the direct and editable-field recipes passes.

Run `python scripts/compare_baseline.py` for the saved baseline contract, or
pass `--baseline PATH` to compare with a checkout at `77f33cf`. The probe does
not normalize failure hashes. It excludes generated IDs and clock readings,
and compares warning caller functions rather than relocated source line numbers.

The preserved SDK frame positions are compatibility labels, not links into
current source. Application frames keep their actual locations. Fingerprints
still depend on the complete captured stack; changes to application code or
dependencies can change them. Real Unity Catalog permissions, Delta transactions
and serverless behavior still require the opt-in Databricks integration tests.
