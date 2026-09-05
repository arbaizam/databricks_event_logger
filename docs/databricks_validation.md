# Validate on the team's Databricks runtime

Local schema tests cannot establish Unity Catalog permissions, Delta transaction
behavior, or Spark Connect/serverless behavior. Run this procedure on the compute
and identity the team intends to use. It is a procedure, not a record of a
completed live validation.

## Run the integration tests

Install the package and pytest. Open a Databricks Python notebook with the
repository as its working directory, and choose an existing disposable schema
where the test identity can create, read, insert into, alter, and drop tables:

```python
import os

import pytest

os.environ["EVENT_LOGGER_TEST_SCHEMA"] = "main.event_logger_tests"
result = pytest.main(["tests/integration", "-q", "-rs"])
assert result == 0
```

Run pytest in the notebook's Python process so it can use the active Spark
session. Tests create uniquely named tables and remove only those generated
tables afterward. Check the skip reasons: a successful pytest exit can include
skipped tests. The suite skips without the opt-in schema or an active session;
runtime context discovery can also skip when notebook JSON is restricted.

The suite covers actual inserts, schema compatibility, generated Delta DDL,
missing-table behavior, UTC partition dates, and accessible context JSON. Also
check concurrent jobs appending to a shared disposable table and the intended
task identity's insert permission. A `validate()` success only establishes
schema compatibility.

The UTC boundary test inserts an event near UTC midnight through a separate
session using `America/Chicago`. It checks that stored `event_date` remains UTC
while the displayed timestamp and `to_date(event_ts)` use the previous local
date. The application's session timezone is unchanged. The
[query examples](event_queries.sql) select UTC explicitly in a separate query
session.

## Measure synchronous delivery

This optional notebook snippet creates and removes one uniquely named table in
the chosen test schema. Choose `event_count` to approximate the team's busiest
task, and use representative metadata. Every event is delivered immediately.

```python
from statistics import median
from time import perf_counter
from uuid import uuid4

from databricks_event_logger import DeltaSink, EventLogger, create_table_sql

test_schema = os.environ["EVENT_LOGGER_TEST_SCHEMA"].strip()
table = f"{test_schema}.event_logger_measure_{uuid4().hex}"
ddl = create_table_sql(table)  # Validates the three-part table identifier.
event_count = 20
latency_ms = []
try:
    spark.sql(ddl).collect()
    logger = EventLogger(sink=DeltaSink(spark, table), strict_logging=True)
    commits_before = spark.sql(f"DESCRIBE HISTORY {table}").count()
    files_before = spark.sql(f"DESCRIBE DETAIL {table}").first()["numFiles"]
    for index in range(event_count):
        started = perf_counter()
        logger.record_event("delivery.measure", metadata={"sample": index})
        latency_ms.append((perf_counter() - started) * 1000)
    commits_after = spark.sql(f"DESCRIBE HISTORY {table}").count()
    files_after = spark.sql(f"DESCRIBE DETAIL {table}").first()["numFiles"]
    assert spark.table(table).count() == event_count
    print({
        "health": logger.health,
        "latency_ms": latency_ms,
        "first_ms": latency_ms[0],
        "median_ms": median(latency_ms),
        "max_ms": max(latency_ms),
        "new_history_entries": commits_after - commits_before,
        "active_data_files_before": files_before,
        "active_data_files_after": files_after,
        "net_active_data_files": files_after - files_before,
    })
finally:
    spark.sql(f"DROP TABLE IF EXISTS {table}").collect()
```

Keep the first-event cost visible and record runtime, compute type, event count,
and metadata size with the results. History entries count recorded table
operations; inspect the history if maintenance also ran during measurement.
`numFiles` measures active data files, not all historical files or transaction-log
files. Compaction can change the net count, so do not assume one file per event.
See [table history](https://docs.databricks.com/aws/en/tables/history) and
[table details](https://learn.microsoft.com/en-us/azure/databricks/tables/operations/table-details).

Measure representative concurrent tasks separately. Delivery failures remain
visible in health and are not automatically retried; there is no batching.
