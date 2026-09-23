"""Independent baseline/refactor observations; run once per source tree."""

# Callbacks passed to observe run immediately; no loop closure is retained.
# ruff: noqa: B023
import ast
import asyncio
import importlib
import itertools
import json
import random
import sys
import warnings
from collections import UserDict, namedtuple
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from dataclasses import asdict, dataclass, fields
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(sys.argv[1]).resolve() / "src"))
import numpy as np
from pyspark.sql import Row

import databricks_event_logger as api
import databricks_event_logger.logger as logger_module
import databricks_event_logger.sinks.delta as delta_module
from databricks_event_logger.databricks import resolve_context

is_new = hasattr(logger_module.EventLogger, "_finish_record")
meta = importlib.import_module(
    "databricks_event_logger." + ("metadata" if is_new else "serialization")
)
record_module = importlib.import_module(
    "databricks_event_logger." + ("record" if is_new else "event")
)
diag = importlib.import_module("databricks_event_logger.diagnostics") if is_new else meta
out = {}


def observation(fn):
    try:
        value = fn()
        if isinstance(value, api.EventRecord):
            value = value.as_json_dict()
        elif isinstance(value, api.RuntimeContext):
            value = value.as_dict()
        return {"value": value}
    except BaseException as exc:
        return {"error": type(exc).__name__, "message": diag.safe_text(exc)}


def observe(name, fn):
    out[name] = observation(fn)


@dataclass
class SecretRecord:
    password: object
    normal: object


class DictEnum(Enum):
    VALUE = {"password": {"not": "visited"}, "safe": ["x", 7]}


class BrokenText(Exception):
    def __str__(self):
        raise KeyboardInterrupt("broken text")


class NoInspect:
    def __str__(self):
        raise AssertionError("str called")

    def __repr__(self):
        raise AssertionError("repr called")


class Malformed(tuple):
    pass


named = namedtuple("Named", "token normal")
cyclic = {}
cyclic["self"] = cyclic
malformed = []
for names in [("x", "x"), ("x",), (1, 2), "ab", None]:
    obj = Malformed((1, 2))
    obj._fields = names
    malformed.append(obj)
atoms = [
    None,
    True,
    False,
    0,
    -1,
    2**70,
    0.5,
    float("nan"),
    float("inf"),
    "abc",
    "\ud800\udfff\U0001f600",
    'a"\\\n' * 200,
    Decimal("1.20"),
    Path("item"),
    date(2020, 1, 2),
    datetime(2020, 1, 2, tzinfo=timezone.utc),
    np.bool_(True),
    np.int64(8),
    np.float64(1.2),
    NoInspect(),
    {1, 2},
    SecretRecord(NoInspect(), {"a": "b"}),
    named(NoInspect(), "ok"),
    Row(password=NoInspect(), normal="ok"),
    DictEnum.VALUE,
    *malformed,
]
rng = random.Random(90437)


def generated(depth=0):
    if depth > 4 or rng.random() < 0.45:
        return rng.choice(atoms)
    if rng.random() < 0.5:
        return [generated(depth + 1) for _ in range(rng.randrange(4))]
    return {
        rng.choice(["safe", "TOKEN", "k", "nested"]): generated(depth + 1)
        for _ in range(rng.randrange(4))
    }


for i in range(3500):
    data = {"root": generated()}
    settings = dict(
        string_max_chars=rng.choice([None, 0, 1, 4, 12, 13, 14, 2000]),
        max_depth=rng.choice([1, 2, 3, 8, 12]),
        redact_keys=rng.choice([(), ("",), ("token",), meta.DEFAULT_REDACT_KEYS]),
    )
    observe(f"metadata/sanitize/{i}", lambda: meta.sanitize_metadata(data, **settings))
    observe(
        f"metadata/serialize/{i}",
        lambda: meta.serialize_metadata(
            data, **settings, max_bytes=rng.choice([None, 2, 17, 19, 49, 50, 60, 80, 200, 4000])
        ),
    )
for budget in range(2, 201):
    data = {"x": '\ud800\U0001f600"\\\n' * 250}
    result = meta.serialize_metadata(data, max_bytes=budget)
    assert len(result.encode("utf-8")) <= budget
    decoded = json.loads(result)
    if "_preview" in decoded:
        encoded = meta.serialize_metadata(data, max_bytes=None)
        longer = {**decoded, "_preview": encoded[: len(decoded["_preview"]) + 1]}
        assert len(json.dumps(longer, sort_keys=True, separators=(",", ":")).encode()) > budget
    out[f"metadata/budget/{budget}"] = result
for i, data in enumerate(
    [cyclic, {1: 2}, {"x": {1: 2}}, {"password": {1: 2}}, {}, None, [], UserDict(a=1)]
):
    observe(f"metadata/edge/{i}", lambda: meta.serialize_metadata(data))
for name, values in [
    ("max_bytes", [True, -1, 1, 2.0, "2"]),
    ("string_max_chars", [True, -1, 2.0, "2"]),
    ("max_depth", [True, 0, -1, None, 2.0]),
    ("redact_keys", [None, [], ("x", 1)]),
]:
    for i, value in enumerate(values):
        observe(
            f"metadata/settings/{name}/{i}", lambda: meta.serialize_metadata(None, **{name: value})
        )

stamp = datetime(2020, 1, 2, tzinfo=timezone.utc)
base_fields = dict(event_name="x", event_id="fixed", event_ts=stamp, created_at=stamp)
for item in fields(api.EventRecord):
    if not item.init:
        continue
    for i, value in enumerate(
        [None, "", "x", True, 1, 1.0, [], stamp, datetime(2020, 1, 1), np.bool_(True)]
    ):
        observe(
            f"record/{item.name}/{i}", lambda: api.EventRecord(**{**base_fields, item.name: value})
        )
for names in itertools.combinations(["event_ts", "start_ts", "end_ts", "created_at"], 2):
    observe(
        "timestamp-order/" + "-".join(names),
        lambda: api.EventRecord(**{**base_fields, **dict.fromkeys(names, "bad")}),
    )
for item in fields(api.RuntimeContext):
    for i, value in enumerate(
        [
            None,
            "",
            "  abc ",
            1,
            True,
            1.0,
            [],
            "ftp://abc",
            "https://ADB-1.example.net/path",
            np.int64(1),
        ]
    ):
        observe(f"context/{item.name}/{i}", lambda: api.RuntimeContext(**{item.name: value}))


class FakeDbutils:
    def __init__(self, payload):
        self.payload = payload

    @property
    def notebook(self):
        return self

    @property
    def entry_point(self):
        return self

    def __call__(self):
        return self

    def getDbutils(self):
        return self

    def getContext(self):
        return self

    def toJson(self):
        if isinstance(self.payload, BaseException):
            raise self.payload
        return json.dumps(self.payload)


for i, payload in enumerate(
    [
        {"tags": {"jobId": 7, "orgId": 8}},
        {"tags": {"jobId": 7, "browserHostName": "ftp://bad"}},
        {"jobId": 1, "tags": {"jobId": 2}, "extraContext": {"jobId": 3}},
        {"tags": {"jobId": [], "orgId": 8}},
        [],
        None,
        RuntimeError("blocked"),
        KeyboardInterrupt("interrupt"),
    ]
):
    for j, values in enumerate(
        [None, {"job_id": None}, {"job_id": 44}, {"jobid": 1}, {"job_id": True}]
    ):
        observe(
            f"discovery/{i}/{j}",
            lambda: resolve_context(dbutils=FakeDbutils(payload), values=values),
        )


class Sink:
    def __init__(self, failure=None):
        self.failure = failure
        self.events = []

    def emit(self, event):
        if self.failure is not None:
            raise self.failure
        self.events.append(event)


def warning_data(caught):
    result = []
    for warning in caught:
        path = Path(warning.filename)
        item = dict(
            text=str(warning.message),
            category=warning.category.__name__,
            file=path.name,
            line=warning.lineno,
        )
        if path.name == "logger.py":
            tree = ast.parse(path.read_text(encoding="utf-8"))
            functions = [
                n
                for n in ast.walk(tree)
                if isinstance(n, ast.FunctionDef) and n.lineno <= warning.lineno <= n.end_lineno
            ]
            caller = min(functions, key=lambda n: n.end_lineno - n.lineno).name
            item["caller"] = "_event_scope" if caller == "_run_scope" else caller
            del item["line"]  # Compare logical caller, not relocated source lines.
        result.append(item)
    return result


def failure_case(strict, business_type, delivery_type, warn_mode):
    business = business_type("business") if business_type else None
    delivery = delivery_type("delivery") if delivery_type else None
    sink = Sink(delivery)
    log = api.EventLogger(sink=sink, strict_logging=strict, correlation_id="c")

    def bad_warning(*args, **kwargs):
        raise SystemExit("warning")

    caught_error = None
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("error" if warn_mode == "error" else "always")
        with patch.object(
            warnings, "warn", bad_warning if warn_mode == "patched" else warnings.warn
        ):
            try:
                with log.event("operation"):
                    if business is not None:
                        raise business
            except BaseException as exc:
                caught_error = exc
    return dict(
        raised=type(caught_error).__name__ if caught_error else None,
        same_business=caught_error is business if business else None,
        same_delivery=caught_error is delivery if delivery else None,
        health=asdict(log.health),
        warnings=warning_data(caught),
        statuses=[e.status for e in sink.events],
    )


for strict, business, delivery, mode in itertools.product(
    [False, True],
    [None, ValueError, KeyboardInterrupt, SystemExit, BrokenText],
    [None, RuntimeError, KeyboardInterrupt, SystemExit, BrokenText],
    ["always", "error", "patched"],
):
    key = f"failure/{strict}/{business}/{delivery}/{mode}"
    out[key] = failure_case(strict, business, delivery, mode)


def fixed_record(event):
    row = event.as_json_dict()
    for key in [
        "event_id",
        "event_ts",
        "event_date",
        "created_at",
        "start_ts",
        "end_ts",
        "duration_ms",
    ]:
        row.pop(key)
    return row


def error_event(mode):
    sink = api.MemorySink()
    log = api.EventLogger(sink=sink, capture_error_frames=True, correlation_id="c")

    def boom():
        raise ValueError("business")

    async def async_boom():
        raise ValueError("business")

    try:
        if mode == "scope":
            with log.event("failed"):
                boom()
        elif mode == "decorator":
            log.logged_event("failed")(boom)()
        elif mode == "task":
            log.run_task("failed", boom)
        else:
            asyncio.run(log.logged_event("failed")(async_boom)())
    except ValueError:
        pass
    return fixed_record(sink.events[0])


for mode in ["scope", "decorator", "task", "async"]:
    out[f"error-fields/{mode}"] = error_event(mode)


def parents():
    sink = api.MemorySink()
    log = api.EventLogger(sink=sink)
    bound = log.bind(a=1)
    with log.event("outer"):
        bound.record_event("direct")
        with bound.event("inner"):
            log.record_event("nested")
        with ThreadPoolExecutor(max_workers=2) as pool:
            pool.submit(bound.record_event, "thread-plain").result()
            pool.submit(copy_context().run, bound.record_event, "thread-copy").result()

        async def run():
            async def work(i):
                with bound.event(f"task-{i}"):
                    await asyncio.sleep(0)
                    log.record_event(f"child-{i}")

            await asyncio.gather(*(work(i) for i in range(4)))

        asyncio.run(run())
    log.record_event("after")
    ids = {e.event_id: e.event_name for e in sink.events}
    result = {e.event_name: ids.get(e.parent_event_id) for e in sink.events}
    assert result["direct"] == result["thread-copy"] == "outer"
    assert result["thread-plain"] is None and result["after"] is None
    for i in range(4):
        assert result[f"child-{i}"] == f"task-{i}"
    assert log.health == bound.health
    return {"parents": result, "health": asdict(log.health)}


out["parents"] = parents()


def reset_after_strict_failure():
    sink = Sink(RuntimeError("delivery"))
    log = api.EventLogger(sink=sink, strict_logging=True)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            with log.event("outer"):
                with log.bind().event("inner"):
                    pass
        except RuntimeError:
            pass
    sink.failure = None
    return log.record_event("after").parent_event_id, asdict(log.health)


out["parent-reset"] = reset_after_strict_failure()


class FakeSpark:
    def __init__(self, stage=None, failure=RuntimeError, cleanup=None):
        self.stage = stage
        self.failure = failure
        self.cleanup = cleanup
        self.calls = []
        self.catalog = self

    def fail(self, stage):
        self.calls.append(stage)
        if self.stage == stage:
            raise self.failure(stage)

    def createDataFrame(self, rows, schema):
        self.row_keys = list(rows[0])
        self.schema = schema.json()
        self.fail("dataframe")
        return self

    def createOrReplaceTempView(self, name):
        self.fail("register")

    def sql(self, sql):
        self.sql_text = sql
        self.fail("sql")
        return self

    def collect(self):
        self.fail("collect")
        return []

    def dropTempView(self, name):
        self.calls.append("drop")
        if self.cleanup:
            raise self.cleanup("cleanup")


for stage, failure, cleanup in itertools.product(
    [None, "dataframe", "register", "sql", "collect"],
    [RuntimeError, KeyboardInterrupt],
    [None, RuntimeError, KeyboardInterrupt],
):
    spark = FakeSpark(stage, failure, cleanup)
    with patch.object(delta_module, "uuid4", lambda: SimpleNamespace(hex="fixed")):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            value = observation(
                lambda: api.DeltaSink(spark, "a.b.c").emit(api.EventRecord(**base_fields))
            )
    out[f"delta/{stage}/{failure}/{cleanup}"] = {
        "result": value,
        "calls": spark.calls,
        "warnings": warning_data(caught),
        "sql": getattr(spark, "sql_text", None),
    }
out["ddl"] = api.create_table_sql("a.b.c")
out["schema"] = getattr(delta_module, "_spark_schema" if is_new else "_event_schema")().json()
out["schema-columns"] = list(delta_module.EVENT_COLUMNS)

for name in ["event", "events", "serialization"]:
    observe(
        f"import/{name}",
        lambda: importlib.import_module("databricks_event_logger." + name).__name__,
    )
out["exports"] = sorted(api.__all__)
# Focused documentation claims.
observe(
    "doc/Mapping",
    lambda: (
        api.EventLogger(sink=api.MemorySink())
        .record_event("x", metadata=UserDict(x=1))
        .metadata_json
    ),
)
observe(
    "doc/masked-nested-key",
    lambda: (
        api.EventLogger(
            sink=api.MemorySink(), default_metadata={"password": {1: 2}}
        ).default_metadata
    ),
)
observe("doc/context-type-error", lambda: resolve_context(values={"job_id": []}))
observe("doc/safe_text_invalid_limit", lambda: diag.safe_text(ValueError("x"), max_chars="bad"))
observe("doc/truncation-small-limit", lambda: diag.safe_text(BrokenText(), max_chars=3))


def metadata_validation_phase(strict):
    log = api.EventLogger(sink=api.MemorySink(), strict_logging=strict)
    ran = []
    manager = observation(lambda: type(log.event("scope", metadata={1: "bad"})).__name__)
    decorate = log.logged_event("wrapped", metadata={1: "bad"})

    @decorate
    def business():
        ran.append(True)
        return 42

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        direct = observation(lambda: log.record_event("direct", metadata={1: "bad"}))
        result = observation(business)
    return dict(
        manager=manager,
        direct=direct,
        decorated_result=result,
        ran=ran,
        health=asdict(log.health),
        warnings=warning_data(caught),
    )


for strict in [False, True]:
    out[f"doc/validation-phase/{strict}"] = metadata_validation_phase(strict)


def direct_warning():
    log = api.EventLogger(sink=Sink(RuntimeError("delivery")))
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        log.record_event("direct")
    return warning_data(caught)


out["direct-warning"] = direct_warning()


def deterministic_pipeline():
    sink = api.MemorySink()
    log = api.EventLogger(
        sink=sink,
        app_name="app",
        component="ingest",
        environment="test",
        context=api.RuntimeContext(job_id=42),
        correlation_id="workflow",
        default_metadata={"base": {"values": []}},
    )
    child = log.bind(batch="B1", secret="hidden")
    child.default_metadata["base"]["values"].append(1)
    with child.event("parent", metadata={"batch": "override"}) as scope:
        scope.status = "warning"
        scope.severity = "info"
        scope.row_count = 17
        child.record_metric("items", np.int64(3))
        log.record_event("explicit", event_id="chosen", parent_event_id="external")
    log.record_event("after")
    names = {e.event_id: e.event_name for e in sink.events}
    rows = []
    for e in sink.events:
        row = fixed_record(e)
        row["parent_event_id"] = names.get(e.parent_event_id, e.parent_event_id)
        rows.append(row)
    return {"rows": rows, "health": asdict(log.health)}


out["pipeline"] = deterministic_pipeline()

Path(sys.argv[2]).write_text(
    json.dumps(out, ensure_ascii=True, sort_keys=True, indent=2), encoding="utf-8"
)
print(f"{len(out)} observations written to {sys.argv[2]}")
