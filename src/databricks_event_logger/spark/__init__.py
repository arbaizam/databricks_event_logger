"""
Low-friction Spark observability helpers.

These helpers are intentionally thin wrappers around common notebook actions.
They use the default logger configured by ``observe_notebook`` unless an
explicit logger is passed, and they do not import PySpark at module import time.
That keeps the package importable in local tooling while still making notebook
instrumentation concise.
"""

from __future__ import annotations

import hashlib
import inspect
import re
import traceback
import warnings
from collections.abc import Mapping, Sequence
from typing import Any

from databricks_event_logger.errors import EventLoggerConfigurationError
from databricks_event_logger.logger import EventLogger, get_default_logger
from databricks_event_logger.timing import elapsed_ms, monotonic_ms, utc_now

SQL_PREVIEW_CHARS = 500
_SINGLE_QUOTED_SQL_LITERAL = re.compile(r"'(?:''|[^'])*'")
_DOUBLE_QUOTED_SQL_LITERAL = re.compile(r'"(?:""|[^"])*"')
_NUMERIC_SQL_LITERAL = re.compile(
    r"(?<![\w.])-?(?:\d+\.\d+|\d+)(?:[eE][+-]?\d+)?(?![\w.])"
)
_IDENTIFIER_PART = r"[A-Za-z_][A-Za-z0-9_]*"
_THREE_PART_TABLE_NAME = re.compile(
    rf"{_IDENTIFIER_PART}\.{_IDENTIFIER_PART}\.{_IDENTIFIER_PART}"
)


def read_table(
    table_name: str,
    *,
    logger: EventLogger | None = None,
    spark: Any | None = None,
    event_name: str = "delta.read",
    metadata: Mapping[str, Any] | None = None,
    **metadata_kwargs: Any,
):
    """
    Read a Spark table and log the DataFrame creation step.

    Parameters
    ----------
    table_name : str
        Source table or view name passed to ``spark.table``.
    logger : EventLogger | None, default None
        Logger to use. When omitted, the current default logger is used.
    spark : Any | None, default None
        Spark session. When omitted, nearby caller frames are inspected for a
        notebook global named ``spark``.
    event_name : str, default "delta.read"
        Event name to emit.
    metadata : Mapping[str, Any] | None, default None
        Optional event metadata.
    **metadata_kwargs : Any
        Convenience metadata fields. These are merged with ``metadata`` and are
        intended for low-friction notebook calls such as
        ``read_table(table, as_of_date=as_of_date)``.

    Returns
    -------
    Any
        DataFrame returned by ``spark.table``.

    Notes
    -----
    Spark table reads are usually lazy. This helper records that the DataFrame
    was requested, not that all rows were materialized. Use
    ``validate_row_count`` or a custom event around an action when you need a
    materialized-count event.
    """
    resolved_logger = _resolve_logger(logger)
    resolved_spark = _resolve_spark(spark)
    event_metadata = _merge_metadata(
        metadata,
        {"spark_operation": "table"},
        metadata_kwargs,
    )
    with resolved_logger.event(
        event_name,
        event_type="delta_read",
        source_table=table_name,
        metadata=event_metadata,
    ):
        return resolved_spark.table(table_name)


def write_delta(
    dataframe: Any,
    table_name: str | None = None,
    *,
    table: str | None = None,
    logger: EventLogger | None = None,
    mode: str = "append",
    event_name: str = "delta.write",
    overwrite_schema: bool = False,
    merge_schema: bool = False,
    replace_where: str | None = None,
    row_count: int | None = None,
    partition_by: Sequence[str] | None = None,
    options: Mapping[str, Any] | None = None,
    metadata: Mapping[str, Any] | None = None,
    **metadata_kwargs: Any,
) -> None:
    """
    Write a DataFrame to a Delta table and log success or failure.

    Parameters
    ----------
    dataframe : Any
        Spark DataFrame to write.
    table_name : str | None
        Target table name passed to ``saveAsTable``.
    table : str | None, default None
        Keyword alias for ``table_name``. This supports call sites that read
        more naturally as ``write_delta(df, table="catalog.schema.table")``.
    logger : EventLogger | None, default None
        Logger to use. When omitted, the current default logger is used.
    mode : str, default "append"
        Spark write mode.
    event_name : str, default "delta.write"
        Event name to emit.
    overwrite_schema : bool, default False
        When True, pass ``overwriteSchema=true`` to the Spark writer.
    merge_schema : bool, default False
        When True, pass ``mergeSchema=true`` to the Spark writer.
    replace_where : str | None, default None
        Optional Delta predicate passed as the ``replaceWhere`` writer option.
    row_count : int | None, default None
        Optional known row count. The helper does not call ``count()`` because
        that can add a costly extra Spark action.
    partition_by : Sequence[str] | None, default None
        Optional columns passed to ``writer.partitionBy``.
    options : Mapping[str, Any] | None, default None
        Optional Spark writer options.
    metadata : Mapping[str, Any] | None, default None
        Optional event metadata.
    **metadata_kwargs : Any
        Convenience metadata fields merged with ``metadata``.
    """
    resolved_logger = _resolve_logger(logger)
    resolved_table = _resolve_table_name(table_name, table)
    writer_options = dict(options or {})
    if overwrite_schema:
        writer_options["overwriteSchema"] = "true"
    if merge_schema:
        writer_options["mergeSchema"] = "true"
    if replace_where:
        writer_options["replaceWhere"] = replace_where
    event_metadata = _merge_metadata(
        metadata,
        {
            "mode": mode,
            "options": dict(writer_options),
            "overwrite_schema": overwrite_schema,
            "merge_schema": merge_schema,
            "replace_where": replace_where,
            "partition_by": list(partition_by or []),
        },
        metadata_kwargs,
    )
    with resolved_logger.event(
        event_name,
        event_type="delta_write",
        target_table=resolved_table,
        row_count=row_count,
        metadata=event_metadata,
    ):
        writer = dataframe.write.format("delta").mode(mode)
        for key, value in writer_options.items():
            writer = writer.option(str(key), str(value))
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.saveAsTable(resolved_table)


def run_sql(
    sql_text: str,
    *,
    logger: EventLogger | None = None,
    spark: Any | None = None,
    event_name: str = "sql.execute",
    include_sql_preview: bool = False,
    sql_preview_chars: int = SQL_PREVIEW_CHARS,
    metadata: Mapping[str, Any] | None = None,
    **metadata_kwargs: Any,
):
    """
    Execute a Spark SQL statement and log the SQL call.

    Parameters
    ----------
    sql_text : str
        SQL text passed to ``spark.sql``.
    logger : EventLogger | None, default None
        Logger to use. When omitted, the current default logger is used.
    spark : Any | None, default None
        Spark session. When omitted, nearby caller frames are inspected for a
        notebook global named ``spark``.
    event_name : str, default "sql.execute"
        Event name to emit.
    include_sql_preview : bool, default False
        When True, include a redacted/truncated SQL preview in event metadata.
        The default logs only a hash to avoid accidental predicate/literal
        leakage.
    sql_preview_chars : int, default SQL_PREVIEW_CHARS
        Maximum redacted SQL preview length when ``include_sql_preview`` is
        enabled.
    metadata : Mapping[str, Any] | None, default None
        Optional event metadata.
    **metadata_kwargs : Any
        Convenience metadata fields merged with ``metadata``.

    Returns
    -------
    Any
        Result returned by ``spark.sql``.

    Notes
    -----
    The emitted metadata includes a SHA-256 hash of the SQL text by default.
    SQL preview is opt-in and redacts quoted strings and obvious numeric
    literals before truncation. Put business identifiers in ``metadata`` when a
    dashboard needs searchable context.
    """
    resolved_logger = _resolve_logger(logger)
    resolved_spark = _resolve_spark(spark)
    helper_metadata = {
        "sql_hash": hashlib.sha256(sql_text.encode("utf-8")).hexdigest(),
    }
    if include_sql_preview:
        helper_metadata["sql_preview"] = _sql_preview(
            sql_text,
            max_chars=sql_preview_chars,
        )
    event_metadata = _merge_metadata(
        metadata,
        helper_metadata,
        metadata_kwargs,
    )
    with resolved_logger.event(
        event_name,
        event_type="sql",
        metadata=event_metadata,
    ):
        return resolved_spark.sql(sql_text)


def validate_row_count(
    table_name: str | None = None,
    *,
    table: str | None = None,
    logger: EventLogger | None = None,
    spark: Any | None = None,
    event_name: str = "validation.row_count",
    expected_min: int | None = None,
    expected_exact: int | None = None,
    metadata: Mapping[str, Any] | None = None,
    **metadata_kwargs: Any,
) -> int:
    """
    Count rows in a table, log the validation result, and return the count.

    Parameters
    ----------
    table_name : str | None
        Table or view to count.
    table : str | None, default None
        Keyword alias for ``table_name``.
    logger : EventLogger | None, default None
        Logger to use. When omitted, the current default logger is used.
    spark : Any | None, default None
        Spark session. When omitted, nearby caller frames are inspected for a
        notebook global named ``spark``.
    event_name : str, default "validation.row_count"
        Event name to emit.
    expected_min : int | None, default None
        Optional minimum acceptable row count.
    expected_exact : int | None, default None
        Optional exact acceptable row count.
    metadata : Mapping[str, Any] | None, default None
        Optional event metadata.
    **metadata_kwargs : Any
        Convenience metadata fields merged with ``metadata``.

    Returns
    -------
    int
        Observed row count.

    Raises
    ------
    ValueError
        If the observed count does not satisfy the configured expectation.
    """
    if expected_min is None and expected_exact is None:
        raise ValueError("validate_row_count requires expected_min or expected_exact.")

    resolved_table = _resolve_table_name(table_name, table)
    resolved_logger = _resolve_logger(logger)
    resolved_spark = _resolve_spark(spark)
    start_ts = utc_now()
    start_ms = monotonic_ms()
    row_count: int | None = None
    event_metadata = _merge_metadata(
        metadata,
        {
            "expected_min": expected_min,
            "expected_exact": expected_exact,
        },
        metadata_kwargs,
    )
    try:
        row_count = int(resolved_spark.table(resolved_table).count())
        _raise_for_row_count(
            resolved_table,
            row_count,
            expected_min=expected_min,
            expected_exact=expected_exact,
        )
    except Exception as exc:
        _record_helper_failure(
            resolved_logger,
            event_name=event_name,
            event_type="validation",
            exc=exc,
            source_table=resolved_table,
            row_count=row_count,
            metadata=event_metadata,
            start_ts=start_ts,
            start_ms=start_ms,
        )
        raise
    resolved_logger.record_event(
        event_name,
        event_type="validation",
        status="success",
        severity="info",
        source_table=resolved_table,
        row_count=row_count,
        metadata=event_metadata,
        start_ts=start_ts,
        end_ts=utc_now(),
        duration_ms=elapsed_ms(start_ms),
    )
    return row_count


def count_rows(
    target: Any,
    *,
    logger: EventLogger | None = None,
    spark: Any | None = None,
    event_name: str = "spark.count",
    table_name: str | None = None,
    metadata: Mapping[str, Any] | None = None,
    **metadata_kwargs: Any,
) -> int:
    """
    Materialize a DataFrame or table row count and log the count event.

    Parameters
    ----------
    target : Any
        Spark DataFrame or table name. Passing a table name requires ``spark`` or
        a notebook global named ``spark``.
    logger : EventLogger | None, default None
        Logger to use. When omitted, the current default logger is used.
    spark : Any | None, default None
        Spark session used when ``target`` is a table name.
    event_name : str, default "spark.count"
        Event name to emit.
    table_name : str | None, default None
        Optional source-table label when ``target`` is a DataFrame.
    metadata : Mapping[str, Any] | None, default None
        Optional event metadata.
    **metadata_kwargs : Any
        Convenience metadata fields merged with ``metadata``.

    Returns
    -------
    int
        Observed row count.

    Notes
    -----
    This helper intentionally performs a Spark action. Use it only when a
    materialized row count is worth the extra job cost.
    """
    resolved_logger = _resolve_logger(logger)
    start_ts = utc_now()
    start_ms = monotonic_ms()
    source_table = table_name
    dataframe = target
    if isinstance(target, str):
        source_table = target
        dataframe = _resolve_spark(spark).table(target)
    event_metadata = _merge_metadata(
        metadata,
        {"spark_operation": "count"},
        metadata_kwargs,
    )
    row_count: int | None = None
    try:
        row_count = int(dataframe.count())
    except Exception as exc:
        _record_helper_failure(
            resolved_logger,
            event_name=event_name,
            event_type="spark_action",
            exc=exc,
            source_table=source_table,
            row_count=row_count,
            metadata=event_metadata,
            start_ts=start_ts,
            start_ms=start_ms,
        )
        raise
    resolved_logger.record_event(
        event_name,
        event_type="spark_action",
        status="success",
        severity="info",
        source_table=source_table,
        row_count=row_count,
        metadata=event_metadata,
        start_ts=start_ts,
        end_ts=utc_now(),
        duration_ms=elapsed_ms(start_ms),
    )
    return row_count


def table_exists(
    table_name: str | None = None,
    *,
    table: str | None = None,
    logger: EventLogger | None = None,
    spark: Any | None = None,
    event_name: str = "validation.table_exists",
    metadata: Mapping[str, Any] | None = None,
    **metadata_kwargs: Any,
) -> bool:
    """
    Check whether a Spark table exists and log the validation result.

    Parameters
    ----------
    table_name : str | None
        Table name to check.
    table : str | None, default None
        Keyword alias for ``table_name``.
    logger : EventLogger | None, default None
        Logger to use. When omitted, the current default logger is used.
    spark : Any | None, default None
        Spark session. When omitted, nearby caller frames are inspected for a
        notebook global named ``spark``.
    event_name : str, default "validation.table_exists"
        Event name to emit.
    metadata : Mapping[str, Any] | None, default None
        Optional event metadata.
    **metadata_kwargs : Any
        Convenience metadata fields merged with ``metadata``.

    Returns
    -------
    bool
        True when the table exists, otherwise False.
    """
    resolved_table = _resolve_table_name(table_name, table)
    resolved_logger = _resolve_logger(logger)
    resolved_spark = _resolve_spark(spark)
    start_ts = utc_now()
    start_ms = monotonic_ms()
    event_metadata = _merge_metadata(
        metadata,
        {"validation_name": "table_exists"},
        metadata_kwargs,
    )
    try:
        exists = bool(resolved_spark.catalog.tableExists(resolved_table))
    except Exception as catalog_exc:
        try:
            exists = _table_exists_by_describe(
                resolved_spark,
                resolved_table,
            )
        except Exception:
            _record_helper_failure(
                resolved_logger,
                event_name=event_name,
                event_type="validation",
                exc=catalog_exc,
                source_table=resolved_table,
                metadata=event_metadata,
                start_ts=start_ts,
                start_ms=start_ms,
            )
            raise catalog_exc from None
    resolved_logger.record_event(
        event_name,
        event_type="validation",
        status="success" if exists else "warning",
        severity="info" if exists else "warning",
        source_table=resolved_table,
        metadata=_metadata_with(event_metadata, table_exists=exists),
        start_ts=start_ts,
        end_ts=utc_now(),
        duration_ms=elapsed_ms(start_ms),
    )
    return exists


def _resolve_logger(logger: EventLogger | None) -> EventLogger:
    """
    Return an explicit logger or the current default logger.
    """
    return logger or get_default_logger()


def _resolve_spark(spark: Any | None) -> Any:
    """
    Return an explicit Spark session or a nearby notebook global.
    """
    if spark is not None:
        return spark
    resolved_spark = _caller_value("spark")
    if resolved_spark is None:
        raise EventLoggerConfigurationError(
            "No Spark session was supplied. Pass spark=... or call this helper "
            "from a Databricks notebook where spark is defined."
        )
    return resolved_spark


def _caller_value(name: str, *, max_depth: int = 10) -> Any | None:
    """
    Return a named value from nearby caller frames.
    """
    frame = inspect.currentframe()
    frame = frame.f_back if frame is not None else None
    for _ in range(max_depth):
        if frame is None:
            break
        local_value = frame.f_locals.get(name)
        if local_value is not None:
            return local_value
        global_value = frame.f_globals.get(name)
        if global_value is not None:
            return global_value
        frame = frame.f_back
    return None


def _merge_metadata(
    user_metadata: Mapping[str, Any] | None,
    helper_metadata: Mapping[str, Any],
    metadata_kwargs: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Merge helper diagnostics with caller metadata.
    """
    merged = dict(helper_metadata)
    if user_metadata:
        merged.update(dict(user_metadata))
    if metadata_kwargs:
        merged.update(dict(metadata_kwargs))
    return merged


def _metadata_with(metadata: Mapping[str, Any], **values: Any) -> dict[str, Any]:
    """
    Return metadata with trusted helper-computed values applied last.
    """
    merged = dict(metadata)
    merged.update(values)
    return merged


def _resolve_table_name(table_name: str | None, table: str | None) -> str:
    """
    Resolve table name aliases used by the helper API.
    """
    if table_name and table:
        raise ValueError("Pass either table_name or table, not both.")
    resolved = table_name or table
    if not resolved:
        raise ValueError("A table name is required.")
    return resolved


def _record_helper_failure(
    logger: EventLogger,
    *,
    event_name: str,
    event_type: str,
    exc: Exception,
    metadata: Mapping[str, Any],
    start_ts: Any,
    start_ms: float,
    source_table: str | None = None,
    target_table: str | None = None,
    row_count: int | None = None,
) -> None:
    """
    Emit helper failure events without masking the original Spark exception.
    """
    try:
        logger.record_event(
            event_name,
            event_type=event_type,
            status="failed",
            severity="error",
            source_table=source_table,
            target_table=target_table,
            row_count=row_count,
            metadata=dict(metadata),
            start_ts=start_ts,
            end_ts=utc_now(),
            duration_ms=elapsed_ms(start_ms),
            error_class=exc.__class__.__name__,
            error_message=str(exc),
            stack_trace_hash=_stack_trace_hash(exc),
        )
    except Exception as logging_exc:
        warnings.warn(
            f"Failed to emit helper failure event {event_name!r}: {logging_exc}",
            RuntimeWarning,
            stacklevel=2,
        )


def _sql_preview(sql_text: str, *, max_chars: int = SQL_PREVIEW_CHARS) -> str:
    """
    Return a normalized, redacted SQL preview for metadata.
    """
    preview = _redact_sql_literals(" ".join(sql_text.split()))
    if max_chars < 0 or len(preview) <= max_chars:
        return preview
    return f"{preview[:max_chars]}...[TRUNCATED]"


def _redact_sql_literals(sql_text: str) -> str:
    """
    Redact common literal forms from SQL preview text.
    """
    redacted = _SINGLE_QUOTED_SQL_LITERAL.sub("'[REDACTED]'", sql_text)
    redacted = _DOUBLE_QUOTED_SQL_LITERAL.sub('"[REDACTED]"', redacted)
    return _NUMERIC_SQL_LITERAL.sub("?", redacted)


def _table_exists_by_describe(spark: Any, table_name: str) -> bool:
    """
    Use DESCRIBE TABLE as a fallback for simple Unity Catalog table names.
    """
    if not _THREE_PART_TABLE_NAME.fullmatch(table_name):
        raise RuntimeError(
            "DESCRIBE TABLE fallback only supports simple three-part table names."
        )
    spark.sql(f"DESCRIBE TABLE {table_name}")
    return True


def _stack_trace_hash(exc: BaseException) -> str:
    """
    Return a stable hash for one helper exception traceback.
    """
    trace_text = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    return hashlib.sha256(trace_text.encode("utf-8")).hexdigest()


def _raise_for_row_count(
    table_name: str,
    row_count: int,
    *,
    expected_min: int | None,
    expected_exact: int | None,
) -> None:
    """
    Raise when a row-count expectation is not satisfied.
    """
    if expected_exact is not None and row_count != expected_exact:
        raise ValueError(
            f"{table_name} row count was {row_count}; expected exactly {expected_exact}."
        )
    if expected_min is not None and row_count < expected_min:
        raise ValueError(
            f"{table_name} row count was {row_count}; expected at least {expected_min}."
        )


__all__ = [
    "count_rows",
    "read_table",
    "run_sql",
    "table_exists",
    "validate_row_count",
    "write_delta",
]
