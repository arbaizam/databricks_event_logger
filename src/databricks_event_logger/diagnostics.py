"""
Observability readiness diagnostics.

These helpers inspect configuration without emitting events or installing a
default logger. They are intended for notebook setup cells, smoke tests, and
support tickets where the first question is whether events can persist.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from databricks_event_logger._widget_utils import (
    context_from_widgets,
    string_or_none,
    widget_values,
)
from databricks_event_logger.context import resolve_databricks_context
from databricks_event_logger.errors import EventLoggerConfigurationError
from databricks_event_logger.sinks.delta import DeltaSink
from databricks_event_logger.version import __version__


@dataclass(frozen=True)
class ObservabilityReadinessReport:
    """
    Result returned by observability readiness checks.
    """

    package_version: str
    sink_type: str
    event_table: str | None
    checks: dict[str, bool]
    issues: tuple[str, ...]
    context: dict[str, str | None]

    @property
    def ready(self) -> bool:
        """
        Return whether no blocking issues were found.
        """
        return not self.issues

    def as_dict(self) -> dict[str, Any]:
        """
        Return the report as a plain dictionary.
        """
        return asdict(self)

    def summary(self) -> str:
        """
        Return a compact multi-line text summary for notebook display.
        """
        lines = [
            f"package_version: {self.package_version}",
            f"sink_type: {self.sink_type}",
            f"event_table: {self.event_table or '<missing>'}",
            f"ready: {self.ready}",
        ]
        for check_name, passed in self.checks.items():
            lines.append(f"{check_name}: {'ok' if passed else 'failed'}")
        if self.issues:
            lines.append("issues:")
            lines.extend(f"- {issue}" for issue in self.issues)
        return "\n".join(lines)


def check_observability_ready(
    *,
    dbutils: Any | None = None,
    spark: Any | None = None,
    event_table: str | None = None,
    require_persistence: bool = True,
    validate_sink: bool = True,
) -> ObservabilityReadinessReport:
    """
    Return a compact readiness report without emitting any events.
    """
    captured_widget_values = widget_values(dbutils)
    resolved_event_table = string_or_none(event_table) or captured_widget_values.get(
        "observability_event_table"
    )
    context = resolve_databricks_context(
        dbutils=dbutils,
        spark=spark,
        fallback=context_from_widgets(captured_widget_values),
    )
    checks = {
        "dbutils_supplied": dbutils is not None,
        "spark_supplied": spark is not None,
        "event_table_configured": bool(resolved_event_table),
        "persistent_sink": bool(spark is not None and resolved_event_table),
        "table_validated": False,
    }
    issues: list[str] = []
    sink_type = "DeltaSink" if checks["persistent_sink"] else "MemorySink"

    if require_persistence and not checks["persistent_sink"]:
        issues.append(
            "Persistent event logging is required, but spark or "
            "observability_event_table is missing."
        )

    if validate_sink and checks["persistent_sink"]:
        try:
            DeltaSink(spark=spark, table_name=resolved_event_table).validate()
        except Exception as exc:
            issues.append(f"DeltaSink validation failed: {exc}")
        else:
            checks["table_validated"] = True
    elif not validate_sink:
        checks["table_validated"] = True

    return ObservabilityReadinessReport(
        package_version=__version__,
        sink_type=sink_type,
        event_table=resolved_event_table,
        checks=checks,
        issues=tuple(issues),
        context=context.as_dict(),
    )


def assert_observability_ready(
    *,
    dbutils: Any | None = None,
    spark: Any | None = None,
    event_table: str | None = None,
    require_persistence: bool = True,
    validate_sink: bool = True,
) -> ObservabilityReadinessReport:
    """
    Raise when the current notebook/job is not ready to persist events.
    """
    report = check_observability_ready(
        dbutils=dbutils,
        spark=spark,
        event_table=event_table,
        require_persistence=require_persistence,
        validate_sink=validate_sink,
    )
    if not report.ready:
        raise EventLoggerConfigurationError(report.summary())
    return report
