"""
Observability readiness diagnostics.

These helpers inspect configuration without emitting events or installing a
default logger. They are intended for notebook setup cells, smoke tests, and
support tickets where the first question is whether the configured sink works.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from databricks_event_logger.context import resolve_databricks_context
from databricks_event_logger.errors import EventLoggerConfigurationError
from databricks_event_logger.sinks.base import EventSink
from databricks_event_logger.sinks.console import ConsoleSink
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
    dbutils: Any,
    spark: Any,
    sink: EventSink | None = None,
    validate_sink: bool = True,
) -> ObservabilityReadinessReport:
    """
    Return a compact readiness report without emitting any events.
    """
    resolved_sink = sink if sink is not None else ConsoleSink()
    context = resolve_databricks_context(dbutils=dbutils, spark=spark)
    checks = {
        "runtime_context_resolved": True,
        "sink_validated": not validate_sink,
    }
    issues: list[str] = []
    if validate_sink and hasattr(resolved_sink, "validate"):
        try:
            resolved_sink.validate()
        except Exception as exc:
            issues.append(f"{type(resolved_sink).__name__} validation failed: {exc}")
        else:
            checks["sink_validated"] = True
    elif validate_sink:
        checks["sink_validated"] = True

    return ObservabilityReadinessReport(
        package_version=__version__,
        sink_type=type(resolved_sink).__name__,
        event_table=getattr(resolved_sink, "table_name", None),
        checks=checks,
        issues=tuple(issues),
        context=context.as_dict(),
    )


def assert_observability_ready(
    *,
    dbutils: Any,
    spark: Any,
    sink: EventSink | None = None,
    validate_sink: bool = True,
) -> ObservabilityReadinessReport:
    """
    Raise when the current notebook/job is not ready to persist events.
    """
    report = check_observability_ready(
        dbutils=dbutils,
        spark=spark,
        sink=sink,
        validate_sink=validate_sink,
    )
    if not report.ready:
        raise EventLoggerConfigurationError(report.summary())
    return report
