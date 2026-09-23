"""Helpers that only work inside Databricks.

This is kept separate from the core package, so that the core never needs
``dbutils`` or Spark.
"""

from databricks_event_logger.databricks.context import resolve_context

__all__ = ["resolve_context"]
