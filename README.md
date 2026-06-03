# Databricks Event Logger

`databricks-event-logger` is a small Python package for structured runtime event logging in Databricks jobs, tasks, and notebooks.

The v1 design is documented in [docs/databricks-event-logger-design-spec.md](docs/databricks-event-logger-design-spec.md).

## V1 Boundaries

- Package import name: `databricks_event_logger`
- Distribution name: `databricks-event-logger`
- Tests run in Databricks.
- Unity Catalog object names are fully configurable.
- Permissions, ownership, and retention are handled outside the package.
- Delta writes are immediate in v1.
- Context manager and task wrapper APIs are part of v1.
- Private packages do not need internal event logging in v1.

## Bundle

Asset Bundle configuration lives outside the committed package source. The local
`databricks.yaml` file is intentionally ignored so environment-specific bundle
settings do not enter the package repository.
