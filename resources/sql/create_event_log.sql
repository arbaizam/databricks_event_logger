-- Template DDL for the configured event log table.
-- The deployment layer is responsible for substituting the table name and managing grants/retention.
-- For higher-volume production deployments, choose layout outside this template based on workload:
--   - partitioning by event_date can help retention and date filters
--   - clustering or liquid clustering by event_date, job_id, run_id, or task_key can help dashboards
-- Keep layout decisions environment-owned so small dev/test tables do not inherit unnecessary complexity.

CREATE TABLE IF NOT EXISTS ${observability_event_table} (
  event_name STRING NOT NULL,
  event_type STRING NOT NULL,
  status STRING NOT NULL,
  event_id STRING NOT NULL,
  correlation_id STRING,
  parent_event_id STRING,
  event_ts TIMESTAMP NOT NULL,
  event_date DATE NOT NULL,
  start_ts TIMESTAMP,
  end_ts TIMESTAMP,
  duration_ms BIGINT,
  severity STRING,
  app_name STRING,
  component STRING,
  environment STRING,
  sdk_version STRING NOT NULL,
  workspace_id STRING,
  workspace_url STRING,
  cluster_id STRING,
  job_id STRING,
  run_id STRING,
  task_key STRING,
  task_run_id STRING,
  task_attempt_number STRING,
  job_start_time STRING,
  job_trigger_type STRING,
  notebook_path STRING,
  user_name STRING,
  run_as_user_name STRING,
  source_table STRING,
  target_table STRING,
  row_count BIGINT,
  metric_name STRING,
  metric_value DOUBLE,
  error_class STRING,
  error_message STRING,
  stack_trace_hash STRING,
  metadata_json STRING,
  created_at TIMESTAMP NOT NULL
)
USING DELTA;
