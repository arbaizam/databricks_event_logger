-- Optional dashboard view templates for the configured event log table.
-- The deployment layer owns substitution, grants, and validation against the
-- system table schema available in the target workspace.
--
-- Expected substitutions:
--   ${observability_event_table}
--   ${observability_event_log_recent_view}
--   ${observability_event_log_failures_view}
--   ${observability_event_log_run_summary_view}

CREATE OR REPLACE VIEW ${observability_event_log_recent_view} AS
WITH normalized AS (
  SELECT
    e.*,
    regexp_replace(
      CASE
        WHEN workspace_url IS NULL OR workspace_url = '' THEN NULL
        WHEN workspace_url RLIKE '^https?://' THEN lower(workspace_url)
        ELSE concat('https://', lower(workspace_url))
      END,
      '/+$',
      ''
    ) AS workspace_url_normalized
  FROM ${observability_event_table} AS e
)
SELECT
  event_ts,
  event_date,
  event_name,
  event_type,
  status,
  severity,
  duration_ms,
  app_name,
  component,
  environment,
  workspace_id,
  workspace_url,
  workspace_url_normalized,
  CASE
    WHEN workspace_url_normalized IS NOT NULL AND job_id IS NOT NULL THEN
      concat(workspace_url_normalized, '/jobs/', job_id)
  END AS job_url,
  CASE
    WHEN workspace_url_normalized IS NOT NULL AND job_id IS NOT NULL AND run_id IS NOT NULL THEN
      concat(workspace_url_normalized, '/jobs/', job_id, '/runs/', run_id)
  END AS job_run_url,
  job_id,
  run_id,
  task_key,
  task_run_id,
  task_attempt_number,
  job_start_time,
  job_trigger_type,
  notebook_path,
  user_name,
  run_as_user_name,
  source_table,
  target_table,
  row_count,
  metric_name,
  metric_value,
  error_class,
  error_message,
  stack_trace_hash,
  metadata_json,
  correlation_id,
  parent_event_id,
  event_id,
  created_at
FROM normalized;

CREATE OR REPLACE VIEW ${observability_event_log_failures_view} AS
SELECT
  event_ts,
  event_date,
  event_name,
  event_type,
  status,
  severity,
  duration_ms,
  app_name,
  component,
  environment,
  job_run_url,
  job_url,
  job_id,
  run_id,
  task_key,
  task_run_id,
  task_attempt_number,
  error_class,
  error_message,
  stack_trace_hash,
  source_table,
  target_table,
  row_count,
  metadata_json,
  correlation_id,
  parent_event_id,
  event_id
FROM ${observability_event_log_recent_view}
WHERE status = 'failed';

CREATE OR REPLACE VIEW ${observability_event_log_run_summary_view} AS
SELECT
  workspace_id,
  coalesce(run_id, correlation_id) AS run_group_id,
  max(app_name) AS app_name,
  max(component) AS component,
  max(environment) AS environment,
  max(job_id) AS job_id,
  max(run_id) AS run_id,
  max(task_key) AS task_key,
  max(task_run_id) AS task_run_id,
  max(task_attempt_number) AS task_attempt_number,
  max(job_trigger_type) AS job_trigger_type,
  max(job_url) AS job_url,
  max(job_run_url) AS job_run_url,
  min(event_ts) AS first_event_ts,
  max(event_ts) AS last_event_ts,
  count(*) AS event_count,
  sum(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_event_count,
  sum(CASE WHEN status = 'warning' OR severity = 'warning' THEN 1 ELSE 0 END)
    AS warning_event_count,
  max(CASE WHEN status = 'failed' THEN event_name END) AS sample_failed_event_name,
  max(CASE WHEN status = 'failed' THEN error_class END) AS sample_error_class,
  max(CASE WHEN status = 'failed' THEN error_message END) AS sample_error_message
FROM ${observability_event_log_recent_view}
GROUP BY workspace_id, coalesce(run_id, correlation_id);

-- Example only. Verify system table names and columns in the target workspace.
-- CREATE OR REPLACE VIEW ${observability_event_log_run_enriched_view} AS
-- SELECT
--   e.*,
--   r.run_name,
--   r.result_state,
--   r.trigger_type AS system_trigger_type
-- FROM ${observability_event_log_recent_view} AS e
-- LEFT JOIN system.lakeflow.job_run_timeline AS r
--   ON e.run_id = CAST(r.run_id AS STRING);
