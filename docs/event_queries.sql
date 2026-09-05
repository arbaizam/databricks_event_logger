-- Replace main.observability.event_log with your event table.
-- Keep a bounded event_date filter so Delta can prune partitions.
-- Run these examples in a separate SQL query session using UTC. This explicit
-- query-session setting is not applied by the logger to the application session.
-- event_date is UTC; to_date(event_ts) in another timezone can differ at midnight.
SET TIME ZONE 'UTC';

-- One workflow's timeline. Child scopes finish before their parents, so sort by
-- start time when it exists. Replace the literal with the logger's correlation_id.
SELECT
  correlation_id,
  event_id,
  parent_event_id,
  COALESCE(start_ts, event_ts) AS operation_started_at,
  event_name,
  status,
  duration_ms,
  row_count,
  metadata_json
FROM main.observability.event_log
WHERE event_date >= date_sub(current_date(), 7)
  AND correlation_id = 'paste-correlation-id'
ORDER BY operation_started_at, event_ts, event_id;

-- Recent failures, with runtime identity for locating the execution.
SELECT
  event_ts,
  app_name,
  component,
  event_name,
  correlation_id,
  job_id,
  run_id,
  task_key,
  error_class,
  error_message,
  error_frames_json,
  stack_trace_hash
FROM main.observability.event_log
WHERE event_date >= date_sub(current_date(), 7)
  AND status = 'failed'
ORDER BY event_ts DESC
LIMIT 100;

-- Compare successful operation durations over the last month.
-- The array contains the approximate median and 95th percentile, in milliseconds.
SELECT
  app_name,
  component,
  event_name,
  COUNT(*) AS observations,
  ROUND(AVG(duration_ms), 1) AS mean_duration_ms,
  percentile_approx(duration_ms, array(0.5, 0.95)) AS p50_p95_duration_ms,
  MAX(duration_ms) AS max_duration_ms
FROM main.observability.event_log
WHERE event_date >= date_sub(current_date(), 30)
  AND status = 'success'
  AND duration_ms IS NOT NULL
GROUP BY app_name, component, event_name
ORDER BY mean_duration_ms DESC;
