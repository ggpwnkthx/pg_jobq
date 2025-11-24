/*
 =====================================================================
 13_jobq_monitoring.sql – Types, views, and metrics for monitoring
 
 Run as:
 - Any login that is a member of jobq_worker.
 
 Assumptions:
 - jobq.jobs table exists.
 
 This script:
 - Defines jobq.queue_metrics composite type
 - Defines v_queue_overview, v_running_jobs, v_recent_jobs, v_stalled_jobs
 - Defines jobq.get_queue_metrics() for simple metrics retrieval
 - Is designed to be re-runnable.
 =====================================================================
 */
SET ROLE jobq_worker;
------------------------------
-- 8. Queue metrics type (composite for get_queue_metrics)
------------------------------
DO $$ BEGIN IF NOT EXISTS (
  SELECT 1
  FROM pg_type t
    JOIN pg_namespace n ON n.oid = t.typnamespace
  WHERE t.typname = 'queue_metrics'
    AND n.nspname = 'jobq'
) THEN CREATE TYPE jobq.queue_metrics AS (
  pending BIGINT,
  running BIGINT,
  succeeded BIGINT,
  failed BIGINT,
  cancelled BIGINT,
  oldest_pending_wait INTERVAL,
  avg_pending_wait INTERVAL
);
END IF;
END;
$$ LANGUAGE plpgsql;
------------------------------
-- 9. Monitoring views & metrics function
------------------------------
-- Aggregate queue overview (single-row snapshot).
CREATE OR REPLACE VIEW jobq.v_queue_overview AS
SELECT COUNT(*) FILTER (
    WHERE status = 'pending'
  ) AS pending,
  COUNT(*) FILTER (
    WHERE status = 'running'
  ) AS running,
  COUNT(*) FILTER (
    WHERE status = 'succeeded'
  ) AS succeeded,
  COUNT(*) FILTER (
    WHERE status = 'failed'
  ) AS failed,
  COUNT(*) FILTER (
    WHERE status = 'cancelled'
  ) AS cancelled,
  MAX(now() - scheduled_at) FILTER (
    WHERE status = 'pending'
  ) AS oldest_pending_wait,
  AVG(now() - scheduled_at) FILTER (
    WHERE status = 'pending'
  ) AS avg_pending_wait
FROM jobq.jobs;
CREATE OR REPLACE FUNCTION jobq.get_queue_metrics() RETURNS jobq.queue_metrics LANGUAGE sql STABLE AS $$
SELECT pending,
  running,
  succeeded,
  failed,
  cancelled,
  oldest_pending_wait,
  avg_pending_wait
FROM jobq.v_queue_overview;
$$;
-- Running jobs + pg_stat_activity snapshot for live diagnostics.
CREATE OR REPLACE VIEW jobq.v_running_jobs AS
SELECT j.job_id,
  j.status,
  j.priority,
  j.correlation_id,
  j.storage_account,
  j.storage_container,
  j.result_blob_path,
  j.scheduled_at,
  j.started_at,
  now() - j.started_at AS elapsed,
  j.max_runtime,
  j.attempt_count,
  j.max_attempts,
  j.backend_pid,
  sa.datname,
  sa.usename,
  sa.application_name,
  sa.client_addr,
  sa.state,
  sa.backend_start,
  sa.xact_start,
  sa.query_start,
  sa.wait_event_type,
  sa.wait_event,
  sa.query
FROM jobq.jobs j
  LEFT JOIN pg_stat_activity sa ON sa.pid = j.backend_pid
WHERE j.status = 'running'
ORDER BY j.started_at NULLS LAST,
  j.job_id;
-- Recent job history (last 7 days, newest first).
CREATE OR REPLACE VIEW jobq.v_recent_jobs AS
SELECT job_id,
  created_at,
  updated_at,
  scheduled_at,
  started_at,
  finished_at,
  status,
  priority,
  correlation_id,
  storage_account,
  storage_container,
  result_blob_path,
  attempt_count,
  max_attempts,
  run_by,
  last_error,
  max_runtime
FROM jobq.jobs
WHERE created_at >= now() - interval '7 days'
ORDER BY job_id DESC;
-- Stalled jobs: elapsed > max_runtime (fallback to 30 minutes when NULL).
CREATE OR REPLACE VIEW jobq.v_stalled_jobs AS
SELECT *
FROM jobq.v_running_jobs
WHERE elapsed > COALESCE(max_runtime, interval '30 minutes');