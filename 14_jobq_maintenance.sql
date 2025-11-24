/*
 =====================================================================
 14_jobq_maintenance.sql – Operational control & maintenance APIs
 
 Run as:
 - Any login that is a member of jobq_worker.
 
 Assumptions:
 - jobq.jobs table exists
 - 13_jobq_monitoring.sql applied (for v_running_jobs, etc.)
 
 This script:
 - Defines jobq.kill() to hard-kill a running job/backend (best effort)
 - Defines jobq.requeue_orphaned_running_jobs() for missing backends
 - Defines jobq.purge_old_jobs() for retention control
 - Is designed to be re-runnable.
 
 Notes:
 - jobq.kill() is designed to work even when jobq_worker does NOT have
 pg_signal_backend. In that case, it marks the job cancelled but does
 not signal the backend. Where supported, 00_admin_bootstrap.sql
 best-effort grants pg_signal_backend to jobq_worker so that kill()
 can call pg_terminate_backend().
 =====================================================================
 */
SET ROLE jobq_worker;
------------------------------
-- 10. Hard kill API – terminate backend and mark job cancelled
------------------------------
CREATE OR REPLACE FUNCTION jobq.kill(p_job_id BIGINT) RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog,
  jobq AS $$
DECLARE v_pid INTEGER;
v_status jobq.job_status;
v_backend_query TEXT;
v_killed BOOLEAN := false;
v_can_signal BOOLEAN := false;
BEGIN -- Look up the job and the currently running backend (if any),
-- and lock the row to avoid concurrent status changes while we act.
SELECT j.backend_pid,
  j.status,
  sa.query INTO v_pid,
  v_status,
  v_backend_query
FROM jobq.jobs j
  LEFT JOIN pg_stat_activity sa ON sa.pid = j.backend_pid
WHERE j.job_id = p_job_id FOR
UPDATE;
IF NOT FOUND THEN RAISE EXCEPTION 'job_id % not found',
p_job_id;
END IF;
-- Only act on jobs that are actually marked as running.
IF v_status <> 'running' THEN RETURN false;
END IF;
------------------------------------------------------------------
-- Defensive guard against PID reuse:
-- Only terminate a backend if:
--   - the PID is still present in pg_stat_activity, AND
--   - its current query text looks like a jobq runner or export.
-- Additional hardening:
--   - Check whether jobq_worker is a member of pg_signal_backend.
--   - Wrap pg_terminate_backend in an EXCEPTION block so permission
--     issues do not blow up the call.
--
  -- Return value:
--   TRUE  = we successfully called pg_terminate_backend()
--   FALSE = we did not signal the backend (either not applicable or
--           not permitted), though the job may still be marked cancelled.
------------------------------------------------------------------
IF v_pid IS NOT NULL
AND v_backend_query IS NOT NULL
AND (
  v_backend_query ILIKE '%jobq.run%'
  OR v_backend_query ILIKE '%azure_storage.blob_put%'
) THEN -- Check whether jobq_worker is allowed to signal backends.
SELECT pg_has_role('jobq_worker', 'pg_signal_backend', 'member') INTO v_can_signal;
IF v_can_signal THEN BEGIN PERFORM pg_terminate_backend(v_pid);
v_killed := true;
EXCEPTION
WHEN insufficient_privilege THEN -- Platform may not allow this despite membership attempts;
-- fall through and treat as "not killed".
v_killed := false;
END;
END IF;
END IF;
------------------------------------------------------------------
-- Only mark the job as cancelled if it is STILL in 'running'
-- status. This avoids clobbering jobs that finished between the
-- initial lookup and this update.
------------------------------------------------------------------
UPDATE jobq.jobs
SET status = 'cancelled',
  finished_at = clock_timestamp(),
  last_error = COALESCE(last_error, '') || CASE
    WHEN v_killed THEN ' [cancelled via kill]'
    ELSE ' [cancelled via kill (backend not signaled)]'
  END,
  backend_pid = NULL
WHERE job_id = p_job_id
  AND status = 'running';
RETURN v_killed;
END;
$$;
------------------------------
-- 11. Requeue orphaned running jobs
--     (jobs marked running but with no active backend)
------------------------------
CREATE OR REPLACE FUNCTION jobq.requeue_orphaned_running_jobs(p_limit INTEGER DEFAULT 100) RETURNS INTEGER LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog,
  jobq AS $$
DECLARE v_job jobq.jobs %ROWTYPE;
v_attempt INTEGER;
v_requeued INTEGER := 0;
v_backoff INTERVAL;
BEGIN FOR v_job IN
SELECT j.*
FROM jobq.jobs j
  LEFT JOIN pg_stat_activity sa ON sa.pid = j.backend_pid
WHERE j.status = 'running'
  AND (
    j.backend_pid IS NULL
    OR sa.pid IS NULL
  )
ORDER BY j.job_id
LIMIT p_limit FOR
UPDATE SKIP LOCKED LOOP v_attempt := v_job.attempt_count + 1;
v_backoff := LEAST(v_attempt, 10) * interval '1 minute';
IF v_attempt >= v_job.max_attempts THEN
UPDATE jobq.jobs
SET status = 'failed',
  finished_at = clock_timestamp(),
  last_error = COALESCE(last_error, '') || ' [auto-failed: missing backend]',
  backend_pid = NULL,
  attempt_count = v_attempt
WHERE job_id = v_job.job_id;
ELSE
UPDATE jobq.jobs
SET status = 'pending',
  scheduled_at = clock_timestamp() + v_backoff,
  started_at = NULL,
  finished_at = NULL,
  last_error = COALESCE(last_error, '') || ' [requeued: missing backend]',
  backend_pid = NULL,
  attempt_count = v_attempt
WHERE job_id = v_job.job_id;
END IF;
v_requeued := v_requeued + 1;
END LOOP;
RETURN v_requeued;
END;
$$;
------------------------------
-- 12. Purge / retention helper (batch delete of old finished jobs)
------------------------------
CREATE OR REPLACE FUNCTION jobq.purge_old_jobs(
    p_older_than INTERVAL DEFAULT interval '30 days',
    p_limit INTEGER DEFAULT 10000
  ) RETURNS BIGINT LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog,
  jobq AS $$
DECLARE v_deleted BIGINT := 0;
BEGIN WITH to_del AS (
  SELECT ctid
  FROM jobq.jobs
  WHERE finished_at IS NOT NULL
    AND finished_at < now() - p_older_than
  LIMIT p_limit
), del AS (
  DELETE FROM jobq.jobs j USING to_del t
  WHERE j.ctid = t.ctid
  RETURNING 1
)
SELECT COUNT(*) INTO v_deleted
FROM del;
RETURN COALESCE(v_deleted, 0);
END;
$$;