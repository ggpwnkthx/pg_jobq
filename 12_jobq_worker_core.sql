/*
 =====================================================================
 12_jobq_worker_core.sql – Worker claim, runner, and wrapper procedures
 
 Run as:
 - Any login that is a member of jobq_worker.
 
 Assumptions:
 - 10_jobq_types_and_table.sql and 11_jobq_enqueue_and_cancel.sql applied.
 - jobq.jobs table and jobq.exec_readonly_to_blob() exist.
 
 This script:
 - Implements the claim helper with global parallelism and connection caps
 (via jobq.max_parallel_jobs and jobq.min_free_connections GUCs)
 - Implements per-job runner procedure (no transaction control)
 - Implements jobq.run_next_job() wrapper (owns COMMITs)
 - Is designed to be re-runnable.
 =====================================================================
 */
SET ROLE jobq_worker;
------------------------------
-- 7. Worker core: claim helper + per-job runner + wrapper
------------------------------
-- 7.1 Claim helper: choose a job, enforce parallelism, mark as RUNNING
DROP FUNCTION IF EXISTS jobq.claim_next_job();
CREATE OR REPLACE FUNCTION jobq.claim_next_job(OUT o_job_id BIGINT, OUT o_slot_id INTEGER) RETURNS RECORD LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog,
  jobq AS $$
DECLARE -- Concurrency / throttling
  v_global_parallel_cap INTEGER;
v_min_free_conns INTEGER;
v_cfg TEXT;
v_max_connections INTEGER;
v_total_backends INTEGER;
v_free_connections INTEGER;
-- Advisory lock slot for parallelism
v_slot_id INTEGER;
v_got_slot BOOLEAN := false;
v_job jobq.jobs %ROWTYPE;
v_attempt INTEGER;
BEGIN o_job_id := NULL;
o_slot_id := NULL;
BEGIN --------------------------------------------------------------------
-- 1) Load config: jobq.max_parallel_jobs (global cap)
--    Example (per-database):
--      ALTER DATABASE current_database() SET jobq.max_parallel_jobs = '8';
--------------------------------------------------------------------
v_cfg := current_setting('jobq.max_parallel_jobs', true);
IF v_cfg IS NULL
OR NULLIF(trim(v_cfg), '') IS NULL THEN v_global_parallel_cap := 4;
-- default if not configured
ELSE BEGIN v_global_parallel_cap := LEAST(10000, GREATEST(1, v_cfg::INTEGER));
EXCEPTION
WHEN OTHERS THEN -- Defensive: fall back if somebody sets a garbage value.
v_global_parallel_cap := 4;
END;
END IF;
--------------------------------------------------------------------
-- 2) Load config: jobq.min_free_connections (connection headroom)
--    Example:
--      ALTER DATABASE current_database() SET jobq.min_free_connections = '10';
--------------------------------------------------------------------
v_cfg := current_setting('jobq.min_free_connections', true);
IF v_cfg IS NULL
OR NULLIF(trim(v_cfg), '') IS NULL THEN v_min_free_conns := 5;
-- default safety buffer
ELSE BEGIN v_min_free_conns := LEAST(1000, GREATEST(0, v_cfg::INTEGER));
EXCEPTION
WHEN OTHERS THEN v_min_free_conns := 5;
END;
END IF;
--------------------------------------------------------------------
-- 3) Compute current connection load
--------------------------------------------------------------------
v_max_connections := current_setting('max_connections')::INTEGER;
SELECT COUNT(*)::INTEGER INTO v_total_backends
FROM pg_stat_activity;
v_free_connections := GREATEST(v_max_connections - v_total_backends, 0);
-- No connection headroom → no-op, avoid piling on to an already hot server.
IF v_free_connections <= v_min_free_conns THEN RETURN;
END IF;
--------------------------------------------------------------------
-- 4) Acquire a parallelism slot via advisory locks
--    Slots are numbered [1 .. v_global_parallel_cap].
--    NOTE: advisory locks are cluster-global; this enforces a global
--    parallel cap across all databases sharing this server.
--------------------------------------------------------------------
v_slot_id := 1;
WHILE v_slot_id <= v_global_parallel_cap
AND NOT v_got_slot LOOP v_got_slot := pg_try_advisory_lock(42, v_slot_id);
IF NOT v_got_slot THEN v_slot_id := v_slot_id + 1;
END IF;
END LOOP;
-- All slots busy → no capacity for another runner.
IF NOT v_got_slot THEN RETURN;
END IF;
--------------------------------------------------------------------
-- 5) Claim a job and mark it as RUNNING (single transaction)
--------------------------------------------------------------------
SELECT * INTO v_job
FROM jobq.jobs
WHERE status = 'pending'
  AND scheduled_at <= now()
  AND attempt_count < max_attempts
ORDER BY priority DESC,
  scheduled_at,
  job_id
LIMIT 1 FOR
UPDATE SKIP LOCKED;
IF NOT FOUND THEN -- Nothing to do; release slot and exit.
PERFORM pg_advisory_unlock(42, v_slot_id);
v_got_slot := false;
RETURN;
END IF;
v_attempt := v_job.attempt_count + 1;
UPDATE jobq.jobs
SET status = 'running',
  started_at = clock_timestamp(),
  attempt_count = v_attempt,
  run_by = session_user,
  backend_pid = pg_backend_pid()
WHERE job_id = v_job.job_id;
o_job_id := v_job.job_id;
o_slot_id := v_slot_id;
RETURN;
EXCEPTION
WHEN OTHERS THEN -- Defensive: if we grabbed a slot and then errored, release it so
-- we don't leak global capacity.
IF v_got_slot
AND v_slot_id IS NOT NULL THEN PERFORM pg_advisory_unlock(42, v_slot_id);
END IF;
RAISE;
END;
END;
$$;
-- 7.2 Per-job runner: execute a specific job_id under a given slot
--      SECURITY DEFINER, NO transaction control.
DROP PROCEDURE IF EXISTS jobq.runner(BIGINT, INTEGER);
CREATE OR REPLACE PROCEDURE jobq.runner(IN p_job_id BIGINT, IN p_slot_id INTEGER) LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog,
  jobq AS $$
DECLARE v_job jobq.jobs %ROWTYPE;
v_blob_prefix TEXT;
v_blob_path TEXT;
v_err_msg TEXT;
v_err_detail TEXT;
v_err_hint TEXT;
v_err_context TEXT;
v_sqlstate TEXT;
v_attempt INTEGER;
v_timeout INTERVAL;
v_timeout_ms INTEGER;
v_prev_timeout TEXT;
v_backoff INTERVAL;
v_slot_released BOOLEAN := false;
BEGIN BEGIN --------------------------------------------------------------------
-- Re-fetch and lock the job row
--------------------------------------------------------------------
SELECT * INTO v_job
FROM jobq.jobs
WHERE job_id = p_job_id FOR
UPDATE;
IF NOT FOUND THEN -- Job disappeared; just release slot and bail.
PERFORM pg_advisory_unlock(42, p_slot_id);
v_slot_released := true;
RETURN;
END IF;
IF v_job.status <> 'running' THEN -- Somebody else changed it; release slot and bail.
PERFORM pg_advisory_unlock(42, p_slot_id);
v_slot_released := true;
RETURN;
END IF;
v_attempt := v_job.attempt_count;
--------------------------------------------------------------------
-- Build blob path once per attempt
--------------------------------------------------------------------
v_blob_prefix := regexp_replace(
  COALESCE(v_job.correlation_id, v_job.job_id::TEXT),
  '[^0-9A-Za-z_\\-]+',
  '_',
  'g'
);
v_blob_path := format(
  '%s/%s/%s.parquet',
  v_blob_prefix,
  v_job.job_id::TEXT,
  to_char(clock_timestamp(), 'YYYYMMDDHH24MISS')
);
v_prev_timeout := current_setting('statement_timeout', true);
--------------------------------------------------------------------
-- Execute job with per-job timeout and error handling
--    NOTE: no COMMIT/ROLLBACK here – outer wrapper owns txn boundaries.
--------------------------------------------------------------------
BEGIN -- Per-job timeout: default 30 minutes, clamp to [1s, 24h].
v_timeout := COALESCE(v_job.max_runtime, interval '30 minutes');
IF v_timeout <= interval '0' THEN v_timeout := interval '1 second';
ELSIF v_timeout > interval '24 hours' THEN v_timeout := interval '24 hours';
END IF;
v_timeout_ms := CEIL(
  EXTRACT(
    EPOCH
    FROM v_timeout
  ) * 1000
)::INTEGER;
PERFORM set_config('statement_timeout', v_timeout_ms::TEXT, true);
-- Stream result set to Azure Blob as Parquet via exec_readonly_to_blob.
PERFORM jobq.exec_readonly_to_blob(
  v_job.query_sql,
  v_job.storage_account,
  v_job.storage_container,
  v_blob_path
);
-- Restore previous timeout (best effort).
IF v_prev_timeout IS NOT NULL THEN PERFORM set_config('statement_timeout', v_prev_timeout, true);
END IF;
EXCEPTION
WHEN OTHERS THEN GET STACKED DIAGNOSTICS v_err_msg = MESSAGE_TEXT,
v_sqlstate = RETURNED_SQLSTATE,
v_err_detail = PG_EXCEPTION_DETAIL,
v_err_hint = PG_EXCEPTION_HINT,
v_err_context = PG_EXCEPTION_CONTEXT;
v_err_msg := substring(
  format(
    '[sqlstate=%s] %s | detail=%s | hint=%s | context=%s',
    COALESCE(v_sqlstate, '00000'),
    COALESCE(v_err_msg, '<no message>'),
    COALESCE(v_err_detail, '<none>'),
    COALESCE(v_err_hint, '<none>'),
    COALESCE(v_err_context, '<none>')
  )
  FROM 1 FOR 4000
);
-- Restore previous timeout even on failure.
IF v_prev_timeout IS NOT NULL THEN PERFORM set_config('statement_timeout', v_prev_timeout, true);
END IF;
-- Backoff based on this attempt.
v_backoff := LEAST(v_attempt, 10) * interval '1 minute';
IF v_attempt >= v_job.max_attempts THEN -- Final failure after max_attempts reached.
UPDATE jobq.jobs
SET status = 'failed',
  finished_at = clock_timestamp(),
  last_error = v_err_msg,
  backend_pid = NULL
WHERE job_id = v_job.job_id;
ELSE -- Requeue for retry after backoff.
UPDATE jobq.jobs
SET status = 'pending',
  scheduled_at = clock_timestamp() + v_backoff,
  started_at = NULL,
  finished_at = NULL,
  last_error = v_err_msg,
  backend_pid = NULL
WHERE job_id = v_job.job_id;
END IF;
-- Release the parallelism slot and bail out.
PERFORM pg_advisory_unlock(42, p_slot_id);
v_slot_released := true;
RETURN;
END;
--------------------------------------------------------------------
-- Success path – mark job succeeded
--------------------------------------------------------------------
UPDATE jobq.jobs
SET status = 'succeeded',
  finished_at = clock_timestamp(),
  result_blob_path = v_blob_path,
  last_error = NULL,
  backend_pid = NULL
WHERE job_id = v_job.job_id;
--------------------------------------------------------------------
-- Always release the parallelism slot
--------------------------------------------------------------------
PERFORM pg_advisory_unlock(42, p_slot_id);
v_slot_released := true;
EXCEPTION
WHEN OTHERS THEN -- Absolute last line of defense: if something unexpected escaped
-- the normal paths, make sure we don't leak the slot.
IF NOT v_slot_released THEN PERFORM pg_advisory_unlock(42, p_slot_id);
END IF;
RAISE;
END;
END;
$$;
-- 7.3 Wrapper: two-phase execution with transaction boundaries
--      SECURITY INVOKER, owns COMMITs, calls definer helpers.
DROP PROCEDURE IF EXISTS jobq.run_next_job();
CREATE OR REPLACE PROCEDURE jobq.run_next_job() LANGUAGE plpgsql AS $$
DECLARE v_job_id BIGINT;
v_slot_id INTEGER;
BEGIN --------------------------------------------------------------------
-- Phase 1 (TX1): claim a job and mark it RUNNING.
--   This sets started_at, attempt_count, run_by, backend_pid.
--------------------------------------------------------------------
SELECT o_job_id,
  o_slot_id INTO v_job_id,
  v_slot_id
FROM jobq.claim_next_job();
IF v_job_id IS NULL THEN -- Nothing to do; commit any incidental work and exit.
COMMIT;
RETURN;
END IF;
-- Make status=running and started_at visible to other sessions.
COMMIT;
--------------------------------------------------------------------
-- Phase 2 (TX2): run the claimed job to completion or retry/fail.
--------------------------------------------------------------------
CALL jobq.runner(v_job_id, v_slot_id);
-- Persist final status update (succeeded / failed / requeued).
COMMIT;
-- NOTE:
-- - This procedure MUST be called as a top-level CALL (not inside
--   an explicit BEGIN/COMMIT block), or COMMIT here will error.
-- - From pg_cron or app code, schedule/execute:
--       CALL jobq.run_next_job();
END;
$$;