/*
  =====================================================================
  50_production_hardening_and_version.sql – Indexes, constraints, version stamp

  Run as:
    - Any login that is a member of jobq_worker, e.g.:
        GRANT jobq_worker TO <admin_role>;
    - This script will SET ROLE jobq_worker.

  Assumptions:
    - 00_admin_bootstrap.sql, 10_jobq_types_and_table.sql,
      11_jobq_enqueue_and_cancel.sql, 12_jobq_worker_core.sql,
      13_jobq_monitoring.sql, 14_jobq_maintenance.sql, and
      20_security_and_cron.sql have been applied.
    - jobq.jobs exists and is owned by jobq_worker.

  This script:
    - Adds indexes that keep purge and job-claim operations efficient
      at scale.
    - Adds an extra CHECK constraint to enforce a sensible max_runtime
      window at the table level (defensive against manual UPDATEs).
    - Introduces a simple schema version marker and jobq.version()
      helper so you can track installed jobq versions per database.
    - Is designed to be re-runnable.
  =====================================================================
*/

SET ROLE jobq_worker;

------------------------------
-- 1. Indexes for production workloads
------------------------------

-- 1.1 Purge / retention helper index
--     Used by jobq.purge_old_jobs(), which filters on finished_at.
--     Partial index avoids bloating on unfinished rows.
CREATE INDEX IF NOT EXISTS idx_jobq_jobs_finished_at
  ON jobq.jobs (finished_at)
  WHERE finished_at IS NOT NULL;

-- 1.2 Pending-queue index for the claim path
--     jobq.claim_next_job() filters and orders like:
--       WHERE status = 'pending'
--         AND scheduled_at <= now()
--         AND attempt_count < max_attempts
--       ORDER BY priority DESC, scheduled_at, job_id
--
--     This partial index keeps "find the next job" fast even when the
--     table contains a large history of finished jobs.
CREATE INDEX IF NOT EXISTS idx_jobq_jobs_pending_schedule
  ON jobq.jobs (priority DESC, scheduled_at, job_id)
  WHERE status = 'pending';

------------------------------
-- 2. Extra max_runtime guardrail
------------------------------

/*
  The jobq.enqueue() function already validates p_max_runtime to be in:
    (0, 24h]

  jobq.runner() also clamps max_runtime into the same window at runtime.

  This constraint adds the invariant at the storage layer as well so
  that manual UPDATEs or future callers cannot silently violate it.
*/

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'jobq_jobs_max_runtime_window_chk'
      AND conrelid = 'jobq.jobs'::regclass
  ) THEN
    ALTER TABLE jobq.jobs
      ADD CONSTRAINT jobq_jobs_max_runtime_window_chk
      CHECK (
        max_runtime > interval '0 seconds'
        AND max_runtime <= interval '24 hours'
      );
  END IF;
END;
$$ LANGUAGE plpgsql;

------------------------------
-- 3. Schema versioning
------------------------------

/*
  Lightweight version stamp so you can tell which jobq build is
  installed in a given database.

  - jobq.schema_version holds one or more version rows; you normally
    insert once per release.
  - jobq.version() returns the highest version string; you can use it
    from tooling or preflight checks.

  This script stamps version '1.0.0'. On future releases, bump the
  literal and re-run; the INSERT is idempotent via ON CONFLICT.
*/

CREATE TABLE IF NOT EXISTS jobq.schema_version (
  version       TEXT PRIMARY KEY,
  installed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  installed_by  TEXT NOT NULL DEFAULT session_user
);

CREATE OR REPLACE FUNCTION jobq.version()
RETURNS TEXT
LANGUAGE sql
AS $$
  SELECT max(version) FROM jobq.schema_version
$$;

INSERT INTO jobq.schema_version(version)
VALUES ('1.0.0')
ON CONFLICT (version) DO NOTHING;

