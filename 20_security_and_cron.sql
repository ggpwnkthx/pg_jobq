/*
  =====================================================================
  20_security_and_cron.sql – Ownership hardening & pg_cron wiring

  Assumptions:
    - 00_admin_bootstrap.sql applied successfully.
    - 10_jobq_types_and_table.sql, 11_jobq_enqueue_and_cancel.sql,
      12_jobq_worker_core.sql, 13_jobq_monitoring.sql,
      14_jobq_maintenance.sql applied successfully in this database.
    - jobq_worker exists and should own all jobq objects.
    - Any role executing cron.schedule_in_database has EXECUTE on the
      jobq functions it invokes.
    - pg_cron is installed in exactly one database per server. This
      script can be run in any database:
        * If pg_cron is installed in the current database, it will also
          create jobq.install_cron_jobs() which wires pg_cron jobs for
          any jobq-enabled database.
        * If pg_cron is NOT installed in the current database, the cron
          wiring helper is skipped; ownership and privilege hardening
          still runs.

  This script:
    - Aligns ownership of jobq schema, table, sequence, and views to jobq_worker
    - Revokes PUBLIC access and grants least-privilege to jobq_* roles
    - Grants EXECUTE/SELECT to client, reporting, and ops roles
    - Locks down worker internals; exposes jobq.run_next_job() as the
      single worker entrypoint, plus maintenance functions for ops/cron
    - Optionally defines a helper to wire pg_cron jobs (in a database
      where pg_cron is installed) using cron.schedule_in_database with a
      configurable target database
  =====================================================================
*/

------------------------------
-- 14. Final hardening: ownership & privileges
------------------------------

-- 14.1 Table ownership (ensure jobq_worker is owner of jobq.jobs)
ALTER TABLE jobq.jobs OWNER TO jobq_worker;

-- 14.2 Sequence ownership & privileges after table owner change
DO $$
DECLARE
  v_seq_name text;
BEGIN
  v_seq_name := pg_get_serial_sequence('jobq.jobs', 'job_id');
  IF v_seq_name IS NOT NULL THEN
    -- Align sequence owner with table owner.
    EXECUTE format('ALTER SEQUENCE %s OWNER TO jobq_worker', v_seq_name);
    -- Optional hardening: remove PUBLIC access to the sequence.
    EXECUTE format('REVOKE ALL ON SEQUENCE %s FROM PUBLIC', v_seq_name);
  END IF;
END;
$$ LANGUAGE plpgsql;

-- 14.3 Schema ownership & usage (restrict to explicit roles)
ALTER SCHEMA jobq OWNER TO jobq_worker;

-- Lock schema down; only explicitly granted roles should use it.
REVOKE ALL ON SCHEMA jobq FROM PUBLIC;

GRANT USAGE ON SCHEMA jobq TO jobq_worker;
GRANT USAGE ON SCHEMA jobq TO jobq_reader;
GRANT USAGE ON SCHEMA jobq TO jobq_reporting;
GRANT USAGE ON SCHEMA jobq TO jobq_client;
GRANT USAGE ON SCHEMA jobq TO jobq_ops;

-- 14.4 Table privileges (worker full access; reporting read-only)
REVOKE ALL ON TABLE jobq.jobs FROM PUBLIC;

GRANT SELECT, INSERT, UPDATE, DELETE ON jobq.jobs TO jobq_worker;
GRANT SELECT ON jobq.jobs TO jobq_reporting;

-- Optional: give ops read-only visibility to the raw jobs table
GRANT SELECT ON jobq.jobs TO jobq_ops;

-- 14.5 Function / procedure EXECUTE grants (role-based access)
--      Aligns with actual signatures in jobq_* scripts.

-- Enqueue: client-facing API for creating jobs.
-- NOTE: jobq_client must be considered a trusted backend role; enqueue
-- runs dynamic SQL under jobq_worker's privileges.
REVOKE ALL ON FUNCTION jobq.enqueue(
  TEXT, TEXT, TEXT, TIMESTAMPTZ, INTEGER, TEXT, INTERVAL
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION jobq.enqueue(
  TEXT, TEXT, TEXT, TIMESTAMPTZ, INTEGER, TEXT, INTERVAL
) TO jobq_client;

-- Cancel: client-facing soft cancel for pending jobs.
REVOKE ALL ON FUNCTION jobq.cancel(BIGINT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION jobq.cancel(BIGINT) TO jobq_client;

-- Internal read-only executor helper: worker-only.
REVOKE ALL ON FUNCTION jobq.exec_readonly_to_blob(TEXT, TEXT, TEXT, TEXT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION jobq.exec_readonly_to_blob(TEXT, TEXT, TEXT, TEXT) TO jobq_worker;

-- Worker internals: claim_next_job() and runner(job_id, slot_id)
--   These are internal to the worker; only jobq_worker should call them.
REVOKE ALL ON FUNCTION jobq.claim_next_job() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION jobq.claim_next_job() TO jobq_worker;

REVOKE ALL ON PROCEDURE jobq.runner(BIGINT, INTEGER) FROM PUBLIC;
GRANT EXECUTE ON PROCEDURE jobq.runner(BIGINT, INTEGER) TO jobq_worker;

-- Worker entrypoint: run_next_job()
--   This is what pg_cron and any manual "run one job" calls should use.
--   SECURITY INVOKER; must be callable by jobq_worker and the cron user.
REVOKE ALL ON PROCEDURE jobq.run_next_job() FROM PUBLIC;
GRANT EXECUTE ON PROCEDURE jobq.run_next_job() TO jobq_worker;
GRANT EXECUTE ON PROCEDURE jobq.run_next_job() TO azure_pg_admin;
-- On Azure Flexible Server, you cannot GRANT azure_pg_admin to users, but
-- assigning privileges to azure_pg_admin is valid; the server admin login
-- inherits these privileges.

-- Metrics surface: reporting-only.
REVOKE ALL ON FUNCTION jobq.get_queue_metrics() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION jobq.get_queue_metrics() TO jobq_reporting;

-- Operational APIs: kill / requeue / purge
REVOKE ALL ON FUNCTION jobq.kill(BIGINT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION jobq.kill(BIGINT) TO jobq_ops;

REVOKE ALL ON FUNCTION jobq.requeue_orphaned_running_jobs(INTEGER) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION jobq.requeue_orphaned_running_jobs(INTEGER) TO jobq_ops;
GRANT EXECUTE ON FUNCTION jobq.requeue_orphaned_running_jobs(INTEGER) TO azure_pg_admin;

REVOKE ALL ON FUNCTION jobq.purge_old_jobs(INTERVAL, INTEGER) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION jobq.purge_old_jobs(INTERVAL, INTEGER) TO jobq_ops;
GRANT EXECUTE ON FUNCTION jobq.purge_old_jobs(INTERVAL, INTEGER) TO azure_pg_admin;

-- 14.6 View ownership & visibility (reporting-only surface area)
ALTER VIEW jobq.v_queue_overview  OWNER TO jobq_worker;
ALTER VIEW jobq.v_running_jobs    OWNER TO jobq_worker;
ALTER VIEW jobq.v_recent_jobs     OWNER TO jobq_worker;
ALTER VIEW jobq.v_stalled_jobs    OWNER TO jobq_worker;

REVOKE ALL ON TABLE jobq.v_queue_overview  FROM PUBLIC;
REVOKE ALL ON TABLE jobq.v_running_jobs    FROM PUBLIC;
REVOKE ALL ON TABLE jobq.v_recent_jobs     FROM PUBLIC;
REVOKE ALL ON TABLE jobq.v_stalled_jobs    FROM PUBLIC;

GRANT SELECT ON jobq.v_queue_overview,
             jobq.v_running_jobs,
             jobq.v_recent_jobs,
             jobq.v_stalled_jobs
  TO jobq_reporting;

-- Optional: give ops read-only access to the views as well.
GRANT SELECT ON jobq.v_queue_overview,
             jobq.v_running_jobs,
             jobq.v_recent_jobs,
             jobq.v_stalled_jobs
  TO jobq_ops;

------------------------------
-- 15. Runner starters (idempotent helper for pg_cron wiring)
------------------------------

-- NOTE (Azure Flexible Server):
--   - The cron schema is off-limits to custom roles (like jobq_worker).
--   - Any *user* that is a member of azure_pg_admin can schedule jobs.
--   - This installer is SECURITY INVOKER: it runs as the caller, which
--     must be an azure_pg_admin-backed login when you want to wire pg_cron.
--   - Uses cron.schedule_in_database so the target database name can be
--     explicitly specified.
--   - Idempotent: existing jobs with the same jobname are unscheduled
--     and re-created.
--   - This block only defines jobq.install_cron_jobs() in a database
--     where pg_cron is actually installed. In other databases, a NOTICE
--     is raised and the cron helper is skipped.

DO $jobq_cron$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_extension
    WHERE extname = 'pg_cron'
  ) THEN
    RAISE NOTICE
      'pg_cron is not installed in database %, skipping jobq.install_cron_jobs() helper creation',
      current_database();
    RETURN;
  END IF;

  -- Clean up legacy versions if they exist.
  EXECUTE 'DROP FUNCTION IF EXISTS jobq.install_cron_jobs()';
  EXECUTE 'DROP FUNCTION IF EXISTS jobq.install_cron_jobs(TEXT)';

  EXECUTE $create$
    CREATE OR REPLACE FUNCTION jobq.install_cron_jobs(
      p_target_database_name TEXT DEFAULT current_database()
    )
    RETURNS void
    LANGUAGE plpgsql
    -- SECURITY INVOKER is the default; do NOT use SECURITY DEFINER here.
    SET search_path = pg_catalog, jobq
    AS $body$
    BEGIN
      IF p_target_database_name IS NULL OR btrim(p_target_database_name) = '' THEN
        RAISE EXCEPTION 'p_target_database_name cannot be null or empty';
      END IF;

      --------------------------------------------------------------------
      -- jobq-runner: core worker loop
      --   Default schedule: once per minute using standard cron syntax,
      --   which is supported by all pg_cron builds.
      --   If your pg_cron version supports sub-minute schedules, you can
      --   manually adjust this job afterward to a seconds-level cadence.
      --------------------------------------------------------------------
      PERFORM cron.unschedule(jobid)
      FROM cron.job
      WHERE jobname = 'jobq-runner';

      PERFORM cron.schedule_in_database(
        'jobq-runner',
        '*/1 * * * *',
        -- '1 seconds',
        'CALL jobq.run_next_job();',
        p_target_database_name
      );

      --------------------------------------------------------------------
      -- jobq-requeue-orphans: fix jobs stuck in RUNNING with no backend
      --------------------------------------------------------------------
      PERFORM cron.unschedule(jobid)
      FROM cron.job
      WHERE jobname = 'jobq-requeue-orphans';

      PERFORM cron.schedule_in_database(
        'jobq-requeue-orphans',
        '*/5 * * * *',
        'SELECT jobq.requeue_orphaned_running_jobs(100);',
        p_target_database_name
      );

      --------------------------------------------------------------------
      -- jobq-purge-old: retention clean-up for finished jobs
      --------------------------------------------------------------------
      PERFORM cron.unschedule(jobid)
      FROM cron.job
      WHERE jobname = 'jobq-purge-old';

      PERFORM cron.schedule_in_database(
        'jobq-purge-old',
        '0 3 * * *',
        'SELECT jobq.purge_old_jobs(''30 days'', 50000);',
        p_target_database_name
      );
    END;
    $body$;
  $create$;

  -- Lock down cron installer to admin role only.
  EXECUTE 'REVOKE ALL ON FUNCTION jobq.install_cron_jobs(TEXT) FROM PUBLIC';
  EXECUTE 'GRANT EXECUTE ON FUNCTION jobq.install_cron_jobs(TEXT) TO azure_pg_admin';
END;
$jobq_cron$ LANGUAGE plpgsql;

-- Example:
--   -- In the database that has pg_cron and jobq installed:
--   SELECT jobq.install_cron_jobs();              -- current database
--   -- Or schedule workers into another jobq-enabled database:
--   SELECT jobq.install_cron_jobs('my_database'); -- explicit target
