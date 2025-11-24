/*
  =====================================================================
  00_admin_bootstrap.sql – Admin bootstrap (roles, extensions, schema)

  Run as: server admin / azure_pg_admin

  Target: Azure Database for PostgreSQL – Flexible Server (PostgreSQL 18)

  Overall solution: long-running, read-only job queue that can be
  installed per-database and uses:
    - pg_cron (installed in exactly one database per server) for scheduling
    - azure_storage for Parquet exports
    - monitoring, timeouts, and kill APIs

  Server-level prereqs (via Azure Portal/CLI, NOT in SQL):
    - shared_preload_libraries includes: pg_cron, azure_storage
    - azure.extensions includes: pg_cron, azure_storage
    - cron.database_name points at the database where pg_cron is installed
      (the pg_cron "home" database).

  Database-level usage pattern:
    - You may install jobq in **any** database that should own its own
      queue and execute queries from that database’s context.
    - In each such database, run this bootstrap plus the other jobq
      scripts.
    - In exactly one database per server, install the pg_cron extension;
      when you also install jobq there and run 20_security_and_cron.sql,
      you get jobq.install_cron_jobs() which wires pg_cron schedules for
      any jobq-enabled database.

  This bootstrap script:
    - Creates cluster-level roles for workers, clients, reporting, and ops
    - Installs azure_storage in the current database (if missing; will
      fail if the server is not configured to allow/load azure_storage)
    - Creates the jobq schema owned by jobq_worker
    - Best-effort: grants pg_signal_backend to jobq_worker so jobq.kill()
      can terminate backends where supported

  IMPORTANT:
    - jobq can be installed in multiple databases. Each database has its
      own jobq.jobs table and executes jobq queries within that database.
    - To use pg_cron for scheduling, ensure:
        * pg_cron is installed in exactly one database on the server
        * jobq is also installed in that same database
        * 20_security_and_cron.sql has been applied there so that
          jobq.install_cron_jobs() is available for wiring cron jobs
          into any jobq-enabled database.
  =====================================================================
*/

------------------------------
-- 1. Cluster-level roles used by the job queue
------------------------------
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = 'jobq_worker'
  ) THEN
    -- Service/owner role for jobq objects and SECURITY DEFINER functions.
    -- NOLOGIN/NOINHERIT so it is only used as an execution/ownership context.
    CREATE ROLE jobq_worker NOINHERIT NOLOGIN;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = 'jobq_reader'
  ) THEN
    -- Marker role for read-only execution against data exposed to the queue.
    -- This script does not grant object privileges; wire those separately.
    CREATE ROLE jobq_reader NOINHERIT NOLOGIN;
  END IF;

  -- Application / reporting / ops roles; adjust as needed:
  IF NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = 'jobq_client'
  ) THEN
    -- Intended for applications/services that enqueue and cancel jobs.
    CREATE ROLE jobq_client NOINHERIT;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = 'jobq_reporting'
  ) THEN
    -- Intended for BI/reporting; will receive SELECT on views later.
    CREATE ROLE jobq_reporting NOINHERIT;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = 'jobq_ops'
  ) THEN
    -- Intended for operational controls (kill, requeue, purge).
    CREATE ROLE jobq_ops NOINHERIT;
  END IF;
END;
$$ LANGUAGE plpgsql;

-- NOTE:
-- Grant jobq_worker to a deployment/admin login explicitly, outside this script, e.g.:
--   GRANT jobq_worker TO my_admin_login;
-- On Azure Flexible Server, if you run this as the server admin login
-- (member of azure_pg_admin), that login automatically gets membership
-- in roles it creates, which allows subsequent SET ROLE jobq_worker.

------------------------------
-- 1b. Optional: grant pg_signal_backend to jobq_worker (for jobq.kill)
------------------------------

DO $$
BEGIN
  /*
    Best-effort: pg_signal_backend lets jobq.kill() terminate backends.

    On some managed platforms (including some Azure tiers), GRANTing
    pg_signal_backend may not be permitted even for admin users. In that
    case this block emits a NOTICE and continues, and jobq.kill() will
    operate in "mark cancelled only" mode (no backend signal).
  */
  BEGIN
    IF EXISTS (
      SELECT 1
      FROM pg_roles
      WHERE rolname = 'pg_signal_backend'
    ) THEN
      EXECUTE 'GRANT pg_signal_backend TO jobq_worker';
    END IF;
  EXCEPTION
    WHEN insufficient_privilege THEN
      RAISE NOTICE
        'Could not GRANT pg_signal_backend TO jobq_worker (insufficient_privilege). If your service supports it, grant this manually so jobq.kill() can terminate backends.';
    WHEN undefined_object THEN
      -- Role not present on this server; nothing to do.
      NULL;
  END;
END;
$$ LANGUAGE plpgsql;

------------------------------
-- 2. Extensions (database-level; require admin)
------------------------------

-- NOTE:
--   - pg_cron is no longer required to be installed in this database.
--     You may install jobq in databases without pg_cron; pg_cron is only
--     needed in whichever database you use to wire cron jobs via
--     jobq.install_cron_jobs().
--
-- azure_storage is installed here if missing.
-- Prereqs (via Azure server parameters) must already be satisfied:
--   - shared_preload_libraries includes 'azure_storage'
--   - azure.extensions includes 'azure_storage'
CREATE EXTENSION IF NOT EXISTS azure_storage;

------------------------------
-- 3. Schema (owned by jobq_worker)
------------------------------

-- Create schema if missing; ensure jobq_worker is the owner for new installs.
-- For existing installs with a different owner, 20_security_and_cron.sql will
-- realign ownership via ALTER SCHEMA.
CREATE SCHEMA IF NOT EXISTS jobq AUTHORIZATION jobq_worker;
