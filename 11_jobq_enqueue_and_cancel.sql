/*
 =====================================================================
 11_jobq_enqueue_and_cancel.sql – Enqueue, cancel, and read-only executor
 
 Run as:
 - Any login that is a member of jobq_worker.
 - This script will SET ROLE jobq_worker.
 
 Assumptions:
 - 00_admin_bootstrap.sql and 10_jobq_types_and_table.sql applied successfully.
 - jobq.jobs exists and is owned by jobq_worker.
 - azure_storage extension is installed in this database (for blob_put).
 
 This script:
 - Defines jobq.enqueue() with strong validation for read-only SELECT/WITH
 and job metadata (priority, correlation_id, max_runtime)
 - Defines jobq.cancel() for soft-cancelling pending jobs
 - Defines jobq.exec_readonly_to_blob() for streaming results to Azure Blob
 - Is designed to be re-runnable.
 =====================================================================
 */
SET ROLE jobq_worker;
------------------------------
-- 4. Enqueue API (SECURITY DEFINER with strong validation)
------------------------------
-- SECURITY NOTE:
--   This function implements best-effort validation to ensure queries are
--   read-only SELECT/WITH. It is NOT a full SQL sandbox; it does not
--   inspect function volatility or side effects inside string literals.
--   Any role that can call jobq.enqueue can cause arbitrary read-only
--   SQL to run under jobq_worker's privileges. Only trusted backend
--   application roles should be granted EXECUTE on this function, and
--   jobq_worker must only have the minimal SELECT privileges required.
--
--   p_max_runtime controls the per-job statement_timeout used by
--   jobq.runner(), with validation enforced via jobq.jobs.max_runtime.
--
-- Drop legacy and current signatures if present (idempotent).
DROP FUNCTION IF EXISTS jobq.enqueue(TEXT, TEXT, TEXT, TIMESTAMPTZ, INTEGER, TEXT);
DROP FUNCTION IF EXISTS jobq.enqueue(
  TEXT,
  TEXT,
  TEXT,
  TIMESTAMPTZ,
  INTEGER,
  TEXT,
  INTERVAL
);
-- IMPORTANT:
--   All SECURITY DEFINER functions here run with:
--     SET search_path = pg_catalog, jobq
--   Any SQL submitted via p_query_sql MUST therefore use fully-qualified
--   object names (e.g. app_schema.table_name), not rely on the caller's
--   search_path. This is intentional for security/isolation.
CREATE OR REPLACE FUNCTION jobq.enqueue(
    p_query_sql TEXT,
    p_storage_account TEXT,
    p_storage_container TEXT,
    p_scheduled_at TIMESTAMPTZ DEFAULT now(),
    p_priority INTEGER DEFAULT 0,
    p_correlation_id TEXT DEFAULT NULL,
    p_max_runtime INTERVAL DEFAULT interval '30 minutes'
  ) RETURNS BIGINT LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog,
  jobq AS $$
DECLARE v_job_id BIGINT;
v_trim_sql TEXT;
v_sql_scan TEXT;
v_in_string BOOLEAN := false;
v_len INTEGER;
v_i INTEGER;
v_ch TEXT;
v_runtime INTERVAL;
v_scheduled_at TIMESTAMPTZ;
BEGIN --------------------------------------------------------------------
-- Basic argument validation
--------------------------------------------------------------------
v_trim_sql := btrim(p_query_sql, E' \t\n\r\f\v');
IF v_trim_sql IS NULL
OR length(v_trim_sql) = 0 THEN RAISE EXCEPTION 'p_query_sql cannot be empty';
END IF;
IF length(v_trim_sql) > 100000 THEN RAISE EXCEPTION 'query_sql too long (limit: 100000 characters)';
END IF;
IF p_storage_account IS NULL
OR btrim(p_storage_account) = '' THEN RAISE EXCEPTION 'p_storage_account cannot be empty';
END IF;
IF p_storage_container IS NULL
OR btrim(p_storage_container) = '' THEN RAISE EXCEPTION 'p_storage_container cannot be empty';
END IF;
IF p_priority < -1000
OR p_priority > 1000 THEN RAISE EXCEPTION 'p_priority must be between -1000 and 1000 (got %)',
p_priority;
END IF;
-- Normalize scheduled_at so we never violate NOT NULL on the column,
-- even if callers explicitly pass NULL.
v_scheduled_at := COALESCE(p_scheduled_at, now());
-- Normalize and validate max_runtime up front so stored value matches
-- what the runner will enforce.
v_runtime := COALESCE(p_max_runtime, interval '30 minutes');
IF v_runtime <= interval '0 seconds' THEN RAISE EXCEPTION 'p_max_runtime must be greater than 0 seconds (got %)',
p_max_runtime;
END IF;
IF v_runtime > interval '24 hours' THEN RAISE EXCEPTION 'p_max_runtime must not exceed 24 hours (got %)',
p_max_runtime;
END IF;
--------------------------------------------------------------------
-- Strip out the contents of string literals before regex checks.
-- This avoids false positives when keywords only appear in literals,
-- e.g. 'security', 'into', ';', '--', etc.
--------------------------------------------------------------------
v_len := length(v_trim_sql);
v_sql_scan := '';
v_i := 1;
WHILE v_i <= v_len LOOP v_ch := substr(v_trim_sql, v_i, 1);
IF NOT v_in_string THEN IF v_ch = '''' THEN v_in_string := true;
v_sql_scan := v_sql_scan || v_ch;
ELSE v_sql_scan := v_sql_scan || v_ch;
END IF;
v_i := v_i + 1;
ELSE -- Inside string literal: preserve delimiters, blank out contents.
IF v_ch = '''' THEN IF v_i < v_len
AND substr(v_trim_sql, v_i + 1, 1) = '''' THEN -- Escaped quote: skip second quote, keep a placeholder.
v_sql_scan := v_sql_scan || ' ';
v_i := v_i + 2;
ELSE v_in_string := false;
v_sql_scan := v_sql_scan || v_ch;
v_i := v_i + 1;
END IF;
ELSE v_sql_scan := v_sql_scan || ' ';
v_i := v_i + 1;
END IF;
END IF;
END LOOP;
--------------------------------------------------------------------
-- Validation on v_sql_scan (strings blanked out)
--------------------------------------------------------------------
IF v_sql_scan !~* E'^(select|with)\\y' THEN RAISE EXCEPTION 'query_sql must start with SELECT or WITH';
END IF;
-- Disallow semicolons to prevent multi-statement payloads.
IF v_sql_scan ~ ';' THEN RAISE EXCEPTION 'query_sql must not contain semicolons';
END IF;
-- Disallow SQL comments to reduce surface for trickery.
IF v_sql_scan ~ '--'
OR v_sql_scan ~ '/\\*' THEN RAISE EXCEPTION 'query_sql must not contain SQL comments';
END IF;
-- Disallow any use of INTO (e.g. SELECT ... INTO).
IF v_sql_scan ~* E'\\yinto\\y' THEN RAISE EXCEPTION 'query_sql must not contain INTO';
END IF;
-- Additional hardening: disallow obvious mutating/DDL keywords
-- (outside literals).
IF v_sql_scan ~* E'\\b(insert|update|delete|merge|truncate|create|alter|drop|grant|revoke|copy|vacuum|analyze|cluster|refresh|reindex|call|do|lock)\\b' THEN RAISE EXCEPTION 'query_sql must be a read-only SELECT/WITH (no DML/DDL keywords allowed)';
END IF;
--------------------------------------------------------------------
-- Persist the job
--------------------------------------------------------------------
INSERT INTO jobq.jobs (
    query_sql,
    storage_account,
    storage_container,
    scheduled_at,
    priority,
    correlation_id,
    max_runtime
  )
VALUES (
    v_trim_sql,
    p_storage_account,
    p_storage_container,
    v_scheduled_at,
    p_priority,
    p_correlation_id,
    v_runtime
  )
RETURNING job_id INTO v_job_id;
RETURN v_job_id;
END;
$$;
------------------------------
-- 5. Cancel API (soft cancel for pending jobs only)
------------------------------
CREATE OR REPLACE FUNCTION jobq.cancel(p_job_id BIGINT) RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog,
  jobq AS $$
DECLARE v_rows INTEGER := 0;
BEGIN
/*
 Best-effort cancel:
 - Only touches jobs that are still pending
 - Uses FOR UPDATE SKIP LOCKED so we do NOT block behind a worker
 - Returns TRUE iff we actually cancelled something
 */
WITH locked AS (
  SELECT job_id
  FROM jobq.jobs
  WHERE job_id = p_job_id
    AND status = 'pending' FOR
  UPDATE SKIP LOCKED
)
UPDATE jobq.jobs j
SET status = 'cancelled',
  finished_at = clock_timestamp(),
  backend_pid = NULL
FROM locked l
WHERE j.job_id = l.job_id
RETURNING 1 INTO v_rows;
RETURN COALESCE(v_rows, 0) = 1;
END;
$$;
------------------------------
-- 6. Read-only exec helper (assumes query_sql already validated)
------------------------------
CREATE OR REPLACE FUNCTION jobq.exec_readonly_to_blob(
    p_query_sql TEXT,
    p_storage_account TEXT,
    p_storage_container TEXT,
    p_blob_path TEXT
  ) RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog,
  jobq AS $$ BEGIN -- Execute the supplied SELECT/WITH and stream results to Azure Blob as Parquet.
  -- This helper is intended to be called by jobq.runner() under jobq_worker.
  EXECUTE format(
    'SELECT azure_storage.blob_put(%L, %L, %L, q, %L)
       FROM (%s) AS q',
    p_storage_account,
    p_storage_container,
    p_blob_path,
    'parquet',
    p_query_sql
  );
END;
$$;