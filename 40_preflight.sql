-- 40_preflight.sql
-- Preflight check: ensure azure_storage + core jobq objects exist.
DO $$
DECLARE v_missing text := '';
BEGIN -- azure_storage extension
IF NOT EXISTS (
  SELECT 1
  FROM pg_catalog.pg_extension
  WHERE extname = 'azure_storage'
) THEN v_missing := v_missing || ' azure_storage_extension';
END IF;
-- jobq schema
IF NOT EXISTS (
  SELECT 1
  FROM pg_catalog.pg_namespace
  WHERE nspname = 'jobq'
) THEN v_missing := v_missing || ' jobq_schema';
END IF;
-- jobq.jobs table (ordinary or partitioned)
IF NOT EXISTS (
  SELECT 1
  FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = 'jobq'
    AND c.relname = 'jobs'
    AND c.relkind IN ('r', 'p') -- 'r' = table, 'p' = partitioned table
) THEN v_missing := v_missing || ' jobq_jobs_table';
END IF;
-- jobq.job_status type
IF NOT EXISTS (
  SELECT 1
  FROM pg_catalog.pg_type t
    JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
  WHERE n.nspname = 'jobq'
    AND t.typname = 'job_status'
) THEN v_missing := v_missing || ' jobq_job_status_type';
END IF;
-- key functions: enqueue & run_next_job (existence only)
IF NOT EXISTS (
  SELECT 1
  FROM pg_catalog.pg_proc p
    JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
  WHERE n.nspname = 'jobq'
    AND p.proname = 'enqueue'
) THEN v_missing := v_missing || ' jobq.enqueue';
END IF;
IF NOT EXISTS (
  SELECT 1
  FROM pg_catalog.pg_proc p
    JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
  WHERE n.nspname = 'jobq'
    AND p.proname = 'run_next_job'
) THEN v_missing := v_missing || ' jobq.run_next_job';
END IF;
IF v_missing <> '' THEN RAISE EXCEPTION 'Preflight failed; missing:%',
v_missing;
END IF;
END;
$$ LANGUAGE plpgsql;