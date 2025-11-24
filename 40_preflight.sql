-- 40_preflight.sql
-- Preflight check: ensure azure_storage + core jobq objects exist.
DO $$
DECLARE v_missing text := '';
BEGIN -- azure_storage extension
IF NOT EXISTS (
  SELECT 1
  FROM pg_extension
  WHERE extname = 'azure_storage'
) THEN v_missing := v_missing || ' azure_storage_extension';
END IF;
-- jobq schema
IF NOT EXISTS (
  SELECT 1
  FROM pg_namespace
  WHERE nspname = 'jobq'
) THEN v_missing := v_missing || ' jobq_schema';
END IF;
-- jobq.jobs table
IF NOT EXISTS (
  SELECT 1
  FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = 'jobq'
    AND c.relname = 'jobs'
    AND c.relkind = 'r'
) THEN v_missing := v_missing || ' jobq_jobs_table';
END IF;
-- jobq.job_status type
IF NOT EXISTS (
  SELECT 1
  FROM pg_type t
    JOIN pg_namespace n ON n.oid = t.typnamespace
  WHERE n.nspname = 'jobq'
    AND t.typname = 'job_status'
) THEN v_missing := v_missing || ' jobq_job_status_type';
END IF;
-- key functions: enqueue & run_next_job (existence only)
IF NOT EXISTS (
  SELECT 1
  FROM pg_proc
  WHERE pronamespace = 'jobq'::regnamespace
    AND proname = 'enqueue'
) THEN v_missing := v_missing || ' jobq.enqueue';
END IF;
IF NOT EXISTS (
  SELECT 1
  FROM pg_proc
  WHERE pronamespace = 'jobq'::regnamespace
    AND proname = 'run_next_job'
) THEN v_missing := v_missing || ' jobq.run_next_job';
END IF;
IF v_missing <> '' THEN RAISE EXCEPTION 'Preflight failed; missing:%',
v_missing;
END IF;
END;
$$ LANGUAGE plpgsql;