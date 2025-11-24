\set ON_ERROR_STOP on

/*
  44_enqueue_job.sql

  Purpose:
    - Integration test that:
        * jobq.enqueue() can insert a job row.
        * jobq.run_next_job() can execute the job.
        * result_blob_path is populated.
        * The referenced blob actually exists in Azure Storage.
    - Enqueues exactly ONE job per run and waits for THAT job to finish.

  Assumptions:
    - Core jobq scripts have been applied:
        00_admin_bootstrap.sql
        10_jobq_types_and_table.sql
        11_jobq_enqueue_and_cancel.sql
        12_jobq_worker_core.sql
        13_jobq_monitoring.sql
        14_jobq_maintenance.sql
        20_security_and_cron.sql
    - The caller can SET ROLE jobq_worker (server admin is a member).
    - azure_storage.account_add() and account_user_add('jobq_worker')
      have already been run for :'storage_account'.
*/

------------------------------
-- 1. Enqueue exactly ONE test job as jobq_worker
--    (we call jobq.enqueue() from a tiny helper that returns job_id)
------------------------------

SET ROLE jobq_worker;

CREATE OR REPLACE FUNCTION jobq._enqueue_test_job(
  p_storage_account   TEXT,
  p_storage_container TEXT
)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  v_job_id BIGINT;
BEGIN
  v_job_id := jobq.enqueue(
                p_query_sql         => 'SELECT 1 AS jobq_test_value',
                p_storage_account   => p_storage_account,
                p_storage_container => p_storage_container,
                p_scheduled_at      => '1970-01-01 00:00:00+00'::timestamptz,
                p_priority          => 1000,
                p_correlation_id    => 'jobq_test_export_single_row',
                p_max_runtime       => interval '5 minutes'
              );
  RETURN v_job_id;
END;
$$;

-- Enqueue the job and capture its job_id into a psql variable
SELECT jobq._enqueue_test_job(
         :'storage_account',
         :'storage_container'
       ) AS test_job_id
\gset

DROP FUNCTION jobq._enqueue_test_job(TEXT, TEXT);

------------------------------
-- 2. Run one job via the worker wrapper (top-level CALL)
------------------------------

CALL jobq.run_next_job();

------------------------------
-- 3. Helper: wait for THIS job to reach a terminal status
--    (succeeded / failed / cancelled), up to a timeout.
------------------------------

CREATE OR REPLACE FUNCTION jobq._wait_for_job_terminal_state(
  p_job_id   BIGINT,
  p_timeout  INTERVAL DEFAULT interval '60 seconds'
)
RETURNS jobq.job_status
LANGUAGE plpgsql
AS $$
DECLARE
  v_status    jobq.job_status;
  v_deadline  timestamptz := clock_timestamp() + p_timeout;
BEGIN
  LOOP
    SELECT status
    INTO v_status
    FROM jobq.jobs
    WHERE job_id = p_job_id;

    IF NOT FOUND THEN
      RAISE EXCEPTION
        'jobq test: job_id % not found while waiting for completion',
        p_job_id;
    END IF;

    -- Terminal statuses
    IF v_status IN ('succeeded', 'failed', 'cancelled') THEN
      RETURN v_status;
    END IF;

    IF clock_timestamp() >= v_deadline THEN
      RAISE EXCEPTION
        'jobq test: job_id % did not reach a terminal status within % (last status=%)',
        p_job_id, p_timeout, v_status;
    END IF;

    PERFORM pg_sleep(1);
  END LOOP;
END;
$$;

-- Wait for this specific job to finish (or fail fast with a clear error).
SELECT jobq._wait_for_job_terminal_state(
  :test_job_id,
  interval '60 seconds'
);

------------------------------
-- 4. Assert the job succeeded and the output blob exists
------------------------------

CREATE OR REPLACE FUNCTION jobq._assert_job_completed_and_blob_exists(
  p_job_id           BIGINT,
  p_expected_account TEXT,
  p_expected_cont    TEXT
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  v_count        INTEGER;
  v_status       jobq.job_status;
  v_storage_acct TEXT;
  v_storage_cont TEXT;
  v_blob_path    TEXT;
  v_blob_count   INTEGER;
BEGIN
  SELECT COUNT(*),
         MAX(status),
         MAX(storage_account),
         MAX(storage_container),
         MAX(result_blob_path)
  INTO   v_count,
         v_status,
         v_storage_acct,
         v_storage_cont,
         v_blob_path
  FROM jobq.jobs
  WHERE job_id = p_job_id;

  IF v_count <> 1 THEN
    RAISE EXCEPTION
      'jobq test: expected exactly 1 row for job_id %, found %',
      p_job_id, v_count;
  END IF;

  IF v_status <> 'succeeded' THEN
    RAISE EXCEPTION
      'jobq test: expected status=succeeded for job_id %, got %',
      p_job_id, v_status;
  END IF;

  IF v_storage_acct IS DISTINCT FROM p_expected_account THEN
    RAISE EXCEPTION
      'jobq test: storage_account mismatch for job_id % after run (expected %, got %)',
      p_job_id, p_expected_account, v_storage_acct;
  END IF;

  IF v_storage_cont IS DISTINCT FROM p_expected_cont THEN
    RAISE EXCEPTION
      'jobq test: storage_container mismatch for job_id % after run (expected %, got %)',
      p_job_id, p_expected_cont, v_storage_cont;
  END IF;

  IF v_blob_path IS NULL THEN
    RAISE EXCEPTION
      'jobq test: result_blob_path is NULL for job_id % (export did not record blob path)',
      p_job_id;
  END IF;

  SELECT COUNT(*)
  INTO v_blob_count
  FROM azure_storage.blob_list(p_expected_account, p_expected_cont)
  WHERE path = v_blob_path;

  IF v_blob_count <> 1 THEN
    RAISE EXCEPTION
      'jobq test: expected exactly 1 blob with path % in %.% for job_id %, found %',
      v_blob_path, p_expected_account, p_expected_cont, p_job_id, v_blob_count;
  END IF;
END;
$$;

SELECT jobq._assert_job_completed_and_blob_exists(
  :test_job_id,
  :'storage_account',
  :'storage_container'
);

-- Clean up helpers so we don't leave test-only artifacts around.
DROP FUNCTION jobq._assert_job_completed_and_blob_exists(BIGINT, TEXT, TEXT);
DROP FUNCTION jobq._wait_for_job_terminal_state(BIGINT, INTERVAL);

RESET ROLE;

------------------------------
-- 5. Optional: surface the test job for human inspection
------------------------------

SELECT job_id,
       status,
       storage_account,
       storage_container,
       result_blob_path,
       scheduled_at,
       created_at,
       finished_at
FROM jobq.jobs
WHERE job_id = :test_job_id;
