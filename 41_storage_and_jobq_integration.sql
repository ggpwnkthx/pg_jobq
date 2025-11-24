\set ON_ERROR_STOP on

/*
  41_storage_and_jobq_integration.sql

  Purpose:
    End-to-end integration test that validates:
      - azure_storage is working (account_add, account_user_add, blob_list, blob_put).
      - jobq.enqueue() can insert a job row.
      - jobq.run_next_job() can execute the job.
      - result_blob_path is populated.
      - The referenced blob actually exists in Azure Storage.

  Assumptions:
    - 40_preflight.sql has already been run successfully:
        * azure_storage extension exists.
        * jobq schema, jobq.jobs table, jobq.job_status type, and key functions.
    - The caller is a privileged admin that can:
        * call azure_storage.account_add/account_user_add
        * SET ROLE jobq_worker
    - The following psql variables are provided by test.sh:
        :storage_account
        :storage_key
        :storage_container
*/

------------------------------
-- 1. Register storage account & grant jobq_worker access
------------------------------

-- Add or update account reference with provided key.
SELECT azure_storage.account_add(:'storage_account', :'storage_key') AS account_add_result;

-- Grant jobq_worker access to this storage account reference (idempotent).
SELECT azure_storage.account_user_add(:'storage_account', 'jobq_worker') AS account_user_add_result;

-- Show the account entry for sanity.
SELECT *
FROM azure_storage.account_list()
WHERE account_name = :'storage_account';

------------------------------
-- 2. Connectivity test: list blobs in the target container
------------------------------

SELECT COUNT(*) AS existing_blobs
FROM azure_storage.blob_list(:'storage_account', :'storage_container');

------------------------------
-- 3. Write a small test blob via azure_storage.blob_put()
------------------------------

WITH test_rows AS (
  SELECT 1 AS id, 'jobq-test-ok'::text AS payload
)
SELECT azure_storage.blob_put(
         :'storage_account',
         :'storage_container',
         'jobq_test_' || to_char(clock_timestamp(), 'YYYYMMDDHH24MISS') || '.csv',
         test_rows
       ) AS blob_put_result
FROM test_rows;

-- Show the most recent jobq_test_* blobs we can see.
SELECT path,
       last_modified
FROM azure_storage.blob_list(:'storage_account', :'storage_container')
WHERE path LIKE 'jobq_test_%'
ORDER BY last_modified DESC
LIMIT 5;

------------------------------
-- 4. jobq enqueue + run_next_job() + blob existence sanity check
------------------------------

/*
  This is effectively the logic that used to live in 44_enqueue_job.sql,
  inlined here so the test suite is a single cohesive script.
*/

SET ROLE jobq_worker;

-- Helper to enqueue a single high-priority test job.
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

-- Enqueue the job and capture its job_id into a psql variable.
SELECT jobq._enqueue_test_job(
         :'storage_account',
         :'storage_container'
       ) AS test_job_id
\gset

DROP FUNCTION jobq._enqueue_test_job(TEXT, TEXT);

-- Run one job via the worker wrapper (top-level CALL).
CALL jobq.run_next_job();

-- Helper: wait for THIS job to reach a terminal status.
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

-- Assert the job succeeded and the output blob exists.
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

-- Optional surfacing of the test job for human inspection.
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

