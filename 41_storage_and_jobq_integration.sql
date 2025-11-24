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
  SELECT 1 AS id,
    'jobq-test-ok'::text AS payload
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
 
 IMPORTANT:
 - We avoid all psql meta-commands here (no \gset etc).
 - We use a fixed correlation_id and always look up the latest job_id
 by that correlation id from inside helper functions.
 */
SET ROLE jobq_worker;
-- Constant correlation_id used by this test.
-- (Must match in enqueue + later helpers.)
-- Value is duplicated as a literal where needed to keep script simple.
--   'jobq_test_export_single_row'
-- Helper to enqueue a single high-priority test job.
CREATE OR REPLACE FUNCTION jobq._enqueue_test_job(
    p_storage_account TEXT,
    p_storage_container TEXT
  ) RETURNS BIGINT LANGUAGE plpgsql AS $$
DECLARE v_job_id BIGINT;
BEGIN v_job_id := jobq.enqueue(
  p_query_sql => 'SELECT 1 AS jobq_test_value',
  p_storage_account => p_storage_account,
  p_storage_container => p_storage_container,
  p_scheduled_at => '1970-01-01 00:00:00+00'::timestamptz,
  p_priority => 1000,
  p_correlation_id => 'jobq_test_export_single_row',
  p_max_runtime => interval '5 minutes'
);
RETURN v_job_id;
END;
$$;
-- Enqueue the job (we don't rely on the returned job_id outside the server).
SELECT jobq._enqueue_test_job(
    :'storage_account',
    :'storage_container'
  ) AS test_job_id;
-- Run one job via the worker wrapper (top-level CALL).
CALL jobq.run_next_job();
-- Helper: wait for the latest job with this correlation_id to reach a terminal status.
CREATE OR REPLACE FUNCTION jobq._wait_for_job_terminal_state_by_corr(
    p_correlation_id TEXT,
    p_timeout INTERVAL DEFAULT interval '60 seconds'
  ) RETURNS jobq.job_status LANGUAGE plpgsql AS $$
DECLARE v_status jobq.job_status;
v_job_id BIGINT;
v_deadline timestamptz := clock_timestamp() + p_timeout;
BEGIN LOOP
SELECT j.job_id,
  j.status INTO v_job_id,
  v_status
FROM jobq.jobs j
WHERE j.correlation_id = p_correlation_id
ORDER BY j.job_id DESC
LIMIT 1;
IF v_job_id IS NULL THEN RAISE EXCEPTION 'jobq test: no job found for correlation_id % while waiting for completion',
p_correlation_id;
END IF;
-- Terminal statuses
IF v_status IN ('succeeded', 'failed', 'cancelled') THEN RETURN v_status;
END IF;
IF clock_timestamp() >= v_deadline THEN RAISE EXCEPTION 'jobq test: job with correlation_id % did not reach a terminal status within % (last status=%)',
p_correlation_id,
p_timeout,
v_status;
END IF;
PERFORM pg_sleep(1);
END LOOP;
END;
$$;
-- Wait for the test job (by correlation id) to finish (or fail fast with a clear error).
SELECT jobq._wait_for_job_terminal_state_by_corr(
    'jobq_test_export_single_row',
    interval '60 seconds'
  );
-- Assert the latest job with this correlation_id succeeded and the output blob exists.
CREATE OR REPLACE FUNCTION jobq._assert_job_completed_and_blob_exists_by_corr(
    p_expected_account TEXT,
    p_expected_cont TEXT,
    p_correlation_id TEXT
  ) RETURNS void LANGUAGE plpgsql AS $$
DECLARE v_job_id BIGINT;
v_status jobq.job_status;
v_storage_acct TEXT;
v_storage_cont TEXT;
v_blob_path TEXT;
v_blob_count INTEGER;
BEGIN
SELECT j.job_id,
  j.status,
  j.storage_account,
  j.storage_container,
  j.result_blob_path INTO v_job_id,
  v_status,
  v_storage_acct,
  v_storage_cont,
  v_blob_path
FROM jobq.jobs j
WHERE j.correlation_id = p_correlation_id
ORDER BY j.job_id DESC
LIMIT 1;
IF v_job_id IS NULL THEN RAISE EXCEPTION 'jobq test: expected at least one job for correlation_id %, found none',
p_correlation_id;
END IF;
IF v_status <> 'succeeded' THEN RAISE EXCEPTION 'jobq test: expected status=succeeded for job_id % (correlation_id=%), got %',
v_job_id,
p_correlation_id,
v_status;
END IF;
IF v_storage_acct IS DISTINCT
FROM p_expected_account THEN RAISE EXCEPTION 'jobq test: storage_account mismatch for job_id % after run (expected %, got %)',
  v_job_id,
  p_expected_account,
  v_storage_acct;
END IF;
IF v_storage_cont IS DISTINCT
FROM p_expected_cont THEN RAISE EXCEPTION 'jobq test: storage_container mismatch for job_id % after run (expected %, got %)',
  v_job_id,
  p_expected_cont,
  v_storage_cont;
END IF;
IF v_blob_path IS NULL THEN RAISE EXCEPTION 'jobq test: result_blob_path is NULL for job_id % (export did not record blob path)',
v_job_id;
END IF;
SELECT COUNT(*) INTO v_blob_count
FROM azure_storage.blob_list(p_expected_account, p_expected_cont)
WHERE path = v_blob_path;
IF v_blob_count <> 1 THEN RAISE EXCEPTION 'jobq test: expected exactly 1 blob with path % in %.% for job_id %, found %',
v_blob_path,
p_expected_account,
p_expected_cont,
v_job_id,
v_blob_count;
END IF;
END;
$$;
SELECT jobq._assert_job_completed_and_blob_exists_by_corr(
    :'storage_account',
    :'storage_container',
    'jobq_test_export_single_row'
  );
-- Clean up helpers so we don't leave test-only artifacts around.
DROP FUNCTION jobq._assert_job_completed_and_blob_exists_by_corr(TEXT, TEXT, TEXT);
DROP FUNCTION jobq._wait_for_job_terminal_state_by_corr(TEXT, INTERVAL);
DROP FUNCTION jobq._enqueue_test_job(TEXT, TEXT);
RESET ROLE;
-- Optional surfacing of the latest test job for human inspection.
SELECT job_id,
  status,
  storage_account,
  storage_container,
  result_blob_path,
  scheduled_at,
  created_at,
  finished_at
FROM jobq.jobs
WHERE correlation_id = 'jobq_test_export_single_row'
ORDER BY job_id DESC
LIMIT 1;