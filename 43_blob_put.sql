\set ON_ERROR_STOP on

-- Write a tiny CSV blob based on a synthetic rowset.
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

-- Show the most recent jobq_test_ blobs we can see.
SELECT path,
       last_modified
FROM azure_storage.blob_list(:'storage_account', :'storage_container')
WHERE path LIKE 'jobq_test_%'
ORDER BY last_modified DESC
LIMIT 5;
