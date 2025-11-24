\set ON_ERROR_STOP on

-- Ensure we can list blobs in the target container.
SELECT COUNT(*) AS existing_blobs
FROM azure_storage.blob_list(:'storage_account', :'storage_container');
