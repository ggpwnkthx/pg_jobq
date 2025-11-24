\set ON_ERROR_STOP on

-- Add or update account reference with provided key.
SELECT azure_storage.account_add(:'storage_account', :'storage_key') AS account_add_result;

-- Grant jobq_worker access to this storage account reference (idempotent).
SELECT azure_storage.account_user_add(:'storage_account', 'jobq_worker') AS account_user_add_result;

-- Show the account entry for sanity.
SELECT *
FROM azure_storage.account_list()
WHERE account_name = :'storage_account';
