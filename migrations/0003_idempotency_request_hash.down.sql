DROP INDEX IF EXISTS idx_idempotency_operation_id;

ALTER TABLE idempotency_keys
    DROP COLUMN IF EXISTS request_hash,
    DROP COLUMN IF EXISTS operation_type;
