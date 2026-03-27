ALTER TABLE idempotency_keys
    ADD COLUMN IF NOT EXISTS operation_type TEXT,
    ADD COLUMN IF NOT EXISTS request_hash TEXT;

CREATE INDEX IF NOT EXISTS idx_idempotency_operation_id
    ON idempotency_keys (operation_id);
