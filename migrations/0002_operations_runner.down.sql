DROP INDEX IF EXISTS idx_operations_running_updated_at;
DROP INDEX IF EXISTS idx_operations_pending_runnable;

ALTER TABLE operations
    DROP COLUMN IF EXISTS error_code,
    DROP COLUMN IF EXISTS updated_at,
    DROP COLUMN IF EXISTS next_attempt_at,
    DROP COLUMN IF EXISTS attempt_count,
    DROP COLUMN IF EXISTS request_payload;
