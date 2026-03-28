DROP INDEX IF EXISTS uq_buckets_active_name_per_lake;

ALTER TABLE buckets
    ADD CONSTRAINT buckets_lake_id_name_key UNIQUE (lake_id, name);

ALTER TABLE operations
    DROP CONSTRAINT IF EXISTS operations_operation_type_check;

ALTER TABLE operations
    ADD CONSTRAINT operations_operation_type_check
    CHECK (operation_type IN ('provision','resize','deprovision'));

DROP INDEX IF EXISTS idx_operations_bucket_id;

ALTER TABLE operations
    DROP COLUMN IF EXISTS bucket_id;
