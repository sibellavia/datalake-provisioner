ALTER TABLE operations
    ADD COLUMN IF NOT EXISTS bucket_id UUID REFERENCES buckets(bucket_id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_operations_bucket_id
    ON operations (bucket_id);

ALTER TABLE operations
    DROP CONSTRAINT IF EXISTS operations_operation_type_check;

ALTER TABLE operations
    ADD CONSTRAINT operations_operation_type_check
    CHECK (operation_type IN ('provision','resize','deprovision','bucket_create','bucket_delete'));

ALTER TABLE buckets
    DROP CONSTRAINT IF EXISTS buckets_lake_id_name_key;

CREATE UNIQUE INDEX IF NOT EXISTS uq_buckets_active_name_per_lake
    ON buckets (lake_id, name)
    WHERE status <> 'deleted';
