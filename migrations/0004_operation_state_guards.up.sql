CREATE UNIQUE INDEX IF NOT EXISTS uq_operations_active_lake
    ON operations (tenant_id, lake_id)
    WHERE lake_id IS NOT NULL AND status IN ('pending', 'running');
