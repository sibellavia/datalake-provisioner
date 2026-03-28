CREATE TABLE IF NOT EXISTS buckets (
    bucket_id    UUID PRIMARY KEY,
    lake_id      UUID NOT NULL REFERENCES lakes(lake_id) ON DELETE CASCADE,
    tenant_id    TEXT NOT NULL,
    name         TEXT NOT NULL,
    bucket_name  TEXT NOT NULL,
    status       TEXT NOT NULL CHECK (status IN ('creating','ready','deleting','failed','deleted')),
    last_error   TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (lake_id, name),
    UNIQUE (bucket_name)
);

CREATE INDEX IF NOT EXISTS idx_buckets_lake_id ON buckets(lake_id);
CREATE INDEX IF NOT EXISTS idx_buckets_tenant_id ON buckets(tenant_id);
CREATE INDEX IF NOT EXISTS idx_buckets_status ON buckets(status);

ALTER TABLE lakes DROP COLUMN IF EXISTS url;
ALTER TABLE lakes DROP COLUMN IF EXISTS bucket_name;
