CREATE TABLE IF NOT EXISTS lake_customer_s3_credentials (
    credential_id   UUID PRIMARY KEY,
    lake_id         UUID NOT NULL REFERENCES lakes(lake_id) ON DELETE CASCADE,
    tenant_id       TEXT NOT NULL,
    access_key_id   TEXT NOT NULL UNIQUE,
    status          TEXT NOT NULL CHECK (status IN ('active','revoked')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at      TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_lake_customer_s3_credentials_active
    ON lake_customer_s3_credentials (lake_id)
    WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_lake_customer_s3_credentials_tenant_id
    ON lake_customer_s3_credentials (tenant_id);
