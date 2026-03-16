package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/movincloud/datalake-provisioner/internal/domain"
)

type Repository struct {
	DB *pgxpool.Pool
}

func NewRepository(db *pgxpool.Pool) *Repository {
	return &Repository{DB: db}
}

func (r *Repository) CreateLake(ctx context.Context, lake domain.Lake) error {
	_, err := r.DB.Exec(ctx, `
		INSERT INTO lakes (lake_id, tenant_id, user_id, requested_size_gib, status, url, rgw_user, bucket_name, last_error, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
	`, lake.LakeID, lake.TenantID, lake.UserID, lake.RequestedSizeGiB, lake.Status, nullable(lake.URL), nullable(lake.RGWUser), nullable(lake.BucketName), nullable(lake.LastError), lake.CreatedAt, lake.UpdatedAt)
	return err
}

func (r *Repository) GetLake(ctx context.Context, lakeID, tenantID string) (domain.Lake, error) {
	var lake domain.Lake
	err := r.DB.QueryRow(ctx, `
		SELECT lake_id, tenant_id, user_id, requested_size_gib, status, COALESCE(url,''), COALESCE(rgw_user,''), COALESCE(bucket_name,''), COALESCE(last_error,''), created_at, updated_at
		FROM lakes
		WHERE lake_id = $1 AND tenant_id = $2
	`, lakeID, tenantID).Scan(
		&lake.LakeID,
		&lake.TenantID,
		&lake.UserID,
		&lake.RequestedSizeGiB,
		&lake.Status,
		&lake.URL,
		&lake.RGWUser,
		&lake.BucketName,
		&lake.LastError,
		&lake.CreatedAt,
		&lake.UpdatedAt,
	)
	return lake, err
}

func (r *Repository) MarkLakeProvisioned(ctx context.Context, lakeID, tenantID, rgwUser, bucketName, url string) error {
	_, err := r.DB.Exec(ctx, `
		UPDATE lakes
		SET status = 'ready', rgw_user = $3, bucket_name = $4, url = $5, last_error = NULL, updated_at = NOW()
		WHERE lake_id = $1 AND tenant_id = $2
	`, lakeID, tenantID, rgwUser, bucketName, nullable(url))
	return err
}

func (r *Repository) MarkLakeResizing(ctx context.Context, lakeID, tenantID string) error {
	_, err := r.DB.Exec(ctx, `
		UPDATE lakes
		SET status = 'resizing', updated_at = NOW()
		WHERE lake_id = $1 AND tenant_id = $2
	`, lakeID, tenantID)
	return err
}

func (r *Repository) MarkLakeResized(ctx context.Context, lakeID, tenantID string, sizeGiB int64) error {
	_, err := r.DB.Exec(ctx, `
		UPDATE lakes
		SET status = 'ready', requested_size_gib = $3, last_error = NULL, updated_at = NOW()
		WHERE lake_id = $1 AND tenant_id = $2
	`, lakeID, tenantID, sizeGiB)
	return err
}

func (r *Repository) MarkLakeDeleting(ctx context.Context, lakeID, tenantID string) error {
	_, err := r.DB.Exec(ctx, `
		UPDATE lakes
		SET status = 'deleting', updated_at = NOW()
		WHERE lake_id = $1 AND tenant_id = $2
	`, lakeID, tenantID)
	return err
}

func (r *Repository) MarkLakeDeleted(ctx context.Context, lakeID, tenantID string) error {
	_, err := r.DB.Exec(ctx, `
		UPDATE lakes
		SET status = 'deleted', last_error = NULL, updated_at = NOW()
		WHERE lake_id = $1 AND tenant_id = $2
	`, lakeID, tenantID)
	return err
}

func (r *Repository) MarkLakeFailed(ctx context.Context, lakeID, tenantID, errorMessage string) error {
	_, err := r.DB.Exec(ctx, `
		UPDATE lakes
		SET status = 'failed', last_error = $3, updated_at = NOW()
		WHERE lake_id = $1 AND tenant_id = $2
	`, lakeID, tenantID, errorMessage)
	return err
}

func (r *Repository) CreateOperation(ctx context.Context, op domain.Operation) error {
	_, err := r.DB.Exec(ctx, `
		INSERT INTO operations (operation_id, operation_type, lake_id, tenant_id, status, error_message, started_at, ended_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
	`, op.OperationID, op.OperationType, nullable(op.LakeID), op.TenantID, op.Status, nullable(op.ErrorMessage), op.StartedAt, op.EndedAt)
	return err
}

func (r *Repository) GetOperation(ctx context.Context, operationID, tenantID string) (domain.Operation, error) {
	var op domain.Operation
	err := r.DB.QueryRow(ctx, `
		SELECT operation_id, operation_type, COALESCE(lake_id::text,''), tenant_id, status, COALESCE(error_message,''), started_at, ended_at
		FROM operations
		WHERE operation_id = $1 AND tenant_id = $2
	`, operationID, tenantID).Scan(&op.OperationID, &op.OperationType, &op.LakeID, &op.TenantID, &op.Status, &op.ErrorMessage, &op.StartedAt, &op.EndedAt)
	return op, err
}

func (r *Repository) MarkOperationRunning(ctx context.Context, operationID, tenantID string) error {
	_, err := r.DB.Exec(ctx, `
		UPDATE operations
		SET status = 'running'
		WHERE operation_id = $1 AND tenant_id = $2
	`, operationID, tenantID)
	return err
}

func (r *Repository) MarkOperationSuccess(ctx context.Context, operationID, tenantID string) error {
	_, err := r.DB.Exec(ctx, `
		UPDATE operations
		SET status = 'success', error_message = NULL, ended_at = NOW()
		WHERE operation_id = $1 AND tenant_id = $2
	`, operationID, tenantID)
	return err
}

func (r *Repository) MarkOperationFailed(ctx context.Context, operationID, tenantID, errorMessage string) error {
	_, err := r.DB.Exec(ctx, `
		UPDATE operations
		SET status = 'failed', error_message = $3, ended_at = NOW()
		WHERE operation_id = $1 AND tenant_id = $2
	`, operationID, tenantID, errorMessage)
	return err
}

func nullable(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
