package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/movincloud/datalake-provisioner/internal/domain"
)

type execer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

type Repository struct {
	DB *pgxpool.Pool
}

func NewRepository(db *pgxpool.Pool) *Repository {
	return &Repository{DB: db}
}

func (r *Repository) CreateLake(ctx context.Context, lake domain.Lake) error {
	return r.insertLake(ctx, r.DB, lake)
}

func (r *Repository) GetLake(ctx context.Context, lakeID, tenantID string) (domain.Lake, error) {
	var lake domain.Lake
	err := r.DB.QueryRow(ctx, `
		SELECT lake_id, tenant_id, user_id, requested_size_gib, status, COALESCE(rgw_user,''), COALESCE(last_error,''), created_at, updated_at
		FROM lakes
		WHERE lake_id = $1 AND tenant_id = $2
	`, lakeID, tenantID).Scan(
		&lake.LakeID,
		&lake.TenantID,
		&lake.UserID,
		&lake.RequestedSizeGiB,
		&lake.Status,
		&lake.RGWUser,
		&lake.LastError,
		&lake.CreatedAt,
		&lake.UpdatedAt,
	)
	return lake, err
}

func (r *Repository) MarkLakeProvisioned(ctx context.Context, lakeID, tenantID, rgwUser string) error {
	_, err := r.DB.Exec(ctx, `
		UPDATE lakes
		SET status = 'ready', rgw_user = $3, last_error = NULL, updated_at = NOW()
		WHERE lake_id = $1 AND tenant_id = $2
	`, lakeID, tenantID, rgwUser)
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
	return r.insertOperation(ctx, r.DB, op)
}

func (r *Repository) StartProvisionOperation(ctx context.Context, lake domain.Lake, op domain.Operation, idempotencyKey, requestHash string) (domain.Operation, error) {
	if idempotencyKey != "" {
		existingOp, found, err := r.getIdempotentOperation(ctx, op.TenantID, idempotencyKey, requestHash)
		if err != nil || found {
			return existingOp, err
		}
	}

	err := r.withTx(ctx, func(tx pgx.Tx) error {
		if err := r.insertLake(ctx, tx, lake); err != nil {
			return err
		}
		if err := r.insertOperation(ctx, tx, op); err != nil {
			return err
		}
		if idempotencyKey != "" {
			if err := r.insertIdempotencyKey(ctx, tx, op.TenantID, idempotencyKey, op.OperationID, op.OperationType, requestHash); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		if isUniqueViolation(err) {
			if idempotencyKey != "" {
				existingOp, found, getErr := r.getIdempotentOperation(ctx, op.TenantID, idempotencyKey, requestHash)
				if getErr != nil {
					return domain.Operation{}, getErr
				}
				if found {
					return existingOp, nil
				}
			}
			return domain.Operation{}, domain.ErrConflict
		}
		return domain.Operation{}, err
	}

	return op, nil
}

func (r *Repository) StartOperation(ctx context.Context, op domain.Operation, idempotencyKey, requestHash string) (domain.Operation, error) {
	if idempotencyKey != "" {
		existingOp, found, err := r.getIdempotentOperation(ctx, op.TenantID, idempotencyKey, requestHash)
		if err != nil || found {
			return existingOp, err
		}
	}

	err := r.withTx(ctx, func(tx pgx.Tx) error {
		if err := r.insertOperation(ctx, tx, op); err != nil {
			return err
		}
		if idempotencyKey != "" {
			if err := r.insertIdempotencyKey(ctx, tx, op.TenantID, idempotencyKey, op.OperationID, op.OperationType, requestHash); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		if isUniqueViolation(err) {
			if idempotencyKey != "" {
				existingOp, found, getErr := r.getIdempotentOperation(ctx, op.TenantID, idempotencyKey, requestHash)
				if getErr != nil {
					return domain.Operation{}, getErr
				}
				if found {
					return existingOp, nil
				}
			}
			return domain.Operation{}, domain.ErrConflict
		}
		return domain.Operation{}, err
	}

	return op, nil
}

func (r *Repository) GetOperation(ctx context.Context, operationID, tenantID string) (domain.Operation, error) {
	var op domain.Operation
	err := r.DB.QueryRow(ctx, `
		SELECT operation_id, operation_type, COALESCE(lake_id::text,''), tenant_id, status, COALESCE(error_message,''), started_at, ended_at,
		       COALESCE(request_payload, '{}'::jsonb), COALESCE(attempt_count, 0), COALESCE(next_attempt_at, NOW()), COALESCE(updated_at, started_at), COALESCE(error_code, '')
		FROM operations
		WHERE operation_id = $1 AND tenant_id = $2
	`, operationID, tenantID).Scan(
		&op.OperationID,
		&op.OperationType,
		&op.LakeID,
		&op.TenantID,
		&op.Status,
		&op.ErrorMessage,
		&op.StartedAt,
		&op.EndedAt,
		&op.RequestPayload,
		&op.AttemptCount,
		&op.NextAttemptAt,
		&op.UpdatedAt,
		&op.ErrorCode,
	)
	return op, err
}

func (r *Repository) ClaimNextRunnableOperation(ctx context.Context) (domain.Operation, bool, error) {
	var op domain.Operation
	err := r.DB.QueryRow(ctx, `
		WITH next_op AS (
			SELECT operation_id
			FROM operations
			WHERE status = 'pending'
			  AND next_attempt_at <= NOW()
			ORDER BY next_attempt_at ASC, started_at ASC
			LIMIT 1
		)
		UPDATE operations o
		SET status = 'running',
		    error_message = NULL,
		    ended_at = NULL,
		    updated_at = NOW(),
		    attempt_count = o.attempt_count + 1
		FROM next_op
		WHERE o.operation_id = next_op.operation_id
		RETURNING o.operation_id, o.operation_type, COALESCE(o.lake_id::text,''), o.tenant_id, o.status,
		          COALESCE(o.error_message,''), o.started_at, o.ended_at,
		          COALESCE(o.request_payload, '{}'::jsonb), o.attempt_count, o.next_attempt_at, o.updated_at, COALESCE(o.error_code, '')
	`).Scan(
		&op.OperationID,
		&op.OperationType,
		&op.LakeID,
		&op.TenantID,
		&op.Status,
		&op.ErrorMessage,
		&op.StartedAt,
		&op.EndedAt,
		&op.RequestPayload,
		&op.AttemptCount,
		&op.NextAttemptAt,
		&op.UpdatedAt,
		&op.ErrorCode,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Operation{}, false, nil
		}
		return domain.Operation{}, false, err
	}
	return op, true, nil
}

func (r *Repository) RequeueOperation(ctx context.Context, operationID, tenantID, errorMessage string, nextAttemptAt time.Time) error {
	_, err := r.DB.Exec(ctx, `
		UPDATE operations
		SET status = 'pending', error_message = $3, ended_at = NULL, next_attempt_at = $4, updated_at = NOW()
		WHERE operation_id = $1 AND tenant_id = $2
	`, operationID, tenantID, errorMessage, nextAttemptAt)
	return err
}

func (r *Repository) ResetStaleRunningOperations(ctx context.Context, staleBefore time.Time) (int64, error) {
	result, err := r.DB.Exec(ctx, `
		UPDATE operations
		SET status = 'pending', next_attempt_at = NOW(), ended_at = NULL, updated_at = NOW()
		WHERE status = 'running' AND updated_at < $1
	`, staleBefore)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

func (r *Repository) MarkOperationSuccess(ctx context.Context, operationID, tenantID string) error {
	_, err := r.DB.Exec(ctx, `
		UPDATE operations
		SET status = 'success', error_message = NULL, ended_at = NOW(), updated_at = NOW(), error_code = NULL
		WHERE operation_id = $1 AND tenant_id = $2
	`, operationID, tenantID)
	return err
}

func (r *Repository) MarkOperationFailed(ctx context.Context, operationID, tenantID, errorMessage string) error {
	_, err := r.DB.Exec(ctx, `
		UPDATE operations
		SET status = 'failed', error_message = $3, ended_at = NOW(), updated_at = NOW()
		WHERE operation_id = $1 AND tenant_id = $2
	`, operationID, tenantID, errorMessage)
	return err
}

func (r *Repository) insertLake(ctx context.Context, exec execer, lake domain.Lake) error {
	_, err := exec.Exec(ctx, `
		INSERT INTO lakes (lake_id, tenant_id, user_id, requested_size_gib, status, rgw_user, last_error, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
	`, lake.LakeID, lake.TenantID, lake.UserID, lake.RequestedSizeGiB, lake.Status, nullable(lake.RGWUser), nullable(lake.LastError), lake.CreatedAt, lake.UpdatedAt)
	return err
}

func (r *Repository) insertOperation(ctx context.Context, exec execer, op domain.Operation) error {
	_, err := exec.Exec(ctx, `
		INSERT INTO operations (operation_id, operation_type, lake_id, tenant_id, status, error_message, started_at, ended_at, request_payload, attempt_count, next_attempt_at, updated_at, error_code)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
	`, op.OperationID, op.OperationType, nullable(op.LakeID), op.TenantID, op.Status, nullable(op.ErrorMessage), op.StartedAt, op.EndedAt, op.RequestPayload, op.AttemptCount, op.NextAttemptAt, op.UpdatedAt, nullable(op.ErrorCode))
	return err
}

func (r *Repository) insertIdempotencyKey(ctx context.Context, exec execer, tenantID, idempotencyKey, operationID, operationType, requestHash string) error {
	_, err := exec.Exec(ctx, `
		INSERT INTO idempotency_keys (tenant_id, idempotency_key, operation_id, created_at, operation_type, request_hash)
		VALUES ($1,$2,$3,NOW(),$4,$5)
	`, tenantID, idempotencyKey, operationID, operationType, requestHash)
	return err
}

func (r *Repository) getIdempotentOperation(ctx context.Context, tenantID, idempotencyKey, requestHash string) (domain.Operation, bool, error) {
	var operationID string
	var storedHash string
	err := r.DB.QueryRow(ctx, `
		SELECT operation_id::text, COALESCE(request_hash, '')
		FROM idempotency_keys
		WHERE tenant_id = $1 AND idempotency_key = $2
	`, tenantID, idempotencyKey).Scan(&operationID, &storedHash)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Operation{}, false, nil
		}
		return domain.Operation{}, false, err
	}
	if storedHash != requestHash {
		return domain.Operation{}, false, domain.ErrIdempotencyMismatch
	}

	op, err := r.GetOperation(ctx, operationID, tenantID)
	if err != nil {
		return domain.Operation{}, false, err
	}
	return op, true, nil
}

func (r *Repository) withTx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	tx, err := r.DB.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func nullable(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
