package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/movincloud/datalake-provisioner/internal/ceph"
	"github.com/movincloud/datalake-provisioner/internal/domain"
)

type Repository interface {
	CreateLake(ctx context.Context, lake domain.Lake) error
	GetLake(ctx context.Context, lakeID, tenantID string) (domain.Lake, error)
	MarkLakeProvisioned(ctx context.Context, lakeID, tenantID, rgwUser string) error
	MarkLakeResizing(ctx context.Context, lakeID, tenantID string) error
	MarkLakeResized(ctx context.Context, lakeID, tenantID string, sizeGiB int64) error
	MarkLakeDeleting(ctx context.Context, lakeID, tenantID string) error
	MarkLakeDeleted(ctx context.Context, lakeID, tenantID string) error
	MarkLakeFailed(ctx context.Context, lakeID, tenantID, errorMessage string) error
	CountNonDeletedBuckets(ctx context.Context, lakeID, tenantID string) (int, error)

	StartProvisionOperation(ctx context.Context, lake domain.Lake, op domain.Operation, idempotencyKey, requestHash string) (domain.Operation, error)
	StartBucketCreateOperation(ctx context.Context, bucket domain.Bucket, op domain.Operation, idempotencyKey, requestHash string) (domain.Operation, error)
	StartOperation(ctx context.Context, op domain.Operation, idempotencyKey, requestHash string) (domain.Operation, error)
	GetOperation(ctx context.Context, operationID, tenantID string) (domain.Operation, error)
	ClaimNextRunnableOperation(ctx context.Context) (domain.Operation, bool, error)
	RequeueOperation(ctx context.Context, operationID, tenantID, errorMessage string, nextAttemptAt time.Time) error
	ResetStaleRunningOperations(ctx context.Context, staleBefore time.Time) (int64, error)
	MarkOperationSuccess(ctx context.Context, operationID, tenantID string) error
	MarkOperationFailed(ctx context.Context, operationID, tenantID, errorMessage string) error

	GetBucket(ctx context.Context, bucketID, lakeID, tenantID string) (domain.Bucket, error)
	ListBuckets(ctx context.Context, lakeID, tenantID string) ([]domain.Bucket, error)
	MarkBucketReady(ctx context.Context, bucketID, lakeID, tenantID string) error
	MarkBucketDeleting(ctx context.Context, bucketID, lakeID, tenantID string) error
	MarkBucketDeleted(ctx context.Context, bucketID, lakeID, tenantID string) error
	MarkBucketCreateFailed(ctx context.Context, bucketID, lakeID, tenantID, errorMessage string) error
	MarkBucketDeleteFailed(ctx context.Context, bucketID, lakeID, tenantID, errorMessage string) error
}

type Provisioner struct {
	Repo Repository
	Ceph ceph.Adapter
}

type ProvisionRequest struct {
	TenantID       string
	UserID         string
	SizeGiB        int64
	IdempotencyKey string
}

type ResizeRequest struct {
	TenantID       string
	LakeID         string
	SizeGiB        int64
	IdempotencyKey string
}

type DeprovisionRequest struct {
	TenantID       string
	LakeID         string
	IdempotencyKey string
}

type CreateBucketRequest struct {
	TenantID       string
	LakeID         string
	Name           string
	IdempotencyKey string
}

type DeleteBucketRequest struct {
	TenantID       string
	LakeID         string
	BucketID       string
	IdempotencyKey string
}

func (s *Provisioner) StartProvision(ctx context.Context, req ProvisionRequest) (domain.Operation, error) {
	now := time.Now().UTC()
	lakeID := uuid.NewString()

	requestHash, err := hashProvisionRequest(req)
	if err != nil {
		return domain.Operation{}, fmt.Errorf("hash provision request: %w", err)
	}

	lake := domain.Lake{
		LakeID:           lakeID,
		TenantID:         req.TenantID,
		UserID:           req.UserID,
		RequestedSizeGiB: req.SizeGiB,
		Status:           domain.LakeStatusProvisioning,
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	payload, err := marshalOperationPayload(operationPayload{
		Type:     "provision",
		TenantID: req.TenantID,
		LakeID:   lakeID,
		UserID:   req.UserID,
		SizeGiB:  req.SizeGiB,
	})
	if err != nil {
		return domain.Operation{}, fmt.Errorf("marshal provision payload: %w", err)
	}

	op := domain.Operation{
		OperationID:    uuid.NewString(),
		OperationType:  "provision",
		LakeID:         lakeID,
		TenantID:       req.TenantID,
		Status:         domain.OperationPending,
		StartedAt:      now,
		RequestPayload: payload,
		NextAttemptAt:  now,
		UpdatedAt:      now,
	}

	storedOp, err := s.Repo.StartProvisionOperation(ctx, lake, op, req.IdempotencyKey, requestHash)
	if err != nil {
		return domain.Operation{}, mapStartOperationError("start provision operation", err)
	}

	return storedOp, nil
}

func (s *Provisioner) StartResize(ctx context.Context, req ResizeRequest) (domain.Operation, error) {
	lake, err := s.Repo.GetLake(ctx, req.LakeID, req.TenantID)
	if err != nil {
		return domain.Operation{}, fmt.Errorf("get lake: %w", err)
	}
	if lake.Status != domain.LakeStatusReady {
		return domain.Operation{}, fmt.Errorf("%w: resize allowed only when lake is ready", domain.ErrInvalidState)
	}

	now := time.Now().UTC()
	requestHash, err := hashResizeRequest(req)
	if err != nil {
		return domain.Operation{}, fmt.Errorf("hash resize request: %w", err)
	}

	payload, err := marshalOperationPayload(operationPayload{
		Type:     "resize",
		TenantID: req.TenantID,
		LakeID:   req.LakeID,
		SizeGiB:  req.SizeGiB,
	})
	if err != nil {
		return domain.Operation{}, fmt.Errorf("marshal resize payload: %w", err)
	}

	op := domain.Operation{
		OperationID:    uuid.NewString(),
		OperationType:  "resize",
		LakeID:         req.LakeID,
		TenantID:       req.TenantID,
		Status:         domain.OperationPending,
		StartedAt:      now,
		RequestPayload: payload,
		NextAttemptAt:  now,
		UpdatedAt:      now,
	}

	storedOp, err := s.Repo.StartOperation(ctx, op, req.IdempotencyKey, requestHash)
	if err != nil {
		return domain.Operation{}, mapStartOperationError("start resize operation", err)
	}

	return storedOp, nil
}

func (s *Provisioner) StartDeprovision(ctx context.Context, req DeprovisionRequest) (domain.Operation, error) {
	lake, err := s.Repo.GetLake(ctx, req.LakeID, req.TenantID)
	if err != nil {
		return domain.Operation{}, fmt.Errorf("get lake: %w", err)
	}
	if lake.Status != domain.LakeStatusReady && lake.Status != domain.LakeStatusFailed {
		return domain.Operation{}, fmt.Errorf("%w: deprovision allowed only when lake is ready or failed", domain.ErrInvalidState)
	}

	bucketCount, err := s.Repo.CountNonDeletedBuckets(ctx, req.LakeID, req.TenantID)
	if err != nil {
		return domain.Operation{}, fmt.Errorf("count lake buckets: %w", err)
	}
	if bucketCount > 0 {
		return domain.Operation{}, fmt.Errorf("%w: deprovision allowed only when lake has no buckets", domain.ErrInvalidState)
	}

	now := time.Now().UTC()
	requestHash, err := hashDeprovisionRequest(req)
	if err != nil {
		return domain.Operation{}, fmt.Errorf("hash deprovision request: %w", err)
	}

	payload, err := marshalOperationPayload(operationPayload{
		Type:     "deprovision",
		TenantID: req.TenantID,
		LakeID:   req.LakeID,
	})
	if err != nil {
		return domain.Operation{}, fmt.Errorf("marshal deprovision payload: %w", err)
	}

	op := domain.Operation{
		OperationID:    uuid.NewString(),
		OperationType:  "deprovision",
		LakeID:         req.LakeID,
		TenantID:       req.TenantID,
		Status:         domain.OperationPending,
		StartedAt:      now,
		RequestPayload: payload,
		NextAttemptAt:  now,
		UpdatedAt:      now,
	}

	storedOp, err := s.Repo.StartOperation(ctx, op, req.IdempotencyKey, requestHash)
	if err != nil {
		return domain.Operation{}, mapStartOperationError("start deprovision operation", err)
	}

	return storedOp, nil
}

func (s *Provisioner) StartCreateBucket(ctx context.Context, req CreateBucketRequest) (domain.Operation, error) {
	lake, err := s.Repo.GetLake(ctx, req.LakeID, req.TenantID)
	if err != nil {
		return domain.Operation{}, fmt.Errorf("get lake: %w", err)
	}
	if lake.Status != domain.LakeStatusReady {
		return domain.Operation{}, fmt.Errorf("%w: bucket create allowed only when lake is ready", domain.ErrInvalidState)
	}

	now := time.Now().UTC()
	bucketID := uuid.NewString()
	bucketName := buildPhysicalBucketName(req.LakeID, bucketID, req.Name)

	requestHash, err := hashCreateBucketRequest(req)
	if err != nil {
		return domain.Operation{}, fmt.Errorf("hash bucket create request: %w", err)
	}

	bucket := domain.Bucket{
		BucketID:   bucketID,
		LakeID:     req.LakeID,
		TenantID:   req.TenantID,
		Name:       req.Name,
		BucketName: bucketName,
		Status:     domain.BucketStatusCreating,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	payload, err := marshalOperationPayload(operationPayload{
		Type:       "bucket_create",
		TenantID:   req.TenantID,
		LakeID:     req.LakeID,
		BucketID:   bucketID,
		Name:       req.Name,
		BucketName: bucketName,
	})
	if err != nil {
		return domain.Operation{}, fmt.Errorf("marshal bucket create payload: %w", err)
	}

	op := domain.Operation{
		OperationID:    uuid.NewString(),
		OperationType:  "bucket_create",
		LakeID:         req.LakeID,
		BucketID:       bucketID,
		TenantID:       req.TenantID,
		Status:         domain.OperationPending,
		StartedAt:      now,
		RequestPayload: payload,
		NextAttemptAt:  now,
		UpdatedAt:      now,
	}

	storedOp, err := s.Repo.StartBucketCreateOperation(ctx, bucket, op, req.IdempotencyKey, requestHash)
	if err != nil {
		return domain.Operation{}, mapStartOperationError("start bucket create operation", err)
	}

	return storedOp, nil
}

func (s *Provisioner) StartDeleteBucket(ctx context.Context, req DeleteBucketRequest) (domain.Operation, error) {
	lake, err := s.Repo.GetLake(ctx, req.LakeID, req.TenantID)
	if err != nil {
		return domain.Operation{}, fmt.Errorf("get lake: %w", err)
	}
	if lake.Status != domain.LakeStatusReady {
		return domain.Operation{}, fmt.Errorf("%w: bucket delete allowed only when lake is ready", domain.ErrInvalidState)
	}

	bucket, err := s.Repo.GetBucket(ctx, req.BucketID, req.LakeID, req.TenantID)
	if err != nil {
		return domain.Operation{}, fmt.Errorf("get bucket: %w", err)
	}
	if bucket.Status != domain.BucketStatusReady {
		return domain.Operation{}, fmt.Errorf("%w: bucket delete allowed only when bucket is ready", domain.ErrInvalidState)
	}

	now := time.Now().UTC()
	requestHash, err := hashDeleteBucketRequest(req)
	if err != nil {
		return domain.Operation{}, fmt.Errorf("hash bucket delete request: %w", err)
	}

	payload, err := marshalOperationPayload(operationPayload{
		Type:       "bucket_delete",
		TenantID:   req.TenantID,
		LakeID:     req.LakeID,
		BucketID:   req.BucketID,
		Name:       bucket.Name,
		BucketName: bucket.BucketName,
	})
	if err != nil {
		return domain.Operation{}, fmt.Errorf("marshal bucket delete payload: %w", err)
	}

	op := domain.Operation{
		OperationID:    uuid.NewString(),
		OperationType:  "bucket_delete",
		LakeID:         req.LakeID,
		BucketID:       req.BucketID,
		TenantID:       req.TenantID,
		Status:         domain.OperationPending,
		StartedAt:      now,
		RequestPayload: payload,
		NextAttemptAt:  now,
		UpdatedAt:      now,
	}

	storedOp, err := s.Repo.StartOperation(ctx, op, req.IdempotencyKey, requestHash)
	if err != nil {
		return domain.Operation{}, mapStartOperationError("start bucket delete operation", err)
	}

	return storedOp, nil
}

func (s *Provisioner) ClaimNextRunnableOperation(ctx context.Context) (domain.Operation, bool, error) {
	return s.Repo.ClaimNextRunnableOperation(ctx)
}

func (s *Provisioner) ResetStaleRunningOperations(ctx context.Context, staleBefore time.Time) (int64, error) {
	return s.Repo.ResetStaleRunningOperations(ctx, staleBefore)
}

func (s *Provisioner) RequeueOperation(ctx context.Context, op domain.Operation, err error, nextAttemptAt time.Time) error {
	errorMessage := fmt.Sprintf("operation attempt %d failed: %v", op.AttemptCount, err)
	return s.Repo.RequeueOperation(ctx, op.OperationID, op.TenantID, errorMessage, nextAttemptAt)
}

func (s *Provisioner) MarkOperationExecutionFailed(ctx context.Context, op domain.Operation, err error) error {
	errorMessage := err.Error()
	log.Printf("operation failed op=%s type=%s lake=%s bucket=%s tenant=%s: %s", op.OperationID, op.OperationType, op.LakeID, op.BucketID, op.TenantID, errorMessage)

	var joinErr error
	switch op.OperationType {
	case "bucket_create":
		if op.BucketID != "" {
			if repoErr := s.Repo.MarkBucketCreateFailed(ctx, op.BucketID, op.LakeID, op.TenantID, errorMessage); repoErr != nil {
				joinErr = errors.Join(joinErr, fmt.Errorf("mark bucket create failed: %w", repoErr))
			}
		}
	case "bucket_delete":
		if op.BucketID != "" {
			if repoErr := s.Repo.MarkBucketDeleteFailed(ctx, op.BucketID, op.LakeID, op.TenantID, errorMessage); repoErr != nil {
				joinErr = errors.Join(joinErr, fmt.Errorf("mark bucket delete failed: %w", repoErr))
			}
		}
	default:
		if op.LakeID != "" {
			if repoErr := s.Repo.MarkLakeFailed(ctx, op.LakeID, op.TenantID, errorMessage); repoErr != nil {
				joinErr = errors.Join(joinErr, fmt.Errorf("mark lake failed: %w", repoErr))
			}
		}
	}

	if repoErr := s.Repo.MarkOperationFailed(ctx, op.OperationID, op.TenantID, errorMessage); repoErr != nil {
		joinErr = errors.Join(joinErr, fmt.Errorf("mark operation failed: %w", repoErr))
	}
	return joinErr
}

func (s *Provisioner) ExecuteOperation(ctx context.Context, op domain.Operation) error {
	var payload operationPayload
	if len(op.RequestPayload) > 0 {
		if err := json.Unmarshal(op.RequestPayload, &payload); err != nil {
			return fmt.Errorf("unmarshal operation payload: %w", err)
		}
	}
	if payload.Type == "" {
		payload.Type = op.OperationType
	}
	if payload.TenantID == "" {
		payload.TenantID = op.TenantID
	}
	if payload.LakeID == "" {
		payload.LakeID = op.LakeID
	}
	if payload.BucketID == "" {
		payload.BucketID = op.BucketID
	}

	switch op.OperationType {
	case "provision":
		return s.executeProvision(ctx, op, payload)
	case "resize":
		return s.executeResize(ctx, op, payload)
	case "deprovision":
		return s.executeDeprovision(ctx, op, payload)
	case "bucket_create":
		return s.executeCreateBucket(ctx, op, payload)
	case "bucket_delete":
		return s.executeDeleteBucket(ctx, op, payload)
	default:
		return fmt.Errorf("unsupported operation type %q", op.OperationType)
	}
}

func (s *Provisioner) executeProvision(ctx context.Context, op domain.Operation, payload operationPayload) error {
	if s.Ceph == nil {
		return fmt.Errorf("ceph adapter not configured")
	}
	if payload.LakeID == "" || payload.TenantID == "" || payload.UserID == "" || payload.SizeGiB <= 0 {
		return fmt.Errorf("invalid provision payload")
	}

	lakeAccess, err := s.Ceph.EnsureLake(ctx, payload.LakeID)
	if err != nil {
		return fmt.Errorf("ceph ensure lake failed: %w", err)
	}
	if err := s.Ceph.SetLakeQuota(ctx, payload.LakeID, payload.SizeGiB); err != nil {
		return fmt.Errorf("ceph set lake quota failed: %w", err)
	}

	if err := s.Repo.MarkLakeProvisioned(ctx, payload.LakeID, payload.TenantID, lakeAccess.RGWUser); err != nil {
		return fmt.Errorf("mark lake provisioned failed: %w", err)
	}

	if err := s.Repo.MarkOperationSuccess(ctx, op.OperationID, op.TenantID); err != nil {
		return fmt.Errorf("mark operation success: %w", err)
	}

	log.Printf("provision completed op=%s lake=%s tenant=%s rgwUser=%s", op.OperationID, payload.LakeID, payload.TenantID, lakeAccess.RGWUser)
	return nil
}

func (s *Provisioner) executeResize(ctx context.Context, op domain.Operation, payload operationPayload) error {
	if s.Ceph == nil {
		return fmt.Errorf("ceph adapter not configured")
	}
	if payload.LakeID == "" || payload.TenantID == "" || payload.SizeGiB <= 0 {
		return fmt.Errorf("invalid resize payload")
	}

	if err := s.Repo.MarkLakeResizing(ctx, payload.LakeID, payload.TenantID); err != nil {
		return fmt.Errorf("mark lake resizing failed: %w", err)
	}
	if err := s.Ceph.SetLakeQuota(ctx, payload.LakeID, payload.SizeGiB); err != nil {
		return fmt.Errorf("ceph resize failed: %w", err)
	}

	if err := s.Repo.MarkLakeResized(ctx, payload.LakeID, payload.TenantID, payload.SizeGiB); err != nil {
		return fmt.Errorf("mark lake resized failed: %w", err)
	}

	if err := s.Repo.MarkOperationSuccess(ctx, op.OperationID, op.TenantID); err != nil {
		return fmt.Errorf("mark operation success: %w", err)
	}

	log.Printf("resize completed op=%s lake=%s tenant=%s sizeGiB=%d", op.OperationID, payload.LakeID, payload.TenantID, payload.SizeGiB)
	return nil
}

func (s *Provisioner) executeDeprovision(ctx context.Context, op domain.Operation, payload operationPayload) error {
	if s.Ceph == nil {
		return fmt.Errorf("ceph adapter not configured")
	}
	if payload.LakeID == "" || payload.TenantID == "" {
		return fmt.Errorf("invalid deprovision payload")
	}

	if err := s.Repo.MarkLakeDeleting(ctx, payload.LakeID, payload.TenantID); err != nil {
		return fmt.Errorf("mark lake deleting failed: %w", err)
	}
	if err := s.Ceph.DeleteLake(ctx, payload.LakeID); err != nil {
		return fmt.Errorf("ceph deprovision failed: %w", err)
	}

	if err := s.Repo.MarkLakeDeleted(ctx, payload.LakeID, payload.TenantID); err != nil {
		return fmt.Errorf("mark lake deleted failed: %w", err)
	}

	if err := s.Repo.MarkOperationSuccess(ctx, op.OperationID, op.TenantID); err != nil {
		return fmt.Errorf("mark operation success: %w", err)
	}

	log.Printf("deprovision completed op=%s lake=%s tenant=%s", op.OperationID, payload.LakeID, payload.TenantID)
	return nil
}

func (s *Provisioner) executeCreateBucket(ctx context.Context, op domain.Operation, payload operationPayload) error {
	if s.Ceph == nil {
		return fmt.Errorf("ceph adapter not configured")
	}
	if payload.LakeID == "" || payload.TenantID == "" || payload.BucketID == "" || payload.BucketName == "" {
		return fmt.Errorf("invalid bucket create payload")
	}

	if err := s.Ceph.CreateBucket(ctx, payload.LakeID, payload.BucketName); err != nil {
		return fmt.Errorf("ceph create bucket failed: %w", err)
	}
	if err := s.Repo.MarkBucketReady(ctx, payload.BucketID, payload.LakeID, payload.TenantID); err != nil {
		return fmt.Errorf("mark bucket ready failed: %w", err)
	}
	if err := s.Repo.MarkOperationSuccess(ctx, op.OperationID, op.TenantID); err != nil {
		return fmt.Errorf("mark operation success: %w", err)
	}

	log.Printf("bucket create completed op=%s lake=%s bucket=%s bucketName=%s tenant=%s", op.OperationID, payload.LakeID, payload.BucketID, payload.BucketName, payload.TenantID)
	return nil
}

func (s *Provisioner) executeDeleteBucket(ctx context.Context, op domain.Operation, payload operationPayload) error {
	if s.Ceph == nil {
		return fmt.Errorf("ceph adapter not configured")
	}
	if payload.LakeID == "" || payload.TenantID == "" || payload.BucketID == "" || payload.BucketName == "" {
		return fmt.Errorf("invalid bucket delete payload")
	}

	if err := s.Repo.MarkBucketDeleting(ctx, payload.BucketID, payload.LakeID, payload.TenantID); err != nil {
		return fmt.Errorf("mark bucket deleting failed: %w", err)
	}
	if err := s.Ceph.DeleteBucketIfEmpty(ctx, payload.LakeID, payload.BucketName); err != nil {
		return fmt.Errorf("ceph delete bucket failed: %w", err)
	}
	if err := s.Repo.MarkBucketDeleted(ctx, payload.BucketID, payload.LakeID, payload.TenantID); err != nil {
		return fmt.Errorf("mark bucket deleted failed: %w", err)
	}
	if err := s.Repo.MarkOperationSuccess(ctx, op.OperationID, op.TenantID); err != nil {
		return fmt.Errorf("mark operation success: %w", err)
	}

	log.Printf("bucket delete completed op=%s lake=%s bucket=%s bucketName=%s tenant=%s", op.OperationID, payload.LakeID, payload.BucketID, payload.BucketName, payload.TenantID)
	return nil
}

func mapStartOperationError(prefix string, err error) error {
	if errors.Is(err, domain.ErrIdempotencyMismatch) || errors.Is(err, domain.ErrConflict) || errors.Is(err, domain.ErrInvalidState) || errors.Is(err, domain.ErrNotFound) {
		return fmt.Errorf("%s: %w", prefix, err)
	}
	return fmt.Errorf("%s: %w", prefix, err)
}

func (s *Provisioner) GetOperation(ctx context.Context, operationID, tenantID string) (domain.Operation, error) {
	op, err := s.Repo.GetOperation(ctx, operationID, tenantID)
	if err != nil {
		return domain.Operation{}, fmt.Errorf("get operation: %w", err)
	}
	return op, nil
}

func (s *Provisioner) GetLake(ctx context.Context, lakeID, tenantID string) (domain.Lake, error) {
	lake, err := s.Repo.GetLake(ctx, lakeID, tenantID)
	if err != nil {
		return domain.Lake{}, fmt.Errorf("get lake: %w", err)
	}
	return lake, nil
}

func (s *Provisioner) GetBucket(ctx context.Context, bucketID, lakeID, tenantID string) (domain.Bucket, error) {
	bucket, err := s.Repo.GetBucket(ctx, bucketID, lakeID, tenantID)
	if err != nil {
		return domain.Bucket{}, fmt.Errorf("get bucket: %w", err)
	}
	return bucket, nil
}

func (s *Provisioner) ListBuckets(ctx context.Context, lakeID, tenantID string) ([]domain.Bucket, error) {
	if _, err := s.Repo.GetLake(ctx, lakeID, tenantID); err != nil {
		return nil, fmt.Errorf("get lake: %w", err)
	}
	buckets, err := s.Repo.ListBuckets(ctx, lakeID, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list buckets: %w", err)
	}
	return buckets, nil
}
