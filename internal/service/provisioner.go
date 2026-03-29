package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/movincloud/datalake-provisioner/internal/ceph"
	"github.com/movincloud/datalake-provisioner/internal/domain"
	"github.com/movincloud/datalake-provisioner/internal/observability"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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
	CountActiveLakes(ctx context.Context) (int, error)
	CountActiveLakesByTenant(ctx context.Context, tenantID string) (int, error)
	CountActiveBuckets(ctx context.Context) (int, error)
	CountActiveBucketsByTenant(ctx context.Context, tenantID string) (int, error)
	SumCommittedQuotaBytes(ctx context.Context) (int64, error)
	SumCommittedQuotaBytesByTenant(ctx context.Context, tenantID string) (int64, error)
	ListActiveLakes(ctx context.Context) ([]domain.Lake, error)
	ListActiveLakesByTenant(ctx context.Context, tenantID string) ([]domain.Lake, error)

	StartProvisionOperation(ctx context.Context, lake domain.Lake, op domain.Operation, idempotencyKey, requestHash string) (domain.Operation, error)
	StartBucketCreateOperation(ctx context.Context, bucket domain.Bucket, op domain.Operation, idempotencyKey, requestHash string) (domain.Operation, error)
	StartOperation(ctx context.Context, op domain.Operation, idempotencyKey, requestHash string) (domain.Operation, error)
	GetOperation(ctx context.Context, operationID, tenantID string) (domain.Operation, error)
	ListOperationsByLake(ctx context.Context, lakeID, tenantID string) ([]domain.Operation, error)
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

func startServiceSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return observability.Tracer("service").Start(ctx, name, trace.WithAttributes(attrs...))
}

func operationSpanAttributes(operationID, operationType, tenantID, lakeID, bucketID string) []attribute.KeyValue {
	attrs := []attribute.KeyValue{}
	if operationID != "" {
		attrs = append(attrs, attribute.String("operation.id", operationID))
	}
	if operationType != "" {
		attrs = append(attrs, attribute.String("operation.type", operationType))
	}
	if tenantID != "" {
		attrs = append(attrs, attribute.String("tenant.id", tenantID))
	}
	if lakeID != "" {
		attrs = append(attrs, attribute.String("lake.id", lakeID))
	}
	if bucketID != "" {
		attrs = append(attrs, attribute.String("bucket.id", bucketID))
	}
	return attrs
}

func (s *Provisioner) StartProvision(ctx context.Context, req ProvisionRequest) (storedOp domain.Operation, err error) {
	ctx, span := startServiceSpan(ctx, "service.start_provision",
		attribute.String("operation.type", "provision"),
		attribute.String("tenant.id", req.TenantID),
		attribute.Int64("requested_size_gib", req.SizeGiB),
	)
	defer func() {
		observability.RecordSpanError(span, err)
		span.End()
		observability.ObserveOperationStartRequest("provision", classifyOperationStartRequestResult(err))
	}()

	now := time.Now().UTC()
	lakeID := uuid.NewString()
	span.SetAttributes(attribute.String("lake.id", lakeID))

	requestHash, err := hashProvisionRequest(req)
	if err != nil {
		return domain.Operation{}, fmt.Errorf("hash provision request: %w", err)
	}

	lake := domain.Lake{
		LakeID:           lakeID,
		TenantID:         req.TenantID,
		RequestedSizeGiB: req.SizeGiB,
		Status:           domain.LakeStatusProvisioning,
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	payload, err := marshalOperationPayload(operationPayload{
		Type:     "provision",
		TenantID: req.TenantID,
		LakeID:   lakeID,
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
	span.SetAttributes(attribute.String("operation.id", op.OperationID))

	storedOp, err = s.Repo.StartProvisionOperation(ctx, lake, op, req.IdempotencyKey, requestHash)
	if err != nil {
		err = mapStartOperationError("start provision operation", err)
		return domain.Operation{}, err
	}

	return storedOp, nil
}

func (s *Provisioner) StartResize(ctx context.Context, req ResizeRequest) (storedOp domain.Operation, err error) {
	ctx, span := startServiceSpan(ctx, "service.start_resize",
		attribute.String("operation.type", "resize"),
		attribute.String("tenant.id", req.TenantID),
		attribute.String("lake.id", req.LakeID),
		attribute.Int64("requested_size_gib", req.SizeGiB),
	)
	defer func() {
		observability.RecordSpanError(span, err)
		span.End()
		observability.ObserveOperationStartRequest("resize", classifyOperationStartRequestResult(err))
	}()

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
	span.SetAttributes(attribute.String("operation.id", op.OperationID))

	storedOp, err = s.Repo.StartOperation(ctx, op, req.IdempotencyKey, requestHash)
	if err != nil {
		err = mapStartOperationError("start resize operation", err)
		return domain.Operation{}, err
	}

	return storedOp, nil
}

func (s *Provisioner) StartDeprovision(ctx context.Context, req DeprovisionRequest) (storedOp domain.Operation, err error) {
	ctx, span := startServiceSpan(ctx, "service.start_deprovision",
		attribute.String("operation.type", "deprovision"),
		attribute.String("tenant.id", req.TenantID),
		attribute.String("lake.id", req.LakeID),
	)
	defer func() {
		observability.RecordSpanError(span, err)
		span.End()
		observability.ObserveOperationStartRequest("deprovision", classifyOperationStartRequestResult(err))
	}()

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
	span.SetAttributes(attribute.String("operation.id", op.OperationID))

	storedOp, err = s.Repo.StartOperation(ctx, op, req.IdempotencyKey, requestHash)
	if err != nil {
		err = mapStartOperationError("start deprovision operation", err)
		return domain.Operation{}, err
	}

	return storedOp, nil
}

func (s *Provisioner) StartCreateBucket(ctx context.Context, req CreateBucketRequest) (storedOp domain.Operation, err error) {
	ctx, span := startServiceSpan(ctx, "service.start_bucket_create",
		attribute.String("operation.type", "bucket_create"),
		attribute.String("tenant.id", req.TenantID),
		attribute.String("lake.id", req.LakeID),
		attribute.String("bucket.logical_name", req.Name),
	)
	defer func() {
		observability.RecordSpanError(span, err)
		span.End()
		observability.ObserveOperationStartRequest("bucket_create", classifyOperationStartRequestResult(err))
	}()

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
	span.SetAttributes(
		attribute.String("bucket.id", bucketID),
		attribute.String("bucket.name", bucketName),
	)

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
	span.SetAttributes(attribute.String("operation.id", op.OperationID))

	storedOp, err = s.Repo.StartBucketCreateOperation(ctx, bucket, op, req.IdempotencyKey, requestHash)
	if err != nil {
		err = mapStartOperationError("start bucket create operation", err)
		return domain.Operation{}, err
	}

	return storedOp, nil
}

func (s *Provisioner) StartDeleteBucket(ctx context.Context, req DeleteBucketRequest) (storedOp domain.Operation, err error) {
	ctx, span := startServiceSpan(ctx, "service.start_bucket_delete",
		attribute.String("operation.type", "bucket_delete"),
		attribute.String("tenant.id", req.TenantID),
		attribute.String("lake.id", req.LakeID),
		attribute.String("bucket.id", req.BucketID),
	)
	defer func() {
		observability.RecordSpanError(span, err)
		span.End()
		observability.ObserveOperationStartRequest("bucket_delete", classifyOperationStartRequestResult(err))
	}()

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
	span.SetAttributes(
		attribute.String("operation.id", op.OperationID),
		attribute.String("bucket.name", bucket.BucketName),
	)

	storedOp, err = s.Repo.StartOperation(ctx, op, req.IdempotencyKey, requestHash)
	if err != nil {
		err = mapStartOperationError("start bucket delete operation", err)
		return domain.Operation{}, err
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
	slog.ErrorContext(ctx, "operation execution failed",
		"component", "service",
		"operation.id", op.OperationID,
		"operation.type", op.OperationType,
		"tenant.id", op.TenantID,
		"lake.id", op.LakeID,
		"bucket.id", op.BucketID,
		"error.message", errorMessage,
	)

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
	if joinErr == nil {
		observability.ObserveOperationTerminal(op.OperationType, "failed", op.StartedAt)
	}
	return joinErr
}

func (s *Provisioner) ExecuteOperation(ctx context.Context, op domain.Operation) (err error) {
	ctx, span := startServiceSpan(ctx, "service.execute_operation", operationSpanAttributes(op.OperationID, op.OperationType, op.TenantID, op.LakeID, op.BucketID)...)
	defer func() {
		observability.RecordSpanError(span, err)
		span.End()
	}()

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
	if payload.LakeID == "" || payload.TenantID == "" || payload.SizeGiB <= 0 {
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
	observability.ObserveOperationTerminal(op.OperationType, "success", op.StartedAt)

	slog.InfoContext(ctx, "provision completed",
		"component", "service",
		"operation.id", op.OperationID,
		"operation.type", op.OperationType,
		"tenant.id", payload.TenantID,
		"lake.id", payload.LakeID,
		"rgw.user", lakeAccess.RGWUser,
	)
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
	observability.ObserveOperationTerminal(op.OperationType, "success", op.StartedAt)

	slog.InfoContext(ctx, "resize completed",
		"component", "service",
		"operation.id", op.OperationID,
		"operation.type", op.OperationType,
		"tenant.id", payload.TenantID,
		"lake.id", payload.LakeID,
		"requested_size_gib", payload.SizeGiB,
	)
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
	observability.ObserveOperationTerminal(op.OperationType, "success", op.StartedAt)

	slog.InfoContext(ctx, "deprovision completed",
		"component", "service",
		"operation.id", op.OperationID,
		"operation.type", op.OperationType,
		"tenant.id", payload.TenantID,
		"lake.id", payload.LakeID,
	)
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
	observability.ObserveOperationTerminal(op.OperationType, "success", op.StartedAt)

	slog.InfoContext(ctx, "bucket create completed",
		"component", "service",
		"operation.id", op.OperationID,
		"operation.type", op.OperationType,
		"tenant.id", payload.TenantID,
		"lake.id", payload.LakeID,
		"bucket.id", payload.BucketID,
		"bucket.name", payload.BucketName,
	)
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
	observability.ObserveOperationTerminal(op.OperationType, "success", op.StartedAt)

	slog.InfoContext(ctx, "bucket delete completed",
		"component", "service",
		"operation.id", op.OperationID,
		"operation.type", op.OperationType,
		"tenant.id", payload.TenantID,
		"lake.id", payload.LakeID,
		"bucket.id", payload.BucketID,
		"bucket.name", payload.BucketName,
	)
	return nil
}

func mapStartOperationError(prefix string, err error) error {
	if errors.Is(err, domain.ErrIdempotencyMismatch) || errors.Is(err, domain.ErrConflict) || errors.Is(err, domain.ErrInvalidState) || errors.Is(err, domain.ErrNotFound) {
		return fmt.Errorf("%s: %w", prefix, err)
	}
	return fmt.Errorf("%s: %w", prefix, err)
}

func classifyOperationStartRequestResult(err error) string {
	switch {
	case err == nil:
		return "accepted"
	case errors.Is(err, domain.ErrIdempotencyMismatch), errors.Is(err, domain.ErrConflict):
		return "conflict"
	case errors.Is(err, domain.ErrInvalidState):
		return "invalid_state"
	case errors.Is(err, domain.ErrNotFound):
		return "not_found"
	default:
		return "error"
	}
}

func (s *Provisioner) GetOperation(ctx context.Context, operationID, tenantID string) (op domain.Operation, err error) {
	ctx, span := startServiceSpan(ctx, "service.get_operation", operationSpanAttributes(operationID, "", tenantID, "", "")...)
	defer func() {
		observability.RecordSpanError(span, err)
		span.End()
	}()

	op, err = s.Repo.GetOperation(ctx, operationID, tenantID)
	if err != nil {
		return domain.Operation{}, fmt.Errorf("get operation: %w", err)
	}
	return op, nil
}

func (s *Provisioner) ListLakeOperations(ctx context.Context, lakeID, tenantID string) (ops []domain.Operation, err error) {
	ctx, span := startServiceSpan(ctx, "service.list_lake_operations", operationSpanAttributes("", "", tenantID, lakeID, "")...)
	defer func() {
		observability.RecordSpanError(span, err)
		span.End()
	}()

	if _, err := s.Repo.GetLake(ctx, lakeID, tenantID); err != nil {
		return nil, fmt.Errorf("get lake: %w", err)
	}
	ops, err = s.Repo.ListOperationsByLake(ctx, lakeID, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list lake operations: %w", err)
	}
	return ops, nil
}

func (s *Provisioner) GetLake(ctx context.Context, lakeID, tenantID string) (lake domain.Lake, err error) {
	ctx, span := startServiceSpan(ctx, "service.get_lake", operationSpanAttributes("", "", tenantID, lakeID, "")...)
	defer func() {
		observability.RecordSpanError(span, err)
		span.End()
	}()

	if s.Ceph == nil {
		return domain.Lake{}, fmt.Errorf("ceph adapter not configured")
	}

	lake, err = s.Repo.GetLake(ctx, lakeID, tenantID)
	if err != nil {
		return domain.Lake{}, fmt.Errorf("get lake: %w", err)
	}

	bucketCount, err := s.Repo.CountNonDeletedBuckets(ctx, lakeID, tenantID)
	if err != nil {
		return domain.Lake{}, fmt.Errorf("count lake buckets: %w", err)
	}

	lake.BucketCount = bucketCount
	if lake.RGWUser == "" {
		return lake, nil
	}

	usage, err := s.Ceph.GetLakeUsage(ctx, lakeID)
	if err != nil {
		return domain.Lake{}, fmt.Errorf("get lake usage: %w", err)
	}

	lake.UsedBytes = usage.UsedBytes
	lake.ObjectCount = usage.ObjectCount
	return lake, nil
}

func (s *Provisioner) GetLakeInternalAccess(ctx context.Context, lakeID, tenantID string) (access ceph.LakeInternalAccess, err error) {
	ctx, span := startServiceSpan(ctx, "service.get_lake_internal_access", operationSpanAttributes("", "", tenantID, lakeID, "")...)
	defer func() {
		observability.RecordSpanError(span, err)
		span.End()
	}()

	if s.Ceph == nil {
		return ceph.LakeInternalAccess{}, fmt.Errorf("ceph adapter not configured")
	}

	lake, err := s.Repo.GetLake(ctx, lakeID, tenantID)
	if err != nil {
		return ceph.LakeInternalAccess{}, fmt.Errorf("get lake: %w", err)
	}
	if lake.Status != domain.LakeStatusReady {
		return ceph.LakeInternalAccess{}, fmt.Errorf("%w: internal access allowed only when lake is ready", domain.ErrInvalidState)
	}
	if lake.RGWUser == "" {
		return ceph.LakeInternalAccess{}, fmt.Errorf("%w: lake storage identity not provisioned", domain.ErrInvalidState)
	}

	access, err = s.Ceph.GetLakeInternalAccess(ctx, lakeID)
	if err != nil {
		return ceph.LakeInternalAccess{}, fmt.Errorf("get lake internal access: %w", err)
	}
	return access, nil
}

func (s *Provisioner) GetBucket(ctx context.Context, bucketID, lakeID, tenantID string) (bucket domain.Bucket, err error) {
	ctx, span := startServiceSpan(ctx, "service.get_bucket", operationSpanAttributes("", "", tenantID, lakeID, bucketID)...)
	defer func() {
		observability.RecordSpanError(span, err)
		span.End()
	}()

	if s.Ceph == nil {
		return domain.Bucket{}, fmt.Errorf("ceph adapter not configured")
	}

	bucket, err = s.Repo.GetBucket(ctx, bucketID, lakeID, tenantID)
	if err != nil {
		return domain.Bucket{}, fmt.Errorf("get bucket: %w", err)
	}

	usage, err := s.Ceph.GetBucketUsage(ctx, bucket.BucketName)
	if err != nil {
		return domain.Bucket{}, fmt.Errorf("get bucket usage: %w", err)
	}

	bucket.UsedBytes = usage.UsedBytes
	bucket.ObjectCount = usage.ObjectCount
	return bucket, nil
}

func (s *Provisioner) ListLakes(ctx context.Context, tenantID string) (lakes []domain.Lake, err error) {
	ctx, span := startServiceSpan(ctx, "service.list_lakes", operationSpanAttributes("", "", tenantID, "", "")...)
	defer func() {
		observability.RecordSpanError(span, err)
		span.End()
	}()

	if s.Ceph == nil {
		return nil, fmt.Errorf("ceph adapter not configured")
	}

	lakes, err = s.Repo.ListActiveLakesByTenant(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list active lakes by tenant: %w", err)
	}

	for i := range lakes {
		bucketCount, err := s.Repo.CountNonDeletedBuckets(ctx, lakes[i].LakeID, tenantID)
		if err != nil {
			return nil, fmt.Errorf("count lake buckets for %s: %w", lakes[i].LakeID, err)
		}
		lakes[i].BucketCount = bucketCount

		if lakes[i].RGWUser == "" {
			continue
		}
		usage, err := s.Ceph.GetLakeUsage(ctx, lakes[i].LakeID)
		if err != nil {
			return nil, fmt.Errorf("get lake usage for %s: %w", lakes[i].LakeID, err)
		}
		lakes[i].UsedBytes = usage.UsedBytes
		lakes[i].ObjectCount = usage.ObjectCount
	}

	return lakes, nil
}

func (s *Provisioner) ListBuckets(ctx context.Context, lakeID, tenantID string) (buckets []domain.Bucket, err error) {
	ctx, span := startServiceSpan(ctx, "service.list_buckets", operationSpanAttributes("", "", tenantID, lakeID, "")...)
	defer func() {
		observability.RecordSpanError(span, err)
		span.End()
	}()

	if s.Ceph == nil {
		return nil, fmt.Errorf("ceph adapter not configured")
	}

	if _, err := s.Repo.GetLake(ctx, lakeID, tenantID); err != nil {
		return nil, fmt.Errorf("get lake: %w", err)
	}
	buckets, err = s.Repo.ListBuckets(ctx, lakeID, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list buckets: %w", err)
	}

	usageByBucket, err := s.Ceph.ListLakeBucketUsage(ctx, lakeID)
	if err != nil {
		return nil, fmt.Errorf("list lake bucket usage: %w", err)
	}
	for i := range buckets {
		if usage, ok := usageByBucket[buckets[i].BucketName]; ok {
			buckets[i].UsedBytes = usage.UsedBytes
			buckets[i].ObjectCount = usage.ObjectCount
		}
	}
	return buckets, nil
}

func (s *Provisioner) GetFleetUsageSummary(ctx context.Context) (summary domain.FleetUsageSummary, err error) {
	ctx, span := startServiceSpan(ctx, "service.get_fleet_usage_summary")
	defer func() {
		observability.RecordSpanError(span, err)
		span.End()
	}()

	if s.Ceph == nil {
		return domain.FleetUsageSummary{}, fmt.Errorf("ceph adapter not configured")
	}

	lakeCount, err := s.Repo.CountActiveLakes(ctx)
	if err != nil {
		return domain.FleetUsageSummary{}, fmt.Errorf("count active lakes: %w", err)
	}
	bucketCount, err := s.Repo.CountActiveBuckets(ctx)
	if err != nil {
		return domain.FleetUsageSummary{}, fmt.Errorf("count active buckets: %w", err)
	}
	totalCommittedBytes, err := s.Repo.SumCommittedQuotaBytes(ctx)
	if err != nil {
		return domain.FleetUsageSummary{}, fmt.Errorf("sum committed quota: %w", err)
	}
	lakes, err := s.Repo.ListActiveLakes(ctx)
	if err != nil {
		return domain.FleetUsageSummary{}, fmt.Errorf("list active lakes: %w", err)
	}

	summary = domain.FleetUsageSummary{
		LakeCount:           lakeCount,
		BucketCount:         bucketCount,
		TotalCommittedBytes: totalCommittedBytes,
	}
	for _, lake := range lakes {
		if lake.RGWUser == "" {
			continue
		}
		usage, err := s.Ceph.GetLakeUsage(ctx, lake.LakeID)
		if err != nil {
			return domain.FleetUsageSummary{}, fmt.Errorf("get lake usage for %s: %w", lake.LakeID, err)
		}
		summary.TotalUsedBytes += usage.UsedBytes
		summary.TotalObjectCount += usage.ObjectCount
	}
	return summary, nil
}

func (s *Provisioner) GetTenantUsageSummary(ctx context.Context, tenantID string) (summary domain.FleetUsageSummary, err error) {
	ctx, span := startServiceSpan(ctx, "service.get_tenant_usage_summary", operationSpanAttributes("", "", tenantID, "", "")...)
	defer func() {
		observability.RecordSpanError(span, err)
		span.End()
	}()

	if s.Ceph == nil {
		return domain.FleetUsageSummary{}, fmt.Errorf("ceph adapter not configured")
	}

	lakeCount, err := s.Repo.CountActiveLakesByTenant(ctx, tenantID)
	if err != nil {
		return domain.FleetUsageSummary{}, fmt.Errorf("count active lakes by tenant: %w", err)
	}
	bucketCount, err := s.Repo.CountActiveBucketsByTenant(ctx, tenantID)
	if err != nil {
		return domain.FleetUsageSummary{}, fmt.Errorf("count active buckets by tenant: %w", err)
	}
	totalCommittedBytes, err := s.Repo.SumCommittedQuotaBytesByTenant(ctx, tenantID)
	if err != nil {
		return domain.FleetUsageSummary{}, fmt.Errorf("sum committed quota by tenant: %w", err)
	}
	lakes, err := s.Repo.ListActiveLakesByTenant(ctx, tenantID)
	if err != nil {
		return domain.FleetUsageSummary{}, fmt.Errorf("list active lakes by tenant: %w", err)
	}

	summary = domain.FleetUsageSummary{
		LakeCount:           lakeCount,
		BucketCount:         bucketCount,
		TotalCommittedBytes: totalCommittedBytes,
	}
	for _, lake := range lakes {
		if lake.RGWUser == "" {
			continue
		}
		usage, err := s.Ceph.GetLakeUsage(ctx, lake.LakeID)
		if err != nil {
			return domain.FleetUsageSummary{}, fmt.Errorf("get lake usage for %s: %w", lake.LakeID, err)
		}
		summary.TotalUsedBytes += usage.UsedBytes
		summary.TotalObjectCount += usage.ObjectCount
	}
	return summary, nil
}
