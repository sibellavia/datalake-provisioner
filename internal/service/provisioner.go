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
	MarkLakeProvisioned(ctx context.Context, lakeID, tenantID, rgwUser, bucketName, url string) error
	MarkLakeResizing(ctx context.Context, lakeID, tenantID string) error
	MarkLakeResized(ctx context.Context, lakeID, tenantID string, sizeGiB int64) error
	MarkLakeDeleting(ctx context.Context, lakeID, tenantID string) error
	MarkLakeDeleted(ctx context.Context, lakeID, tenantID string) error
	MarkLakeFailed(ctx context.Context, lakeID, tenantID, errorMessage string) error

	CreateOperation(ctx context.Context, op domain.Operation) error
	GetOperation(ctx context.Context, operationID, tenantID string) (domain.Operation, error)
	ClaimNextRunnableOperation(ctx context.Context) (domain.Operation, bool, error)
	RequeueOperation(ctx context.Context, operationID, tenantID, errorMessage string, nextAttemptAt time.Time) error
	ResetStaleRunningOperations(ctx context.Context, staleBefore time.Time) (int64, error)
	MarkOperationSuccess(ctx context.Context, operationID, tenantID string) error
	MarkOperationFailed(ctx context.Context, operationID, tenantID, errorMessage string) error
}

type Provisioner struct {
	Repo Repository
	Ceph ceph.Adapter
}

type ProvisionRequest struct {
	TenantID string
	UserID   string
	SizeGiB  int64
}

type ResizeRequest struct {
	TenantID string
	LakeID   string
	SizeGiB  int64
}

type DeprovisionRequest struct {
	TenantID string
	LakeID   string
}

func (s *Provisioner) StartProvision(ctx context.Context, req ProvisionRequest) (domain.Operation, error) {
	now := time.Now().UTC()
	lakeID := uuid.NewString()

	lake := domain.Lake{
		LakeID:           lakeID,
		TenantID:         req.TenantID,
		UserID:           req.UserID,
		RequestedSizeGiB: req.SizeGiB,
		Status:           domain.LakeStatusProvisioning,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if err := s.Repo.CreateLake(ctx, lake); err != nil {
		return domain.Operation{}, fmt.Errorf("create lake: %w", err)
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
	if err := s.Repo.CreateOperation(ctx, op); err != nil {
		return domain.Operation{}, fmt.Errorf("create operation: %w", err)
	}

	return op, nil
}

func (s *Provisioner) StartResize(ctx context.Context, req ResizeRequest) (domain.Operation, error) {
	if _, err := s.Repo.GetLake(ctx, req.LakeID, req.TenantID); err != nil {
		return domain.Operation{}, fmt.Errorf("get lake: %w", err)
	}

	now := time.Now().UTC()
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
	if err := s.Repo.CreateOperation(ctx, op); err != nil {
		return domain.Operation{}, fmt.Errorf("create operation: %w", err)
	}

	return op, nil
}

func (s *Provisioner) StartDeprovision(ctx context.Context, req DeprovisionRequest) (domain.Operation, error) {
	if _, err := s.Repo.GetLake(ctx, req.LakeID, req.TenantID); err != nil {
		return domain.Operation{}, fmt.Errorf("get lake: %w", err)
	}

	now := time.Now().UTC()
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
	if err := s.Repo.CreateOperation(ctx, op); err != nil {
		return domain.Operation{}, fmt.Errorf("create operation: %w", err)
	}

	return op, nil
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
	log.Printf("operation failed op=%s lake=%s tenant=%s: %s", op.OperationID, op.LakeID, op.TenantID, errorMessage)

	var joinErr error
	if op.LakeID != "" {
		if repoErr := s.Repo.MarkLakeFailed(ctx, op.LakeID, op.TenantID, errorMessage); repoErr != nil {
			joinErr = errors.Join(joinErr, fmt.Errorf("mark lake failed: %w", repoErr))
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

	switch op.OperationType {
	case "provision":
		return s.executeProvision(ctx, op, payload)
	case "resize":
		return s.executeResize(ctx, op, payload)
	case "deprovision":
		return s.executeDeprovision(ctx, op, payload)
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

	cephOut, err := s.Ceph.Provision(ctx, ceph.ProvisionInput{
		LakeID:   payload.LakeID,
		TenantID: payload.TenantID,
		UserID:   payload.UserID,
		SizeGiB:  payload.SizeGiB,
	})
	if err != nil {
		return fmt.Errorf("ceph provision failed: %w", err)
	}

	if err := s.Repo.MarkLakeProvisioned(ctx, payload.LakeID, payload.TenantID, cephOut.RGWUser, cephOut.BucketName, ""); err != nil {
		return fmt.Errorf("mark lake provisioned failed: %w", err)
	}

	if err := s.Repo.MarkOperationSuccess(ctx, op.OperationID, op.TenantID); err != nil {
		return fmt.Errorf("mark operation success: %w", err)
	}

	log.Printf("provision completed op=%s lake=%s tenant=%s bucket=%s", op.OperationID, payload.LakeID, payload.TenantID, cephOut.BucketName)
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
	if err := s.Ceph.Resize(ctx, payload.LakeID, payload.SizeGiB); err != nil {
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
	if err := s.Ceph.Deprovision(ctx, payload.LakeID); err != nil {
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
