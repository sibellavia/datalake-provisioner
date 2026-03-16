package service

import (
	"context"
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
	MarkOperationRunning(ctx context.Context, operationID, tenantID string) error
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

	op := domain.Operation{
		OperationID:   uuid.NewString(),
		OperationType: "provision",
		LakeID:        lakeID,
		TenantID:      req.TenantID,
		Status:        domain.OperationPending,
		StartedAt:     now,
	}
	if err := s.Repo.CreateOperation(ctx, op); err != nil {
		return domain.Operation{}, fmt.Errorf("create operation: %w", err)
	}

	go s.runProvision(context.Background(), op.OperationID, req, lakeID)
	return op, nil
}

func (s *Provisioner) StartResize(ctx context.Context, req ResizeRequest) (domain.Operation, error) {
	if _, err := s.Repo.GetLake(ctx, req.LakeID, req.TenantID); err != nil {
		return domain.Operation{}, fmt.Errorf("get lake: %w", err)
	}

	now := time.Now().UTC()
	op := domain.Operation{
		OperationID:   uuid.NewString(),
		OperationType: "resize",
		LakeID:        req.LakeID,
		TenantID:      req.TenantID,
		Status:        domain.OperationPending,
		StartedAt:     now,
	}
	if err := s.Repo.CreateOperation(ctx, op); err != nil {
		return domain.Operation{}, fmt.Errorf("create operation: %w", err)
	}

	go s.runResize(context.Background(), op.OperationID, req)
	return op, nil
}

func (s *Provisioner) StartDeprovision(ctx context.Context, req DeprovisionRequest) (domain.Operation, error) {
	if _, err := s.Repo.GetLake(ctx, req.LakeID, req.TenantID); err != nil {
		return domain.Operation{}, fmt.Errorf("get lake: %w", err)
	}

	now := time.Now().UTC()
	op := domain.Operation{
		OperationID:   uuid.NewString(),
		OperationType: "deprovision",
		LakeID:        req.LakeID,
		TenantID:      req.TenantID,
		Status:        domain.OperationPending,
		StartedAt:     now,
	}
	if err := s.Repo.CreateOperation(ctx, op); err != nil {
		return domain.Operation{}, fmt.Errorf("create operation: %w", err)
	}

	go s.runDeprovision(context.Background(), op.OperationID, req)
	return op, nil
}

func (s *Provisioner) runProvision(ctx context.Context, operationID string, req ProvisionRequest, lakeID string) {
	if err := s.Repo.MarkOperationRunning(ctx, operationID, req.TenantID); err != nil {
		log.Printf("failed to mark operation running op=%s: %v", operationID, err)
	}

	if s.Ceph == nil {
		s.failProvision(ctx, operationID, req.TenantID, lakeID, "ceph adapter not configured")
		return
	}

	cephOut, err := s.Ceph.Provision(ctx, ceph.ProvisionInput{
		LakeID:   lakeID,
		TenantID: req.TenantID,
		UserID:   req.UserID,
		SizeGiB:  req.SizeGiB,
	})
	if err != nil {
		s.failProvision(ctx, operationID, req.TenantID, lakeID, fmt.Sprintf("ceph provision failed: %v", err))
		return
	}

	if err := s.Repo.MarkLakeProvisioned(ctx, lakeID, req.TenantID, cephOut.RGWUser, cephOut.BucketName, ""); err != nil {
		s.failProvision(ctx, operationID, req.TenantID, lakeID, fmt.Sprintf("mark lake provisioned failed: %v", err))
		return
	}

	if err := s.Repo.MarkOperationSuccess(ctx, operationID, req.TenantID); err != nil {
		log.Printf("failed to mark operation success op=%s: %v", operationID, err)
		return
	}

	log.Printf("provision completed op=%s lake=%s tenant=%s bucket=%s", operationID, lakeID, req.TenantID, cephOut.BucketName)
}

func (s *Provisioner) runResize(ctx context.Context, operationID string, req ResizeRequest) {
	if err := s.Repo.MarkOperationRunning(ctx, operationID, req.TenantID); err != nil {
		log.Printf("failed to mark operation running op=%s: %v", operationID, err)
	}
	if err := s.Repo.MarkLakeResizing(ctx, req.LakeID, req.TenantID); err != nil {
		log.Printf("failed to mark lake resizing lake=%s: %v", req.LakeID, err)
	}

	if err := s.Ceph.Resize(ctx, req.LakeID, req.SizeGiB); err != nil {
		s.failProvision(ctx, operationID, req.TenantID, req.LakeID, fmt.Sprintf("ceph resize failed: %v", err))
		return
	}

	if err := s.Repo.MarkLakeResized(ctx, req.LakeID, req.TenantID, req.SizeGiB); err != nil {
		s.failProvision(ctx, operationID, req.TenantID, req.LakeID, fmt.Sprintf("mark lake resized failed: %v", err))
		return
	}

	if err := s.Repo.MarkOperationSuccess(ctx, operationID, req.TenantID); err != nil {
		log.Printf("failed to mark operation success op=%s: %v", operationID, err)
	}
}

func (s *Provisioner) runDeprovision(ctx context.Context, operationID string, req DeprovisionRequest) {
	if err := s.Repo.MarkOperationRunning(ctx, operationID, req.TenantID); err != nil {
		log.Printf("failed to mark operation running op=%s: %v", operationID, err)
	}
	if err := s.Repo.MarkLakeDeleting(ctx, req.LakeID, req.TenantID); err != nil {
		log.Printf("failed to mark lake deleting lake=%s: %v", req.LakeID, err)
	}

	if err := s.Ceph.Deprovision(ctx, req.LakeID); err != nil {
		s.failProvision(ctx, operationID, req.TenantID, req.LakeID, fmt.Sprintf("ceph deprovision failed: %v", err))
		return
	}

	if err := s.Repo.MarkLakeDeleted(ctx, req.LakeID, req.TenantID); err != nil {
		s.failProvision(ctx, operationID, req.TenantID, req.LakeID, fmt.Sprintf("mark lake deleted failed: %v", err))
		return
	}

	if err := s.Repo.MarkOperationSuccess(ctx, operationID, req.TenantID); err != nil {
		log.Printf("failed to mark operation success op=%s: %v", operationID, err)
	}
}

func (s *Provisioner) failProvision(ctx context.Context, operationID, tenantID, lakeID, errorMessage string) {
	log.Printf("operation failed op=%s lake=%s tenant=%s: %s", operationID, lakeID, tenantID, errorMessage)
	if err := s.Repo.MarkLakeFailed(ctx, lakeID, tenantID, errorMessage); err != nil {
		log.Printf("failed to mark lake failed lake=%s: %v", lakeID, err)
	}
	if err := s.Repo.MarkOperationFailed(ctx, operationID, tenantID, errorMessage); err != nil {
		log.Printf("failed to mark operation failed op=%s: %v", operationID, err)
	}
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
