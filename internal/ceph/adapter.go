package ceph

import "context"

type ProvisionInput struct {
	LakeID   string
	TenantID string
	UserID   string
	SizeGiB  int64
}

type ProvisionOutput struct {
	RGWUser string
}

type Adapter interface {
	Provision(ctx context.Context, in ProvisionInput) (ProvisionOutput, error)
	Resize(ctx context.Context, lakeID string, sizeGiB int64) error
	Deprovision(ctx context.Context, lakeID string) error
}
