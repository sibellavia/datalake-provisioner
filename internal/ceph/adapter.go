package ceph

import "context"

type LakeAccess struct {
	RGWUser string
}

type LakeUsage struct {
	UsedBytes   int64
	ObjectCount int64
}

type BucketUsage struct {
	UsedBytes   int64
	ObjectCount int64
}

type Adapter interface {
	CheckReady(ctx context.Context) error
	EnsureLake(ctx context.Context, lakeID string) (LakeAccess, error)
	SetLakeQuota(ctx context.Context, lakeID string, sizeGiB int64) error
	DeleteLake(ctx context.Context, lakeID string) error

	CreateBucket(ctx context.Context, lakeID, bucketName string) error
	DeleteBucketIfEmpty(ctx context.Context, lakeID, bucketName string) error

	GetLakeUsage(ctx context.Context, lakeID string) (LakeUsage, error)
	GetBucketUsage(ctx context.Context, bucketName string) (BucketUsage, error)
	ListLakeBucketUsage(ctx context.Context, lakeID string) (map[string]BucketUsage, error)
}
