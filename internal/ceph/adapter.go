package ceph

import (
	"context"
	"time"
)

type LakeAccess struct {
	RGWUser string
}

type LakeInternalAccess struct {
	RGWUser         string    `json:"rgwUser"`
	S3Endpoint      string    `json:"s3Endpoint"`
	S3Region        string    `json:"s3Region"`
	AccessKeyID     string    `json:"accessKeyId"`
	SecretAccessKey string    `json:"secretAccessKey"`
	LeaseExpiresAt  time.Time `json:"leaseExpiresAt"`
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
	GetLakeInternalAccess(ctx context.Context, lakeID string) (LakeInternalAccess, error)
	SetLakeQuota(ctx context.Context, lakeID string, sizeGiB int64) error
	DeleteLake(ctx context.Context, lakeID string) error

	CreateBucket(ctx context.Context, lakeID, bucketName string) error
	DeleteBucketIfEmpty(ctx context.Context, lakeID, bucketName string) error

	GetLakeUsage(ctx context.Context, lakeID string) (LakeUsage, error)
	GetBucketUsage(ctx context.Context, bucketName string) (BucketUsage, error)
	ListLakeBucketUsage(ctx context.Context, lakeID string) (map[string]BucketUsage, error)
}
