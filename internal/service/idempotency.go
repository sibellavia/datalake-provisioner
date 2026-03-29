package service

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

type provisionIdempotencyInput struct {
	Type     string `json:"type"`
	TenantID string `json:"tenantId"`
	SizeGiB  int64  `json:"sizeGiB"`
}

type resizeIdempotencyInput struct {
	Type     string `json:"type"`
	TenantID string `json:"tenantId"`
	LakeID   string `json:"lakeId"`
	SizeGiB  int64  `json:"sizeGiB"`
}

type deprovisionIdempotencyInput struct {
	Type     string `json:"type"`
	TenantID string `json:"tenantId"`
	LakeID   string `json:"lakeId"`
}

type createBucketIdempotencyInput struct {
	Type     string `json:"type"`
	TenantID string `json:"tenantId"`
	LakeID   string `json:"lakeId"`
	Name     string `json:"name"`
}

type deleteBucketIdempotencyInput struct {
	Type     string `json:"type"`
	TenantID string `json:"tenantId"`
	LakeID   string `json:"lakeId"`
	BucketID string `json:"bucketId"`
}

func hashProvisionRequest(req ProvisionRequest) (string, error) {
	return hashRequest(provisionIdempotencyInput{
		Type:     "provision",
		TenantID: req.TenantID,
		SizeGiB:  req.SizeGiB,
	})
}

func hashResizeRequest(req ResizeRequest) (string, error) {
	return hashRequest(resizeIdempotencyInput{
		Type:     "resize",
		TenantID: req.TenantID,
		LakeID:   req.LakeID,
		SizeGiB:  req.SizeGiB,
	})
}

func hashDeprovisionRequest(req DeprovisionRequest) (string, error) {
	return hashRequest(deprovisionIdempotencyInput{
		Type:     "deprovision",
		TenantID: req.TenantID,
		LakeID:   req.LakeID,
	})
}

func hashCreateBucketRequest(req CreateBucketRequest) (string, error) {
	return hashRequest(createBucketIdempotencyInput{
		Type:     "bucket_create",
		TenantID: req.TenantID,
		LakeID:   req.LakeID,
		Name:     req.Name,
	})
}

func hashDeleteBucketRequest(req DeleteBucketRequest) (string, error) {
	return hashRequest(deleteBucketIdempotencyInput{
		Type:     "bucket_delete",
		TenantID: req.TenantID,
		LakeID:   req.LakeID,
		BucketID: req.BucketID,
	})
}

func hashRequest(v any) (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:]), nil
}
