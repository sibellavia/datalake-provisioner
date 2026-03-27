package service

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

type provisionIdempotencyInput struct {
	Type     string `json:"type"`
	TenantID string `json:"tenantId"`
	UserID   string `json:"userId"`
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

func hashProvisionRequest(req ProvisionRequest) (string, error) {
	return hashRequest(provisionIdempotencyInput{
		Type:     "provision",
		TenantID: req.TenantID,
		UserID:   req.UserID,
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

func hashRequest(v any) (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:]), nil
}
