package domain

import (
	"encoding/json"
	"time"
)

type LakeStatus string

const (
	LakeStatusProvisioning LakeStatus = "provisioning"
	LakeStatusReady        LakeStatus = "ready"
	LakeStatusResizing     LakeStatus = "resizing"
	LakeStatusDeleting     LakeStatus = "deleting"
	LakeStatusFailed       LakeStatus = "failed"
	LakeStatusDeleted      LakeStatus = "deleted"
)

type OperationStatus string

const (
	OperationPending OperationStatus = "pending"
	OperationRunning OperationStatus = "running"
	OperationSuccess OperationStatus = "success"
	OperationFailed  OperationStatus = "failed"
)

type Lake struct {
	LakeID           string     `json:"lakeId"`
	TenantID         string     `json:"tenantId"`
	UserID           string     `json:"userId"`
	RequestedSizeGiB int64      `json:"requestedSizeGiB"`
	Status           LakeStatus `json:"status"`
	URL              string     `json:"url,omitempty"`
	RGWUser          string     `json:"rgwUser,omitempty"`
	BucketName       string     `json:"bucketName,omitempty"`
	LastError        string     `json:"lastError,omitempty"`
	CreatedAt        time.Time  `json:"createdAt"`
	UpdatedAt        time.Time  `json:"updatedAt"`
}

type Operation struct {
	OperationID    string          `json:"operationId"`
	OperationType  string          `json:"operationType"`
	LakeID         string          `json:"lakeId,omitempty"`
	TenantID       string          `json:"tenantId"`
	Status         OperationStatus `json:"status"`
	ErrorMessage   string          `json:"errorMessage,omitempty"`
	StartedAt      time.Time       `json:"startedAt"`
	EndedAt        *time.Time      `json:"endedAt,omitempty"`
	RequestPayload json.RawMessage `json:"-"`
	AttemptCount   int             `json:"-"`
	NextAttemptAt  time.Time       `json:"-"`
	UpdatedAt      time.Time       `json:"-"`
	ErrorCode      string          `json:"-"`
}
