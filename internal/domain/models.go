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

type BucketStatus string

const (
	BucketStatusCreating BucketStatus = "creating"
	BucketStatusReady    BucketStatus = "ready"
	BucketStatusDeleting BucketStatus = "deleting"
	BucketStatusFailed   BucketStatus = "failed"
	BucketStatusDeleted  BucketStatus = "deleted"
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
	RGWUser          string     `json:"rgwUser,omitempty"`
	BucketCount      int        `json:"bucketCount"`
	UsedBytes        int64      `json:"usedBytes"`
	ObjectCount      int64      `json:"objectCount"`
	LastError        string     `json:"lastError,omitempty"`
	CreatedAt        time.Time  `json:"createdAt"`
	UpdatedAt        time.Time  `json:"updatedAt"`
}

type Bucket struct {
	BucketID    string       `json:"bucketId"`
	LakeID      string       `json:"lakeId"`
	TenantID    string       `json:"tenantId"`
	Name        string       `json:"name"`
	BucketName  string       `json:"bucketName"`
	Status      BucketStatus `json:"status"`
	UsedBytes   int64        `json:"usedBytes"`
	ObjectCount int64        `json:"objectCount"`
	LastError   string       `json:"lastError,omitempty"`
	CreatedAt   time.Time    `json:"createdAt"`
	UpdatedAt   time.Time    `json:"updatedAt"`
}

type Operation struct {
	OperationID    string          `json:"operationId"`
	OperationType  string          `json:"operationType"`
	LakeID         string          `json:"lakeId,omitempty"`
	BucketID       string          `json:"bucketId,omitempty"`
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

type FleetUsageSummary struct {
	LakeCount           int   `json:"lakeCount"`
	BucketCount         int   `json:"bucketCount"`
	TotalUsedBytes      int64 `json:"totalUsedBytes"`
	TotalCommittedBytes int64 `json:"totalCommittedBytes"`
	TotalObjectCount    int64 `json:"totalObjectCount"`
}
