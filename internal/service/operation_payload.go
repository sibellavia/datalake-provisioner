package service

import "encoding/json"

type operationPayload struct {
	Type     string `json:"type"`
	TenantID string `json:"tenantId"`
	LakeID   string `json:"lakeId,omitempty"`
	UserID   string `json:"userId,omitempty"`
	SizeGiB  int64  `json:"sizeGiB,omitempty"`
}

func marshalOperationPayload(payload operationPayload) (json.RawMessage, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return data, nil
}
