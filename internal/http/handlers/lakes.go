package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/movincloud/datalake-provisioner/internal/domain"
	"github.com/movincloud/datalake-provisioner/internal/service"
)

type LakesHandler struct {
	Provisioner *service.Provisioner
}

type provisionRequest struct {
	UserID  string `json:"userId"`
	SizeGiB int64  `json:"sizeGiB"`
}

type resizeRequest struct {
	SizeGiB int64 `json:"sizeGiB"`
}

type createBucketRequest struct {
	Name string `json:"name"`
}

var bucketNamePattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])?$`)

func (h *LakesHandler) Provision(w http.ResponseWriter, r *http.Request) {
	var req provisionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	if req.UserID == "" || req.SizeGiB <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "userId and sizeGiB are required"})
		return
	}

	op, err := h.Provisioner.StartProvision(r.Context(), service.ProvisionRequest{
		TenantID:       tenantFromContext(r.Context()),
		UserID:         req.UserID,
		SizeGiB:        req.SizeGiB,
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
	})
	if err != nil {
		handleServiceError(w, err)
		return
	}

	writeJSON(w, http.StatusAccepted, op)
}

func (h *LakesHandler) Resize(w http.ResponseWriter, r *http.Request) {
	lakeID := chi.URLParam(r, "lakeId")
	var req resizeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	if req.SizeGiB <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "sizeGiB must be > 0"})
		return
	}

	op, err := h.Provisioner.StartResize(r.Context(), service.ResizeRequest{
		TenantID:       tenantFromContext(r.Context()),
		LakeID:         lakeID,
		SizeGiB:        req.SizeGiB,
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
	})
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, op)
}

func (h *LakesHandler) Deprovision(w http.ResponseWriter, r *http.Request) {
	lakeID := chi.URLParam(r, "lakeId")
	op, err := h.Provisioner.StartDeprovision(r.Context(), service.DeprovisionRequest{
		TenantID:       tenantFromContext(r.Context()),
		LakeID:         lakeID,
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
	})
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, op)
}

func (h *LakesHandler) CreateBucket(w http.ResponseWriter, r *http.Request) {
	lakeID := chi.URLParam(r, "lakeId")
	var req createBucketRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}

	req.Name = strings.TrimSpace(req.Name)
	if !bucketNamePattern.MatchString(req.Name) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name must be 3-63 chars of lowercase letters, numbers, or hyphens, and must start/end with a letter or number"})
		return
	}

	op, err := h.Provisioner.StartCreateBucket(r.Context(), service.CreateBucketRequest{
		TenantID:       tenantFromContext(r.Context()),
		LakeID:         lakeID,
		Name:           req.Name,
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
	})
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, op)
}

func (h *LakesHandler) ListBuckets(w http.ResponseWriter, r *http.Request) {
	lakeID := chi.URLParam(r, "lakeId")
	tenant := tenantFromContext(r.Context())

	buckets, err := h.Provisioner.ListBuckets(r.Context(), lakeID, tenant)
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, buckets)
}

func (h *LakesHandler) GetBucket(w http.ResponseWriter, r *http.Request) {
	lakeID := chi.URLParam(r, "lakeId")
	bucketID := chi.URLParam(r, "bucketId")
	tenant := tenantFromContext(r.Context())

	bucket, err := h.Provisioner.GetBucket(r.Context(), bucketID, lakeID, tenant)
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, bucket)
}

func (h *LakesHandler) DeleteBucket(w http.ResponseWriter, r *http.Request) {
	lakeID := chi.URLParam(r, "lakeId")
	bucketID := chi.URLParam(r, "bucketId")

	op, err := h.Provisioner.StartDeleteBucket(r.Context(), service.DeleteBucketRequest{
		TenantID:       tenantFromContext(r.Context()),
		LakeID:         lakeID,
		BucketID:       bucketID,
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
	})
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, op)
}

func isConflictError(err error) bool {
	return errors.Is(err, domain.ErrIdempotencyMismatch) || errors.Is(err, domain.ErrConflict) || errors.Is(err, domain.ErrInvalidState)
}

func handleServiceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
	case isConflictError(err):
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
	default:
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
}

func (h *LakesHandler) GetLake(w http.ResponseWriter, r *http.Request) {
	lakeID := chi.URLParam(r, "lakeId")
	tenant := tenantFromContext(r.Context())

	lake, err := h.Provisioner.GetLake(r.Context(), lakeID, tenant)
	if err != nil {
		handleServiceError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, lake)
}
