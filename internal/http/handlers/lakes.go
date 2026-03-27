package handlers

import (
	"encoding/json"
	"errors"
	"net/http"

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
		if errors.Is(err, domain.ErrIdempotencyMismatch) {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
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
		if errors.Is(err, domain.ErrIdempotencyMismatch) {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
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
		if errors.Is(err, domain.ErrIdempotencyMismatch) {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, op)
}

func (h *LakesHandler) GetLake(w http.ResponseWriter, r *http.Request) {
	lakeID := chi.URLParam(r, "lakeId")
	tenant := tenantFromContext(r.Context())

	lake, err := h.Provisioner.GetLake(r.Context(), lakeID, tenant)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "lake not found"})
		return
	}

	writeJSON(w, http.StatusOK, lake)
}
