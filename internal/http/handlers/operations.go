package handlers

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/movincloud/datalake-provisioner/internal/service"
)

type OperationsHandler struct {
	Provisioner *service.Provisioner
}

func (h *OperationsHandler) GetOperation(w http.ResponseWriter, r *http.Request) {
	opID := chi.URLParam(r, "operationId")
	tenant := tenantFromContext(r.Context())

	op, err := h.Provisioner.GetOperation(r.Context(), opID, tenant)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "operation not found"})
		return
	}
	writeJSON(w, http.StatusOK, op)
}

func (h *OperationsHandler) ListLakeOperations(w http.ResponseWriter, r *http.Request) {
	lakeID := chi.URLParam(r, "lakeId")
	tenant := tenantFromContext(r.Context())

	ops, err := h.Provisioner.ListLakeOperations(r.Context(), lakeID, tenant)
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ops)
}
