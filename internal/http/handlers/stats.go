package handlers

import (
	"net/http"

	"github.com/movincloud/datalake-provisioner/internal/service"
)

type StatsHandler struct {
	Provisioner *service.Provisioner
}

func (h *StatsHandler) GetSummary(w http.ResponseWriter, r *http.Request) {
	summary, err := h.Provisioner.GetFleetUsageSummary(r.Context())
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, summary)
}
