package handlers

import (
	"context"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/movincloud/datalake-provisioner/internal/ceph"
)

type ReadyHandler struct {
	DB      *pgxpool.Pool
	Ceph    ceph.Adapter
	Timeout time.Duration
}

type readinessCheckResult struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

func (h *ReadyHandler) GetReady(w http.ResponseWriter, r *http.Request) {
	timeout := h.Timeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}

	checks := map[string]readinessCheckResult{}
	overallReady := true

	if h.DB == nil {
		overallReady = false
		checks["db"] = readinessCheckResult{Status: "error", Error: "database pool not configured"}
	} else {
		dbCtx, dbCancel := context.WithTimeout(r.Context(), timeout)
		dbErr := h.DB.Ping(dbCtx)
		dbCancel()
		if dbErr != nil {
			overallReady = false
			checks["db"] = readinessCheckResult{Status: "error", Error: dbErr.Error()}
		} else {
			checks["db"] = readinessCheckResult{Status: "ok"}
		}
	}

	if h.Ceph == nil {
		overallReady = false
		checks["rgw"] = readinessCheckResult{Status: "error", Error: "ceph adapter not configured"}
	} else {
		rgwCtx, rgwCancel := context.WithTimeout(r.Context(), timeout)
		rgwErr := h.Ceph.CheckReady(rgwCtx)
		rgwCancel()
		if rgwErr != nil {
			overallReady = false
			checks["rgw"] = readinessCheckResult{Status: "error", Error: rgwErr.Error()}
		} else {
			checks["rgw"] = readinessCheckResult{Status: "ok"}
		}
	}

	status := http.StatusOK
	bodyStatus := "ready"
	if !overallReady {
		status = http.StatusServiceUnavailable
		bodyStatus = "not_ready"
	}

	writeJSON(w, status, map[string]any{
		"status": bodyStatus,
		"checks": checks,
	})
}
