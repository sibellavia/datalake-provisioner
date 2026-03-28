package handlers

import (
	"context"
	"encoding/json"
	"net/http"
)

type contextKey string

const tenantContextKey contextKey = "tenant"

func TenantMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenant := r.Header.Get("X-Tenant")
		if tenant == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing X-Tenant header"})
			return
		}
		ctx := context.WithValue(r.Context(), tenantContextKey, tenant)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func tenantFromContext(ctx context.Context) string {
	v, _ := ctx.Value(tenantContextKey).(string)
	return v
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
