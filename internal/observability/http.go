package observability

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func RequestLoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		startedAt := time.Now()

		next.ServeHTTP(ww, r)

		routePattern := r.URL.Path
		if rctx := chi.RouteContext(r.Context()); rctx != nil {
			if pattern := rctx.RoutePattern(); pattern != "" {
				routePattern = pattern
			}
		}

		attrs := []any{
			"component", "http",
			"http.method", r.Method,
			"http.route", routePattern,
			"http.status_code", ww.Status(),
			"http.response.bytes", ww.BytesWritten(),
			"duration_ms", durationMs(time.Since(startedAt)),
		}
		if requestID := middleware.GetReqID(r.Context()); requestID != "" {
			attrs = append(attrs, "request.id", requestID)
		}
		if tenantID := r.Header.Get("X-Tenant"); tenantID != "" {
			attrs = append(attrs, "tenant.id", tenantID)
		}
		if lakeID := chi.URLParam(r, "lakeId"); lakeID != "" {
			attrs = append(attrs, "lake.id", lakeID)
		}
		if bucketID := chi.URLParam(r, "bucketId"); bucketID != "" {
			attrs = append(attrs, "bucket.id", bucketID)
		}
		if operationID := chi.URLParam(r, "operationId"); operationID != "" {
			attrs = append(attrs, "operation.id", operationID)
		}
		if remoteIP := r.RemoteAddr; remoteIP != "" {
			attrs = append(attrs, "client.address", remoteIP)
		}

		message := "http request completed"
		if ww.Status() >= http.StatusInternalServerError {
			slog.ErrorContext(r.Context(), message, attrs...)
			return
		}
		if ww.Status() >= http.StatusBadRequest {
			slog.WarnContext(r.Context(), message, attrs...)
			return
		}
		slog.InfoContext(r.Context(), message, attrs...)
	})
}

func durationMs(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}
