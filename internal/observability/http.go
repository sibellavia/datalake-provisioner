package observability

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TracingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)

		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := Tracer("http").Start(ctx, r.Method+" "+r.URL.Path,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(attribute.String("http.method", r.Method)),
		)
		defer span.End()

		r = r.WithContext(ctx)
		next.ServeHTTP(ww, r)

		routePattern := routePatternFromRequest(r)
		statusCode := responseStatusCode(ww.Status())

		attrs := []attribute.KeyValue{
			attribute.String("http.method", r.Method),
			attribute.String("http.route", routePattern),
			attribute.String("url.path", r.URL.Path),
			attribute.Int("http.status_code", statusCode),
			attribute.Int64("http.response.bytes", int64(ww.BytesWritten())),
		}
		if requestID := middleware.GetReqID(r.Context()); requestID != "" {
			attrs = append(attrs, attribute.String("request.id", requestID))
		}
		if tenantID := r.Header.Get("X-Tenant"); tenantID != "" {
			attrs = append(attrs, attribute.String("tenant.id", tenantID))
		}
		if lakeID := chi.URLParam(r, "lakeId"); lakeID != "" {
			attrs = append(attrs, attribute.String("lake.id", lakeID))
		}
		if bucketID := chi.URLParam(r, "bucketId"); bucketID != "" {
			attrs = append(attrs, attribute.String("bucket.id", bucketID))
		}
		if operationID := chi.URLParam(r, "operationId"); operationID != "" {
			attrs = append(attrs, attribute.String("operation.id", operationID))
		}
		if remoteIP := r.RemoteAddr; remoteIP != "" {
			attrs = append(attrs, attribute.String("client.address", remoteIP))
		}

		span.SetName(r.Method + " " + routePattern)
		span.SetAttributes(attrs...)
		if statusCode >= http.StatusInternalServerError {
			span.SetStatus(codes.Error, http.StatusText(statusCode))
		}
	})
}

func RequestLoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		startedAt := time.Now()

		next.ServeHTTP(ww, r)

		routePattern := routePatternFromRequest(r)
		statusCode := responseStatusCode(ww.Status())
		requestDuration := time.Since(startedAt)
		ObserveHTTPRequest(r.Method, routePattern, statusCode, requestDuration)

		attrs := []any{
			"component", "http",
			"http.method", r.Method,
			"http.route", routePattern,
			"http.status_code", statusCode,
			"http.response.bytes", ww.BytesWritten(),
			"duration_ms", durationMs(requestDuration),
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
		if statusCode >= http.StatusInternalServerError {
			slog.ErrorContext(r.Context(), message, attrs...)
			return
		}
		if statusCode >= http.StatusBadRequest {
			slog.WarnContext(r.Context(), message, attrs...)
			return
		}
		slog.InfoContext(r.Context(), message, attrs...)
	})
}

func routePatternFromRequest(r *http.Request) string {
	routePattern := r.URL.Path
	if rctx := chi.RouteContext(r.Context()); rctx != nil {
		if pattern := rctx.RoutePattern(); pattern != "" {
			routePattern = pattern
		}
	}
	return routePattern
}

func responseStatusCode(statusCode int) int {
	if statusCode == 0 {
		return http.StatusOK
	}
	return statusCode
}

func durationMs(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}
