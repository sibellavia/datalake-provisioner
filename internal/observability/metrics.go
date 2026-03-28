package observability

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	httpRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "datalake_provisioner",
		Name:      "http_requests_total",
		Help:      "Total number of HTTP requests handled by the service.",
	}, []string{"method", "route", "status_code"})

	httpRequestDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "datalake_provisioner",
		Name:      "http_request_duration_seconds",
		Help:      "HTTP request duration in seconds.",
		Buckets:   []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	}, []string{"method", "route", "status_code"})

	readinessChecksTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "datalake_provisioner",
		Name:      "readiness_checks_total",
		Help:      "Total number of readiness checks executed by dependency and result.",
	}, []string{"check", "result"})

	readinessCheckDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "datalake_provisioner",
		Name:      "readiness_check_duration_seconds",
		Help:      "Readiness check duration in seconds by dependency.",
		Buckets:   []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
	}, []string{"check", "result"})

	readinessCheckUp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "datalake_provisioner",
		Name:      "readiness_check_up",
		Help:      "Last observed readiness state for a dependency check (1=up, 0=down).",
	}, []string{"check"})

	readinessOverallUp = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "datalake_provisioner",
		Name:      "readiness_overall_up",
		Help:      "Last observed overall readiness state (1=ready, 0=not ready).",
	})

	operationStartRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "datalake_provisioner",
		Name:      "operation_start_requests_total",
		Help:      "Total number of operation start requests by operation type and result.",
	}, []string{"operation_type", "result"})

	operationTerminalTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "datalake_provisioner",
		Name:      "operation_terminal_total",
		Help:      "Total number of operations reaching a terminal state by operation type and result.",
	}, []string{"operation_type", "result"})

	operationDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "datalake_provisioner",
		Name:      "operation_duration_seconds",
		Help:      "End-to-end operation duration in seconds until terminal state.",
		Buckets:   []float64{0.01, 0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600},
	}, []string{"operation_type", "result"})

	workerLeader = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "datalake_provisioner",
		Name:      "worker_leader",
		Help:      "Whether this instance currently holds worker leadership (1=yes, 0=no).",
	})

	workerClaimsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "datalake_provisioner",
		Name:      "worker_claims_total",
		Help:      "Total number of worker operation claims by operation type.",
	}, []string{"operation_type"})

	workerExecutionTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "datalake_provisioner",
		Name:      "worker_execution_total",
		Help:      "Total number of worker execution attempts by operation type and result.",
	}, []string{"operation_type", "result"})

	workerExecutionDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "datalake_provisioner",
		Name:      "worker_execution_duration_seconds",
		Help:      "Worker execution attempt duration in seconds.",
		Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120},
	}, []string{"operation_type", "result"})

	workerRequeuesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "datalake_provisioner",
		Name:      "worker_requeues_total",
		Help:      "Total number of worker requeues by operation type.",
	}, []string{"operation_type"})

	workerStaleResetsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "datalake_provisioner",
		Name:      "worker_stale_resets_total",
		Help:      "Total number of stale running operations reset back to pending.",
	})

	cephRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "datalake_provisioner",
		Name:      "ceph_requests_total",
		Help:      "Total number of Ceph adapter calls by method and result.",
	}, []string{"method", "result"})

	cephRequestDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "datalake_provisioner",
		Name:      "ceph_request_duration_seconds",
		Help:      "Ceph adapter call duration in seconds by method and result.",
		Buckets:   []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
	}, []string{"method", "result"})
)

func MetricsHandler() http.Handler {
	return promhttp.Handler()
}

func ObserveHTTPRequest(method, route string, statusCode int, duration time.Duration) {
	status := strconv.Itoa(statusCode)
	httpRequestsTotal.WithLabelValues(method, route, status).Inc()
	httpRequestDurationSeconds.WithLabelValues(method, route, status).Observe(duration.Seconds())
}

func ObserveReadinessCheck(check string, duration time.Duration, err error) {
	result := resultFromError(err)
	readinessChecksTotal.WithLabelValues(check, result).Inc()
	readinessCheckDurationSeconds.WithLabelValues(check, result).Observe(duration.Seconds())
	readinessCheckUp.WithLabelValues(check).Set(boolToFloat64(err == nil))
}

func SetReadinessOverall(ready bool) {
	readinessOverallUp.Set(boolToFloat64(ready))
}

func ObserveOperationStartRequest(operationType, result string) {
	operationStartRequestsTotal.WithLabelValues(operationType, result).Inc()
}

func ObserveOperationTerminal(operationType, result string, startedAt time.Time) {
	operationTerminalTotal.WithLabelValues(operationType, result).Inc()
	if !startedAt.IsZero() {
		operationDurationSeconds.WithLabelValues(operationType, result).Observe(time.Since(startedAt).Seconds())
	}
}

func SetWorkerLeader(isLeader bool) {
	workerLeader.Set(boolToFloat64(isLeader))
}

func ObserveWorkerClaim(operationType string) {
	workerClaimsTotal.WithLabelValues(operationType).Inc()
}

func ObserveWorkerExecution(operationType, result string, duration time.Duration) {
	workerExecutionTotal.WithLabelValues(operationType, result).Inc()
	workerExecutionDurationSeconds.WithLabelValues(operationType, result).Observe(duration.Seconds())
}

func ObserveWorkerRequeue(operationType string) {
	workerRequeuesTotal.WithLabelValues(operationType).Inc()
}

func AddWorkerStaleResets(count int64) {
	if count <= 0 {
		return
	}
	workerStaleResetsTotal.Add(float64(count))
}

func ObserveCephRequest(method string, duration time.Duration, err error) {
	result := resultFromError(err)
	cephRequestsTotal.WithLabelValues(method, result).Inc()
	cephRequestDurationSeconds.WithLabelValues(method, result).Observe(duration.Seconds())
}

func resultFromError(err error) string {
	if err != nil {
		return "error"
	}
	return "success"
}

func boolToFloat64(v bool) float64 {
	if v {
		return 1
	}
	return 0
}
