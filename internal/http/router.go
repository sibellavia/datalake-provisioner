package http

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/movincloud/datalake-provisioner/internal/http/handlers"
)

type Deps struct {
	InternalToken string
	LakesHandler  *handlers.LakesHandler
	OpsHandler    *handlers.OperationsHandler
	StatsHandler  *handlers.StatsHandler
}

func NewRouter(d Deps) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Logger)

	r.Get("/health", handlers.Health)

	r.Route("/v1", func(v1 chi.Router) {
		v1.Use(handlers.InternalTokenMiddleware(d.InternalToken))
		v1.Use(handlers.TenantMiddleware)

		v1.Get("/stats/summary", d.StatsHandler.GetSummary)

		v1.Post("/lakes", d.LakesHandler.Provision)
		v1.Post("/lakes/{lakeId}/resize", d.LakesHandler.Resize)
		v1.Delete("/lakes/{lakeId}", d.LakesHandler.Deprovision)
		v1.Get("/lakes/{lakeId}", d.LakesHandler.GetLake)
		v1.Post("/lakes/{lakeId}/buckets", d.LakesHandler.CreateBucket)
		v1.Get("/lakes/{lakeId}/buckets", d.LakesHandler.ListBuckets)
		v1.Get("/lakes/{lakeId}/buckets/{bucketId}", d.LakesHandler.GetBucket)
		v1.Delete("/lakes/{lakeId}/buckets/{bucketId}", d.LakesHandler.DeleteBucket)

		v1.Get("/operations/{operationId}", d.OpsHandler.GetOperation)
	})

	return r
}
