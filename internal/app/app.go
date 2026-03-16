package app

import (
	"context"
	"fmt"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/movincloud/datalake-provisioner/internal/ceph"
	"github.com/movincloud/datalake-provisioner/internal/config"
	httpapi "github.com/movincloud/datalake-provisioner/internal/http"
	"github.com/movincloud/datalake-provisioner/internal/http/handlers"
	"github.com/movincloud/datalake-provisioner/internal/service"
	"github.com/movincloud/datalake-provisioner/internal/store/postgres"
)

type App struct {
	Config config.Config
	Router http.Handler
	DB     *pgxpool.Pool
}

func New(ctx context.Context) (*App, error) {
	cfg := config.Load()

	db, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}

	cephAdapter, err := ceph.NewRGWAdminAPIAdapter(
		cfg.RGWEndpoint,
		cfg.RGWAdminPath,
		cfg.RGWRegion,
		cfg.RGWAccessKeyID,
		cfg.RGWSecretAccessKey,
		cfg.RGWInsecureSkipVerify,
	)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("init ceph adapter: %w", err)
	}

	repo := postgres.NewRepository(db)

	prov := &service.Provisioner{
		Repo: repo,
		Ceph: cephAdapter,
	}

	lakesHandler := &handlers.LakesHandler{Provisioner: prov}
	opsHandler := &handlers.OperationsHandler{Provisioner: prov}

	router := httpapi.NewRouter(httpapi.Deps{
		InternalToken: cfg.InternalToken,
		LakesHandler:  lakesHandler,
		OpsHandler:    opsHandler,
	})

	return &App{Config: cfg, Router: router, DB: db}, nil
}

func (a *App) Close() {
	if a.DB != nil {
		a.DB.Close()
	}
}
