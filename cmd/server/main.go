package main

import (
	"context"
	"log/slog"
	"net/http"
	"os/signal"
	"syscall"

	"github.com/movincloud/datalake-provisioner/internal/app"
	"github.com/movincloud/datalake-provisioner/internal/config"
	"github.com/movincloud/datalake-provisioner/internal/observability"
)

func main() {
	cfg := config.Load()
	if err := observability.SetupLogger(cfg.LogFormat, cfg.LogLevel); err != nil {
		panic(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	application, err := app.New(ctx)
	if err != nil {
		slog.Error("bootstrap error", "component", "main", "error.message", err.Error())
		panic(err)
	}
	defer application.Close()

	server := &http.Server{
		Addr:              ":" + application.Config.HTTPPort,
		Handler:           application.Router,
		ReadHeaderTimeout: application.Config.ReadHeaderTimeout,
	}

	go func() {
		<-ctx.Done()
		_ = server.Shutdown(context.Background())
	}()

	slog.Info("server listening", "component", "main", "http.addr", server.Addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("http server error", "component", "main", "error.message", err.Error())
		panic(err)
	}
}
