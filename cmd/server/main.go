package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"

	"github.com/movincloud/datalake-provisioner/internal/app"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	application, err := app.New(ctx)
	if err != nil {
		log.Fatalf("bootstrap error: %v", err)
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

	log.Printf("datalake-provisioner listening on :%s", application.Config.HTTPPort)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("http server error: %v", err)
	}
}
