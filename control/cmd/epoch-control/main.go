// Command epoch-control runs the initial managed control-plane API. Customer
// data remains owned by regional Rust data nodes; this process stores only
// provisional in-memory desired resource metadata.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"epoch.local/epoch/control/internal/resources"
)

const (
	defaultAddress  = ":8080"
	shutdownTimeout = 10 * time.Second
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	address := os.Getenv("EPOCH_CONTROL_ADDR")
	if address == "" {
		address = defaultAddress
	}

	registry := resources.NewRegistry()
	server := &http.Server{
		Addr:              address,
		Handler:           resources.NewHTTPHandler(registry),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	rootContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serverErrors := make(chan error, 1)
	go func() {
		logger.Info("epoch control plane listening",
			"address", address,
			"registry", "in_memory",
			"data_path_owner", "rust",
		)
		serverErrors <- server.ListenAndServe()
	}()

	select {
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("epoch control plane stopped", "error", err)
			os.Exit(1)
		}
	case <-rootContext.Done():
		logger.Info("epoch control plane shutting down")
		shutdownContext, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownContext); err != nil {
			logger.Error("graceful shutdown failed", "error", err)
			if closeErr := server.Close(); closeErr != nil {
				logger.Error("forced shutdown failed", "error", closeErr)
			}
			os.Exit(1)
		}
	}
}
