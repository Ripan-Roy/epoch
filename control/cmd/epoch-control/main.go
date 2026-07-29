// Command epoch-control runs the managed control-plane API. Customer data and
// catalog authority remain owned by regional Rust data nodes.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	controlauth "epoch.local/epoch/control/internal/auth"
	"epoch.local/epoch/control/internal/regional"
	"epoch.local/epoch/control/internal/resources"
	epochv1 "epoch.local/epoch/sdk/go/gen/epoch/v1"
	"google.golang.org/grpc"
)

const (
	defaultHTTPAddress       = ":8080"
	defaultGRPCAddress       = ":8081"
	defaultRegionalEndpoints = "http://127.0.0.1:7601"
	defaultAllowedOrigins    = "http://127.0.0.1:5173,http://localhost:5173,http://127.0.0.1:4173,http://localhost:4173"
	defaultStatePath         = "data/control/registry.db"
	defaultReconcileInterval = time.Second
	shutdownTimeout          = 10 * time.Second
)

type controlConfig struct {
	httpAddress       string
	grpcAddress       string
	regionalEndpoints []string
	allowedOrigins    []string
	statePath         string
	authPolicyPath    string
	regionalToken     secret
	reconcileInterval time.Duration
}

// secret prevents accidental credential disclosure through config formatting.
type secret string

func (secret) String() string {
	return "[redacted]"
}

func (secret) GoString() string {
	return "[redacted]"
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	rootContext, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()
	if err := run(rootContext, logger); err != nil {
		logger.Error("epoch control plane stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger) (runError error) {
	config, err := loadConfig()
	if err != nil {
		return err
	}
	policy, err := controlauth.LoadPolicy(config.authPolicyPath)
	if err != nil {
		return fmt.Errorf("load bootstrap auth policy: %w", err)
	}
	audit := controlauth.NewSlogAuditSink(logger)
	authority, err := regional.NewAuthenticatedHTTPAuthority(
		config.regionalEndpoints,
		nil,
		string(config.regionalToken),
	)
	if err != nil {
		return err
	}
	registry, err := resources.OpenDurableRegistry(config.statePath)
	if err != nil {
		return fmt.Errorf("open durable control metadata: %w", err)
	}
	defer func() {
		runError = errors.Join(runError, registry.Close())
	}()
	reconciler := regional.NewReconciler(registry, authority)
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(
		controlauth.NewUnaryServerInterceptor(policy, audit),
	))
	epochv1.RegisterRegionalAdminServiceServer(
		grpcServer,
		regional.NewAuthenticatedRegionalAdminServer(
			registry,
			reconciler,
			policy,
			audit,
		),
	)
	httpHandler, err := resources.NewAuthenticatedHTTPHandler(
		registry,
		config.allowedOrigins,
		policy,
		audit,
	)
	if err != nil {
		return fmt.Errorf("configure control HTTP: %w", err)
	}
	httpServer := &http.Server{
		Addr:              config.httpAddress,
		Handler:           httpHandler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	httpListener, err := net.Listen("tcp", config.httpAddress)
	if err != nil {
		return fmt.Errorf("listen for control HTTP: %w", err)
	}
	defer httpListener.Close()
	grpcListener, err := net.Listen("tcp", config.grpcAddress)
	if err != nil {
		return fmt.Errorf("listen for RegionalAdmin gRPC: %w", err)
	}
	defer grpcListener.Close()

	runContext, cancel := context.WithCancel(ctx)
	defer cancel()
	serverErrors := make(chan error, 3)
	go func() {
		serverErrors <- normalizeHTTPError(httpServer.Serve(httpListener))
	}()
	go func() {
		serverErrors <- grpcServer.Serve(grpcListener)
	}()
	go func() {
		serverErrors <- reconciler.Run(runContext, config.reconcileInterval)
	}()
	logger.Info(
		"epoch control plane listening",
		"http_address",
		config.httpAddress,
		"grpc_address",
		config.grpcAddress,
		"regional_endpoints",
		config.regionalEndpoints,
		"allowed_browser_origins",
		config.allowedOrigins,
		"registry",
		registry.Mode(),
		"auth_policy_id",
		policy.ID(),
		"data_path_owner",
		"rust",
	)

	var servingError error
	select {
	case <-ctx.Done():
		logger.Info("epoch control plane shutting down")
	case servingError = <-serverErrors:
		if servingError != nil {
			logger.Error("control-plane component stopped", "error", servingError)
		}
	}
	cancel()
	shutdownContext, shutdownCancel := context.WithTimeout(
		context.Background(),
		shutdownTimeout,
	)
	defer shutdownCancel()
	httpError := httpServer.Shutdown(shutdownContext)
	grpcError := stopGRPC(shutdownContext, grpcServer)
	return errors.Join(servingError, httpError, grpcError)
}

func normalizeHTTPError(err error) error {
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func stopGRPC(ctx context.Context, server *grpc.Server) error {
	stopped := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
		return nil
	case <-ctx.Done():
		server.Stop()
		<-stopped
		return fmt.Errorf("gRPC graceful shutdown timed out: %w", ctx.Err())
	}
}

func loadConfig() (controlConfig, error) {
	config := controlConfig{
		httpAddress: envOrDefault("EPOCH_CONTROL_ADDR", defaultHTTPAddress),
		grpcAddress: envOrDefault("EPOCH_CONTROL_GRPC_ADDR", defaultGRPCAddress),
		regionalEndpoints: splitEndpoints(
			envOrDefault("EPOCH_CONTROL_REGIONAL_ENDPOINTS", defaultRegionalEndpoints),
		),
		allowedOrigins: splitEndpoints(
			envOrDefault("EPOCH_CONTROL_ALLOWED_ORIGINS", defaultAllowedOrigins),
		),
		statePath:         envOrDefault("EPOCH_CONTROL_STATE_PATH", defaultStatePath),
		authPolicyPath:    strings.TrimSpace(os.Getenv("EPOCH_AUTH_POLICY_PATH")),
		regionalToken:     secret(os.Getenv("EPOCH_CONTROL_REGIONAL_TOKEN")),
		reconcileInterval: defaultReconcileInterval,
	}
	if len(config.regionalEndpoints) == 0 {
		return controlConfig{}, fmt.Errorf(
			"EPOCH_CONTROL_REGIONAL_ENDPOINTS must contain at least one endpoint",
		)
	}
	if config.authPolicyPath == "" {
		return controlConfig{}, fmt.Errorf("EPOCH_AUTH_POLICY_PATH is required")
	}
	if strings.TrimSpace(string(config.regionalToken)) == "" {
		return controlConfig{}, fmt.Errorf("EPOCH_CONTROL_REGIONAL_TOKEN is required")
	}
	if raw := strings.TrimSpace(os.Getenv("EPOCH_CONTROL_RECONCILE_INTERVAL")); raw != "" {
		interval, err := time.ParseDuration(raw)
		if err != nil || interval <= 0 {
			return controlConfig{}, fmt.Errorf(
				"EPOCH_CONTROL_RECONCILE_INTERVAL must be a positive duration",
			)
		}
		config.reconcileInterval = interval
	}
	return config, nil
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func splitEndpoints(raw string) []string {
	var endpoints []string
	for endpoint := range strings.SplitSeq(raw, ",") {
		if endpoint = strings.TrimSpace(endpoint); endpoint != "" {
			endpoints = append(endpoints, endpoint)
		}
	}
	return endpoints
}
