// Command epoch-control runs the managed control-plane API. Customer data and
// catalog authority remain owned by regional Rust data nodes.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	controlauth "epoch.local/epoch/control/internal/auth"
	controlobservability "epoch.local/epoch/control/internal/observability"
	"epoch.local/epoch/control/internal/regional"
	"epoch.local/epoch/control/internal/resources"
	"epoch.local/epoch/control/internal/securetransport"
	epochv1 "epoch.local/epoch/sdk/go/gen/epoch/v1"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	defaultHTTPAddress              = ":8080"
	defaultGRPCAddress              = ":8081"
	defaultMetricsAddress           = "127.0.0.1:9090"
	defaultRegionalEndpoints        = "http://127.0.0.1:7601"
	defaultRegionalMetricsEndpoints = "http://127.0.0.1:7602"
	defaultAllowedOrigins           = "http://127.0.0.1:5173,http://localhost:5173,http://127.0.0.1:4173,http://localhost:4173"
	defaultStatePath                = "data/control/registry.db"
	defaultReconcileInterval        = time.Second
	shutdownTimeout                 = 10 * time.Second
)

type controlConfig struct {
	httpAddress              string
	grpcAddress              string
	metricsAddress           string
	regionalEndpoints        []string
	regionalMetricsEndpoints []string
	allowedOrigins           []string
	statePath                string
	authPolicyPath           string
	regionalToken            secret
	reconcileInterval        time.Duration
	maxMetricTenants         int
	otlpEndpoint             string
	serverTLS                securetransport.ServerOptions
	regionalTLS              securetransport.ClientOptions
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
	telemetry, err := controlobservability.New(ctx, controlobservability.Config{
		Service:      "epoch-control",
		MaxTenants:   config.maxMetricTenants,
		OTLPEndpoint: config.otlpEndpoint,
	}, logger)
	if err != nil {
		return fmt.Errorf("configure control observability: %w", err)
	}
	defer func() {
		shutdownContext, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		runError = errors.Join(runError, telemetry.Shutdown(shutdownContext))
	}()
	policy, err := controlauth.LoadPolicy(config.authPolicyPath)
	if err != nil {
		return fmt.Errorf("load bootstrap auth policy: %w", err)
	}
	audit := controlauth.NewSlogAuditSink(logger)
	serverTLS, err := securetransport.LoadServerTLS(config.serverTLS)
	if err != nil {
		return fmt.Errorf("configure control listener TLS: %w", err)
	}
	regionalTLS, err := securetransport.LoadClientTLS(config.regionalTLS)
	if err != nil {
		return fmt.Errorf("configure regional workload TLS: %w", err)
	}
	regionalClient := &http.Client{
		Timeout: 5 * time.Second,
		Transport: telemetry.HTTPTransport(&http.Transport{
			TLSClientConfig:   regionalTLS,
			ForceAttemptHTTP2: regionalTLS != nil,
		}),
	}
	authority, err := regional.NewAuthenticatedHTTPAuthority(
		config.regionalEndpoints,
		regionalClient,
		string(config.regionalToken),
	)
	if err != nil {
		return err
	}
	diagnostics, err := regional.NewHTTPDiagnosticClient(
		config.regionalMetricsEndpoints,
		regionalClient,
	)
	if err != nil {
		return fmt.Errorf("configure regional diagnostics: %w", err)
	}
	registry, err := resources.OpenDurableRegistry(config.statePath)
	if err != nil {
		return fmt.Errorf("open durable control metadata: %w", err)
	}
	defer func() {
		runError = errors.Join(runError, registry.Close())
	}()
	reconciler := regional.NewObservedReconciler(registry, authority, telemetry)
	grpcOptions := []grpc.ServerOption{
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.UnaryInterceptor(controlauth.NewUnaryServerInterceptor(policy, audit)),
	}
	if serverTLS != nil {
		grpcOptions = append(grpcOptions, grpc.Creds(credentials.NewTLS(serverTLS.Clone())))
	}
	grpcServer := grpc.NewServer(grpcOptions...)
	epochv1.RegisterRegionalAdminServiceServer(
		grpcServer,
		regional.NewAuthenticatedRegionalAdminServer(
			registry,
			reconciler,
			policy,
			audit,
		),
	)
	httpHandler, err := resources.NewAuthenticatedHTTPHandlerWithDiagnostics(
		registry,
		config.allowedOrigins,
		policy,
		audit,
		diagnostics,
	)
	if err != nil {
		return fmt.Errorf("configure control HTTP: %w", err)
	}
	httpServer := &http.Server{
		Addr:              config.httpAddress,
		Handler:           telemetry.HTTPHandler(httpHandler),
		TLSConfig:         cloneTLS(serverTLS),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	metricsServer := &http.Server{
		Addr:              config.metricsAddress,
		Handler:           telemetry.MetricsHandler(),
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
	metricsListener, err := net.Listen("tcp", config.metricsAddress)
	if err != nil {
		return fmt.Errorf("listen for control metrics: %w", err)
	}
	defer metricsListener.Close()

	runContext, cancel := context.WithCancel(ctx)
	defer cancel()
	serverErrors := make(chan error, 4)
	go func() {
		serverErrors <- serveHTTP(httpServer, httpListener, serverTLS != nil)
	}()
	go func() {
		serverErrors <- grpcServer.Serve(grpcListener)
	}()
	go func() {
		serverErrors <- serveHTTP(metricsServer, metricsListener, false)
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
		"metrics_address",
		config.metricsAddress,
		"regional_endpoints",
		config.regionalEndpoints,
		"regional_metrics_endpoint_count",
		len(config.regionalMetricsEndpoints),
		"allowed_browser_origins",
		config.allowedOrigins,
		"registry",
		registry.Mode(),
		"auth_policy_id",
		policy.ID(),
		"data_path_owner",
		"rust",
		"listener_tls",
		serverTLS != nil,
		"regional_mtls",
		regionalTLS != nil && len(regionalTLS.Certificates) == 1,
		"otlp_enabled",
		config.otlpEndpoint != "",
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
	metricsError := metricsServer.Shutdown(shutdownContext)
	grpcError := stopGRPC(shutdownContext, grpcServer)
	return errors.Join(servingError, httpError, metricsError, grpcError)
}

func normalizeHTTPError(err error) error {
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func serveHTTP(server *http.Server, listener net.Listener, tlsEnabled bool) error {
	if tlsEnabled {
		return normalizeHTTPError(server.ServeTLS(listener, "", ""))
	}
	return normalizeHTTPError(server.Serve(listener))
}

func cloneTLS(config *tls.Config) *tls.Config {
	if config == nil {
		return nil
	}
	return config.Clone()
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
		metricsAddress: envOrDefault(
			"EPOCH_CONTROL_METRICS_ADDR",
			defaultMetricsAddress,
		),
		regionalEndpoints: splitEndpoints(
			envOrDefault("EPOCH_CONTROL_REGIONAL_ENDPOINTS", defaultRegionalEndpoints),
		),
		regionalMetricsEndpoints: splitEndpoints(envOrDefault(
			"EPOCH_CONTROL_REGIONAL_METRICS_ENDPOINTS",
			defaultRegionalMetricsEndpoints,
		)),
		allowedOrigins: splitEndpoints(
			envOrDefault("EPOCH_CONTROL_ALLOWED_ORIGINS", defaultAllowedOrigins),
		),
		statePath:         envOrDefault("EPOCH_CONTROL_STATE_PATH", defaultStatePath),
		authPolicyPath:    strings.TrimSpace(os.Getenv("EPOCH_AUTH_POLICY_PATH")),
		regionalToken:     secret(os.Getenv("EPOCH_CONTROL_REGIONAL_TOKEN")),
		reconcileInterval: defaultReconcileInterval,
		maxMetricTenants:  1024,
		otlpEndpoint:      strings.TrimSpace(os.Getenv("EPOCH_OTLP_ENDPOINT")),
		serverTLS: securetransport.ServerOptions{
			CertificatePath: strings.TrimSpace(os.Getenv("EPOCH_CONTROL_TLS_CERT_PATH")),
			PrivateKeyPath:  strings.TrimSpace(os.Getenv("EPOCH_CONTROL_TLS_KEY_PATH")),
			ClientCAPath:    strings.TrimSpace(os.Getenv("EPOCH_CONTROL_TLS_CLIENT_CA_PATH")),
		},
		regionalTLS: securetransport.ClientOptions{
			CAPath:          strings.TrimSpace(os.Getenv("EPOCH_CONTROL_REGIONAL_TLS_CA_PATH")),
			CertificatePath: strings.TrimSpace(os.Getenv("EPOCH_CONTROL_REGIONAL_TLS_CERT_PATH")),
			PrivateKeyPath:  strings.TrimSpace(os.Getenv("EPOCH_CONTROL_REGIONAL_TLS_KEY_PATH")),
			ServerName:      strings.TrimSpace(os.Getenv("EPOCH_CONTROL_REGIONAL_TLS_SERVER_NAME")),
		},
	}
	requireTLS, err := optionalBoolEnvironment("EPOCH_CONTROL_TLS_REQUIRED")
	if err != nil {
		return controlConfig{}, err
	}
	config.serverTLS.Required = requireTLS
	config.regionalTLS.Required = requireTLS
	if len(config.regionalEndpoints) == 0 {
		return controlConfig{}, fmt.Errorf(
			"EPOCH_CONTROL_REGIONAL_ENDPOINTS must contain at least one endpoint",
		)
	}
	if len(config.regionalMetricsEndpoints) == 0 {
		return controlConfig{}, fmt.Errorf(
			"EPOCH_CONTROL_REGIONAL_METRICS_ENDPOINTS must contain at least one endpoint",
		)
	}
	if config.authPolicyPath == "" {
		return controlConfig{}, fmt.Errorf("EPOCH_AUTH_POLICY_PATH is required")
	}
	if strings.TrimSpace(string(config.regionalToken)) == "" {
		return controlConfig{}, fmt.Errorf("EPOCH_CONTROL_REGIONAL_TOKEN is required")
	}
	if requireTLS {
		if config.serverTLS.CertificatePath == "" || config.serverTLS.PrivateKeyPath == "" {
			return controlConfig{}, fmt.Errorf("EPOCH_CONTROL_TLS_CERT_PATH and EPOCH_CONTROL_TLS_KEY_PATH are required when TLS is required")
		}
		if config.regionalTLS.CAPath == "" || config.regionalTLS.CertificatePath == "" || config.regionalTLS.PrivateKeyPath == "" {
			return controlConfig{}, fmt.Errorf("regional CA, certificate, and key paths are required when TLS is required")
		}
		for _, endpoint := range config.regionalEndpoints {
			if !strings.HasPrefix(endpoint, "https://") {
				return controlConfig{}, fmt.Errorf("regional endpoints must use https when TLS is required")
			}
		}
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
	if raw := strings.TrimSpace(os.Getenv("EPOCH_CONTROL_OBSERVABILITY_MAX_TENANTS")); raw != "" {
		limit, parseErr := strconv.Atoi(raw)
		if parseErr != nil || limit < 1 || limit > 4096 {
			return controlConfig{}, fmt.Errorf(
				"EPOCH_CONTROL_OBSERVABILITY_MAX_TENANTS must be between 1 and 4096",
			)
		}
		config.maxMetricTenants = limit
	}
	return config, nil
}

func optionalBoolEnvironment(name string) (bool, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return false, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s must be true or false", name)
	}
	return value, nil
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
