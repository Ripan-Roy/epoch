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
	"epoch.local/epoch/control/internal/regional"
	"epoch.local/epoch/control/internal/resources"
	"epoch.local/epoch/control/internal/securetransport"
	epochv1 "epoch.local/epoch/sdk/go/gen/epoch/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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
	serverTLS         securetransport.ServerOptions
	regionalTLS       securetransport.ClientOptions
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
		Transport: &http.Transport{
			TLSClientConfig:   regionalTLS,
			ForceAttemptHTTP2: regionalTLS != nil,
		},
	}
	authority, err := regional.NewAuthenticatedHTTPAuthority(
		config.regionalEndpoints,
		regionalClient,
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
	grpcOptions := []grpc.ServerOption{grpc.UnaryInterceptor(
		controlauth.NewUnaryServerInterceptor(policy, audit),
	)}
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
		TLSConfig:         cloneTLS(serverTLS),
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
		serverErrors <- serveHTTP(httpServer, httpListener, serverTLS != nil)
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
		"listener_tls",
		serverTLS != nil,
		"regional_mtls",
		regionalTLS != nil && len(regionalTLS.Certificates) == 1,
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
