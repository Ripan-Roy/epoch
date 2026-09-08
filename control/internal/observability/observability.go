// Package observability owns bounded control-plane metrics and trace export.
package observability

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/felixge/httpsnoop"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const (
	requestIDHeader = "X-Request-ID"
	maxTenantLimit  = 4096
)

var (
	servicePattern  = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)
	requestSequence atomic.Uint64
)

// Config is the fail-closed process observability contract.
type Config struct {
	Service      string
	MaxTenants   int
	OTLPEndpoint string
}

// Runtime owns metrics, HTTP instrumentation, and the optional trace provider.
type Runtime struct {
	service       string
	logger        *slog.Logger
	registry      *prometheus.Registry
	requests      *prometheus.CounterVec
	durations     *prometheus.HistogramVec
	reconciles    *prometheus.CounterVec
	reconcileTime *prometheus.HistogramVec
	tenantCount   prometheus.Gauge
	overflow      prometheus.Counter
	maxTenants    int
	tenantMu      sync.Mutex
	tenants       map[string]string
	traceProvider *sdktrace.TracerProvider
}

// New validates cardinality and exporter configuration before allocating collectors.
func New(ctx context.Context, config Config, logger *slog.Logger) (*Runtime, error) {
	if !servicePattern.MatchString(config.Service) {
		return nil, errors.New("observability service must be a bounded static label")
	}
	if config.MaxTenants < 1 || config.MaxTenants > maxTenantLimit {
		return nil, fmt.Errorf("observability max tenants must be between 1 and %d", maxTenantLimit)
	}
	if logger == nil {
		return nil, errors.New("observability logger is required")
	}
	endpoint, err := validateEndpoint(config.OTLPEndpoint)
	if err != nil {
		return nil, err
	}

	registry := prometheus.NewRegistry()
	runtime := &Runtime{
		service:    config.Service,
		logger:     logger,
		registry:   registry,
		maxTenants: config.MaxTenants,
		tenants:    make(map[string]string, config.MaxTenants),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "epoch_control_http_requests_total",
			Help: "Completed Epoch control-plane HTTP requests.",
		}, []string{"service", "tenant", "profile", "operation", "outcome"}),
		durations: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "epoch_control_http_request_duration_seconds",
			Help:    "End-to-end Epoch control-plane HTTP request duration.",
			Buckets: []float64{0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		}, []string{"service", "tenant", "profile", "operation", "outcome"}),
		reconciles: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "epoch_control_reconciliations_total",
			Help: "Completed Epoch desired-state reconciliation attempts.",
		}, []string{"service", "outcome"}),
		reconcileTime: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "epoch_control_reconciliation_duration_seconds",
			Help:    "Epoch desired-state reconciliation duration.",
			Buckets: prometheus.DefBuckets,
		}, []string{"service", "outcome"}),
		tenantCount: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "epoch_control_observability_tenants",
			Help:        "Number of exact tenant fingerprints admitted to control metrics.",
			ConstLabels: prometheus.Labels{"service": config.Service},
		}),
		overflow: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "epoch_control_observability_overflow_total",
			Help:        "Tenant observations assigned to the bounded overflow series.",
			ConstLabels: prometheus.Labels{"service": config.Service},
		}),
	}
	registry.MustRegister(
		runtime.requests,
		runtime.durations,
		runtime.reconciles,
		runtime.reconcileTime,
		runtime.tenantCount,
		runtime.overflow,
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
	)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	if endpoint != "" {
		exporter, exporterErr := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(traceExportEndpoint(endpoint)))
		if exporterErr != nil {
			return nil, fmt.Errorf("configure OTLP trace exporter: %w", exporterErr)
		}
		resource := sdkresource.NewSchemaless(attribute.String("service.name", config.Service))
		runtime.traceProvider = sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exporter),
			sdktrace.WithResource(resource),
		)
		otel.SetTracerProvider(runtime.traceProvider)
	}
	return runtime, nil
}

// HTTPHandler adds W3C extraction, correlation, metrics, and structured completion logs.
func (runtime *Runtime) HTTPHandler(next http.Handler) http.Handler {
	instrumented := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestID := validOrGeneratedRequestID(request.Header.Get(requestIDHeader))
		request.Header.Set(requestIDHeader, requestID)
		writer.Header().Set(requestIDHeader, requestID)
		dimensions := classify(request)
		started := time.Now()
		captured := httpsnoop.CaptureMetrics(next, writer, request)
		outcome := outcomeLabel(captured.Code)
		tenant := runtime.admitTenant(dimensions.tenant)
		labels := []string{runtime.service, tenant, dimensions.profile, dimensions.operation, outcome}
		runtime.requests.WithLabelValues(labels...).Inc()
		runtime.durations.WithLabelValues(labels...).Observe(captured.Duration.Seconds())
		runtime.logger.InfoContext(
			request.Context(),
			"Epoch control HTTP request completed",
			"event_id", requestID,
			"request_id", requestID,
			"trace_id", TraceID(request.Context()),
			"profile", dimensions.profile,
			"operation", dimensions.operation,
			"tenant", tenant,
			"status", captured.Code,
			"duration_ms", time.Since(started).Milliseconds(),
		)
	})
	return otelhttp.NewHandler(
		instrumented,
		"epoch.control.http",
		otelhttp.WithSpanNameFormatter(func(_ string, request *http.Request) string {
			dimensions := classify(request)
			return request.Method + " " + dimensions.operation
		}),
	)
}

// HTTPTransport injects W3C trace context into regional data-plane requests.
func (runtime *Runtime) HTTPTransport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return otelhttp.NewTransport(base)
}

// MetricsHandler exposes only this process's explicitly registered collectors.
func (runtime *Runtime) MetricsHandler() http.Handler {
	return promhttp.HandlerFor(runtime.registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}

// ObserveReconcile records one reconciliation without resource-name labels.
func (runtime *Runtime) ObserveReconcile(elapsed time.Duration, reconcileErr error) {
	outcome := "success"
	if reconcileErr != nil {
		outcome = "error"
	}
	runtime.reconciles.WithLabelValues(runtime.service, outcome).Inc()
	runtime.reconcileTime.WithLabelValues(runtime.service, outcome).Observe(elapsed.Seconds())
}

// Shutdown drains the optional batch trace exporter.
func (runtime *Runtime) Shutdown(ctx context.Context) error {
	if runtime == nil || runtime.traceProvider == nil {
		return nil
	}
	return runtime.traceProvider.Shutdown(ctx)
}

// TraceID returns the current W3C trace identifier, or an empty string when absent.
func TraceID(ctx context.Context) string {
	spanContext := trace.SpanContextFromContext(ctx)
	if !spanContext.IsValid() {
		return ""
	}
	return spanContext.TraceID().String()
}

type dimensions struct {
	tenant    string
	profile   string
	operation string
}

func classify(request *http.Request) dimensions {
	parts := strings.Split(strings.Trim(request.URL.Path, "/"), "/")
	if len(parts) == 1 && parts[0] == "healthz" {
		return dimensions{profile: "runtime", operation: "health"}
	}
	if len(parts) >= 2 && parts[0] == "v1" && parts[1] == "resources" {
		if len(parts) >= 8 {
			return dimensions{
				tenant:    strings.Join(parts[2:6], "\x1f"),
				profile:   normalizeProfile(parts[6]),
				operation: resourceOperation(request.Method),
			}
		}
		operation := "apply"
		if request.Method == http.MethodGet {
			operation = "list"
		}
		return dimensions{profile: "control", operation: operation}
	}
	if len(parts) == 3 && strings.Join(parts, "/") == "v1/regional/resources" {
		return dimensions{profile: "control", operation: "inventory"}
	}
	return dimensions{profile: "runtime", operation: "other"}
}

func resourceOperation(method string) string {
	switch method {
	case http.MethodGet:
		return "read"
	case http.MethodDelete:
		return "delete"
	case http.MethodPut, http.MethodPost, http.MethodPatch:
		return "apply"
	default:
		return "other"
	}
}

func normalizeProfile(value string) string {
	switch value {
	case "cache", "caches":
		return "cache"
	case "stream", "streams":
		return "stream"
	case "queue", "queues":
		return "queue"
	case "event-bus", "bus", "buses":
		return "bus"
	case "connector", "connectors":
		return "connector"
	default:
		return "control"
	}
}

func outcomeLabel(status int) string {
	switch {
	case status >= 200 && status < 400:
		return "success"
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return "auth"
	case status == http.StatusTooManyRequests:
		return "quota"
	case status >= 400 && status < 500:
		return "client_error"
	default:
		return "server_error"
	}
}

func (runtime *Runtime) admitTenant(canonical string) string {
	if canonical == "" {
		return "none"
	}
	runtime.tenantMu.Lock()
	defer runtime.tenantMu.Unlock()
	if admitted, exists := runtime.tenants[canonical]; exists {
		return admitted
	}
	if len(runtime.tenants) >= runtime.maxTenants {
		runtime.overflow.Inc()
		return "overflow"
	}
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(canonical))
	label := fmt.Sprintf("%016x", hash.Sum64())
	runtime.tenants[canonical] = label
	runtime.tenantCount.Set(float64(len(runtime.tenants)))
	return label
}

func validOrGeneratedRequestID(candidate string) string {
	if candidate != "" && len(candidate) <= 128 {
		valid := true
		for _, character := range []byte(candidate) {
			if character < 0x21 || character > 0x7e {
				valid = false
				break
			}
		}
		if valid {
			return candidate
		}
	}
	return fmt.Sprintf("request-%x-%x", time.Now().UnixNano(), requestSequence.Add(1))
}

func validateEndpoint(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	endpoint, err := url.Parse(value)
	if err != nil || endpoint.Host == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") {
		return "", errors.New("OTLP endpoint must be an absolute http(s) URL")
	}
	if endpoint.User != nil || endpoint.EscapedPath() != "" && endpoint.EscapedPath() != "/" || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return "", errors.New("OTLP endpoint must not contain credentials, path, query, or fragment")
	}
	return strings.TrimRight(endpoint.String(), "/"), nil
}

func traceExportEndpoint(base string) string {
	return strings.TrimRight(base, "/") + "/v1/traces"
}
