package observability

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPHandlerCorrelatesRequestsAndBoundsTenantLabels(t *testing.T) {
	var logs bytes.Buffer
	runtime, err := New(context.Background(), Config{
		Service:    "epoch-control",
		MaxTenants: 1,
	}, slog.New(slog.NewJSONHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Shutdown(context.Background())

	handler := runtime.HTTPHandler(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	}))
	for _, path := range []string{
		"/v1/resources/acme/shop/prod/core/stream/orders-secret",
		"/v1/resources/other/risk/prod/core/queue/jobs-secret",
	} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusNoContent {
			t.Fatalf("status = %d", response.Code)
		}
		if !strings.HasPrefix(response.Header().Get("X-Request-ID"), "request-") {
			t.Fatalf("request ID = %q", response.Header().Get("X-Request-ID"))
		}
	}

	metrics := httptest.NewRecorder()
	runtime.MetricsHandler().ServeHTTP(metrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := metrics.Body.String()
	if !strings.Contains(body, `tenant="overflow"`) {
		t.Fatalf("overflow series missing:\n%s", body)
	}
	for _, secret := range []string{"acme", "shop", "orders-secret", "jobs-secret"} {
		if strings.Contains(body, secret) {
			t.Fatalf("metrics leaked %q:\n%s", secret, body)
		}
	}
	if !strings.Contains(logs.String(), `"request_id":"request-`) {
		t.Fatalf("correlated structured log missing: %s", logs.String())
	}
}

func TestHTTPHandlerPreservesValidW3CParentAndCallerRequestID(t *testing.T) {
	runtime, err := New(context.Background(), Config{
		Service:    "epoch-control",
		MaxTenants: 8,
	}, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Shutdown(context.Background())

	const traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	var observedTraceID string
	handler := runtime.HTTPHandler(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		observedTraceID = TraceID(request.Context())
		writer.WriteHeader(http.StatusOK)
	}))
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	request.Header.Set("traceparent", traceparent)
	request.Header.Set("X-Request-ID", "caller-123")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Header().Get("X-Request-ID") != "caller-123" {
		t.Fatalf("request ID = %q", response.Header().Get("X-Request-ID"))
	}
	if observedTraceID != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("trace ID = %q", observedTraceID)
	}
}

func TestConfigRejectsUnsafeCardinalityAndExporterEndpoints(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	for _, config := range []Config{
		{Service: "customer supplied", MaxTenants: 1},
		{Service: "epoch-control", MaxTenants: 0},
		{Service: "epoch-control", MaxTenants: 1, OTLPEndpoint: "file:///tmp/traces"},
		{Service: "epoch-control", MaxTenants: 1, OTLPEndpoint: "https://user:secret@collector:4318"},
		{Service: "epoch-control", MaxTenants: 1, OTLPEndpoint: "https://collector:4318/custom"},
	} {
		if _, err := New(context.Background(), config, logger); err == nil {
			t.Fatalf("New(%+v) succeeded", config)
		}
	}
}

func TestOTLPBaseResolvesCanonicalTracePath(t *testing.T) {
	t.Parallel()
	base, err := validateEndpoint("http://collector:4318/")
	if err != nil {
		t.Fatal(err)
	}
	if base != "http://collector:4318" || traceExportEndpoint(base) != "http://collector:4318/v1/traces" {
		t.Fatalf("unexpected OTLP resolution: base=%q traces=%q", base, traceExportEndpoint(base))
	}
}
