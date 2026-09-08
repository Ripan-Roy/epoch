package resources

import (
	"context"
	"net/http"
	"strings"
	"testing"

	controlauth "epoch.local/epoch/control/internal/auth"
)

type diagnosticStub struct {
	requests []LatencyDiagnosticRequest
	err      error
}

func (stub *diagnosticStub) DiagnoseLatency(
	_ context.Context,
	request LatencyDiagnosticRequest,
) (LatencyDiagnosis, error) {
	stub.requests = append(stub.requests, request)
	if stub.err != nil {
		return LatencyDiagnosis{}, stub.err
	}
	return LatencyDiagnosis{
		Cause:            "replication",
		Stage:            "replication",
		ObservedP99MS:    720,
		Samples:          42,
		Recommendation:   "Inspect replica lag.",
		RegionalEndpoint: "node-1",
	}, nil
}

func TestAuthenticatedLatencyDiagnosticReturnsExplicitNoSample(t *testing.T) {
	stub := &diagnosticStub{err: ErrLatencyDiagnosisNotFound}
	handler, err := NewAuthenticatedHTTPHandlerWithDiagnostics(
		NewRegistry(), nil, loadHTTPAuthPolicy(t), controlauth.NewMemoryAuditSink(), stub,
	)
	if err != nil {
		t.Fatal(err)
	}
	path := "/v1/observability/latency?organization=acme&project=payments&environment=production&namespace=orders&profile=stream"
	response := performRequest(t, handler, http.MethodGet, path, nil, bearerHeaders("epoch-dev-reader-v1"))
	if response.Code != http.StatusNotFound || !containsAll(response.Body.String(), `"code":"not_found"`, "no recent successful latency samples") {
		t.Fatalf("no-sample response = %d %s", response.Code, response.Body.String())
	}
}

func TestAuthenticatedLatencyDiagnosticEnforcesTenantScope(t *testing.T) {
	stub := &diagnosticStub{}
	handler, err := NewAuthenticatedHTTPHandlerWithDiagnostics(
		NewRegistry(),
		nil,
		loadHTTPAuthPolicy(t),
		controlauth.NewMemoryAuditSink(),
		stub,
	)
	if err != nil {
		t.Fatal(err)
	}
	path := "/v1/observability/latency?organization=acme&project=payments&environment=production&namespace=orders&profile=stream"

	missing := performRequest(t, handler, http.MethodGet, path, nil, nil)
	assertAuthFailure(t, missing, http.StatusUnauthorized, "unauthenticated")
	authorized := performRequest(t, handler, http.MethodGet, path, nil, bearerHeaders("epoch-dev-reader-v1"))
	if authorized.Code != http.StatusOK || len(stub.requests) != 1 {
		t.Fatalf("authorized response = %d %s, requests = %+v", authorized.Code, authorized.Body.String(), stub.requests)
	}
	if got := authorized.Body.String(); !containsAll(got, `"cause":"replication"`, `"observed_p99_ms":720`) {
		t.Fatalf("diagnosis = %s", got)
	}

	crossTenant := "/v1/observability/latency?organization=other&project=payments&environment=production&namespace=orders&profile=stream"
	denied := performRequest(t, handler, http.MethodGet, crossTenant, nil, bearerHeaders("epoch-dev-reader-v1"))
	assertAuthFailure(t, denied, http.StatusForbidden, "permission_denied")
	if len(stub.requests) != 1 {
		t.Fatalf("denied request reached provider: %+v", stub.requests)
	}
}

func containsAll(value string, fragments ...string) bool {
	for _, fragment := range fragments {
		if !strings.Contains(value, fragment) {
			return false
		}
	}
	return true
}
