package resources

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	controlauth "epoch.local/epoch/control/internal/auth"
)

func TestAuthenticatedHTTPBoundarySeparatesAuthenticationAndAuthorization(t *testing.T) {
	registry := NewRegistry()
	policy := loadHTTPAuthPolicy(t)
	audit := controlauth.NewMemoryAuditSink()
	handler, err := NewAuthenticatedHTTPHandler(
		registry,
		[]string{"https://console.example.com"},
		policy,
		audit,
	)
	if err != nil {
		t.Fatalf("NewAuthenticatedHTTPHandler() error = %v", err)
	}

	health := performRequest(t, handler, http.MethodGet, "/healthz", nil, nil)
	if health.Code != http.StatusOK {
		t.Fatalf("public health status = %d, body = %s", health.Code, health.Body.String())
	}
	missing := performRequest(t, handler, http.MethodGet, "/v1/resources", nil, nil)
	assertAuthFailure(t, missing, http.StatusUnauthorized, "unauthenticated")
	invalid := performRequest(
		t,
		handler,
		http.MethodGet,
		"/v1/resources",
		nil,
		map[string]string{"Authorization": "Bearer definitely-invalid"},
	)
	assertAuthFailure(t, invalid, http.StatusUnauthorized, "unauthenticated")

	readerHeaders := bearerHeaders("epoch-dev-reader-v1")
	readerHeaders["Origin"] = "https://console.example.com"
	preflight := performRequest(
		t,
		handler,
		http.MethodOptions,
		"/v1/regional/resources",
		nil,
		map[string]string{"Origin": "https://console.example.com"},
	)
	if preflight.Code != http.StatusNoContent ||
		!strings.Contains(preflight.Header().Get("Access-Control-Allow-Headers"), "Authorization") {
		t.Fatalf(
			"preflight = %d, allow headers %q",
			preflight.Code,
			preflight.Header().Get("Access-Control-Allow-Headers"),
		)
	}

	create := []byte(`{
		"request_token":"create-events",
		"expected_generation":0,
		"resource":{
			"organization":"acme",
			"project":"payments",
			"environment":"production",
			"namespace":"orders",
			"kind":"stream",
			"name":"events",
			"spec":{"shard_count":1,"replica_count":3}
		}
	}`)
	forbidden := performRequest(
		t,
		handler,
		http.MethodPut,
		"/v1/resources",
		create,
		readerHeaders,
	)
	assertAuthFailure(t, forbidden, http.StatusForbidden, "permission_denied")

	adminHeaders := bearerHeaders("epoch-dev-admin-v1")
	created := performRequest(
		t,
		handler,
		http.MethodPut,
		"/v1/resources",
		create,
		adminHeaders,
	)
	if created.Code != http.StatusCreated {
		t.Fatalf("admin create status = %d, body = %s", created.Code, created.Body.String())
	}
	listed := performRequest(
		t,
		handler,
		http.MethodGet,
		"/v1/resources",
		nil,
		adminHeaders,
	)
	if listed.Code != http.StatusOK {
		t.Fatalf("admin list status = %d, body = %s", listed.Code, listed.Body.String())
	}

	events := audit.Events()
	if len(events) < 5 {
		t.Fatalf("audit events = %+v", events)
	}
	for _, event := range events {
		if strings.Contains(event.PrincipalID, "epoch-dev-") ||
			strings.Contains(event.RequestID, "epoch-dev-") {
			t.Fatalf("audit event leaked credential material: %+v", event)
		}
	}
}

func TestAuthenticatedRegionalInventoryFiltersUnauthorizedTenants(t *testing.T) {
	registry := NewRegistry()
	for index, organization := range []string{"acme", "otherco"} {
		if _, err := registry.Apply(ApplyRequest{
			RequestToken:       "create-" + organization,
			ExpectedGeneration: uint64Pointer(0),
			Resource: DesiredResource{
				ResourceKey: ResourceKey{
					Organization: organization,
					Project:      "payments",
					Environment:  "production",
					Namespace:    "orders",
					Kind:         KindQueue,
					Name:         "jobs",
				},
				Spec: json.RawMessage(`{"shard_count":1,"replica_count":3}`),
				Labels: map[string]string{
					"index": string(rune('0' + index)),
				},
			},
		}); err != nil {
			t.Fatalf("create %s: %v", organization, err)
		}
	}
	handler, err := NewAuthenticatedHTTPHandler(
		registry,
		nil,
		loadHTTPAuthPolicy(t),
		controlauth.NewMemoryAuditSink(),
	)
	if err != nil {
		t.Fatal(err)
	}
	response := performRequest(
		t,
		handler,
		http.MethodGet,
		"/v1/regional/resources",
		nil,
		bearerHeaders("epoch-dev-reader-v1"),
	)
	if response.Code != http.StatusOK {
		t.Fatalf("inventory status = %d, body = %s", response.Code, response.Body.String())
	}
	var inventory regionalInventoryResponse
	decodeResponse(t, response, &inventory)
	if inventory.Count != 1 ||
		len(inventory.Resources) != 1 ||
		inventory.Resources[0].Organization != "acme" {
		t.Fatalf("inventory leaked unauthorized resources: %+v", inventory)
	}
}

func loadHTTPAuthPolicy(t *testing.T) *controlauth.Policy {
	t.Helper()
	policy, err := controlauth.LoadPolicy(
		filepath.Join("..", "..", "..", "spec", "auth", "bootstrap-policy-v1.example.json"),
	)
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func bearerHeaders(token string) map[string]string {
	return map[string]string{
		"Authorization": "Bearer " + token,
		"X-Request-ID":  "test-request",
	}
}

func assertAuthFailure(t *testing.T, response interface {
	Result() *http.Response
}, status int, code string) {
	t.Helper()
	httpResponse := response.Result()
	defer httpResponse.Body.Close()
	if httpResponse.StatusCode != status {
		t.Fatalf("status = %d, want %d", httpResponse.StatusCode, status)
	}
	var payload struct {
		Code string `json:"code"`
	}
	if err := json.NewDecoder(httpResponse.Body).Decode(&payload); err != nil {
		t.Fatalf("decode auth failure: %v", err)
	}
	if payload.Code != code {
		t.Fatalf("auth failure code = %q, want %q", payload.Code, code)
	}
}
