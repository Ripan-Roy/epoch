package resources

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
			"governance":{"owner":"team:platform","cost_center":"cc-1042","classification":"internal","tags":{"service":"events"}},
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

func TestAuthenticatedAuditExportIsIntegrityVerifiedPagedAndTenantScoped(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.ndjson")
	journal, err := controlauth.OpenDurableAuditJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = journal.Close() })
	handler, err := NewAuthenticatedHTTPHandler(
		NewRegistry(),
		nil,
		loadHTTPAuthPolicy(t),
		journal,
	)
	if err != nil {
		t.Fatal(err)
	}
	admin := bearerHeaders("epoch-dev-admin-v1")
	first := performRequest(t, handler, http.MethodGet, "/v1/audit/events?limit=1", nil, admin)
	if first.Code != http.StatusOK {
		t.Fatalf("first page = %d %s", first.Code, first.Body.String())
	}
	var page controlauth.AuditPage
	decodeResponse(t, first, &page)
	if len(page.Records) != 1 || page.NextSequence != "1" || !page.EndOfJournal {
		t.Fatalf("first audit page = %#v", page)
	}
	if page.Records[0].Event.Action != controlauth.ActionAuditRead ||
		page.Records[0].Event.AuthenticationMethod != controlauth.AuthenticationBootstrapToken {
		t.Fatalf("audit export event = %#v", page.Records[0].Event)
	}
	second := performRequest(
		t,
		handler,
		http.MethodGet,
		"/v1/audit/events?after_sequence=1&limit=10",
		nil,
		admin,
	)
	if second.Code != http.StatusOK {
		t.Fatalf("second page = %d %s", second.Code, second.Body.String())
	}
	decodeResponse(t, second, &page)
	if len(page.Records) != 1 || page.NextSequence != "2" || !page.EndOfJournal {
		t.Fatalf("second audit page = %#v", page)
	}
	for _, target := range []string{
		"/v1/audit/events?after_sequence=01",
		"/v1/audit/events?after_sequence=",
		"/v1/audit/events?limit=0",
		"/v1/audit/events?limit=",
		"/v1/audit/events?limit=1&limit=2",
		"/v1/audit/events?unknown=1",
	} {
		response := performRequest(t, handler, http.MethodGet, target, nil, admin)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("non-canonical audit query %q = %d %s", target, response.Code, response.Body.String())
		}
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"epoch-dev-admin-v1", "dae2068ce258"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("audit journal leaked %q", secret)
		}
	}
}

func TestAuthenticatedHTTPBoundaryAcceptsShortLivedOIDCIdentityAndAuditsItsMethod(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.ndjson")
	journal, err := controlauth.OpenDurableAuditJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = journal.Close() })
	policy, err := controlauth.LoadPolicy(
		filepath.Join("..", "..", "..", "spec", "auth", "identity-policy-v2.example.json"),
	)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewAuthenticatedHTTPHandler(NewRegistry(), nil, policy, journal)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	token := signedHTTPBoundaryOIDCToken(t, map[string]any{
		"iss":                "https://identity.epoch.example",
		"aud":                "epoch-api",
		"sub":                "workload-orders",
		"iat":                now.Add(-10 * time.Second).Unix(),
		"nbf":                now.Add(-10 * time.Second).Unix(),
		"exp":                now.Add(5 * time.Minute).Unix(),
		"jti":                "control-boundary-active-1",
		"epoch_roles":        []string{"reader", "auditor"},
		"epoch_organization": "acme",
		"epoch_project":      "payments",
		"epoch_environment":  "production",
		"epoch_namespace":    "orders",
	})
	headers := bearerHeaders(token)
	listed := performRequest(t, handler, http.MethodGet, "/v1/resources", nil, headers)
	if listed.Code != http.StatusOK {
		t.Fatalf("OIDC list = %d %s", listed.Code, listed.Body.String())
	}
	exported := performRequest(t, handler, http.MethodGet, "/v1/audit/events?limit=10", nil, headers)
	if exported.Code != http.StatusOK {
		t.Fatalf("OIDC audit export = %d %s", exported.Code, exported.Body.String())
	}
	var page controlauth.AuditPage
	decodeResponse(t, exported, &page)
	if len(page.Records) != 2 {
		t.Fatalf("OIDC audit page = %#v", page)
	}
	for _, record := range page.Records {
		if record.Event.AuthenticationMethod != controlauth.AuthenticationOIDCEdDSA ||
			!strings.HasPrefix(record.Event.PrincipalID, "oidc:") {
			t.Fatalf("OIDC audit record = %#v", record)
		}
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), token) {
		t.Fatal("OIDC audit journal leaked the bearer token")
	}
}

func TestAuthenticatedHTTPBoundaryFailsClosedAfterLiveAuditTampering(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.ndjson")
	journal, err := controlauth.OpenDurableAuditJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = journal.Close() })
	handler, err := NewAuthenticatedHTTPHandler(
		NewRegistry(), nil, loadHTTPAuthPolicy(t), journal,
	)
	if err != nil {
		t.Fatal(err)
	}
	admin := bearerHeaders("epoch-dev-admin-v1")
	first := performRequest(t, handler, http.MethodGet, "/v1/resources", nil, admin)
	if first.Code != http.StatusOK {
		t.Fatalf("first request = %d %s", first.Code, first.Body.String())
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	encoded = []byte(strings.Replace(string(encoded), "development-admin", "xevelopment-admin", 1))
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	detected := performRequest(t, handler, http.MethodGet, "/v1/audit/events", nil, admin)
	assertAuthFailure(t, detected, http.StatusServiceUnavailable, "audit_unavailable")
	refused := performRequest(t, handler, http.MethodGet, "/v1/resources", nil, admin)
	assertAuthFailure(t, refused, http.StatusServiceUnavailable, "audit_unavailable")
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
				Spec:       json.RawMessage(`{"shard_count":1,"replica_count":3}`),
				Governance: testGovernance(),
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
		inventory.Resources[0].Organization != "acme" ||
		len(inventory.CostAttribution) != 1 ||
		inventory.CostAttribution[0].ResourceCount != 1 ||
		inventory.CostAttribution[0].ShardCount != 1 {
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

func signedHTTPBoundaryOIDCToken(t *testing.T, claims map[string]any) string {
	t.Helper()
	seed, err := base64.RawURLEncoding.DecodeString("nWGxne_9WmC6hEr0kuwsxERJxWl7MmkZcDusAxyuf2A")
	if err != nil {
		t.Fatal(err)
	}
	header, err := json.Marshal(map[string]any{
		"alg": "EdDSA", "kid": "epoch-test-ed25519-1", "typ": "JWT",
	})
	if err != nil {
		t.Fatal(err)
	}
	encodedClaims, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	signingInput := base64.RawURLEncoding.EncodeToString(header) + "." +
		base64.RawURLEncoding.EncodeToString(encodedClaims)
	signature := ed25519.Sign(ed25519.NewKeyFromSeed(seed), []byte(signingInput))
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(signature)
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
