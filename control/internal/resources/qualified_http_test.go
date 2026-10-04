package resources

import (
	"encoding/json"
	"net/http"
	"testing"

	controlauth "epoch.local/epoch/control/internal/auth"
)

func TestHTTPFullyQualifiedResourceLifecycle(t *testing.T) {
	registry := NewRegistry()
	handler := NewHTTPHandler(registry)
	body := []byte(`{"request_token":"qualified-create","expected_generation":0,"resource":{"organization":"acme","project":"shop","environment":"dev","namespace":"core","kind":"queue","name":"jobs","governance":{"owner":"team:platform","cost_center":"cc-1042","classification":"confidential","tags":{}},"spec":{"shard_count":1,"replica_count":3}}}`)
	created := performRequest(t, handler, http.MethodPut, "/v1/resources", body, nil)
	if created.Code != http.StatusCreated {
		t.Fatalf("create: status %d, body %s", created.Code, created.Body.String())
	}
	path := "/v1/resources/acme/shop/dev/core/queue/jobs"
	got := performRequest(t, handler, http.MethodGet, path, nil, nil)
	if got.Code != http.StatusOK {
		t.Fatalf("qualified GET: status %d, body %s", got.Code, got.Body.String())
	}
	var resource Resource
	decodeResponse(t, got, &resource)
	if resource.Organization != "acme" || resource.Project != "shop" || resource.Environment != "dev" || resource.Generation != 1 {
		t.Fatalf("qualified GET resource: %+v", resource)
	}
	deleted := performRequest(t, handler, http.MethodDelete, path, nil, map[string]string{"Idempotency-Key": "qualified-delete", "If-Match": "1"})
	if deleted.Code != http.StatusOK {
		t.Fatalf("qualified DELETE: status %d, body %s", deleted.Code, deleted.Body.String())
	}
	missing := performRequest(t, handler, http.MethodGet, path, nil, nil)
	if missing.Code != http.StatusNotFound {
		t.Fatalf("GET after delete: status %d, body %s", missing.Code, missing.Body.String())
	}
}

func TestHTTPQualifiedPathPreservesTenantAuthorizationAndDeleteCoordination(t *testing.T) {
	registry := NewRegistry()
	key := ResourceKey{Organization: "acme", Project: "shop", Environment: "dev", Namespace: "core", Kind: KindQueue, Name: "jobs"}
	_, err := registry.Apply(ApplyRequest{RequestToken: "qualified-seed", Resource: DesiredResource{ResourceKey: key, Spec: json.RawMessage(`{"shard_count":1}`), Governance: &ResourceGovernance{Owner: "team:platform", CostCenter: "cc-1042", Classification: ClassificationConfidential}}})
	if err != nil {
		t.Fatal(err)
	}
	coordinator := &recordingDeleteCoordinator{result: DeleteResult{Key: key, Generation: 2, Deleted: true}}
	handler, err := NewAuthenticatedHTTPHandlerWithDiagnosticsAndDeleteCoordinator(registry, nil, loadHTTPAuthPolicy(t), controlauth.NewMemoryAuditSink(), &diagnosticStub{}, coordinator)
	if err != nil {
		t.Fatal(err)
	}
	path := "/v1/resources/acme/shop/dev/core/queue/jobs"
	for _, method := range []string{http.MethodGet, http.MethodDelete} {
		denied := performRequest(t, handler, method, path, nil, map[string]string{"Authorization": "Bearer epoch-dev-reader-v1", "Idempotency-Key": "denied-delete"})
		if denied.Code != http.StatusForbidden {
			t.Fatalf("cross-tenant %s: status %d, body %s", method, denied.Code, denied.Body.String())
		}
	}
	if coordinator.request.RequestToken != "" {
		t.Fatal("denied deletion reached its coordinator")
	}
	deleted := performRequest(t, handler, http.MethodDelete, path, nil, map[string]string{"Authorization": "Bearer epoch-dev-admin-v1", "Idempotency-Key": "qualified-coordinated-delete", "If-Match": "1"})
	if deleted.Code != http.StatusOK || coordinator.request.Key != key || coordinator.request.ExpectedGeneration == nil || *coordinator.request.ExpectedGeneration != 1 {
		t.Fatalf("qualified coordinated delete: status %d, request %+v, body %s", deleted.Code, coordinator.request, deleted.Body.String())
	}
}

func TestHTTPQualifiedLookupDoesNotCollideWithOtherTenantsOrLegacyKeys(t *testing.T) {
	registry := NewRegistry()
	keys := []ResourceKey{
		{Organization: "acme", Project: "payments", Environment: "production", Namespace: "orders", Kind: KindQueue, Name: "jobs"},
		{Organization: "otherco", Project: "payments", Environment: "production", Namespace: "orders", Kind: KindQueue, Name: "jobs"},
		{Namespace: "orders", Kind: KindQueue, Name: "jobs"},
	}
	for index, key := range keys {
		desired := DesiredResource{ResourceKey: key, Spec: json.RawMessage(`{"shard_count":1}`)}
		if key.Organization != "" {
			desired.Governance = &ResourceGovernance{Owner: "team:platform", CostCenter: "cc-1042", Classification: ClassificationConfidential}
		}
		if _, err := registry.Apply(ApplyRequest{RequestToken: []string{"scope-acme", "scope-otherco", "scope-legacy"}[index], Resource: desired}); err != nil {
			t.Fatal(err)
		}
	}
	handler, err := NewAuthenticatedHTTPHandler(registry, nil, loadHTTPAuthPolicy(t), controlauth.NewMemoryAuditSink())
	if err != nil {
		t.Fatal(err)
	}
	paths := []string{
		"/v1/resources/acme/payments/production/orders/queue/jobs",
		"/v1/resources/otherco/payments/production/orders/queue/jobs",
		"/v1/resources/orders/queue/jobs",
	}
	for index, path := range paths {
		got := performRequest(t, handler, http.MethodGet, path, nil, bearerHeaders("epoch-dev-admin-v1"))
		if got.Code != http.StatusOK {
			t.Fatalf("GET %s: status %d, body %s", path, got.Code, got.Body.String())
		}
		var resource Resource
		decodeResponse(t, got, &resource)
		if resource.ResourceKey != keys[index] {
			t.Fatalf("GET %s returned another identity: %+v", path, resource.ResourceKey)
		}
	}
	for index, path := range paths {
		got := performRequest(t, handler, http.MethodGet, path, nil, bearerHeaders("epoch-dev-reader-v1"))
		want := http.StatusForbidden
		if index == 0 {
			want = http.StatusOK
		}
		if got.Code != want {
			t.Fatalf("tenant-scoped reader GET %s: status %d, want %d, body %s", path, got.Code, want, got.Body.String())
		}
	}
}

func TestHTTPQualifiedPathRejectsIncompleteTenantSegments(t *testing.T) {
	for _, path := range []string{
		"/v1/resources///dev/core/queue/jobs",
		"/v1/resources/acme//dev/core/queue/jobs",
		"/v1/resources/acme/shop//core/queue/jobs",
		"/v1/resources////core/queue/jobs",
		"/v1/resources/acme/shop/dev/core/queue",
	} {
		t.Run(path, func(t *testing.T) {
			if _, err := keyFromPath(path); err == nil {
				t.Fatal("incomplete qualified key was accepted")
			}
		})
	}
}
