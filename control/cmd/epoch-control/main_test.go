package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"epoch.local/epoch/control/internal/regional"
	"epoch.local/epoch/control/internal/resources"
)

func TestLoadConfigUsesExplicitRegionalEndpointsAndInterval(t *testing.T) {
	t.Setenv("EPOCH_CONTROL_ADDR", "127.0.0.1:18080")
	t.Setenv("EPOCH_CONTROL_GRPC_ADDR", "127.0.0.1:18081")
	t.Setenv("EPOCH_CONTROL_METRICS_ADDR", "127.0.0.1:19090")
	t.Setenv(
		"EPOCH_CONTROL_REGIONAL_ENDPOINTS",
		" http://node-1:7601,https://node-2:7601 ,,",
	)
	t.Setenv(
		"EPOCH_CONTROL_REGIONAL_METRICS_ENDPOINTS",
		" http://node-1:7602,https://node-2:7602 ,,",
	)
	t.Setenv(
		"EPOCH_CONTROL_ALLOWED_ORIGINS",
		" http://127.0.0.1:5173,https://console.example.com ,,",
	)
	t.Setenv("EPOCH_CONTROL_STATE_PATH", "/tmp/epoch-control-test/registry.db")
	t.Setenv("EPOCH_CONTROL_INSTANCE_ID", "epoch-control-0")
	t.Setenv("EPOCH_CONTROL_AUDIT_PATH", "/tmp/epoch-control-test/audit.ndjson")
	t.Setenv("EPOCH_CONTROL_RECONCILE_INTERVAL", "250ms")
	t.Setenv("EPOCH_CONTROL_OBSERVABILITY_MAX_TENANTS", "12")
	t.Setenv("EPOCH_OTLP_ENDPOINT", "http://collector:4318")
	t.Setenv("EPOCH_AUTH_POLICY_PATH", "/etc/epoch/bootstrap-policy.json")
	t.Setenv("EPOCH_CONTROL_REGIONAL_TOKEN", "control-workload-token")
	config, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if config.httpAddress != "127.0.0.1:18080" ||
		config.grpcAddress != "127.0.0.1:18081" ||
		config.metricsAddress != "127.0.0.1:19090" ||
		len(config.regionalEndpoints) != 2 ||
		len(config.regionalMetricsEndpoints) != 2 ||
		len(config.allowedOrigins) != 2 ||
		config.instanceID != "epoch-control-0" ||
		config.legacyStatePath != "/tmp/epoch-control-test/registry.db" ||
		config.auditPath != "/tmp/epoch-control-test/audit.ndjson" ||
		config.authPolicyPath != "/etc/epoch/bootstrap-policy.json" ||
		string(config.regionalToken) != "control-workload-token" ||
		config.reconcileInterval != 250*time.Millisecond ||
		config.maxMetricTenants != 12 ||
		config.otlpEndpoint != "http://collector:4318" {
		t.Fatalf("config = %+v", config)
	}
}

func TestLoadConfigPrefersExplicitLegacyStatePath(t *testing.T) {
	t.Setenv("EPOCH_AUTH_POLICY_PATH", "/etc/epoch/bootstrap-policy.json")
	t.Setenv("EPOCH_CONTROL_REGIONAL_TOKEN", "control-workload-token")
	t.Setenv("EPOCH_CONTROL_INSTANCE_ID", "epoch-control-0")
	t.Setenv("EPOCH_CONTROL_STATE_PATH", "/old/default.db")
	t.Setenv("EPOCH_CONTROL_LEGACY_STATE_PATH", "/old/explicit.db")
	config, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if config.legacyStatePath != "/old/explicit.db" {
		t.Fatalf("legacyStatePath = %q", config.legacyStatePath)
	}
}

func TestLoadConfigRejectsInvalidObservabilityTenantLimit(t *testing.T) {
	for _, value := range []string{"nope", "0", "4097"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("EPOCH_AUTH_POLICY_PATH", "/etc/epoch/bootstrap-policy.json")
			t.Setenv("EPOCH_CONTROL_REGIONAL_TOKEN", "control-workload-token")
			t.Setenv("EPOCH_CONTROL_OBSERVABILITY_MAX_TENANTS", value)
			if _, err := loadConfig(); err == nil {
				t.Fatal("loadConfig() succeeded, want error")
			}
		})
	}
}

func TestMigrateLegacyRegistryPreservesLiveAndTombstonedGenerations(t *testing.T) {
	legacyPath := filepath.Join(t.TempDir(), "registry.db")
	legacy, err := resources.OpenDurableRegistry(legacyPath)
	if err != nil {
		t.Fatal(err)
	}
	governance := &resources.ResourceGovernance{
		Owner:          "team:platform",
		CostCenter:     "cc-1042",
		Classification: resources.ClassificationInternal,
	}
	key := resources.ResourceKey{
		Organization: "acme",
		Project:      "shop",
		Environment:  "dev",
		Namespace:    "core",
		Kind:         resources.KindStream,
		Name:         "orders",
	}
	created, err := legacy.Apply(resources.ApplyRequest{
		RequestToken: "create-orders",
		Resource: resources.DesiredResource{
			ResourceKey: key,
			Governance:  governance,
			Spec:        json.RawMessage(`{"shard_count":1,"replica_count":3}`),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = legacy.Apply(resources.ApplyRequest{
		RequestToken:       "update-orders",
		ExpectedGeneration: &created.Resource.Generation,
		Resource: resources.DesiredResource{
			ResourceKey: key,
			Governance:  governance,
			Spec:        json.RawMessage(`{"shard_count":2,"replica_count":3}`),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	tombstoneKey := key
	tombstoneKey.Name = "audit"
	tombstone, err := legacy.Apply(resources.ApplyRequest{
		RequestToken: "create-audit",
		Resource: resources.DesiredResource{
			ResourceKey: tombstoneKey,
			Governance:  governance,
			Spec:        json.RawMessage(`{"shard_count":1,"replica_count":3}`),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.Delete(resources.DeleteRequest{
		RequestToken:       "delete-audit",
		ExpectedGeneration: &tombstone.Resource.Generation,
		Key:                tombstoneKey,
	}); err != nil {
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}

	var imported bool
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/experimental/v1/regional/control/import" {
			t.Errorf("unexpected request: %s %s", request.Method, request.URL.Path)
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		var body struct {
			RequestToken string `json:"request_token"`
			Resources    []struct {
				Name       map[string]any  `json:"name"`
				Generation string          `json:"generation"`
				Desired    json.RawMessage `json:"desired"`
				Status     json.RawMessage `json:"status"`
			} `json:"resources"`
			Generations []struct {
				Name       map[string]any `json:"name"`
				Generation string         `json:"generation"`
			} `json:"generations"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Errorf("decode import: %v", err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		if len(body.Resources) != 1 || body.Resources[0].Generation != "2" ||
			len(body.Generations) != 2 || body.Generations[0].Name["name"] != "audit" ||
			body.Generations[0].Generation != "2" || body.Generations[1].Generation != "2" {
			t.Errorf("unexpected migration body: %+v", body)
		}
		imported = true
		writer.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"request_replayed": false,
			"mutation": map[string]any{
				"kind": "desired_applied",
				"resources": []any{map[string]any{
					"resource": map[string]any{
						"name": body.Resources[0].Name, "generation": "2",
						"desired": body.Resources[0].Desired, "status": body.Resources[0].Status,
						"deletion_requested": false,
					},
					"created": true, "changed": true,
				}},
				"changed": true, "replayed": false,
			},
		})
	}))
	defer server.Close()
	authority, err := regional.NewHTTPAuthority([]string{server.URL}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	registry, err := regional.NewCatalogRegistry(authority, "epoch-control-0")
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := migrateLegacyRegistry(legacyPath, registry, logger); err != nil {
		t.Fatalf("migrateLegacyRegistry() error = %v", err)
	}
	if !imported || registry.Count() != 1 {
		t.Fatalf("imported = %v, count = %d", imported, registry.Count())
	}
}

func TestLoadConfigRejectsInvalidReconcileInterval(t *testing.T) {
	for _, value := range []string{"not-a-duration", "0s", "-1s"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("EPOCH_AUTH_POLICY_PATH", "/etc/epoch/bootstrap-policy.json")
			t.Setenv("EPOCH_CONTROL_REGIONAL_TOKEN", "control-workload-token")
			t.Setenv("EPOCH_CONTROL_RECONCILE_INTERVAL", value)
			if _, err := loadConfig(); err == nil {
				t.Fatal("loadConfig() succeeded, want error")
			}
		})
	}
}

func TestLoadConfigRequiresAuthPolicyAndRegionalWorkloadCredential(t *testing.T) {
	t.Setenv("EPOCH_AUTH_POLICY_PATH", "")
	t.Setenv("EPOCH_CONTROL_REGIONAL_TOKEN", "")
	if _, err := loadConfig(); err == nil {
		t.Fatal("loadConfig() succeeded without auth policy")
	}

	t.Setenv("EPOCH_AUTH_POLICY_PATH", "/etc/epoch/bootstrap-policy.json")
	if _, err := loadConfig(); err == nil {
		t.Fatal("loadConfig() succeeded without regional workload credential")
	}
}

func TestLoadConfigRequiresCompleteSecureRegionalTransport(t *testing.T) {
	t.Setenv("EPOCH_AUTH_POLICY_PATH", "/etc/epoch/bootstrap-policy.json")
	t.Setenv("EPOCH_CONTROL_REGIONAL_TOKEN", "control-workload-token")
	t.Setenv("EPOCH_CONTROL_TLS_REQUIRED", "true")
	if _, err := loadConfig(); err == nil {
		t.Fatal("required TLS succeeded without certificate material")
	}

	t.Setenv("EPOCH_CONTROL_TLS_CERT_PATH", "/etc/epoch/tls/tls.crt")
	t.Setenv("EPOCH_CONTROL_TLS_KEY_PATH", "/etc/epoch/tls/tls.key")
	t.Setenv("EPOCH_CONTROL_REGIONAL_TLS_CA_PATH", "/etc/epoch/tls/ca.crt")
	t.Setenv("EPOCH_CONTROL_REGIONAL_TLS_CERT_PATH", "/etc/epoch/tls/tls.crt")
	t.Setenv("EPOCH_CONTROL_REGIONAL_TLS_KEY_PATH", "/etc/epoch/tls/tls.key")
	if _, err := loadConfig(); err == nil {
		t.Fatal("required TLS accepted a plaintext regional endpoint")
	}

	t.Setenv("EPOCH_CONTROL_REGIONAL_ENDPOINTS", "https://node-1:7601,https://node-2:7601")
	config, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if !config.serverTLS.Required || !config.regionalTLS.Required {
		t.Fatalf("required secure transport was not propagated: %+v", config)
	}
}

func TestLoadConfigRejectsMalformedTLSRequirement(t *testing.T) {
	t.Setenv("EPOCH_AUTH_POLICY_PATH", "/etc/epoch/bootstrap-policy.json")
	t.Setenv("EPOCH_CONTROL_REGIONAL_TOKEN", "control-workload-token")
	t.Setenv("EPOCH_CONTROL_TLS_REQUIRED", "sometimes")
	if _, err := loadConfig(); err == nil {
		t.Fatal("malformed TLS requirement was accepted")
	}
}

func TestControlSecretFormattingIsAlwaysRedacted(t *testing.T) {
	credential := secret("must-never-be-formatted")
	if got := credential.String(); got != "[redacted]" {
		t.Fatalf("String() = %q", got)
	}
	if got := credential.GoString(); got != "[redacted]" {
		t.Fatalf("GoString() = %q", got)
	}
}
