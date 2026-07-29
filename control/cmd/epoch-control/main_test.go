package main

import (
	"testing"
	"time"
)

func TestLoadConfigUsesExplicitRegionalEndpointsAndInterval(t *testing.T) {
	t.Setenv("EPOCH_CONTROL_ADDR", "127.0.0.1:18080")
	t.Setenv("EPOCH_CONTROL_GRPC_ADDR", "127.0.0.1:18081")
	t.Setenv(
		"EPOCH_CONTROL_REGIONAL_ENDPOINTS",
		" http://node-1:7601,https://node-2:7601 ,,",
	)
	t.Setenv(
		"EPOCH_CONTROL_ALLOWED_ORIGINS",
		" http://127.0.0.1:5173,https://console.example.com ,,",
	)
	t.Setenv("EPOCH_CONTROL_STATE_PATH", "/tmp/epoch-control-test/registry.db")
	t.Setenv("EPOCH_CONTROL_RECONCILE_INTERVAL", "250ms")
	t.Setenv("EPOCH_AUTH_POLICY_PATH", "/etc/epoch/bootstrap-policy.json")
	t.Setenv("EPOCH_CONTROL_REGIONAL_TOKEN", "control-workload-token")
	config, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if config.httpAddress != "127.0.0.1:18080" ||
		config.grpcAddress != "127.0.0.1:18081" ||
		len(config.regionalEndpoints) != 2 ||
		len(config.allowedOrigins) != 2 ||
		config.statePath != "/tmp/epoch-control-test/registry.db" ||
		config.authPolicyPath != "/etc/epoch/bootstrap-policy.json" ||
		string(config.regionalToken) != "control-workload-token" ||
		config.reconcileInterval != 250*time.Millisecond {
		t.Fatalf("config = %+v", config)
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

func TestControlSecretFormattingIsAlwaysRedacted(t *testing.T) {
	credential := secret("must-never-be-formatted")
	if got := credential.String(); got != "[redacted]" {
		t.Fatalf("String() = %q", got)
	}
	if got := credential.GoString(); got != "[redacted]" {
		t.Fatalf("GoString() = %q", got)
	}
}
