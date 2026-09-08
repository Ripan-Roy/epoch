package repository_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

func observabilityPath(name string) string {
	return filepath.Join("..", "..", "deploy", "observability", name)
}

func TestObservabilityCollectorUsesBoundedVendorNeutralPipeline(t *testing.T) {
	data, err := os.ReadFile(observabilityPath("otel-collector.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := yaml.Unmarshal(data, &document); err != nil {
		t.Fatalf("parse collector config: %v", err)
	}
	text := string(data)
	for _, required := range []string{"otlp:", "memory_limiter:", "batch:", "otlphttp/upstream:", "OTEL_EXPORTER_OTLP_ENDPOINT"} {
		if !strings.Contains(text, required) {
			t.Errorf("collector config is missing %q", required)
		}
	}
	if strings.Contains(strings.ToLower(text), "password") || strings.Contains(strings.ToLower(text), "token:") {
		t.Fatal("collector template must not embed credentials")
	}
}

func TestObservabilityAlertTemplatesAreActionableAndLowCardinality(t *testing.T) {
	data, err := os.ReadFile(observabilityPath("prometheus-rules.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	type rule struct {
		Alert       string            `json:"alert"`
		Expr        string            `json:"expr"`
		For         string            `json:"for"`
		Labels      map[string]string `json:"labels"`
		Annotations map[string]string `json:"annotations"`
	}
	var document struct {
		Groups []struct {
			Name     string `json:"name"`
			Interval string `json:"interval"`
			Rules    []rule `json:"rules"`
		} `json:"groups"`
	}
	if err := yaml.UnmarshalStrict(data, &document); err != nil {
		t.Fatalf("parse alert rules: %v", err)
	}
	alerts := make(map[string]bool)
	for _, group := range document.Groups {
		if group.Name == "" || len(group.Rules) == 0 {
			t.Fatal("each alert group needs a stable name and at least one rule")
		}
		for _, candidate := range group.Rules {
			if candidate.Alert == "" || candidate.Expr == "" || candidate.For == "" || candidate.Labels["severity"] == "" || !strings.HasPrefix(candidate.Annotations["runbook"], "docs/OBSERVABILITY.md#") {
				t.Fatalf("alert is incomplete: %#v", candidate)
			}
			if alerts[candidate.Alert] {
				t.Fatalf("duplicate alert %q", candidate.Alert)
			}
			alerts[candidate.Alert] = true
			for _, unsafe := range []string{"organization=", "project=", "namespace=", "resource=", "resource_name=", "key="} {
				if strings.Contains(candidate.Expr, unsafe) {
					t.Fatalf("alert %s uses unsafe customer-controlled label %q", candidate.Alert, unsafe)
				}
			}
		}
	}
	for _, required := range []string{
		"EpochDataPlaneUnavailable",
		"EpochHTTPErrorBudgetBurn",
		"EpochHTTPP99LatencyHigh",
		"EpochReplicationStageP99High",
		"EpochMetricCardinalityOverflow",
		"EpochControlReconciliationFailures",
		"EpochCompatibilityErrorRateHigh",
	} {
		if !alerts[required] {
			t.Errorf("required alert %q is absent", required)
		}
	}
}

func TestObservabilityDashboardCoversRuntimeControlAndCompatibility(t *testing.T) {
	data, err := os.ReadFile(observabilityPath("grafana-dashboard.json"))
	if err != nil {
		t.Fatal(err)
	}
	var dashboard struct {
		Title         string `json:"title"`
		UID           string `json:"uid"`
		SchemaVersion int    `json:"schemaVersion"`
		Panels        []struct {
			ID      int    `json:"id"`
			Title   string `json:"title"`
			Targets []struct {
				Expr string `json:"expr"`
			} `json:"targets"`
		} `json:"panels"`
	}
	if err := json.Unmarshal(data, &dashboard); err != nil {
		t.Fatalf("parse Grafana dashboard: %v", err)
	}
	if dashboard.Title == "" || dashboard.UID == "" || dashboard.SchemaVersion < 40 || len(dashboard.Panels) < 6 {
		t.Fatalf("dashboard metadata or panel coverage is incomplete: %#v", dashboard)
	}
	var expressions strings.Builder
	ids := make(map[int]bool)
	for _, panel := range dashboard.Panels {
		if panel.ID <= 0 || ids[panel.ID] || panel.Title == "" || len(panel.Targets) == 0 {
			t.Fatalf("dashboard panel is incomplete or duplicated: %#v", panel)
		}
		ids[panel.ID] = true
		for _, target := range panel.Targets {
			expressions.WriteString(target.Expr)
			expressions.WriteByte('\n')
		}
	}
	for _, metric := range []string{
		"epoch_http_requests_total",
		"epoch_http_request_duration_seconds_bucket",
		"epoch_operation_stage_duration_p99_seconds",
		"epoch_control_reconciliations_total",
		"epoch_compat_requests_total",
		"epoch_observability_tenants",
	} {
		if !strings.Contains(expressions.String(), metric) {
			t.Errorf("dashboard does not query %s", metric)
		}
	}
}
