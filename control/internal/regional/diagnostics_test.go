package regional

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"epoch.local/epoch/control/internal/resources"
)

func TestHTTPDiagnosticClientFailsOverAndPreservesBoundedQuery(t *testing.T) {
	missing := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, "missing", http.StatusNotFound)
	}))
	defer missing.Close()
	working := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/diagnostics/latency" ||
			request.URL.Query().Get("organization") != "acme" ||
			request.URL.Query().Get("profile") != "stream" {
			t.Fatalf("diagnostic request = %s", request.URL.String())
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"cause":"replication","stage":"replication","observed_p99_ms":640,"samples":12,"recommendation":"Inspect replica lag."}`))
	}))
	defer working.Close()

	client, err := NewHTTPDiagnosticClient(
		[]string{missing.URL, working.URL},
		&http.Client{Timeout: time.Second},
	)
	if err != nil {
		t.Fatal(err)
	}
	diagnosis, err := client.DiagnoseLatency(context.Background(), resources.LatencyDiagnosticRequest{
		Organization: "acme",
		Project:      "payments",
		Environment:  "prod",
		Namespace:    "core",
		Profile:      "stream",
	})
	if err != nil {
		t.Fatal(err)
	}
	if diagnosis.Cause != "replication" || diagnosis.ObservedP99MS != 640 || diagnosis.RegionalEndpoint != "regional-2" {
		t.Fatalf("diagnosis = %+v", diagnosis)
	}
}

func TestHTTPDiagnosticClientRejectsUnsafeEndpoints(t *testing.T) {
	for _, endpoint := range []string{
		"file:///tmp/metrics",
		"https://user:secret@node:7602",
		"https://node:7602?token=secret",
		"https://node:7602/unexpected-prefix",
	} {
		if _, err := NewHTTPDiagnosticClient([]string{endpoint}, http.DefaultClient); err == nil {
			t.Fatalf("accepted %q", endpoint)
		}
	}
}

func TestHTTPDiagnosticClientPreservesNoSampleResult(t *testing.T) {
	t.Parallel()
	missing := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, "missing", http.StatusNotFound)
	}))
	defer missing.Close()
	client, err := NewHTTPDiagnosticClient([]string{missing.URL}, missing.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.DiagnoseLatency(context.Background(), resources.LatencyDiagnosticRequest{
		Organization: "acme", Project: "payments", Environment: "prod", Namespace: "core", Profile: "stream",
	})
	if !errors.Is(err, resources.ErrLatencyDiagnosisNotFound) {
		t.Fatalf("missing samples returned %v", err)
	}
}

func TestHTTPDiagnosticClientDoesNotHideRegionalFailureAsNoSamples(t *testing.T) {
	t.Parallel()
	failing := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, "failed", http.StatusInternalServerError)
	}))
	defer failing.Close()
	client, err := NewHTTPDiagnosticClient([]string{failing.URL}, failing.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.DiagnoseLatency(context.Background(), resources.LatencyDiagnosticRequest{
		Organization: "acme", Project: "payments", Environment: "prod", Namespace: "core", Profile: "stream",
	})
	if err == nil || errors.Is(err, resources.ErrLatencyDiagnosisNotFound) {
		t.Fatalf("regional failure was misclassified: %v", err)
	}
}

func TestHTTPDiagnosticClientRejectsUnboundedCauseLabels(t *testing.T) {
	t.Parallel()
	malformed := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"cause":"customer-controlled","stage":"routing","observed_p99_ms":1,"samples":1,"recommendation":"Inspect routing."}`))
	}))
	defer malformed.Close()
	client, err := NewHTTPDiagnosticClient([]string{malformed.URL}, malformed.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.DiagnoseLatency(context.Background(), resources.LatencyDiagnosticRequest{
		Organization: "acme", Project: "payments", Environment: "prod", Namespace: "core", Profile: "stream",
	})
	if err == nil || errors.Is(err, resources.ErrLatencyDiagnosisNotFound) {
		t.Fatalf("unbounded cause was accepted or misclassified: %v", err)
	}
}
