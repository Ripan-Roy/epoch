package regional

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"epoch.local/epoch/control/internal/resources"
)

const maxDiagnosticResponseBytes = 1 << 20

// HTTPDiagnosticClient reads the internal bounded diagnostic surface of regional nodes.
type HTTPDiagnosticClient struct {
	endpoints []*url.URL
	client    *http.Client
}

// NewHTTPDiagnosticClient validates every operator-supplied endpoint before use.
func NewHTTPDiagnosticClient(endpoints []string, client *http.Client) (*HTTPDiagnosticClient, error) {
	if len(endpoints) == 0 {
		return nil, errors.New("at least one regional metrics endpoint is required")
	}
	if client == nil {
		return nil, errors.New("regional diagnostics HTTP client is required")
	}
	parsed := make([]*url.URL, 0, len(endpoints))
	for _, raw := range endpoints {
		endpoint, err := url.Parse(strings.TrimSpace(raw))
		if err != nil || endpoint.Host == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") {
			return nil, fmt.Errorf("regional metrics endpoint %q must be an absolute http(s) URL", raw)
		}
		if endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || (endpoint.Path != "" && endpoint.Path != "/") {
			return nil, fmt.Errorf("regional metrics endpoint %q must contain only scheme and authority", raw)
		}
		endpoint.Path = ""
		parsed = append(parsed, endpoint)
	}
	return &HTTPDiagnosticClient{endpoints: parsed, client: client}, nil
}

// DiagnoseLatency fails over across configured regional nodes without exposing their URLs.
func (client *HTTPDiagnosticClient) DiagnoseLatency(
	ctx context.Context,
	request resources.LatencyDiagnosticRequest,
) (resources.LatencyDiagnosis, error) {
	var failures []error
	nonNotFoundFailure := false
	for index, endpoint := range client.endpoints {
		target := *endpoint
		target.Path = "/v1/diagnostics/latency"
		query := target.Query()
		query.Set("organization", request.Organization)
		query.Set("project", request.Project)
		query.Set("environment", request.Environment)
		query.Set("namespace", request.Namespace)
		query.Set("profile", request.Profile)
		target.RawQuery = query.Encode()
		httpRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
		if err != nil {
			return resources.LatencyDiagnosis{}, fmt.Errorf("build regional diagnostic request: %w", err)
		}
		httpRequest.Header.Set("Accept", "application/json")
		response, err := client.client.Do(httpRequest)
		if err != nil {
			failures = append(failures, fmt.Errorf("regional-%d: %w", index+1, err))
			nonNotFoundFailure = true
			continue
		}
		diagnosis, err := decodeLatencyDiagnosis(response)
		if err != nil {
			failures = append(failures, fmt.Errorf("regional-%d: %w", index+1, err))
			nonNotFoundFailure = nonNotFoundFailure || !errors.Is(err, resources.ErrLatencyDiagnosisNotFound)
			continue
		}
		diagnosis.RegionalEndpoint = fmt.Sprintf("regional-%d", index+1)
		return diagnosis, nil
	}
	if len(failures) > 0 && !nonNotFoundFailure {
		return resources.LatencyDiagnosis{}, resources.ErrLatencyDiagnosisNotFound
	}
	return resources.LatencyDiagnosis{}, errors.Join(failures...)
}

func decodeLatencyDiagnosis(response *http.Response) (resources.LatencyDiagnosis, error) {
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxDiagnosticResponseBytes))
		if response.StatusCode == http.StatusNotFound {
			return resources.LatencyDiagnosis{}, resources.ErrLatencyDiagnosisNotFound
		}
		return resources.LatencyDiagnosis{}, fmt.Errorf("diagnostic endpoint returned HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxDiagnosticResponseBytes+1))
	if err != nil {
		return resources.LatencyDiagnosis{}, fmt.Errorf("read diagnostic response: %w", err)
	}
	if len(body) > maxDiagnosticResponseBytes {
		return resources.LatencyDiagnosis{}, errors.New("diagnostic response exceeds 1 MiB")
	}
	var diagnosis resources.LatencyDiagnosis
	if err := json.Unmarshal(body, &diagnosis); err != nil {
		return resources.LatencyDiagnosis{}, fmt.Errorf("decode diagnostic response: %w", err)
	}
	if !validDiagnosticStage(diagnosis.Stage) || diagnosis.Cause != diagnosis.Stage || diagnosis.Samples < 1 ||
		diagnosis.Recommendation == "" || len(diagnosis.Recommendation) > 1024 {
		return resources.LatencyDiagnosis{}, errors.New("diagnostic response violates the bounded contract")
	}
	return diagnosis, nil
}

func validDiagnosticStage(stage string) bool {
	switch stage {
	case "quota", "hot_partition", "replication", "storage", "routing", "target", "client":
		return true
	default:
		return false
	}
}
