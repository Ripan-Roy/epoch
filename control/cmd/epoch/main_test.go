package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	epochv1 "epoch.local/epoch/sdk/go/gen/epoch/v1"
	"google.golang.org/grpc"
)

type fakeAdminClient struct {
	apply  *epochv1.ApplyResourceRequest
	get    *epochv1.GetResourceRequest
	list   *epochv1.ListResourcesRequest
	delete *epochv1.DeleteResourceRequest
}

func (client *fakeAdminClient) ApplyResource(_ context.Context, request *epochv1.ApplyResourceRequest, _ ...grpc.CallOption) (*epochv1.ApplyResourceResponse, error) {
	client.apply = request
	return &epochv1.ApplyResourceResponse{Resource: &epochv1.Resource{Name: request.Name, Spec: request.Spec}, Created: true}, nil
}

func (client *fakeAdminClient) GetResource(_ context.Context, request *epochv1.GetResourceRequest, _ ...grpc.CallOption) (*epochv1.GetResourceResponse, error) {
	client.get = request
	return &epochv1.GetResourceResponse{Resource: &epochv1.Resource{Name: request.Name}}, nil
}

func (client *fakeAdminClient) ListResources(_ context.Context, request *epochv1.ListResourcesRequest, _ ...grpc.CallOption) (*epochv1.ListResourcesResponse, error) {
	client.list = request
	return &epochv1.ListResourcesResponse{}, nil
}

func (client *fakeAdminClient) DeleteResource(_ context.Context, request *epochv1.DeleteResourceRequest, _ ...grpc.CallOption) (*epochv1.DeleteResourceResponse, error) {
	client.delete = request
	return &epochv1.DeleteResourceResponse{Name: request.Name, Deleted: true}, nil
}

func TestApplyAcceptsStrictYAMLAndGeneratesRetryToken(t *testing.T) {
	t.Parallel()
	client := &fakeAdminClient{}
	input := `
name:
  organization: acme
  project: shop
  environment: dev
  namespace: core
  kind: RESOURCE_KIND_STREAM
  name: orders
spec:
  workloadProfile: WORKLOAD_PROFILE_STREAM_LOG
  durability: DURABILITY_PROFILE_QUORUM_DURABLE
  delivery: DELIVERY_SEMANTICS_AT_LEAST_ONCE
  ordering: ORDERING_SCOPE_PARTITION
  replicas: 3
  configuration:
    shard_count: 2
`
	var output bytes.Buffer
	if err := execute(context.Background(), client, cliConfig{}, []string{"apply", "--file", "-"}, strings.NewReader(input), &output, &output); err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if client.apply == nil || !strings.HasPrefix(client.apply.RequestToken, "epoch-cli-") {
		t.Fatalf("request token was not generated: %#v", client.apply)
	}
	if client.apply.Name.GetKind() != epochv1.ResourceKind_RESOURCE_KIND_STREAM || client.apply.Spec.GetReplicas() != 3 {
		t.Fatalf("typed request was not preserved: %#v", client.apply)
	}
	if !strings.Contains(output.String(), `"created": true`) {
		t.Fatalf("response was not rendered: %s", output.String())
	}
}

func TestNamesAndListFiltersRemainFullyQualified(t *testing.T) {
	t.Parallel()
	client := &fakeAdminClient{}
	var output bytes.Buffer
	if err := execute(context.Background(), client, cliConfig{}, []string{"get", "acme/shop/prod/core/event-bus/orders"}, strings.NewReader(""), &output, &output); err != nil {
		t.Fatal(err)
	}
	if client.get.Name.GetKind() != epochv1.ResourceKind_RESOURCE_KIND_EVENT_BUS || client.get.Name.GetEnvironment() != "prod" {
		t.Fatalf("fully qualified name was not parsed: %#v", client.get.Name)
	}
	if err := execute(context.Background(), client, cliConfig{}, []string{"list", "--organization", "acme", "--project", "shop", "--kind", "queue", "--page-size", "25"}, strings.NewReader(""), &output, &output); err != nil {
		t.Fatal(err)
	}
	if client.list.Organization != "acme" || client.list.Kind != epochv1.ResourceKind_RESOURCE_KIND_QUEUE || client.list.PageSize != 25 {
		t.Fatalf("list filters were not preserved: %#v", client.list)
	}
}

func TestDeleteCarriesOptimisticConcurrency(t *testing.T) {
	t.Parallel()
	client := &fakeAdminClient{}
	var output bytes.Buffer
	if err := execute(context.Background(), client, cliConfig{}, []string{"delete", "--expected-generation", "7", "acme/shop/dev/core/cache/sessions"}, strings.NewReader(""), &output, &output); err != nil {
		t.Fatal(err)
	}
	if client.delete.ExpectedGeneration == nil || *client.delete.ExpectedGeneration != 7 || !strings.HasPrefix(client.delete.RequestToken, "epoch-cli-") {
		t.Fatalf("delete fencing was not preserved: %#v", client.delete)
	}
}

func TestDoctorChecksPublicHealthAndAuthenticatedGRPC(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/healthz" {
			http.NotFound(writer, request)
			return
		}
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	client := &fakeAdminClient{}
	var output bytes.Buffer
	config := cliConfig{httpEndpoint: server.URL, grpcEndpoint: "control:8081", timeout: time.Second}
	if err := doctorCommand(context.Background(), client, config, &output); err != nil {
		t.Fatal(err)
	}
	if client.list == nil || client.list.PageSize != 1 || !strings.Contains(output.String(), "auth=accepted") {
		t.Fatalf("doctor did not exercise both boundaries: %s %#v", output.String(), client.list)
	}
}

func TestApplyRejectsUnknownFields(t *testing.T) {
	t.Parallel()
	client := &fakeAdminClient{}
	var output bytes.Buffer
	err := execute(context.Background(), client, cliConfig{}, []string{"apply"}, strings.NewReader("unknown: true\n"), &output, &output)
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown manifest fields must fail closed: %v", err)
	}
}

func TestApplyRejectsDuplicateYAMLFields(t *testing.T) {
	t.Parallel()
	client := &fakeAdminClient{}
	var output bytes.Buffer
	err := execute(
		context.Background(),
		client,
		cliConfig{},
		[]string{"apply"},
		strings.NewReader("requestToken: first\nrequestToken: second\n"),
		&output,
		&output,
	)
	if err == nil || !strings.Contains(err.Error(), "key \"requestToken\" already set") {
		t.Fatalf("duplicate manifest fields must fail closed: %v", err)
	}
}

func TestReadInputEnforcesStdinSizeLimit(t *testing.T) {
	t.Parallel()
	accepted, err := readInput("-", strings.NewReader(strings.Repeat("x", maxInputSize)))
	if err != nil {
		t.Fatalf("exactly sized stdin must be accepted: %v", err)
	}
	if len(accepted) != maxInputSize {
		t.Fatalf("stdin length = %d, want %d", len(accepted), maxInputSize)
	}

	_, err = readInput("-", strings.NewReader(strings.Repeat("x", maxInputSize+1)))
	if err == nil || !strings.Contains(err.Error(), "input exceeds 1 MiB") {
		t.Fatalf("oversized stdin must fail closed: %v", err)
	}
}
