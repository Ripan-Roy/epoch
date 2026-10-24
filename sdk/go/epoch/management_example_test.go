package epoch_test

import (
	"context"
	"fmt"
	"os"
	"time"

	"epoch.local/epoch/sdk/go/epoch"
	epochv1 "epoch.local/epoch/sdk/go/gen/epoch/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

// This example compiles in the ordinary SDK test suite. It is not executed
// there: run it only against a dedicated, trusted test controller and resource.
func ExampleManagementClient() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := epoch.NewManagementClient(epoch.ManagementConfig{
		Endpoints: []string{"localhost:8081"}, BearerToken: os.Getenv("EPOCH_BEARER_TOKEN"), Timeout: 5 * time.Second,
		TLS: &epoch.TLSConfig{RootCAPath: os.Getenv("EPOCH_TLS_CA_PATH"), CertificatePath: os.Getenv("EPOCH_TLS_CERT_PATH"), PrivateKeyPath: os.Getenv("EPOCH_TLS_KEY_PATH")},
	})
	if err != nil {
		return
	}
	defer client.Close()
	name := &epochv1.ResourceName{Organization: "acme", Project: "payments", Environment: "production", Namespace: "orders", Kind: epochv1.ResourceKind_RESOURCE_KIND_CACHE, Name: "sdk-management-example"}
	configuration, err := structpb.NewStruct(map[string]any{"shard_count": 1})
	if err != nil {
		return
	}
	spec := &epochv1.ResourceSpec{
		WorkloadProfile: epochv1.WorkloadProfile_WORKLOAD_PROFILE_CACHE, Durability: epochv1.DurabilityProfile_DURABILITY_PROFILE_QUORUM_DURABLE, Replicas: 3, Configuration: configuration,
		Governance: &epochv1.ResourceGovernance{Owner: "team:sdk-example", CostCenter: "cc-sdk", Classification: epochv1.DataClassification_DATA_CLASSIFICATION_INTERNAL},
		Placement:  &epochv1.PlacementPolicy{AllowedRegions: []string{"ap-south"}, MinimumZones: 3, RequiredNodeClass: "general-purpose"},
	}
	// The caller supplies a stable token; the SDK never invents a replacement
	// after a timeout or changes omitted/zero/nonzero generation presence.
	token := os.Getenv("EPOCH_EXAMPLE_REQUEST_TOKEN")
	_, info, err := client.BatchApplyResources(ctx, &epochv1.BatchApplyResourcesRequest{
		RequestToken: token, Resources: []*epochv1.BatchApplyResource{{Name: name, Spec: spec, ExpectedGeneration: proto.Uint64(0)}},
	})
	if err != nil {
		fmt.Printf("attempts=%d; outcome may be unknown=%v\n", info.Attempts, info.OutcomeMayBeUnknown)
		// Resolve this same token and exact scope before choosing any new write.
		lookup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_, _, _ = client.GetOperation(lookup, &epochv1.GetOperationRequest{RequestToken: token, AffectedResources: []*epochv1.ResourceName{name}})
		return
	}
	operation, _, err := client.GetOperation(ctx, &epochv1.GetOperationRequest{RequestToken: token, AffectedResources: []*epochv1.ResourceName{name}})
	if err != nil {
		return
	}
	resource, _, err := client.GetResource(ctx, &epochv1.GetResourceRequest{Name: name})
	if err != nil {
		return
	}
	_, _, err = client.ListResources(ctx, &epochv1.ListResourcesRequest{Organization: name.Organization, Project: name.Project, Environment: name.Environment, Namespace: name.Namespace, PageSize: 50})
	if err != nil {
		return
	}
	// A no-op update still has its own stable caller token and OCC precondition.
	_, _, err = client.ApplyResource(ctx, &epochv1.ApplyResourceRequest{RequestToken: token + "-apply", Name: name, Spec: spec, ExpectedGeneration: proto.Uint64(resource.Resource.Generation)})
	if err != nil {
		return
	}
	watch, err := client.WatchResourceChanges(ctx, &epochv1.WatchResourceChangesRequest{AfterCursor: operation.LastChangeCursor, BatchSize: 2, Organization: name.Organization, Project: name.Project, Environment: name.Environment, Namespace: name.Namespace})
	if err != nil {
		return
	}
	defer watch.Close()
	page, err := watch.Recv()
	if err != nil {
		return
	}
	// Process changes and atomically persist page.NextCursor with application
	// progress here, then acknowledge. This example has no business side effect.
	if err := watch.Acknowledge(page.NextCursor); err != nil {
		return
	}
	// This cleanup is exclusively for the dedicated example-owned resource.
	_, _, _ = client.DeleteResource(ctx, &epochv1.DeleteResourceRequest{RequestToken: token + "-delete", Name: name, ExpectedGeneration: proto.Uint64(resource.Resource.Generation)})
}
