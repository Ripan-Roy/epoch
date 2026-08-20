package regional

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	"epoch.local/epoch/control/internal/resources"
	epochv1 "epoch.local/epoch/sdk/go/gen/epoch/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestRegionalAdminGRPCLifecycleIsIdempotentAndObserved(t *testing.T) {
	registry := resources.NewRegistry()
	key := regionalKey(resources.KindStream, "orders")
	observation := servingObservation(1, 1, 3)
	var authorityDeletes atomic.Int32
	authority := &fakeAuthority{
		apply: func(AuthorityApplyRequest) (AuthorityObservation, error) {
			return observation, nil
		},
		observe: func(resources.ResourceKey) (AuthorityObservation, error) {
			return observation, nil
		},
		delete: func(request AuthorityDeleteRequest) (AuthorityDeleteObservation, error) {
			if authorityDeletes.Add(1) != 1 {
				return AuthorityDeleteObservation{}, conflictError("delete was applied twice")
			}
			if request.ExpectedGeneration != 1 {
				t.Fatalf("Delete request = %+v", request)
			}
			return AuthorityDeleteObservation{Generation: 2, Deleted: true}, nil
		},
	}
	client := startRegionalAdminClient(t, registry, authority)
	request := applyProtoRequest(t, key, "grpc-create-orders")

	created, err := client.ApplyResource(t.Context(), request)
	if err != nil {
		t.Fatalf("ApplyResource() error = %v", err)
	}
	assertProtoReady(t, created.GetResource(), 1)
	if !created.GetCreated() || !created.GetChanged() || created.GetReplayed() {
		t.Fatalf("created flags = %+v", created)
	}
	replayed, err := client.ApplyResource(t.Context(), request)
	if err != nil {
		t.Fatalf("ApplyResource(replay) error = %v", err)
	}
	if !replayed.GetReplayed() || replayed.GetResource().GetGeneration() != 1 {
		t.Fatalf("replayed response = %+v", replayed)
	}

	got, err := client.GetResource(t.Context(), &epochv1.GetResourceRequest{Name: request.Name})
	if err != nil {
		t.Fatalf("GetResource() error = %v", err)
	}
	assertProtoReady(t, got.GetResource(), 1)
	listed, err := client.ListResources(t.Context(), &epochv1.ListResourcesRequest{
		Organization: key.Organization,
		Project:      key.Project,
		Environment:  key.Environment,
		Namespace:    key.Namespace,
		Kind:         epochv1.ResourceKind_RESOURCE_KIND_STREAM,
		PageSize:     10,
	})
	if err != nil {
		t.Fatalf("ListResources() error = %v", err)
	}
	if len(listed.GetResources()) != 1 || listed.GetNextPageToken() != "" {
		t.Fatalf("ListResources() = %+v", listed)
	}

	deleteRequest := &epochv1.DeleteResourceRequest{
		RequestToken:       "grpc-delete-orders",
		Name:               request.Name,
		ExpectedGeneration: uint64Pointer(1),
	}
	deleted, err := client.DeleteResource(t.Context(), deleteRequest)
	if err != nil {
		t.Fatalf("DeleteResource() error = %v", err)
	}
	if !deleted.GetDeleted() || deleted.GetGeneration() != 2 {
		t.Fatalf("DeleteResource() = %+v", deleted)
	}
	replayedDelete, err := client.DeleteResource(t.Context(), deleteRequest)
	if err != nil {
		t.Fatalf("DeleteResource(replay) error = %v", err)
	}
	if !replayedDelete.GetReplayed() ||
		!replayedDelete.GetDeleted() ||
		replayedDelete.GetGeneration() != 2 ||
		authorityDeletes.Load() != 1 {
		t.Fatalf(
			"DeleteResource(replay) = %+v, authority deletes = %d",
			replayedDelete,
			authorityDeletes.Load(),
		)
	}
	_, err = client.GetResource(t.Context(), &epochv1.GetResourceRequest{Name: request.Name})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("GetResource(deleted) error = %v", err)
	}
}

func TestRegionalAdminRetainsPendingDesiredStateDuringDisconnect(t *testing.T) {
	registry := resources.NewRegistry()
	key := regionalKey(resources.KindQueue, "jobs")
	var connected atomic.Bool
	authority := &fakeAuthority{
		apply: func(AuthorityApplyRequest) (AuthorityObservation, error) {
			if !connected.Load() {
				return AuthorityObservation{}, availabilityError("region disconnected")
			}
			return servingObservation(1, 1, 3), nil
		},
		observe: func(resources.ResourceKey) (AuthorityObservation, error) {
			return servingObservation(1, 1, 3), nil
		},
	}
	client := startRegionalAdminClient(t, registry, authority)
	request := applyProtoRequest(t, key, "grpc-create-jobs")

	pending, err := client.ApplyResource(t.Context(), request)
	if err != nil {
		t.Fatalf("ApplyResource(disconnected) error = %v", err)
	}
	if pending.GetResource().GetStatus().GetPhase() != epochv1.ResourcePhase_RESOURCE_PHASE_PENDING ||
		pending.GetResource().GetStatus().GetObservedGeneration() != 0 {
		t.Fatalf("pending response = %+v", pending)
	}
	connected.Store(true)
	ready, err := client.ApplyResource(t.Context(), request)
	if err != nil {
		t.Fatalf("ApplyResource(reconnected) error = %v", err)
	}
	assertProtoReady(t, ready.GetResource(), 1)
	if !ready.GetReplayed() {
		t.Fatal("reconnected exact request should be reported as replayed")
	}
}

func TestRegionalAdminSurfacesConflictAndRetainsFailedStatus(t *testing.T) {
	registry := resources.NewRegistry()
	key := regionalKey(resources.KindCache, "sessions")
	authority := &fakeAuthority{
		apply: func(AuthorityApplyRequest) (AuthorityObservation, error) {
			return AuthorityObservation{}, conflictError("catalog generation conflict")
		},
		observe: func(resources.ResourceKey) (AuthorityObservation, error) {
			panic("Observe should not run")
		},
	}
	client := startRegionalAdminClient(t, registry, authority)
	request := applyProtoRequest(t, key, "grpc-create-sessions")
	_, err := client.ApplyResource(t.Context(), request)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ApplyResource() error = %v", err)
	}
	got, err := client.GetResource(t.Context(), &epochv1.GetResourceRequest{Name: request.Name})
	if err != nil {
		t.Fatalf("GetResource() error = %v", err)
	}
	if got.GetResource().GetStatus().GetPhase() != epochv1.ResourcePhase_RESOURCE_PHASE_FAILED {
		t.Fatalf("failed resource = %+v", got.GetResource())
	}
}

func TestRegionalAdminRequiresGovernanceAndFiltersByExactMetadata(t *testing.T) {
	registry := resources.NewRegistry()
	key := regionalKey(resources.KindStream, "governed-orders")
	authority := &fakeAuthority{
		apply: func(request AuthorityApplyRequest) (AuthorityObservation, error) {
			if request.Governance == nil ||
				request.Governance.Owner != "team:payments" ||
				request.Governance.Tags["service"] != "checkout" {
				t.Fatalf("governance was not forwarded: %+v", request.Governance)
			}
			return servingObservation(1, 1, 3), nil
		},
	}
	client := startRegionalAdminClient(t, registry, authority)
	missing := applyProtoRequest(t, key, "grpc-missing-governance")
	missing.Spec.Governance = nil
	if _, err := client.ApplyResource(t.Context(), missing); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ApplyResource(missing governance) error = %v", err)
	}

	request := applyProtoRequest(t, key, "grpc-create-governed-orders")
	if _, err := client.ApplyResource(t.Context(), request); err != nil {
		t.Fatalf("ApplyResource() error = %v", err)
	}
	listed, err := client.ListResources(t.Context(), &epochv1.ListResourcesRequest{
		Organization:   key.Organization,
		Environment:    key.Environment,
		Owner:          "TEAM:PAYMENTS",
		CostCenter:     "CC-1042",
		Classification: epochv1.DataClassification_DATA_CLASSIFICATION_CONFIDENTIAL,
		Tags:           map[string]string{"service": "checkout", "tier": "critical"},
		PageSize:       10,
	})
	if err != nil || len(listed.GetResources()) != 1 {
		t.Fatalf("ListResources(governance) = %+v, %v", listed, err)
	}
	governance := listed.GetResources()[0].GetSpec().GetGovernance()
	if governance.GetOwner() != "team:payments" ||
		governance.GetCostCenter() != "cc-1042" ||
		governance.GetClassification() != epochv1.DataClassification_DATA_CLASSIFICATION_CONFIDENTIAL ||
		governance.GetTags()["tier"] != "critical" {
		t.Fatalf("listed governance = %+v", governance)
	}
}

func startRegionalAdminClient(
	t *testing.T,
	registry *resources.Registry,
	authority Authority,
) epochv1.RegionalAdminServiceClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	epochv1.RegisterRegionalAdminServiceServer(
		server,
		NewRegionalAdminServer(registry, NewReconciler(registry, authority)),
	)
	go func() {
		if err := server.Serve(listener); err != nil {
			t.Errorf("gRPC Serve() error = %v", err)
		}
	}()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient(
		"passthrough:///regional-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	t.Cleanup(func() {
		if err := connection.Close(); err != nil {
			t.Errorf("gRPC Close() error = %v", err)
		}
	})
	return epochv1.NewRegionalAdminServiceClient(connection)
}

func applyProtoRequest(
	t *testing.T,
	key resources.ResourceKey,
	token string,
) *epochv1.ApplyResourceRequest {
	t.Helper()
	configuration, err := structpb.NewStruct(map[string]any{"shard_count": 1})
	if err != nil {
		t.Fatalf("structpb.NewStruct() error = %v", err)
	}
	return &epochv1.ApplyResourceRequest{
		RequestToken: token,
		Name: &epochv1.ResourceName{
			Organization: key.Organization,
			Project:      key.Project,
			Environment:  key.Environment,
			Namespace:    key.Namespace,
			Kind:         protoKind(key.Kind),
			Name:         key.Name,
		},
		Spec: &epochv1.ResourceSpec{
			WorkloadProfile: profileForKind(key.Kind),
			Replicas:        3,
			Labels:          map[string]string{"owner": "integration"},
			Configuration:   configuration,
			Placement: &epochv1.PlacementPolicy{
				AllowedRegions:    []string{"ap-south"},
				MinimumZones:      3,
				RequiredNodeClass: "general-purpose",
			},
			Governance: &epochv1.ResourceGovernance{
				Owner:          "team:payments",
				CostCenter:     "cc-1042",
				Classification: epochv1.DataClassification_DATA_CLASSIFICATION_CONFIDENTIAL,
				Tags: map[string]string{
					"service": "checkout",
					"tier":    "critical",
				},
			},
		},
		ExpectedGeneration: uint64Pointer(0),
	}
}

func assertProtoReady(t *testing.T, resource *epochv1.Resource, tablets int) {
	t.Helper()
	if resource.GetGeneration() != 1 ||
		resource.GetStatus().GetPhase() != epochv1.ResourcePhase_RESOURCE_PHASE_READY ||
		resource.GetStatus().GetObservedGeneration() != 1 ||
		len(resource.GetStatus().GetTablets()) != tablets {
		t.Fatalf("resource is not ready: %+v", resource)
	}
	tablet := resource.GetStatus().GetTablets()[0]
	if len(tablet.GetVoterNodeIds()) != 3 || tablet.GetLeaderNodeId() == 0 {
		t.Fatalf("tablet placement = %+v", tablet)
	}
	placement := resource.GetStatus().GetPlacement()
	if placement.GetMinimumZones() != 3 ||
		placement.GetAchievedZones() != 3 ||
		len(placement.GetNodes()) != 3 {
		t.Fatalf("achieved topology = %+v", placement)
	}
}

func uint64Pointer(value uint64) *uint64 {
	return &value
}
