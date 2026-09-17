package regional

import (
	"context"
	"net"
	"slices"
	"sort"
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

type testControlRegistry struct {
	resources.Store
	operation ControlOperation
	changes   []ControlChange
}

func (registry *testControlRegistry) BatchApply(
	_ context.Context,
	request BatchApplyRequest,
) (BatchApplyResult, error) {
	sort.Slice(request.Resources, func(left, right int) bool {
		return controlNameLess(
			controlName(request.Resources[left].Resource.ResourceKey),
			controlName(request.Resources[right].Resource.ResourceKey),
		)
	})
	result := BatchApplyResult{Results: make([]resources.ApplyResult, 0, len(request.Resources))}
	for _, item := range request.Resources {
		item.RequestToken = request.RequestToken + "-" + item.Resource.Name
		applied, err := registry.Store.Apply(item)
		if err != nil {
			return BatchApplyResult{}, err
		}
		result.Results = append(result.Results, applied)
	}
	return result, nil
}

func (registry *testControlRegistry) ControlOperation(
	_ context.Context,
	_ string,
) (ControlOperation, error) {
	return registry.operation, nil
}

func (registry *testControlRegistry) ControlChanges(
	_ context.Context,
	after uint64,
	limit uint32,
) (ControlChangePage, error) {
	page := ControlChangePage{EarliestCursor: 1, LatestCursor: uint64(len(registry.changes))}
	for _, change := range registry.changes {
		if change.Cursor > after && len(page.Changes) < int(limit) {
			page.Changes = append(page.Changes, change)
		}
	}
	return page, nil
}

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

func TestRegionalAdminAtomicBatchOperationAndResumableWatch(t *testing.T) {
	local := resources.NewRegistry()
	auditKey := regionalKey(resources.KindStream, "audit")
	ordersKey := regionalKey(resources.KindStream, "orders")
	queueKey := regionalKey(resources.KindQueue, "jobs")
	registry := &testControlRegistry{
		Store: local,
		operation: ControlOperation{
			RequestToken:      "grpc-batch-1",
			ProposalID:        42,
			State:             ControlOperationSucceeded,
			ResourceKeys:      []resources.ResourceKey{auditKey, ordersKey},
			FirstChangeCursor: 1,
			LastChangeCursor:  2,
		},
		changes: []ControlChange{
			{Cursor: 1, Kind: ControlChangeDesiredApplied, Key: auditKey, Generation: 1},
			{Cursor: 2, Kind: ControlChangeDesiredApplied, Key: queueKey, Generation: 1},
		},
	}
	authority := &fakeAuthority{
		apply: func(AuthorityApplyRequest) (AuthorityObservation, error) {
			panic("atomic desired batch must not materialize resources inline")
		},
	}
	client := startRegionalAdminClient(t, registry, authority)
	orders := applyProtoRequest(t, ordersKey, "unused")
	audit := applyProtoRequest(t, auditKey, "unused")
	applied, err := client.BatchApplyResources(t.Context(), &epochv1.BatchApplyResourcesRequest{
		RequestToken: "grpc-batch-1",
		Resources: []*epochv1.BatchApplyResource{
			{Name: orders.Name, Spec: orders.Spec, ExpectedGeneration: uint64Pointer(0)},
			{Name: audit.Name, Spec: audit.Spec, ExpectedGeneration: uint64Pointer(0)},
		},
	})
	if err != nil || len(applied.GetResults()) != 2 ||
		applied.GetResults()[0].GetResource().GetName().GetName() != "audit" ||
		applied.GetResults()[1].GetResource().GetName().GetName() != "orders" {
		t.Fatalf("BatchApplyResources() = %+v, %v", applied, err)
	}
	operation, err := client.GetOperation(t.Context(), &epochv1.GetOperationRequest{
		RequestToken:      "grpc-batch-1",
		AffectedResources: []*epochv1.ResourceName{orders.Name, audit.Name},
	})
	if err != nil || operation.GetState() != epochv1.OperationState_OPERATION_STATE_SUCCEEDED ||
		operation.GetProposalId() != 42 || operation.GetLastChangeCursor() != 2 {
		t.Fatalf("GetOperation() = %+v, %v", operation, err)
	}
	_, err = client.GetOperation(t.Context(), &epochv1.GetOperationRequest{
		RequestToken:      "grpc-batch-1",
		AffectedResources: []*epochv1.ResourceName{orders.Name},
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("GetOperation(scope mismatch) error = %v", err)
	}
	registry.operation = ControlOperation{
		RequestToken: "grpc-batch-1",
		ProposalID:   43,
		State:        ControlOperationPending,
	}
	_, err = client.GetOperation(t.Context(), &epochv1.GetOperationRequest{
		RequestToken:      "grpc-batch-1",
		AffectedResources: []*epochv1.ResourceName{orders.Name, audit.Name},
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("GetOperation(unauthorizable pending operation) error = %v", err)
	}
	watchContext, cancel := context.WithCancel(t.Context())
	watch, err := client.WatchResourceChanges(watchContext, &epochv1.WatchResourceChangesRequest{
		BatchSize: 10,
		Namespace: "core",
		Kind:      epochv1.ResourceKind_RESOURCE_KIND_STREAM,
	})
	if err != nil {
		t.Fatalf("WatchResourceChanges() error = %v", err)
	}
	batch, err := watch.Recv()
	cancel()
	if err != nil || batch.GetEarliestCursor() != 1 || batch.GetLatestCursor() != 2 ||
		batch.GetNextCursor() != 2 || len(batch.GetChanges()) != 1 ||
		batch.GetChanges()[0].GetName().GetName() != "audit" {
		t.Fatalf("WatchResourceChanges().Recv() = %+v, %v", batch, err)
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
	registry resources.Store,
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
				MinimumRacks:      3,
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
		resource.GetStatus().GetCatalogGeneration() != 1 ||
		len(resource.GetStatus().GetTablets()) != tablets {
		t.Fatalf("resource is not ready: %+v", resource)
	}
	tablet := resource.GetStatus().GetTablets()[0]
	if len(tablet.GetAssignedNodeIds()) != 3 ||
		len(tablet.GetVoterNodeIds()) != 3 ||
		len(tablet.GetReachableVoterNodeIds()) != 3 ||
		len(tablet.GetTargetVoterNodeIds()) != 0 ||
		tablet.GetLeaderNodeId() == 0 {
		t.Fatalf("tablet placement = %+v", tablet)
	}
	placement := resource.GetStatus().GetPlacement()
	if placement.GetMinimumZones() != 3 ||
		placement.GetMinimumRacks() != 3 ||
		placement.GetAchievedZones() != 3 ||
		placement.GetAchievedRacks() != 3 ||
		len(placement.GetNodes()) != 3 {
		t.Fatalf("achieved topology = %+v", placement)
	}
}

func TestStatusProtoKeepsControlAndCatalogGenerationsSeparate(t *testing.T) {
	status := statusToProto(resources.ResourceStatus{
		Phase:              resources.PhaseReady,
		ObservedGeneration: 8,
		CatalogGeneration:  7,
	})
	if status.GetObservedGeneration() != 8 || status.GetCatalogGeneration() != 7 {
		t.Fatalf("status generations = %+v", status)
	}
}

func TestRegionalAdminContractAcceptsFiveVoterPlacement(t *testing.T) {
	request := applyProtoRequest(t, regionalKey(resources.KindStream, "five-voter"), "five-voter")
	request.Spec.Replicas = 5
	request.Spec.Placement.MinimumZones = 5
	request.Spec.Placement.MinimumRacks = 5
	request.Spec.Placement.ExcludedNodeIds = []uint64{9}
	_, desired, err := desiredFromProto(request)
	if err != nil {
		t.Fatalf("desiredFromProto() error = %v", err)
	}
	spec, err := decodeDesiredSpec(desired.Spec)
	if err != nil {
		t.Fatalf("decodeDesiredSpec() error = %v", err)
	}
	if spec.ReplicaCount != 5 || spec.Placement.MinimumZones != 5 || spec.Placement.MinimumRacks != 5 ||
		!slices.Equal(spec.Placement.ExcludedNodeIDs, []uint64{9}) {
		t.Fatalf("decoded spec = %+v", spec)
	}
}

func uint64Pointer(value uint64) *uint64 {
	return &value
}
