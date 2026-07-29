package regional

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"epoch.local/epoch/control/internal/resources"
	epochv1 "epoch.local/epoch/sdk/go/gen/epoch/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	defaultPageSize = 50
	maxPageSize     = 100
)

// RegionalAdminServer exposes the versioned managed-control contract while
// delegating catalog and placement authority to Rust.
type RegionalAdminServer struct {
	epochv1.UnimplementedRegionalAdminServiceServer
	registry   *resources.Registry
	reconciler *Reconciler
}

// NewRegionalAdminServer constructs the gRPC lifecycle service.
func NewRegionalAdminServer(
	registry *resources.Registry,
	reconciler *Reconciler,
) *RegionalAdminServer {
	if registry == nil {
		panic("regional: nil resource registry")
	}
	if reconciler == nil {
		panic("regional: nil reconciler")
	}
	return &RegionalAdminServer{registry: registry, reconciler: reconciler}
}

// ApplyResource accepts desired metadata idempotently and performs an
// immediate reconciliation attempt. Regional disconnection returns a pending
// resource rather than discarding accepted desired state.
func (server *RegionalAdminServer) ApplyResource(
	ctx context.Context,
	request *epochv1.ApplyResourceRequest,
) (*epochv1.ApplyResourceResponse, error) {
	key, desired, err := desiredFromProto(request)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	applied, err := server.registry.Apply(resources.ApplyRequest{
		RequestToken:       request.GetRequestToken(),
		ExpectedGeneration: request.ExpectedGeneration,
		Resource:           desired,
	})
	if err != nil {
		return nil, registryStatus(err)
	}
	reconciled, reconcileErr := server.reconciler.Reconcile(ctx, key)
	if reconcileErr != nil && !IsRetryable(reconcileErr) {
		return nil, reconciliationStatus(reconcileErr)
	}
	encoded, err := resourceToProto(reconciled)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &epochv1.ApplyResourceResponse{
		Resource: encoded,
		Created:  applied.Created,
		Changed:  applied.Changed,
		Replayed: applied.Replayed,
	}, nil
}

// GetResource returns desired and achieved state from the Go registry.
func (server *RegionalAdminServer) GetResource(
	_ context.Context,
	request *epochv1.GetResourceRequest,
) (*epochv1.GetResourceResponse, error) {
	key, err := keyFromProto(request.GetName())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	resource, err := server.registry.Get(key)
	if err != nil {
		return nil, registryStatus(err)
	}
	encoded, err := resourceToProto(resource)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &epochv1.GetResourceResponse{Resource: encoded}, nil
}

// ListResources returns one bounded deterministic page. The first slice has no
// continuation token because the in-memory registry is bounded and local.
func (server *RegionalAdminServer) ListResources(
	_ context.Context,
	request *epochv1.ListResourcesRequest,
) (*epochv1.ListResourcesResponse, error) {
	if request.GetPageToken() != "" {
		return nil, status.Error(codes.InvalidArgument, "page_token is not supported")
	}
	pageSize := request.GetPageSize()
	if pageSize == 0 {
		pageSize = defaultPageSize
	}
	if pageSize < 0 || pageSize > maxPageSize {
		return nil, status.Errorf(codes.InvalidArgument, "page_size must be between 1 and %d", maxPageSize)
	}
	kind, err := optionalKindFromProto(request.GetKind())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	listed, err := server.registry.List(resources.ListFilter{
		Organization: request.GetOrganization(),
		Project:      request.GetProject(),
		Environment:  request.GetEnvironment(),
		Namespace:    request.GetNamespace(),
		Kind:         kind,
	})
	if err != nil {
		return nil, registryStatus(err)
	}
	if len(listed) > int(pageSize) {
		listed = listed[:pageSize]
	}
	response := &epochv1.ListResourcesResponse{
		Resources: make([]*epochv1.Resource, 0, len(listed)),
	}
	for _, resource := range listed {
		encoded, err := resourceToProto(resource)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		response.Resources = append(response.Resources, encoded)
	}
	return response, nil
}

// DeleteResource commits the Rust tombstone before removing Go desired state.
func (server *RegionalAdminServer) DeleteResource(
	ctx context.Context,
	request *epochv1.DeleteResourceRequest,
) (*epochv1.DeleteResourceResponse, error) {
	key, err := keyFromProto(request.GetName())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	deleted, err := server.reconciler.Delete(ctx, resources.DeleteRequest{
		RequestToken:       request.GetRequestToken(),
		ExpectedGeneration: request.ExpectedGeneration,
		Key:                key,
	})
	if err != nil {
		if IsRetryable(err) {
			return nil, status.Error(codes.Unavailable, err.Error())
		}
		return nil, registryOrReconciliationStatus(err)
	}
	return &epochv1.DeleteResourceResponse{
		Name:       request.GetName(),
		Generation: deleted.Generation,
		Deleted:    deleted.Deleted,
		Replayed:   deleted.Replayed,
	}, nil
}

func desiredFromProto(
	request *epochv1.ApplyResourceRequest,
) (resources.ResourceKey, resources.DesiredResource, error) {
	if request == nil {
		return resources.ResourceKey{}, resources.DesiredResource{}, fmt.Errorf("request is required")
	}
	key, err := keyFromProto(request.GetName())
	if err != nil {
		return resources.ResourceKey{}, resources.DesiredResource{}, err
	}
	spec := request.GetSpec()
	if spec == nil {
		return resources.ResourceKey{}, resources.DesiredResource{}, fmt.Errorf("spec is required")
	}
	expectedProfile := profileForKind(key.Kind)
	if spec.GetWorkloadProfile() != expectedProfile {
		return resources.ResourceKey{}, resources.DesiredResource{}, fmt.Errorf(
			"workload profile %s does not match resource kind %s",
			spec.GetWorkloadProfile(),
			key.Kind,
		)
	}
	if spec.GetReplicas() != 3 {
		return resources.ResourceKey{}, resources.DesiredResource{}, fmt.Errorf(
			"the current regional runtime requires replicas 3",
		)
	}
	configuration := spec.GetConfiguration()
	if configuration == nil {
		return resources.ResourceKey{}, resources.DesiredResource{}, fmt.Errorf(
			"configuration.shard_count is required",
		)
	}
	shards := configuration.GetFields()["shard_count"].GetNumberValue()
	if shards < 1 || shards > math.MaxUint32 || shards != math.Trunc(shards) {
		return resources.ResourceKey{}, resources.DesiredResource{}, fmt.Errorf(
			"configuration.shard_count must be an unsigned 32-bit integer",
		)
	}
	encoded, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(spec)
	if err != nil {
		return resources.ResourceKey{}, resources.DesiredResource{}, err
	}
	return key, resources.DesiredResource{
		ResourceKey: key,
		Labels:      cloneStringMap(spec.GetLabels()),
		Spec:        json.RawMessage(encoded),
	}, nil
}

func keyFromProto(name *epochv1.ResourceName) (resources.ResourceKey, error) {
	if name == nil {
		return resources.ResourceKey{}, fmt.Errorf("resource name is required")
	}
	kind, err := kindFromProto(name.GetKind())
	if err != nil {
		return resources.ResourceKey{}, err
	}
	key := resources.ResourceKey{
		Organization: name.GetOrganization(),
		Project:      name.GetProject(),
		Environment:  name.GetEnvironment(),
		Namespace:    name.GetNamespace(),
		Kind:         kind,
		Name:         name.GetName(),
	}
	// Registry validation remains the single source for name constraints.
	key, err = resources.NormalizeKey(key)
	if err != nil {
		return resources.ResourceKey{}, err
	}
	return key, nil
}

func resourceToProto(resource resources.Resource) (*epochv1.Resource, error) {
	spec := &epochv1.ResourceSpec{}
	if err := protojson.Unmarshal(resource.Spec, spec); err != nil {
		return nil, fmt.Errorf("stored resource spec is not a RegionalAdmin contract: %w", err)
	}
	return &epochv1.Resource{
		Name: &epochv1.ResourceName{
			Organization: resource.Organization,
			Project:      resource.Project,
			Environment:  resource.Environment,
			Namespace:    resource.Namespace,
			Kind:         protoKind(resource.Kind),
			Name:         resource.Name,
		},
		Generation: resource.Generation,
		Spec:       spec,
		Status:     statusToProto(resource.Status),
	}, nil
}

func statusToProto(observed resources.ResourceStatus) *epochv1.ResourceStatus {
	tablets := make([]*epochv1.TabletDescriptor, 0, len(observed.Tablets))
	for _, tablet := range observed.Tablets {
		phase := epochv1.TabletPhase_TABLET_PHASE_PENDING
		if tablet.LeaderNodeID != 0 &&
			len(tablet.VoterNodeIDs) >= int(tablet.DesiredReplicas) {
			phase = epochv1.TabletPhase_TABLET_PHASE_SERVING
		}
		tablets = append(tablets, &epochv1.TabletDescriptor{
			TabletId:           tablet.TabletID,
			ConsensusGroupId:   tablet.ConsensusGroupID,
			ShardIndex:         tablet.ShardIndex,
			TabletEpoch:        tablet.TabletEpoch,
			ResourceGeneration: tablet.ResourceGeneration,
			DesiredReplicas:    tablet.DesiredReplicas,
			VoterNodeIds:       append([]uint64(nil), tablet.VoterNodeIDs...),
			LeaderNodeId:       tablet.LeaderNodeID,
			Phase:              phase,
		})
	}
	conditionState := epochv1.ConditionState_CONDITION_STATE_UNKNOWN
	if observed.Phase == resources.PhaseReady {
		conditionState = epochv1.ConditionState_CONDITION_STATE_TRUE
	} else if observed.Phase == resources.PhaseFailed {
		conditionState = epochv1.ConditionState_CONDITION_STATE_FALSE
	}
	return &epochv1.ResourceStatus{
		Phase:              protoPhase(observed.Phase),
		ObservedGeneration: observed.ObservedGeneration,
		DeploymentMode:     epochv1.DeploymentMode_DEPLOYMENT_MODE_MANAGED,
		Conditions: []*epochv1.Condition{{
			Type:               "Reconciled",
			State:              conditionState,
			Reason:             string(observed.Phase),
			Message:            observed.Message,
			ObservedGeneration: observed.ObservedGeneration,
		}},
		Tablets: tablets,
	}
}

func kindFromProto(kind epochv1.ResourceKind) (resources.Kind, error) {
	switch kind {
	case epochv1.ResourceKind_RESOURCE_KIND_CACHE:
		return resources.KindCache, nil
	case epochv1.ResourceKind_RESOURCE_KIND_TABLE:
		return resources.KindTable, nil
	case epochv1.ResourceKind_RESOURCE_KIND_STREAM:
		return resources.KindStream, nil
	case epochv1.ResourceKind_RESOURCE_KIND_QUEUE:
		return resources.KindQueue, nil
	case epochv1.ResourceKind_RESOURCE_KIND_EVENT_BUS:
		return resources.KindEventBus, nil
	default:
		return "", fmt.Errorf("resource kind %s is not data-bearing", kind)
	}
}

func optionalKindFromProto(kind epochv1.ResourceKind) (resources.Kind, error) {
	if kind == epochv1.ResourceKind_RESOURCE_KIND_UNSPECIFIED {
		return "", nil
	}
	return kindFromProto(kind)
}

func protoKind(kind resources.Kind) epochv1.ResourceKind {
	switch kind {
	case resources.KindCache:
		return epochv1.ResourceKind_RESOURCE_KIND_CACHE
	case resources.KindTable:
		return epochv1.ResourceKind_RESOURCE_KIND_TABLE
	case resources.KindStream:
		return epochv1.ResourceKind_RESOURCE_KIND_STREAM
	case resources.KindQueue:
		return epochv1.ResourceKind_RESOURCE_KIND_QUEUE
	case resources.KindEventBus:
		return epochv1.ResourceKind_RESOURCE_KIND_EVENT_BUS
	default:
		return epochv1.ResourceKind_RESOURCE_KIND_UNSPECIFIED
	}
}

func profileForKind(kind resources.Kind) epochv1.WorkloadProfile {
	switch kind {
	case resources.KindCache:
		return epochv1.WorkloadProfile_WORKLOAD_PROFILE_CACHE
	case resources.KindTable:
		return epochv1.WorkloadProfile_WORKLOAD_PROFILE_STATE_TABLE
	case resources.KindStream:
		return epochv1.WorkloadProfile_WORKLOAD_PROFILE_STREAM_LOG
	case resources.KindQueue:
		return epochv1.WorkloadProfile_WORKLOAD_PROFILE_WORK_QUEUE
	case resources.KindEventBus:
		return epochv1.WorkloadProfile_WORKLOAD_PROFILE_EVENT_BUS
	default:
		return epochv1.WorkloadProfile_WORKLOAD_PROFILE_UNSPECIFIED
	}
}

func protoPhase(phase resources.ResourcePhase) epochv1.ResourcePhase {
	switch phase {
	case resources.PhasePending:
		return epochv1.ResourcePhase_RESOURCE_PHASE_PENDING
	case resources.PhaseReady:
		return epochv1.ResourcePhase_RESOURCE_PHASE_READY
	case resources.PhaseDegraded:
		return epochv1.ResourcePhase_RESOURCE_PHASE_DEGRADED
	case resources.PhaseFailed:
		return epochv1.ResourcePhase_RESOURCE_PHASE_FAILED
	default:
		return epochv1.ResourcePhase_RESOURCE_PHASE_UNSPECIFIED
	}
}

func registryStatus(err error) error {
	var registryError *resources.RegistryError
	if !errors.As(err, &registryError) {
		return status.Error(codes.Internal, "internal control-plane error")
	}
	switch registryError.Code {
	case resources.CodeInvalidArgument:
		return status.Error(codes.InvalidArgument, registryError.Message)
	case resources.CodeNotFound:
		return status.Error(codes.NotFound, registryError.Message)
	case resources.CodeConflict:
		return status.Error(codes.Aborted, registryError.Message)
	default:
		return status.Error(codes.Internal, "internal control-plane error")
	}
}

func reconciliationStatus(err error) error {
	if IsRetryable(err) {
		return status.Error(codes.Unavailable, err.Error())
	}
	return status.Error(codes.FailedPrecondition, err.Error())
}

func registryOrReconciliationStatus(err error) error {
	var registryError *resources.RegistryError
	if errors.As(err, &registryError) {
		return registryStatus(err)
	}
	return reconciliationStatus(err)
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}
