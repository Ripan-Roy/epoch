package regional

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	controlauth "epoch.local/epoch/control/internal/auth"
	"epoch.local/epoch/control/internal/resources"
	epochv1 "epoch.local/epoch/sdk/go/gen/epoch/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
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
	policy     *controlauth.Policy
	audit      controlauth.AuditSink
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

// NewAuthenticatedRegionalAdminServer constructs the public lifecycle service
// with explicit per-action and per-tenant authorization.
func NewAuthenticatedRegionalAdminServer(
	registry *resources.Registry,
	reconciler *Reconciler,
	policy *controlauth.Policy,
	audit controlauth.AuditSink,
) *RegionalAdminServer {
	server := NewRegionalAdminServer(registry, reconciler)
	if policy == nil {
		panic("regional: nil auth policy")
	}
	if audit == nil {
		panic("regional: nil auth audit sink")
	}
	server.policy = policy
	server.audit = audit
	return server
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
	if err := server.authorize(
		ctx,
		controlauth.ActionResourceApply,
		authScopeFromKey(key),
	); err != nil {
		return nil, err
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
	ctx context.Context,
	request *epochv1.GetResourceRequest,
) (*epochv1.GetResourceResponse, error) {
	key, err := keyFromProto(request.GetName())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := server.authorize(
		ctx,
		controlauth.ActionResourceRead,
		authScopeFromKey(key),
	); err != nil {
		return nil, err
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
	ctx context.Context,
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
	principal, err := server.authorizeCollection(ctx, controlauth.ActionResourceRead)
	if err != nil {
		return nil, err
	}
	listed, err := server.registry.List(resources.ListFilter{
		Organization:   request.GetOrganization(),
		Project:        request.GetProject(),
		Environment:    request.GetEnvironment(),
		Namespace:      request.GetNamespace(),
		Kind:           kind,
		Owner:          request.GetOwner(),
		CostCenter:     request.GetCostCenter(),
		Classification: optionalClassificationFromProto(request.GetClassification()),
		Tags:           cloneStringMap(request.GetTags()),
	})
	if err != nil {
		return nil, registryStatus(err)
	}
	if server.policy != nil {
		authorized := listed[:0]
		for _, resource := range listed {
			if principal.Allows(
				controlauth.ActionResourceRead,
				authScopeFromKey(resource.ResourceKey),
			) {
				authorized = append(authorized, resource)
			}
		}
		listed = authorized
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
	if err := server.authorize(
		ctx,
		controlauth.ActionResourceDelete,
		authScopeFromKey(key),
	); err != nil {
		return nil, err
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

func (server *RegionalAdminServer) authorize(
	ctx context.Context,
	action controlauth.Action,
	scope controlauth.Scope,
) error {
	if server.policy == nil {
		return nil
	}
	principal, ok := controlauth.PrincipalFromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "authentication required")
	}
	allowed := principal.Allows(action, scope)
	server.recordAuthorization(ctx, principal, action, scope, allowed)
	if !allowed {
		return status.Error(
			codes.PermissionDenied,
			"principal is not authorized for this resource action",
		)
	}
	return nil
}

func (server *RegionalAdminServer) authorizeCollection(
	ctx context.Context,
	action controlauth.Action,
) (controlauth.Principal, error) {
	if server.policy == nil {
		return controlauth.Principal{}, nil
	}
	principal, ok := controlauth.PrincipalFromContext(ctx)
	if !ok {
		return controlauth.Principal{}, status.Error(
			codes.Unauthenticated,
			"authentication required",
		)
	}
	allowed := principal.HasAction(action)
	server.recordAuthorization(ctx, principal, action, principal.Scope(), allowed)
	if !allowed {
		return controlauth.Principal{}, status.Error(
			codes.PermissionDenied,
			"principal is not authorized for this resource action",
		)
	}
	return principal, nil
}

func (server *RegionalAdminServer) recordAuthorization(
	ctx context.Context,
	principal controlauth.Principal,
	action controlauth.Action,
	scope controlauth.Scope,
	allowed bool,
) {
	decision := controlauth.DecisionDeny
	reason := controlauth.ReasonActionNotGranted
	if allowed {
		decision = controlauth.DecisionAllow
		reason = controlauth.ReasonPolicyGrant
	} else if principal.HasAction(action) {
		reason = controlauth.ReasonScopeMismatch
	}
	requestID, ok := controlauth.RequestIDFromContext(ctx)
	if !ok {
		requestID = "internal-request"
	}
	server.audit.Record(ctx, controlauth.DecisionEvent{
		Timestamp:   time.Now().UTC(),
		RequestID:   requestID,
		PrincipalID: principal.ID(),
		PolicyID:    principal.PolicyID(),
		Action:      action,
		Decision:    decision,
		Reason:      reason,
		Scope:       scope,
	})
}

func authScopeFromKey(key resources.ResourceKey) controlauth.Scope {
	return controlauth.Scope{
		Organization: key.Organization,
		Project:      key.Project,
		Environment:  key.Environment,
		Namespace:    key.Namespace,
	}
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
	if spec.GetGovernance() == nil {
		return resources.ResourceKey{}, resources.DesiredResource{}, fmt.Errorf(
			"spec.governance is required",
		)
	}
	governance, err := governanceFromProto(spec.GetGovernance())
	if err != nil {
		return resources.ResourceKey{}, resources.DesiredResource{}, err
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
	normalizedSpec := proto.Clone(spec).(*epochv1.ResourceSpec)
	normalizedSpec.Governance = governanceToProto(governance)
	encoded, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(normalizedSpec)
	if err != nil {
		return resources.ResourceKey{}, resources.DesiredResource{}, err
	}
	return key, resources.DesiredResource{
		ResourceKey: key,
		Labels:      cloneStringMap(spec.GetLabels()),
		Governance:  governance,
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
	spec.Governance = governanceToProto(resource.Governance)
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

func governanceFromProto(
	governance *epochv1.ResourceGovernance,
) (*resources.ResourceGovernance, error) {
	if governance == nil {
		return nil, nil
	}
	classification := classificationFromProto(governance.GetClassification())
	normalized, err := resources.NormalizeGovernance(&resources.ResourceGovernance{
		Owner:          governance.GetOwner(),
		CostCenter:     governance.GetCostCenter(),
		Classification: classification,
		Tags:           cloneStringMap(governance.GetTags()),
	})
	if err != nil {
		return nil, err
	}
	return normalized, nil
}

func governanceToProto(governance *resources.ResourceGovernance) *epochv1.ResourceGovernance {
	if governance == nil {
		return nil
	}
	return &epochv1.ResourceGovernance{
		Owner:          governance.Owner,
		CostCenter:     governance.CostCenter,
		Classification: protoClassification(governance.Classification),
		Tags:           cloneStringMap(governance.Tags),
	}
}

func classificationFromProto(classification epochv1.DataClassification) resources.DataClassification {
	switch classification {
	case epochv1.DataClassification_DATA_CLASSIFICATION_PUBLIC:
		return resources.ClassificationPublic
	case epochv1.DataClassification_DATA_CLASSIFICATION_INTERNAL:
		return resources.ClassificationInternal
	case epochv1.DataClassification_DATA_CLASSIFICATION_CONFIDENTIAL:
		return resources.ClassificationConfidential
	case epochv1.DataClassification_DATA_CLASSIFICATION_RESTRICTED:
		return resources.ClassificationRestricted
	default:
		return resources.ClassificationUnspecified
	}
}

func optionalClassificationFromProto(
	classification epochv1.DataClassification,
) resources.DataClassification {
	if classification == epochv1.DataClassification_DATA_CLASSIFICATION_UNSPECIFIED {
		return ""
	}
	return classificationFromProto(classification)
}

func protoClassification(classification resources.DataClassification) epochv1.DataClassification {
	switch classification {
	case resources.ClassificationPublic:
		return epochv1.DataClassification_DATA_CLASSIFICATION_PUBLIC
	case resources.ClassificationInternal:
		return epochv1.DataClassification_DATA_CLASSIFICATION_INTERNAL
	case resources.ClassificationConfidential:
		return epochv1.DataClassification_DATA_CLASSIFICATION_CONFIDENTIAL
	case resources.ClassificationRestricted:
		return epochv1.DataClassification_DATA_CLASSIFICATION_RESTRICTED
	default:
		return epochv1.DataClassification_DATA_CLASSIFICATION_UNSPECIFIED
	}
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
		Tablets:   tablets,
		Placement: placementToProto(observed.Placement),
	}
}

func placementToProto(observed *resources.PlacementStatus) *epochv1.PlacementStatus {
	if observed == nil {
		return nil
	}
	nodes := make([]*epochv1.RegionalNodeObservation, 0, len(observed.Nodes))
	for _, node := range observed.Nodes {
		nodes = append(nodes, &epochv1.RegionalNodeObservation{
			NodeId:                   node.NodeID,
			Region:                   node.Region,
			Zone:                     node.Zone,
			NodeClass:                node.NodeClass,
			ConsensusVoterNodeIds:    append([]uint64(nil), node.ConsensusVoterNodeIDs...),
			MaxConsensusGroups:       node.MaxConsensusGroups,
			UsedConsensusGroups:      node.UsedConsensusGroups,
			AvailableConsensusGroups: node.AvailableConsensusGroups,
		})
	}
	return &epochv1.PlacementStatus{
		AllowedRegions:    append([]string(nil), observed.AllowedRegions...),
		MinimumZones:      observed.MinimumZones,
		RequiredNodeClass: observed.RequiredNodeClass,
		AchievedZones:     observed.AchievedZones,
		Nodes:             nodes,
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
