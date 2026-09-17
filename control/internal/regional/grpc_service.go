package regional

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"time"

	controlauth "epoch.local/epoch/control/internal/auth"
	"epoch.local/epoch/control/internal/resources"
	epochv1 "epoch.local/epoch/sdk/go/gen/epoch/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	defaultPageSize        = 50
	maxPageSize            = 100
	defaultChangeBatchSize = 100
	maxChangeBatchSize     = 1_000
	changePollInterval     = 250 * time.Millisecond
)

type batchApplyStore interface {
	BatchApply(context.Context, BatchApplyRequest) (BatchApplyResult, error)
}

type controlOperationStore interface {
	ControlOperation(context.Context, string) (ControlOperation, error)
}

type controlChangeStore interface {
	ControlChanges(context.Context, uint64, uint32) (ControlChangePage, error)
}

// RegionalAdminServer exposes the versioned managed-control contract while
// delegating catalog and placement authority to Rust.
type RegionalAdminServer struct {
	epochv1.UnimplementedRegionalAdminServiceServer
	registry   resources.Store
	reconciler *Reconciler
	policy     *controlauth.Policy
	audit      controlauth.AuditSink
}

// NewRegionalAdminServer constructs the gRPC lifecycle service.
func NewRegionalAdminServer(
	registry resources.Store,
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
	registry resources.Store,
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
	responseResource := applied.Resource
	if reconcileErr == nil {
		responseResource = reconciled
	}
	encoded, err := resourceToProto(responseResource)
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

// GetResource returns desired and achieved state from the replicated Catalog.
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

// ListResources returns one bounded deterministic page. The current contract
// has no continuation token; larger hosted inventory pagination remains open.
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

// BatchApplyResources commits all desired-state changes in one Catalog
// command. Materialization remains an observable reconciliation phase.
func (server *RegionalAdminServer) BatchApplyResources(
	ctx context.Context,
	request *epochv1.BatchApplyResourcesRequest,
) (*epochv1.BatchApplyResourcesResponse, error) {
	if request == nil || len(request.GetResources()) == 0 || len(request.GetResources()) > 128 {
		return nil, status.Error(codes.InvalidArgument, "batch must contain between 1 and 128 resources")
	}
	store, ok := server.registry.(batchApplyStore)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "atomic desired-state batches require the replicated Catalog store")
	}
	batchRequest := BatchApplyRequest{
		RequestToken: request.GetRequestToken(),
		Resources:    make([]resources.ApplyRequest, 0, len(request.GetResources())),
	}
	for _, item := range request.GetResources() {
		if item == nil {
			return nil, status.Error(codes.InvalidArgument, "batch resources cannot be null")
		}
		key, desired, err := desiredFromProto(&epochv1.ApplyResourceRequest{
			Name:               item.GetName(),
			Spec:               item.GetSpec(),
			ExpectedGeneration: item.ExpectedGeneration,
		})
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if err := server.authorize(ctx, controlauth.ActionResourceApply, authScopeFromKey(key)); err != nil {
			return nil, err
		}
		batchRequest.Resources = append(batchRequest.Resources, resources.ApplyRequest{
			ExpectedGeneration: item.ExpectedGeneration,
			Resource:           desired,
		})
	}
	applied, err := store.BatchApply(ctx, batchRequest)
	if err != nil {
		return nil, registryStatus(err)
	}
	response := &epochv1.BatchApplyResourcesResponse{
		Results:  make([]*epochv1.ApplyResourceResponse, 0, len(applied.Results)),
		Replayed: applied.Replayed,
	}
	for _, result := range applied.Results {
		encoded, err := resourceToProto(result.Resource)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		response.Results = append(response.Results, &epochv1.ApplyResourceResponse{
			Resource: encoded,
			Created:  result.Created,
			Changed:  result.Changed,
			Replayed: result.Replayed,
		})
	}
	return response, nil
}

// GetOperation returns one request-token outcome only when the caller supplies
// and is authorized for the exact durable set of affected resources.
func (server *RegionalAdminServer) GetOperation(
	ctx context.Context,
	request *epochv1.GetOperationRequest,
) (*epochv1.GetOperationResponse, error) {
	if request == nil || len(request.GetAffectedResources()) == 0 || len(request.GetAffectedResources()) > 128 {
		return nil, status.Error(codes.InvalidArgument, "affected_resources must contain between 1 and 128 names")
	}
	store, ok := server.registry.(controlOperationStore)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "operation lookup requires the replicated Catalog store")
	}
	expected := make([]resources.ResourceKey, 0, len(request.GetAffectedResources()))
	for _, name := range request.GetAffectedResources() {
		key, err := keyFromProto(name)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if err := server.authorize(ctx, controlauth.ActionResourceRead, authScopeFromKey(key)); err != nil {
			return nil, err
		}
		expected = append(expected, key)
	}
	sortResourceKeys(expected)
	if adjacentResourceKeyDuplicate(expected) {
		return nil, status.Error(codes.InvalidArgument, "affected_resources must be distinct")
	}
	operation, err := store.ControlOperation(ctx, request.GetRequestToken())
	if err != nil {
		return nil, registryStatus(err)
	}
	// An uncommitted proposal has no durable affected-resource set yet. Do not
	// reveal even its existence to a caller that guessed the token and supplied
	// an unrelated scope; the committed outcome becomes queryable once Catalog
	// can authorize its exact identities.
	if len(operation.ResourceKeys) == 0 {
		return nil, status.Error(codes.NotFound, "operation was not found for the affected resources")
	}
	if !slices.Equal(operation.ResourceKeys, expected) {
		return nil, status.Error(codes.NotFound, "operation was not found for the affected resources")
	}
	return &epochv1.GetOperationResponse{
		RequestToken:      operation.RequestToken,
		ProposalId:        operation.ProposalID,
		State:             protoOperationState(operation.State),
		AffectedResources: protoResourceNames(expected),
		FailureCode:       operation.FailureCode,
		FailureMessage:    operation.FailureMessage,
		FirstChangeCursor: operation.FirstChangeCursor,
		LastChangeCursor:  operation.LastChangeCursor,
	}, nil
}

// WatchResourceChanges streams global-cursor checkpoints with only changes
// visible to the authenticated principal and requested exact filter.
func (server *RegionalAdminServer) WatchResourceChanges(
	request *epochv1.WatchResourceChangesRequest,
	stream grpc.ServerStreamingServer[epochv1.WatchResourceChangesResponse],
) error {
	if request == nil {
		return status.Error(codes.InvalidArgument, "request is required")
	}
	store, ok := server.registry.(controlChangeStore)
	if !ok {
		return status.Error(codes.Unimplemented, "resource change watch requires the replicated Catalog store")
	}
	batchSize := request.GetBatchSize()
	if batchSize == 0 {
		batchSize = defaultChangeBatchSize
	}
	if batchSize > maxChangeBatchSize {
		return status.Errorf(codes.InvalidArgument, "batch_size must be between 1 and %d", maxChangeBatchSize)
	}
	kind, err := optionalKindFromProto(request.GetKind())
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	principal, err := server.authorizeCollection(stream.Context(), controlauth.ActionResourceRead)
	if err != nil {
		return err
	}
	filter := resources.ListFilter{
		Organization: request.GetOrganization(),
		Project:      request.GetProject(),
		Environment:  request.GetEnvironment(),
		Namespace:    request.GetNamespace(),
		Kind:         kind,
	}
	cursor := request.GetAfterCursor()
	first := true
	for {
		page, err := store.ControlChanges(stream.Context(), cursor, batchSize)
		if err != nil {
			return registryStatus(err)
		}
		nextCursor := cursor
		if len(page.Changes) > 0 {
			nextCursor = page.Changes[len(page.Changes)-1].Cursor
		}
		if first || nextCursor > cursor {
			response := &epochv1.WatchResourceChangesResponse{
				EarliestCursor: page.EarliestCursor,
				LatestCursor:   page.LatestCursor,
				NextCursor:     nextCursor,
				Changes:        make([]*epochv1.ResourceChange, 0, len(page.Changes)),
			}
			for _, change := range page.Changes {
				if !resourceKeyMatchesFilter(change.Key, filter) ||
					(server.policy != nil && !principal.Allows(controlauth.ActionResourceRead, authScopeFromKey(change.Key))) {
					continue
				}
				response.Changes = append(response.Changes, &epochv1.ResourceChange{
					Cursor:     change.Cursor,
					Kind:       protoControlChangeKind(change.Kind),
					Name:       protoResourceName(change.Key),
					Generation: change.Generation,
				})
			}
			if err := stream.Send(response); err != nil {
				return err
			}
			cursor = nextCursor
			first = false
			if cursor < page.LatestCursor && len(page.Changes) == int(batchSize) {
				continue
			}
		}
		timer := time.NewTimer(changePollInterval)
		select {
		case <-stream.Context().Done():
			if !timer.Stop() {
				<-timer.C
			}
			return stream.Context().Err()
		case <-timer.C:
		}
	}
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
	if err := server.recordAuthorization(ctx, principal, action, scope, allowed); err != nil {
		return status.Error(codes.Unavailable, "audit journal is unavailable")
	}
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
	if err := server.recordAuthorization(ctx, principal, action, principal.Scope(), allowed); err != nil {
		return controlauth.Principal{}, status.Error(codes.Unavailable, "audit journal is unavailable")
	}
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
) error {
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
	return server.audit.Record(ctx, controlauth.DecisionEvent{
		Timestamp:            time.Now().UTC(),
		RequestID:            requestID,
		PrincipalID:          principal.ID(),
		PolicyID:             principal.PolicyID(),
		AuthenticationMethod: principal.AuthenticationMethod(),
		Action:               action,
		Decision:             decision,
		Reason:               reason,
		Scope:                scope,
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
	if spec.GetReplicas() != 3 && spec.GetReplicas() != 5 {
		return resources.ResourceKey{}, resources.DesiredResource{}, fmt.Errorf(
			"the regional runtime requires replicas 3 or 5",
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
			len(tablet.TargetVoterNodeIDs) == 0 &&
			len(tablet.VoterNodeIDs) == int(tablet.DesiredReplicas) &&
			len(tablet.ReachableVoterNodeIDs) == int(tablet.DesiredReplicas) {
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
			AssignedNodeIds:    append([]uint64(nil), tablet.AssignedNodeIDs...),
			ReachableVoterNodeIds: append(
				[]uint64(nil),
				tablet.ReachableVoterNodeIDs...,
			),
			BootstrapVoterNodeIds: append(
				[]uint64(nil),
				tablet.BootstrapVoterNodeIDs...,
			),
			TargetVoterNodeIds: append([]uint64(nil), tablet.TargetVoterNodeIDs...),
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
		CatalogGeneration:  observed.EffectiveCatalogGeneration(),
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
			Rack:                     node.Rack,
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
		MinimumRacks:      observed.MinimumRacks,
		RequiredNodeClass: observed.RequiredNodeClass,
		ExcludedNodeIds:   append([]uint64(nil), observed.ExcludedNodeIDs...),
		AchievedZones:     observed.AchievedZones,
		AchievedRacks:     observed.AchievedRacks,
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
	case resources.CodeUnavailable:
		return status.Error(codes.Unavailable, registryError.Message)
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

func protoResourceName(key resources.ResourceKey) *epochv1.ResourceName {
	return &epochv1.ResourceName{
		Organization: key.Organization,
		Project:      key.Project,
		Environment:  key.Environment,
		Namespace:    key.Namespace,
		Kind:         protoKind(key.Kind),
		Name:         key.Name,
	}
}

func protoResourceNames(keys []resources.ResourceKey) []*epochv1.ResourceName {
	names := make([]*epochv1.ResourceName, 0, len(keys))
	for _, key := range keys {
		names = append(names, protoResourceName(key))
	}
	return names
}

func sortResourceKeys(keys []resources.ResourceKey) {
	sort.Slice(keys, func(left, right int) bool {
		return controlNameLess(controlName(keys[left]), controlName(keys[right]))
	})
}

func adjacentResourceKeyDuplicate(keys []resources.ResourceKey) bool {
	for index := 1; index < len(keys); index++ {
		if keys[index] == keys[index-1] {
			return true
		}
	}
	return false
}

func protoOperationState(state ControlOperationState) epochv1.OperationState {
	switch state {
	case ControlOperationPending:
		return epochv1.OperationState_OPERATION_STATE_PENDING
	case ControlOperationSucceeded:
		return epochv1.OperationState_OPERATION_STATE_SUCCEEDED
	case ControlOperationFailed:
		return epochv1.OperationState_OPERATION_STATE_FAILED
	default:
		return epochv1.OperationState_OPERATION_STATE_UNSPECIFIED
	}
}

func protoControlChangeKind(kind ControlChangeKind) epochv1.ResourceChangeKind {
	switch kind {
	case ControlChangeDesiredApplied:
		return epochv1.ResourceChangeKind_RESOURCE_CHANGE_KIND_DESIRED_APPLIED
	case ControlChangeDesiredDeleted:
		return epochv1.ResourceChangeKind_RESOURCE_CHANGE_KIND_DESIRED_DELETED
	case ControlChangeStatusUpdated:
		return epochv1.ResourceChangeKind_RESOURCE_CHANGE_KIND_STATUS_UPDATED
	default:
		return epochv1.ResourceChangeKind_RESOURCE_CHANGE_KIND_UNSPECIFIED
	}
}

func resourceKeyMatchesFilter(key resources.ResourceKey, filter resources.ListFilter) bool {
	return (filter.Organization == "" || key.Organization == filter.Organization) &&
		(filter.Project == "" || key.Project == filter.Project) &&
		(filter.Environment == "" || key.Environment == filter.Environment) &&
		(filter.Namespace == "" || key.Namespace == filter.Namespace) &&
		(filter.Kind == "" || key.Kind == filter.Kind)
}
