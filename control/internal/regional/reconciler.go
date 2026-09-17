// Package regional reconciles Go-owned desired metadata through the Rust
// regional catalog authority without taking ownership of customer data.
package regional

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	"epoch.local/epoch/control/internal/resources"
)

// AuthorityApplyRequest is the minimal desired state accepted by the current
// Rust regional catalog API.
type AuthorityApplyRequest struct {
	RequestToken       string
	Key                resources.ResourceKey
	ExpectedGeneration uint64
	ShardCount         uint32
	ReplicaCount       uint16
	TabletPlacements   []TabletPlacement
	Configuration      map[string]any
	Governance         *resources.ResourceGovernance
}

// AuthorityControlLease fences every managed topology mutation to one current
// Go controller instance.
type AuthorityControlLease struct {
	OwnerID string
	Fence   uint64
	NowMS   uint64
}

// AuthorityManagedApplyRequest atomically validates desired generation,
// controller ownership, and the complete capacity observation with the native
// Catalog apply.
type AuthorityManagedApplyRequest struct {
	AuthorityApplyRequest
	DesiredGeneration uint64
	Lease             AuthorityControlLease
}

// AuthorityManagedMembershipPlanRequest extends a learner-first transition
// with replicated desired-state and controller-ownership fences.
type AuthorityManagedMembershipPlanRequest struct {
	AuthorityMembershipPlanRequest
	DesiredGeneration uint64
	Lease             AuthorityControlLease
}

// AuthorityObservation contains achieved catalog identity and placement.
type AuthorityObservation struct {
	Generation uint64
	Tablets    []resources.TabletStatus
}

// AuthorityDeleteRequest removes one observed catalog generation.
type AuthorityDeleteRequest struct {
	RequestToken       string
	Key                resources.ResourceKey
	ExpectedGeneration uint64
}

// AuthorityDeleteObservation reports the monotonic catalog tombstone.
type AuthorityDeleteObservation struct {
	Generation uint64
	Deleted    bool
}

// AuthorityMembershipPlanRequest commits one generation- and epoch-fenced
// learner-first target for an existing tablet.
type AuthorityMembershipPlanRequest struct {
	RequestToken               string
	Key                        resources.ResourceKey
	TabletID                   uint64
	ExpectedTabletEpoch        uint64
	ExpectedResourceGeneration uint64
	TargetVoterNodeIDs         []uint64
}

// Authority is the narrow boundary owned by regional Rust control.
type Authority interface {
	Inventory(context.Context) (NodeInventory, error)
	Apply(context.Context, AuthorityApplyRequest) (AuthorityObservation, error)
	Observe(context.Context, resources.ResourceKey) (AuthorityObservation, error)
	PlanMembership(context.Context, AuthorityMembershipPlanRequest) (AuthorityObservation, error)
	Delete(context.Context, AuthorityDeleteRequest) (AuthorityDeleteObservation, error)
}

type managedAuthority interface {
	ApplyManaged(context.Context, AuthorityManagedApplyRequest) (AuthorityObservation, error)
	PlanManagedMembership(
		context.Context,
		AuthorityManagedMembershipPlanRequest,
	) (AuthorityObservation, error)
}

type controlLeaseStore interface {
	ControlLease() (AuthorityControlLease, error)
}

type managedDeleteStore interface {
	DeleteManaged(
		context.Context,
		resources.DeleteRequest,
		uint64,
		uint64,
	) (resources.DeleteResult, error)
}

type managedDeleteReplayStore interface {
	ReplayManagedDelete(
		context.Context,
		resources.DeleteRequest,
	) (resources.DeleteResult, bool, error)
}

type authorityErrorKind uint8

const (
	authorityUnavailable authorityErrorKind = iota + 1
	authorityConflict
	authorityInvalid
)

type authorityError struct {
	kind    authorityErrorKind
	message string
}

func (err *authorityError) Error() string {
	return err.message
}

func availabilityError(message string) error {
	return &authorityError{kind: authorityUnavailable, message: message}
}

func conflictError(message string) error {
	return &authorityError{kind: authorityConflict, message: message}
}

func invalidAuthorityError(message string) error {
	return &authorityError{kind: authorityInvalid, message: message}
}

type reconcileError struct {
	message   string
	retryable bool
	cause     error
}

func (err *reconcileError) Error() string {
	return err.message
}

func (err *reconcileError) Unwrap() error {
	return err.cause
}

// IsRetryable reports whether another reconciliation attempt may succeed
// without changing desired input.
func IsRetryable(err error) bool {
	var reconciliation *reconcileError
	if errors.As(err, &reconciliation) {
		return reconciliation.retryable
	}
	var authority *authorityError
	return errors.As(err, &authority) && authority.kind == authorityUnavailable
}

// Reconciler generation-fences all observed status updates.
type Reconciler struct {
	registry  resources.Store
	authority Authority
	observer  ReconcileObserver
	mutations sync.Mutex
}

// ReconcileObserver records bounded reconciliation outcomes.
type ReconcileObserver interface {
	ObserveReconcile(time.Duration, error)
}

// NewReconciler constructs a regional reconciler.
func NewReconciler(registry resources.Store, authority Authority) *Reconciler {
	return NewObservedReconciler(registry, authority, nil)
}

// NewObservedReconciler constructs a reconciler with lifecycle metrics.
func NewObservedReconciler(
	registry resources.Store,
	authority Authority,
	observer ReconcileObserver,
) *Reconciler {
	if registry == nil {
		panic("regional: nil resource registry")
	}
	if authority == nil {
		panic("regional: nil authority")
	}
	return &Reconciler{registry: registry, authority: authority, observer: observer}
}

type desiredSpec struct {
	ShardCount    uint32          `json:"shard_count"`
	ReplicaCount  uint16          `json:"replica_count"`
	Replicas      uint32          `json:"replicas"`
	Placement     PlacementPolicy `json:"placement"`
	Configuration map[string]any  `json:"configuration"`
}

// Reconcile applies a new desired generation once, then observes the already
// applied generation until placement converges.
func (reconciler *Reconciler) Reconcile(
	ctx context.Context,
	key resources.ResourceKey,
) (result resources.Resource, reconcileErr error) {
	started := time.Now()
	if reconciler.observer != nil {
		defer func() {
			reconciler.observer.ObserveReconcile(time.Since(started), reconcileErr)
		}()
	}
	reconciler.mutations.Lock()
	defer reconciler.mutations.Unlock()

	resource, err := reconciler.registry.Get(key)
	if err != nil {
		return resources.Resource{}, err
	}
	spec, err := decodeDesiredSpec(resource.Spec)
	if err != nil {
		return reconciler.fail(resource, false, err)
	}
	var observation AuthorityObservation
	var placement PlacementDecision
	var inventory NodeInventory
	var freshInventory bool
	applyingDesiredGeneration := resource.Status.ObservedGeneration < resource.Generation
	expectedCatalogGeneration := resource.Status.EffectiveCatalogGeneration()
	if applyingDesiredGeneration {
		existingPlacements := placementsFromStatus(resource.Status.Tablets)
		placement, inventory, freshInventory, err = reconciler.admit(
			ctx,
			resource,
			spec,
			existingPlacements,
		)
		if err != nil {
			return reconciler.fail(resource, IsRetryable(err), err)
		}
		applyRequest := AuthorityApplyRequest{
			RequestToken:       applyToken(resource),
			Key:                resource.ResourceKey,
			ExpectedGeneration: expectedCatalogGeneration,
			ShardCount:         spec.ShardCount,
			ReplicaCount:       spec.ReplicaCount,
			TabletPlacements:   cloneTabletPlacements(placement.TabletPlacements),
			Configuration:      spec.Configuration,
			Governance:         resource.Governance,
		}
		leaseStore, replicatedStore := reconciler.registry.(controlLeaseStore)
		managed, managedAuthorityAvailable := reconciler.authority.(managedAuthority)
		if replicatedStore != managedAuthorityAvailable {
			return reconciler.fail(
				resource,
				false,
				invalidAuthorityError("replicated registry and managed authority must be configured together"),
			)
		}
		if replicatedStore {
			lease, leaseErr := leaseStore.ControlLease()
			if leaseErr != nil {
				return reconciler.fail(resource, true, leaseErr)
			}
			observation, err = managed.ApplyManaged(ctx, AuthorityManagedApplyRequest{
				AuthorityApplyRequest: applyRequest,
				DesiredGeneration:     resource.Generation,
				Lease:                 lease,
			})
		} else {
			observation, err = reconciler.authority.Apply(ctx, applyRequest)
		}
	} else {
		observation, err = reconciler.authority.Observe(ctx, resource.ResourceKey)
		if err == nil {
			placement, inventory, freshInventory, err = reconciler.admit(
				ctx,
				resource,
				spec,
				placementsFromObservation(observation),
			)
		}
	}
	if err != nil {
		return reconciler.fail(resource, IsRetryable(err), err)
	}
	if err := validateObservation(
		resource,
		spec,
		placement,
		observation,
		expectedCatalogGeneration,
		applyingDesiredGeneration,
	); err != nil {
		return reconciler.fail(resource, false, err)
	}
	transitionReason := PlacementTransitionReason("")
	if !applyingDesiredGeneration && freshInventory && !hasActiveMembershipTransition(observation) {
		transition, planErr := PlanNextPlacementTransition(
			spec.Placement,
			uint32(spec.ReplicaCount),
			placementsFromObservation(observation),
			inventory,
		)
		if planErr != nil {
			return reconciler.fail(resource, IsRetryable(planErr), planErr)
		}
		if transition != nil {
			tablet, found := observedTabletForShard(observation.Tablets, transition.ShardIndex)
			if found && membershipTransitionSafe(tablet, transition.Reason) {
				observation, placement, planErr = reconciler.commitPlacementTransition(
					ctx,
					resource,
					spec,
					placement,
					observation,
					*transition,
				)
				if planErr != nil {
					return reconciler.fail(resource, IsRetryable(planErr), planErr)
				}
				transitionReason = transition.Reason
			}
		}
	}

	status := statusFromObservation(
		resource.Generation,
		spec,
		placement,
		observation,
		transitionReason,
	)
	updated, err := reconciler.registry.UpdateStatus(
		resource.ResourceKey,
		resource.Generation,
		status,
	)
	if err != nil {
		return resources.Resource{}, staleObservationError(err)
	}
	return updated, nil
}

func placementsFromObservation(observation AuthorityObservation) []TabletPlacement {
	placements := make([]TabletPlacement, 0, len(observation.Tablets))
	for _, tablet := range observation.Tablets {
		voters := tablet.AssignedNodeIDs
		if len(tablet.TargetVoterNodeIDs) > 0 {
			voters = tablet.TargetVoterNodeIDs
		}
		placements = append(placements, TabletPlacement{
			ShardIndex:   tablet.ShardIndex,
			VoterNodeIDs: append([]uint64(nil), voters...),
		})
	}
	return placements
}

func (reconciler *Reconciler) admit(
	ctx context.Context,
	resource resources.Resource,
	spec desiredSpec,
	existing []TabletPlacement,
) (PlacementDecision, NodeInventory, bool, error) {
	inventory, inventoryErr := reconciler.authority.Inventory(ctx)
	if inventoryErr == nil {
		decision, err := AdmitPlacement(
			spec.Placement,
			uint32(spec.ReplicaCount),
			spec.ShardCount,
			existing,
			inventory,
		)
		return decision, inventory, true, err
	}
	// A catalog mutation always requires a fresh, complete capacity sample.
	if resource.Status.ObservedGeneration < resource.Generation ||
		!IsRetryable(inventoryErr) ||
		resource.Status.Placement == nil {
		return PlacementDecision{}, NodeInventory{}, false, inventoryErr
	}
	// During a transient node outage, the last generation-fenced admission
	// remains evidence of intended fixed-voter topology. Route sampling below
	// still determines current serving voters and degrades honestly.
	fallback := inventoryFromStatus(resource.Status.Placement)
	decision, err := AdmitPlacement(
		spec.Placement,
		uint32(spec.ReplicaCount),
		spec.ShardCount,
		existing,
		fallback,
	)
	return decision, fallback, false, err
}

func inventoryFromStatus(status *resources.PlacementStatus) NodeInventory {
	nodes := make([]RegionalNode, 0, len(status.Nodes))
	for _, node := range status.Nodes {
		nodes = append(nodes, RegionalNode{
			NodeID:                   node.NodeID,
			Region:                   node.Region,
			Zone:                     node.Zone,
			Rack:                     legacyRack(node.Rack),
			NodeClass:                node.NodeClass,
			ConsensusVoterNodeIDs:    append([]uint64(nil), node.ConsensusVoterNodeIDs...),
			MaxConsensusGroups:       node.MaxConsensusGroups,
			UsedConsensusGroups:      node.UsedConsensusGroups,
			AvailableConsensusGroups: node.AvailableConsensusGroups,
		})
	}
	return NodeInventory{Nodes: nodes}
}

func legacyRack(rack string) string {
	if rack != "" {
		return rack
	}
	return "unassigned"
}

func hasActiveMembershipTransition(observation AuthorityObservation) bool {
	for _, tablet := range observation.Tablets {
		if len(tablet.TargetVoterNodeIDs) > 0 {
			return true
		}
	}
	return false
}

func (reconciler *Reconciler) commitPlacementTransition(
	ctx context.Context,
	resource resources.Resource,
	spec desiredSpec,
	placement PlacementDecision,
	observation AuthorityObservation,
	transition PlacementTransition,
) (AuthorityObservation, PlacementDecision, error) {
	tablet, ok := observedTabletForShard(observation.Tablets, transition.ShardIndex)
	if !ok || !slices.Equal(tablet.AssignedNodeIDs, transition.CurrentVoterNodeIDs) {
		return AuthorityObservation{}, PlacementDecision{}, invalidAuthorityError(
			"automatic membership plan no longer matches the observed tablet assignment",
		)
	}
	request := AuthorityMembershipPlanRequest{
		RequestToken:               membershipPlanToken(resource, tablet, transition),
		Key:                        resource.ResourceKey,
		TabletID:                   tablet.TabletID,
		ExpectedTabletEpoch:        tablet.TabletEpoch,
		ExpectedResourceGeneration: tablet.ResourceGeneration,
		TargetVoterNodeIDs:         append([]uint64(nil), transition.TargetVoterNodeIDs...),
	}
	leaseStore, replicatedStore := reconciler.registry.(controlLeaseStore)
	managed, managedAuthorityAvailable := reconciler.authority.(managedAuthority)
	if replicatedStore != managedAuthorityAvailable {
		return AuthorityObservation{}, PlacementDecision{}, invalidAuthorityError(
			"replicated registry and managed authority must be configured together",
		)
	}
	var planned AuthorityObservation
	var err error
	if replicatedStore {
		lease, leaseErr := leaseStore.ControlLease()
		if leaseErr != nil {
			return AuthorityObservation{}, PlacementDecision{}, leaseErr
		}
		planned, err = managed.PlanManagedMembership(ctx, AuthorityManagedMembershipPlanRequest{
			AuthorityMembershipPlanRequest: request,
			DesiredGeneration:              resource.Generation,
			Lease:                          lease,
		})
	} else {
		planned, err = reconciler.authority.PlanMembership(ctx, request)
	}
	if err != nil {
		return AuthorityObservation{}, PlacementDecision{}, err
	}
	placement = placementAfterTransition(placement, transition)
	if err := validateObservation(
		resource,
		spec,
		placement,
		planned,
		observation.Generation,
		false,
	); err != nil {
		return AuthorityObservation{}, PlacementDecision{}, err
	}
	return planned, placement, nil
}

func observedTabletForShard(
	tablets []resources.TabletStatus,
	shard uint32,
) (resources.TabletStatus, bool) {
	for _, tablet := range tablets {
		if tablet.ShardIndex == shard {
			return tablet, true
		}
	}
	return resources.TabletStatus{}, false
}

func membershipTransitionSafe(
	tablet resources.TabletStatus,
	reason PlacementTransitionReason,
) bool {
	if tablet.LeaderNodeID == 0 ||
		!slices.Equal(tablet.AssignedNodeIDs, tablet.VoterNodeIDs) ||
		!slices.Contains(tablet.ReachableVoterNodeIDs, tablet.LeaderNodeID) {
		return false
	}
	requiredReachable := len(tablet.VoterNodeIDs)
	if reason != TransitionRebalance {
		requiredReachable = len(tablet.VoterNodeIDs)/2 + 1
	}
	return len(tablet.ReachableVoterNodeIDs) >= requiredReachable
}

func membershipPlanToken(
	resource resources.Resource,
	tablet resources.TabletStatus,
	transition PlacementTransition,
) string {
	encoded, err := json.Marshal(struct {
		Operation          string                `json:"operation"`
		Key                resources.ResourceKey `json:"key"`
		Generation         uint64                `json:"generation"`
		TabletID           uint64                `json:"tablet_id"`
		TabletEpoch        uint64                `json:"tablet_epoch"`
		TargetVoterNodeIDs []uint64              `json:"target_voter_node_ids"`
	}{
		Operation:          "plan-membership",
		Key:                resource.ResourceKey,
		Generation:         resource.Generation,
		TabletID:           tablet.TabletID,
		TabletEpoch:        tablet.TabletEpoch,
		TargetVoterNodeIDs: transition.TargetVoterNodeIDs,
	})
	if err != nil {
		panic("validated membership transition must encode")
	}
	digest := sha256.Sum256(encoded)
	return "epoch-control.plan-membership.v1." + hex.EncodeToString(digest[:])
}

func placementAfterTransition(
	decision PlacementDecision,
	transition PlacementTransition,
) PlacementDecision {
	decision.TabletPlacements = cloneTabletPlacements(decision.TabletPlacements)
	for index := range decision.TabletPlacements {
		if decision.TabletPlacements[index].ShardIndex == transition.ShardIndex {
			decision.TabletPlacements[index].VoterNodeIDs = append(
				[]uint64(nil),
				transition.TargetVoterNodeIDs...,
			)
			break
		}
	}
	decision.AdditionalGroupsByNode = cloneGroupReservations(decision.AdditionalGroupsByNode)
	for _, nodeID := range transition.TargetVoterNodeIDs {
		if !slices.Contains(transition.CurrentVoterNodeIDs, nodeID) {
			decision.AdditionalGroupsByNode[nodeID]++
		}
	}
	nodes := make(map[uint64]RegionalNode, len(decision.Nodes))
	for _, node := range decision.Nodes {
		nodes[node.NodeID] = node
	}
	decision.AchievedZones = minimumPlacementZones(decision.TabletPlacements, nodes)
	decision.AchievedRacks = minimumPlacementRacks(decision.TabletPlacements, nodes)
	return decision
}

func cloneGroupReservations(reservations map[uint64]uint32) map[uint64]uint32 {
	cloned := make(map[uint64]uint32, len(reservations)+1)
	for nodeID, groups := range reservations {
		cloned[nodeID] = groups
	}
	return cloned
}

func placementsFromStatus(tablets []resources.TabletStatus) []TabletPlacement {
	placements := make([]TabletPlacement, 0, len(tablets))
	for _, tablet := range tablets {
		assigned := tablet.AssignedNodeIDs
		if len(assigned) == 0 && len(tablet.VoterNodeIDs) == int(tablet.DesiredReplicas) {
			// Backward-compatible recovery of pre-placement status. A partial
			// serving sample is never promoted into desired placement.
			assigned = tablet.VoterNodeIDs
		}
		placements = append(placements, TabletPlacement{
			ShardIndex:   tablet.ShardIndex,
			VoterNodeIDs: append([]uint64(nil), assigned...),
		})
	}
	sort.Slice(placements, func(left, right int) bool {
		return placements[left].ShardIndex < placements[right].ShardIndex
	})
	return placements
}

func placementForShard(placements []TabletPlacement, shard uint32) (TabletPlacement, bool) {
	index := sort.Search(len(placements), func(index int) bool {
		return placements[index].ShardIndex >= shard
	})
	if index == len(placements) || placements[index].ShardIndex != shard {
		return TabletPlacement{}, false
	}
	return placements[index], true
}

// Delete removes observed regional state before deleting Go desired metadata.
// A disconnected authority leaves the desired resource intact for safe retry.
func (reconciler *Reconciler) Delete(
	ctx context.Context,
	request resources.DeleteRequest,
) (resources.DeleteResult, error) {
	reconciler.mutations.Lock()
	defer reconciler.mutations.Unlock()

	resource, err := reconciler.registry.Get(request.Key)
	if err != nil {
		var storeError *resources.RegistryError
		if !errors.As(err, &storeError) || storeError.Code != resources.CodeNotFound {
			return resources.DeleteResult{}, err
		}
		if replayStore, ok := reconciler.registry.(managedDeleteReplayStore); ok {
			replayed, found, replayErr := replayStore.ReplayManagedDelete(ctx, request)
			if replayErr != nil || found {
				return replayed, replayErr
			}
		}
		// The local registry checks completed tokens before evaluating missing
		// state. The replicated store reaches this fallback only when no managed
		// delete outcome exists for the token.
		return reconciler.registry.Delete(request)
	}
	if request.ExpectedGeneration != nil &&
		*request.ExpectedGeneration != resource.Generation {
		return resources.DeleteResult{}, &reconcileError{
			message:   "delete expected generation does not match desired state",
			retryable: false,
			cause:     conflictError("desired generation conflict"),
		}
	}
	if managed, ok := reconciler.registry.(managedDeleteStore); ok {
		catalogGeneration := resource.Status.EffectiveCatalogGeneration()
		if resource.Generation == math.MaxUint64 || catalogGeneration == math.MaxUint64 {
			return resources.DeleteResult{}, &reconcileError{
				message:   "resource generation is exhausted",
				retryable: false,
				cause:     conflictError("resource generation exhausted"),
			}
		}
		deleted, deleteErr := managed.DeleteManaged(
			ctx,
			request,
			resource.Generation,
			catalogGeneration,
		)
		if deleteErr != nil {
			var storeError *resources.RegistryError
			retryable := errors.As(deleteErr, &storeError) && storeError.Code == resources.CodeUnavailable
			return resources.DeleteResult{}, &reconcileError{
				message:   "managed regional delete failed: " + deleteErr.Error(),
				retryable: retryable,
				cause:     deleteErr,
			}
		}
		return deleted, nil
	}
	if resource.Status.ObservedGeneration > 0 {
		expectedCatalogGeneration := resource.Status.EffectiveCatalogGeneration()
		if resource.Generation == math.MaxUint64 || expectedCatalogGeneration == math.MaxUint64 {
			return resources.DeleteResult{}, &reconcileError{
				message:   "resource or Catalog generation is exhausted",
				retryable: false,
				cause:     conflictError("resource generation exhausted"),
			}
		}
		observation, err := reconciler.authority.Delete(ctx, AuthorityDeleteRequest{
			RequestToken:       deleteToken(resource),
			Key:                resource.ResourceKey,
			ExpectedGeneration: expectedCatalogGeneration,
		})
		if err != nil {
			return resources.DeleteResult{}, &reconcileError{
				message:   "regional delete failed: " + err.Error(),
				retryable: IsRetryable(err),
				cause:     err,
			}
		}
		if !observation.Deleted || observation.Generation != expectedCatalogGeneration+1 {
			return resources.DeleteResult{}, &reconcileError{
				message:   "regional delete returned an inconsistent tombstone generation",
				retryable: false,
				cause:     conflictError("regional delete generation conflict"),
			}
		}
	}
	return reconciler.registry.Delete(request)
}

func (reconciler *Reconciler) fail(
	resource resources.Resource,
	retryable bool,
	cause error,
) (resources.Resource, error) {
	status := resource.Status
	status.Phase = resources.PhaseFailed
	if retryable {
		status.Phase = resources.PhasePending
	}
	status.Message = cause.Error()
	// Without a successful current observation, the last voter/leader sample
	// is not evidence of present serving placement. Keep the last observed
	// generation for reconciliation routing, but fail closed on topology.
	status.Tablets = nil
	status.Placement = nil
	updated, updateErr := reconciler.registry.UpdateStatus(
		resource.ResourceKey,
		resource.Generation,
		status,
	)
	if updateErr != nil {
		return resources.Resource{}, staleObservationError(updateErr)
	}
	return updated, &reconcileError{
		message:   fmt.Sprintf("regional reconciliation failed: %s", cause),
		retryable: retryable,
		cause:     cause,
	}
}

func staleObservationError(cause error) error {
	return &reconcileError{
		message:   "desired generation changed while regional state was being observed",
		retryable: true,
		cause:     cause,
	}
}

func decodeDesiredSpec(raw json.RawMessage) (desiredSpec, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var spec desiredSpec
	if err := decoder.Decode(&spec); err != nil {
		return desiredSpec{}, invalidAuthorityError(
			fmt.Sprintf("regional resource spec is invalid: %v", err),
		)
	}
	if spec.ReplicaCount == 0 && spec.Replicas <= math.MaxUint16 {
		spec.ReplicaCount = uint16(spec.Replicas)
	}
	if spec.ShardCount == 0 && spec.Configuration != nil {
		shards, err := configurationUint32(spec.Configuration["shard_count"])
		if err != nil {
			return desiredSpec{}, invalidAuthorityError(err.Error())
		}
		spec.ShardCount = shards
	}
	if spec.ShardCount == 0 {
		return desiredSpec{}, invalidAuthorityError("shard_count must be non-zero")
	}
	if spec.ReplicaCount != 3 && spec.ReplicaCount != 5 {
		return desiredSpec{}, invalidAuthorityError(
			"the regional runtime requires replica_count 3 or 5",
		)
	}
	return spec, nil
}

func configurationUint32(value any) (uint32, error) {
	number, ok := value.(json.Number)
	if !ok {
		return 0, fmt.Errorf("configuration.shard_count must be an unsigned integer")
	}
	parsed, err := strconv.ParseUint(number.String(), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("configuration.shard_count must be an unsigned 32-bit integer")
	}
	return uint32(parsed), nil
}

func applyToken(resource resources.Resource) string {
	return mutationToken("apply", resource)
}

func deleteToken(resource resources.Resource) string {
	return mutationToken("delete", resource)
}

func mutationToken(operation string, resource resources.Resource) string {
	encoded, err := json.Marshal(struct {
		Operation  string                `json:"operation"`
		Key        resources.ResourceKey `json:"key"`
		Generation uint64                `json:"generation"`
	}{
		Operation:  operation,
		Key:        resource.ResourceKey,
		Generation: resource.Generation,
	})
	if err != nil {
		panic("validated resource identity must encode")
	}
	digest := sha256.Sum256(encoded)
	return "epoch-control." + operation + ".v1." + hex.EncodeToString(digest[:])
}

func validateObservation(
	resource resources.Resource,
	spec desiredSpec,
	placement PlacementDecision,
	observation AuthorityObservation,
	expectedCatalogGeneration uint64,
	applyingDesiredGeneration bool,
) error {
	if observation.Generation == 0 {
		return invalidAuthorityError(
			"regional authority returned zero Catalog generation for an observed resource",
		)
	}
	validGeneration := observation.Generation == expectedCatalogGeneration
	if applyingDesiredGeneration && expectedCatalogGeneration == 0 {
		// Rust compares an apply for an absent resource with generation zero,
		// while retaining its tombstone counter internally. A recreate can
		// therefore return any positive historical successor no greater than
		// the Go desired generation. The authenticated Catalog response becomes
		// the explicit cursor for every later operation.
		validGeneration = observation.Generation <= resource.Generation
	} else if applyingDesiredGeneration && expectedCatalogGeneration < math.MaxUint64 {
		validGeneration = validGeneration || observation.Generation == expectedCatalogGeneration+1
	}
	if !validGeneration {
		return conflictError(fmt.Sprintf(
			"regional Catalog generation %d did not continue from %d for desired generation %d",
			observation.Generation,
			expectedCatalogGeneration,
			resource.Generation,
		))
	}
	if len(observation.Tablets) != int(spec.ShardCount) {
		return invalidAuthorityError(fmt.Sprintf(
			"regional authority returned %d tablets for %d desired shards",
			len(observation.Tablets),
			spec.ShardCount,
		))
	}
	sorted := append([]resources.TabletStatus(nil), observation.Tablets...)
	sort.Slice(sorted, func(left, right int) bool {
		return sorted[left].ShardIndex < sorted[right].ShardIndex
	})
	for index, tablet := range sorted {
		if tablet.ShardIndex != uint32(index) {
			return invalidAuthorityError("regional authority returned a non-contiguous shard set")
		}
		if tablet.ResourceGeneration != observation.Generation ||
			tablet.DesiredReplicas != uint32(spec.ReplicaCount) {
			return invalidAuthorityError(
				"regional tablet generation or desired replicas do not match the resource",
			)
		}
		expected, ok := placementForShard(placement.TabletPlacements, tablet.ShardIndex)
		if !ok {
			return invalidAuthorityError(
				"regional tablet has no admitted placement",
			)
		}
		transitioning := len(tablet.TargetVoterNodeIDs) > 0
		if transitioning {
			if !slices.Equal(tablet.TargetVoterNodeIDs, expected.VoterNodeIDs) ||
				!singleVoterReplacement(tablet.AssignedNodeIDs, tablet.TargetVoterNodeIDs) {
				return invalidAuthorityError(
					"regional tablet membership target does not match a single-voter admitted replacement",
				)
			}
		} else if !slices.Equal(tablet.AssignedNodeIDs, expected.VoterNodeIDs) {
			return invalidAuthorityError(
				"regional tablet assignment does not match the admitted placement",
			)
		}
		if len(tablet.VoterNodeIDs) > 0 &&
			!slices.Equal(tablet.VoterNodeIDs, tablet.AssignedNodeIDs) &&
			(!transitioning || !slices.Equal(tablet.VoterNodeIDs, tablet.TargetVoterNodeIDs)) {
			return invalidAuthorityError(
				"regional tablet reported membership matching neither its current nor target assignment",
			)
		}
		for _, reachable := range tablet.ReachableVoterNodeIDs {
			if !slices.Contains(tablet.VoterNodeIDs, reachable) {
				return invalidAuthorityError(
					"regional tablet reported a reachable node outside committed membership",
				)
			}
		}
	}
	return nil
}

func singleVoterReplacement(current, target []uint64) bool {
	if len(current) != len(target) || len(current) == 0 ||
		!slices.IsSorted(current) || !slices.IsSorted(target) ||
		hasAdjacentDuplicate(current) || hasAdjacentDuplicate(target) {
		return false
	}
	removed := 0
	for _, nodeID := range current {
		if !slices.Contains(target, nodeID) {
			removed++
		}
	}
	added := 0
	for _, nodeID := range target {
		if !slices.Contains(current, nodeID) {
			added++
		}
	}
	return removed == 1 && added == 1
}

func statusFromObservation(
	generation uint64,
	spec desiredSpec,
	placement PlacementDecision,
	observation AuthorityObservation,
	transitionReason PlacementTransitionReason,
) resources.ResourceStatus {
	tablets := append([]resources.TabletStatus(nil), observation.Tablets...)
	policySatisfied := placementSatisfiesPolicy(placement)
	servingComplete := true
	transitioning := false
	for index := range tablets {
		expected, ok := placementForShard(placement.TabletPlacements, tablets[index].ShardIndex)
		tablets[index].VoterNodeIDs = append([]uint64(nil), tablets[index].VoterNodeIDs...)
		tablets[index].AssignedNodeIDs = append([]uint64(nil), tablets[index].AssignedNodeIDs...)
		tablets[index].BootstrapVoterNodeIDs = append(
			[]uint64(nil),
			tablets[index].BootstrapVoterNodeIDs...,
		)
		tablets[index].TargetVoterNodeIDs = append(
			[]uint64(nil),
			tablets[index].TargetVoterNodeIDs...,
		)
		tablets[index].ReachableVoterNodeIDs = append(
			[]uint64(nil),
			tablets[index].ReachableVoterNodeIDs...,
		)
		transitioning = transitioning || len(tablets[index].TargetVoterNodeIDs) > 0
		servingComplete = servingComplete &&
			ok &&
			len(tablets[index].TargetVoterNodeIDs) == 0 &&
			slices.Equal(tablets[index].AssignedNodeIDs, expected.VoterNodeIDs) &&
			slices.Equal(tablets[index].VoterNodeIDs, expected.VoterNodeIDs) &&
			slices.Equal(tablets[index].ReachableVoterNodeIDs, expected.VoterNodeIDs) &&
			tablets[index].LeaderNodeID != 0
	}
	phase := resources.PhaseReady
	message := "regional catalog generation and serving placement converged"
	if transitioning {
		phase = resources.PhasePending
		message = transitionStatusMessage(transitionReason)
	} else if !servingComplete {
		phase = resources.PhaseDegraded
		message = "regional catalog applied; serving placement is incomplete"
	} else if !policySatisfied {
		phase = resources.PhasePending
		message = "regional catalog generation converged; automatic placement repair is pending"
	}
	return resources.ResourceStatus{
		Phase:              phase,
		ObservedGeneration: generation,
		CatalogGeneration:  observation.Generation,
		ObservedShardCount: spec.ShardCount,
		Message:            message,
		Tablets:            tablets,
		Placement:          placementStatus(placement),
	}
}

func transitionStatusMessage(reason PlacementTransitionReason) string {
	switch reason {
	case TransitionPolicyRepair:
		return "regional catalog generation converged; automatic policy repair is in progress"
	case TransitionTopologyRepair:
		return "regional catalog generation converged; automatic topology repair is in progress"
	case TransitionRebalance:
		return "regional catalog generation converged; automatic placement rebalance is in progress"
	default:
		return "regional catalog generation converged; learner-first voter replacement is in progress"
	}
}

func placementSatisfiesPolicy(decision PlacementDecision) bool {
	if decision.AchievedZones < decision.Policy.MinimumZones ||
		decision.AchievedRacks < decision.Policy.MinimumRacks {
		return false
	}
	eligible := make(map[uint64]struct{}, len(decision.EligibleNodeIDs))
	for _, nodeID := range decision.EligibleNodeIDs {
		eligible[nodeID] = struct{}{}
	}
	for _, placement := range decision.TabletPlacements {
		for _, nodeID := range placement.VoterNodeIDs {
			if _, ok := eligible[nodeID]; !ok {
				return false
			}
		}
	}
	return true
}

func placementStatus(decision PlacementDecision) *resources.PlacementStatus {
	nodes := make([]resources.RegionalNodeStatus, 0, len(decision.Nodes))
	for _, node := range decision.Nodes {
		additional := decision.AdditionalGroupsByNode[node.NodeID]
		used := node.UsedConsensusGroups + additional
		available := node.AvailableConsensusGroups - additional
		nodes = append(nodes, resources.RegionalNodeStatus{
			NodeID:                   node.NodeID,
			Region:                   node.Region,
			Zone:                     node.Zone,
			Rack:                     node.Rack,
			NodeClass:                node.NodeClass,
			ConsensusVoterNodeIDs:    append([]uint64(nil), node.ConsensusVoterNodeIDs...),
			MaxConsensusGroups:       node.MaxConsensusGroups,
			UsedConsensusGroups:      used,
			AvailableConsensusGroups: available,
		})
	}
	return &resources.PlacementStatus{
		AllowedRegions:    append([]string(nil), decision.Policy.AllowedRegions...),
		MinimumZones:      decision.Policy.MinimumZones,
		MinimumRacks:      decision.Policy.MinimumRacks,
		RequiredNodeClass: decision.Policy.RequiredNodeClass,
		ExcludedNodeIDs:   append([]uint64(nil), decision.Policy.ExcludedNodeIDs...),
		AchievedZones:     decision.AchievedZones,
		AchievedRacks:     decision.AchievedRacks,
		Nodes:             nodes,
	}
}

// Run periodically reconciles every live desired resource until cancellation.
func (reconciler *Reconciler) Run(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("regional reconciliation interval must be positive")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		// Per-resource status retains each failure. A disconnected region must
		// not terminate the managed control-plane process.
		_ = reconciler.reconcileAll(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (reconciler *Reconciler) reconcileAll(ctx context.Context) error {
	all, err := reconciler.registry.List(resources.ListFilter{})
	if err != nil {
		return err
	}
	var failures []error
	for _, resource := range all {
		if _, err := reconciler.Reconcile(ctx, resource.ResourceKey); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}
