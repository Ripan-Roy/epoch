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

// Authority is the narrow boundary owned by regional Rust control.
type Authority interface {
	Inventory(context.Context) (NodeInventory, error)
	Apply(context.Context, AuthorityApplyRequest) (AuthorityObservation, error)
	Observe(context.Context, resources.ResourceKey) (AuthorityObservation, error)
	Delete(context.Context, AuthorityDeleteRequest) (AuthorityDeleteObservation, error)
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
	registry  *resources.Registry
	authority Authority
	mutations sync.Mutex
}

// NewReconciler constructs a regional reconciler.
func NewReconciler(registry *resources.Registry, authority Authority) *Reconciler {
	if registry == nil {
		panic("regional: nil resource registry")
	}
	if authority == nil {
		panic("regional: nil authority")
	}
	return &Reconciler{registry: registry, authority: authority}
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
) (resources.Resource, error) {
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
	existingShards := resource.Status.ObservedShardCount
	if existingShards == 0 {
		existingShards = uint32(len(resource.Status.Tablets))
	}
	placement, err := reconciler.admit(ctx, resource, spec, existingShards)
	if err != nil {
		return reconciler.fail(resource, IsRetryable(err), err)
	}

	var observation AuthorityObservation
	if resource.Status.ObservedGeneration < resource.Generation {
		observation, err = reconciler.authority.Apply(ctx, AuthorityApplyRequest{
			RequestToken:       applyToken(resource),
			Key:                resource.ResourceKey,
			ExpectedGeneration: resource.Status.ObservedGeneration,
			ShardCount:         spec.ShardCount,
			ReplicaCount:       spec.ReplicaCount,
		})
	} else {
		observation, err = reconciler.authority.Observe(ctx, resource.ResourceKey)
	}
	if err != nil {
		return reconciler.fail(resource, IsRetryable(err), err)
	}
	if err := validateObservation(resource, spec, placement, observation); err != nil {
		return reconciler.fail(resource, false, err)
	}

	status := statusFromObservation(resource.Generation, spec, placement, observation)
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

func (reconciler *Reconciler) admit(
	ctx context.Context,
	resource resources.Resource,
	spec desiredSpec,
	existingShards uint32,
) (PlacementDecision, error) {
	inventory, inventoryErr := reconciler.authority.Inventory(ctx)
	if inventoryErr == nil {
		return AdmitPlacement(
			spec.Placement,
			uint32(spec.ReplicaCount),
			spec.ShardCount,
			existingShards,
			inventory,
		)
	}
	// A catalog mutation always requires a fresh, complete capacity sample.
	if resource.Status.ObservedGeneration < resource.Generation ||
		!IsRetryable(inventoryErr) ||
		resource.Status.Placement == nil {
		return PlacementDecision{}, inventoryErr
	}
	// During a transient node outage, the last generation-fenced admission
	// remains evidence of intended fixed-voter topology. Route sampling below
	// still determines current serving voters and degrades honestly.
	return AdmitPlacement(
		spec.Placement,
		uint32(spec.ReplicaCount),
		spec.ShardCount,
		existingShards,
		inventoryFromStatus(resource.Status.Placement),
	)
}

func inventoryFromStatus(status *resources.PlacementStatus) NodeInventory {
	nodes := make([]RegionalNode, 0, len(status.Nodes))
	for _, node := range status.Nodes {
		nodes = append(nodes, RegionalNode{
			NodeID:                   node.NodeID,
			Region:                   node.Region,
			Zone:                     node.Zone,
			NodeClass:                node.NodeClass,
			ConsensusVoterNodeIDs:    append([]uint64(nil), node.ConsensusVoterNodeIDs...),
			MaxConsensusGroups:       node.MaxConsensusGroups,
			UsedConsensusGroups:      node.UsedConsensusGroups,
			AvailableConsensusGroups: node.AvailableConsensusGroups,
		})
	}
	return NodeInventory{Nodes: nodes}
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
		// The registry checks completed tokens before it evaluates missing
		// state. This preserves an exact delete replay after the first request
		// has removed desired metadata, without invoking Rust authority twice.
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
	if resource.Status.ObservedGeneration > 0 {
		if resource.Generation == math.MaxUint64 {
			return resources.DeleteResult{}, &reconcileError{
				message:   "resource generation is exhausted",
				retryable: false,
				cause:     conflictError("resource generation exhausted"),
			}
		}
		observation, err := reconciler.authority.Delete(ctx, AuthorityDeleteRequest{
			RequestToken:       deleteToken(resource),
			Key:                resource.ResourceKey,
			ExpectedGeneration: resource.Status.ObservedGeneration,
		})
		if err != nil {
			return resources.DeleteResult{}, &reconcileError{
				message:   "regional delete failed: " + err.Error(),
				retryable: IsRetryable(err),
				cause:     err,
			}
		}
		if !observation.Deleted || observation.Generation != resource.Generation+1 {
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
	if spec.ReplicaCount != 3 {
		return desiredSpec{}, invalidAuthorityError(
			"the current regional runtime requires replica_count 3",
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
) error {
	if observation.Generation != resource.Generation {
		return conflictError(fmt.Sprintf(
			"regional generation %d does not match desired generation %d",
			observation.Generation,
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
		for _, voter := range tablet.VoterNodeIDs {
			if !slices.Contains(placement.VoterNodeIDs, voter) {
				return invalidAuthorityError(
					"regional tablet reported a voter outside the admitted fixed voter set",
				)
			}
		}
	}
	return nil
}

func statusFromObservation(
	generation uint64,
	spec desiredSpec,
	placement PlacementDecision,
	observation AuthorityObservation,
) resources.ResourceStatus {
	tablets := append([]resources.TabletStatus(nil), observation.Tablets...)
	ready := true
	for index := range tablets {
		tablets[index].VoterNodeIDs = append([]uint64(nil), tablets[index].VoterNodeIDs...)
		ready = ready &&
			len(tablets[index].VoterNodeIDs) >= int(spec.ReplicaCount) &&
			tablets[index].LeaderNodeID != 0
	}
	phase := resources.PhaseReady
	message := "regional catalog generation and serving placement converged"
	if !ready {
		phase = resources.PhaseDegraded
		message = "regional catalog applied; serving placement is incomplete"
	}
	return resources.ResourceStatus{
		Phase:              phase,
		ObservedGeneration: generation,
		ObservedShardCount: spec.ShardCount,
		Message:            message,
		Tablets:            tablets,
		Placement:          placementStatus(placement),
	}
}

func placementStatus(decision PlacementDecision) *resources.PlacementStatus {
	nodes := make([]resources.RegionalNodeStatus, 0, len(decision.Nodes))
	for _, node := range decision.Nodes {
		used := node.UsedConsensusGroups + decision.AdditionalGroupsPerNode
		available := node.AvailableConsensusGroups - decision.AdditionalGroupsPerNode
		nodes = append(nodes, resources.RegionalNodeStatus{
			NodeID:                   node.NodeID,
			Region:                   node.Region,
			Zone:                     node.Zone,
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
		RequiredNodeClass: decision.Policy.RequiredNodeClass,
		AchievedZones:     decision.AchievedZones,
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
