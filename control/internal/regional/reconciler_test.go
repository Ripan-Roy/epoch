package regional

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"epoch.local/epoch/control/internal/resources"
)

type fakeAuthority struct {
	mu             sync.Mutex
	applyCalls     []AuthorityApplyRequest
	observeCalls   []resources.ResourceKey
	planCalls      []AuthorityMembershipPlanRequest
	inventoryCalls int
	inventory      func() (NodeInventory, error)
	apply          func(AuthorityApplyRequest) (AuthorityObservation, error)
	observe        func(resources.ResourceKey) (AuthorityObservation, error)
	plan           func(AuthorityMembershipPlanRequest) (AuthorityObservation, error)
	delete         func(AuthorityDeleteRequest) (AuthorityDeleteObservation, error)
}

func (authority *fakeAuthority) PlanMembership(
	_ context.Context,
	request AuthorityMembershipPlanRequest,
) (AuthorityObservation, error) {
	authority.mu.Lock()
	authority.planCalls = append(authority.planCalls, request)
	plan := authority.plan
	authority.mu.Unlock()
	if plan == nil {
		panic("unexpected PlanMembership call")
	}
	return plan(request)
}

func (authority *fakeAuthority) Inventory(
	_ context.Context,
) (NodeInventory, error) {
	authority.mu.Lock()
	authority.inventoryCalls++
	inventory := authority.inventory
	authority.mu.Unlock()
	if inventory == nil {
		return threeZoneInventory(8), nil
	}
	return inventory()
}

func (authority *fakeAuthority) Apply(
	_ context.Context,
	request AuthorityApplyRequest,
) (AuthorityObservation, error) {
	authority.mu.Lock()
	authority.applyCalls = append(authority.applyCalls, request)
	apply := authority.apply
	authority.mu.Unlock()
	return apply(request)
}

func (authority *fakeAuthority) Observe(
	_ context.Context,
	key resources.ResourceKey,
) (AuthorityObservation, error) {
	authority.mu.Lock()
	authority.observeCalls = append(authority.observeCalls, key)
	observe := authority.observe
	authority.mu.Unlock()
	return observe(key)
}

func (authority *fakeAuthority) Delete(
	_ context.Context,
	request AuthorityDeleteRequest,
) (AuthorityDeleteObservation, error) {
	authority.mu.Lock()
	remove := authority.delete
	authority.mu.Unlock()
	if remove == nil {
		panic("unexpected Delete call")
	}
	return remove(request)
}

func TestReconcilerAppliesThenObservesCurrentRegionalState(t *testing.T) {
	registry := resources.NewRegistry()
	resource := applyDesired(t, registry, "create-orders", regionalKey(resources.KindStream, "orders"), 2, 3)
	observation := servingObservation(resource.Generation, 2, 3)
	authority := &fakeAuthority{
		apply: func(request AuthorityApplyRequest) (AuthorityObservation, error) {
			if request.ExpectedGeneration != 0 || request.ShardCount != 2 || request.ReplicaCount != 3 {
				t.Fatalf("Apply request = %+v", request)
			}
			if len(request.TabletPlacements) != 2 ||
				!slices.Equal(request.TabletPlacements[0].VoterNodeIDs, []uint64{1, 2, 3}) ||
				!slices.Equal(request.TabletPlacements[1].VoterNodeIDs, []uint64{1, 2, 3}) {
				t.Fatalf("tablet placements = %+v", request.TabletPlacements)
			}
			return observation, nil
		},
		observe: func(resources.ResourceKey) (AuthorityObservation, error) {
			return observation, nil
		},
	}
	reconciler := NewReconciler(registry, authority)

	ready, err := reconciler.Reconcile(t.Context(), resource.ResourceKey)
	if err != nil {
		t.Fatalf("Reconcile(apply) error = %v", err)
	}
	assertReady(t, ready, 2)
	again, err := reconciler.Reconcile(t.Context(), resource.ResourceKey)
	if err != nil {
		t.Fatalf("Reconcile(observe) error = %v", err)
	}
	assertReady(t, again, 2)
	if len(authority.applyCalls) != 1 || len(authority.observeCalls) != 1 {
		t.Fatalf("calls = apply %d, observe %d", len(authority.applyCalls), len(authority.observeCalls))
	}
}

func TestReconcilerAdoptsACompletedPolicyCompliantVoterReplacement(t *testing.T) {
	registry := resources.NewRegistry()
	resource := applyDesired(
		t,
		registry,
		"create-replaceable-orders",
		regionalKey(resources.KindStream, "replaceable-orders"),
		1,
		3,
	)
	initial := servingObservation(resource.Generation, 1, 3)
	planned := initial
	planned.Tablets = append([]resources.TabletStatus(nil), initial.Tablets...)
	planned.Tablets[0].BootstrapVoterNodeIDs = []uint64{1, 2, 3}
	planned.Tablets[0].TargetVoterNodeIDs = []uint64{1, 2, 4}
	replaced := initial
	replaced.Tablets = append([]resources.TabletStatus(nil), initial.Tablets...)
	replaced.Tablets[0].AssignedNodeIDs = []uint64{1, 2, 4}
	replaced.Tablets[0].BootstrapVoterNodeIDs = []uint64{1, 2, 3}
	replaced.Tablets[0].VoterNodeIDs = []uint64{1, 2, 4}
	replaced.Tablets[0].ReachableVoterNodeIDs = []uint64{1, 2, 4}
	stage := 0
	authority := &fakeAuthority{
		inventory: func() (NodeInventory, error) {
			return regionalInventory(4, 8), nil
		},
		apply: func(AuthorityApplyRequest) (AuthorityObservation, error) {
			return initial, nil
		},
		observe: func(resources.ResourceKey) (AuthorityObservation, error) {
			switch stage {
			case 1:
				return planned, nil
			case 2:
				return replaced, nil
			default:
				return initial, nil
			}
		},
	}
	reconciler := NewReconciler(registry, authority)
	if _, err := reconciler.Reconcile(t.Context(), resource.ResourceKey); err != nil {
		t.Fatalf("initial Reconcile() error = %v", err)
	}

	stage = 1
	pending, err := reconciler.Reconcile(t.Context(), resource.ResourceKey)
	if err != nil {
		t.Fatalf("planned replacement Reconcile() error = %v", err)
	}
	if pending.Status.Phase != resources.PhasePending ||
		!slices.Equal(pending.Status.Tablets[0].TargetVoterNodeIDs, []uint64{1, 2, 4}) {
		t.Fatalf("planned replacement status = %+v", pending.Status)
	}

	stage = 2
	updated, err := reconciler.Reconcile(t.Context(), resource.ResourceKey)
	if err != nil {
		t.Fatalf("replacement Reconcile() error = %v", err)
	}
	assertReady(t, updated, 1)
	if !slices.Equal(updated.Status.Tablets[0].AssignedNodeIDs, []uint64{1, 2, 4}) ||
		!slices.Equal(updated.Status.Tablets[0].VoterNodeIDs, []uint64{1, 2, 4}) {
		t.Fatalf("replacement status = %+v", updated.Status.Tablets[0])
	}
	if len(authority.applyCalls) != 1 {
		t.Fatalf("replacement triggered %d catalog applies, want one initial apply", len(authority.applyCalls))
	}
}

func TestReconcilerAutomaticallyPlansOneSafeMembershipRepair(t *testing.T) {
	registry := resources.NewRegistry()
	resource := applyDesired(t, registry, "create-auto-repair-orders", regionalKey(resources.KindStream, "auto-repair-orders"), 1, 3)
	initial := servingObservation(resource.Generation, 1, 3)
	var currentObservation = initial
	authority := &fakeAuthority{
		inventory: func() (NodeInventory, error) { return regionalInventory(4, 8), nil },
		apply: func(request AuthorityApplyRequest) (AuthorityObservation, error) {
			if request.ExpectedGeneration == 0 {
				currentObservation = servingObservation(1, 1, 3)
			}
			// Placement policy is Go-owned metadata. The real Rust authority
			// returns its unchanged Catalog generation when only that policy
			// changes, while still validating the complete desired payload.
			return currentObservation, nil
		},
		observe: func(resources.ResourceKey) (AuthorityObservation, error) {
			return currentObservation, nil
		},
		plan: func(request AuthorityMembershipPlanRequest) (AuthorityObservation, error) {
			if request.TabletID != 10 || request.ExpectedTabletEpoch != 1 ||
				!slices.Equal(request.TargetVoterNodeIDs, []uint64{2, 3, 4}) ||
				request.RequestToken == "" {
				t.Fatalf("PlanMembership request = %+v", request)
			}
			planned := currentObservation
			planned.Tablets = append([]resources.TabletStatus(nil), currentObservation.Tablets...)
			planned.Tablets[0].BootstrapVoterNodeIDs = []uint64{1, 2, 3}
			planned.Tablets[0].TargetVoterNodeIDs = []uint64{2, 3, 4}
			currentObservation = planned
			return planned, nil
		},
	}
	reconciler := NewReconciler(registry, authority)

	if _, err := reconciler.Reconcile(t.Context(), resource.ResourceKey); err != nil {
		t.Fatalf("initial Reconcile() error = %v", err)
	}
	updated := applyDesiredWithPlacement(
		t,
		registry,
		"exclude-auto-repair-node",
		resource.ResourceKey,
		1,
		3,
		PlacementPolicy{MinimumZones: 3, MinimumRacks: 3, ExcludedNodeIDs: []uint64{1}},
	)
	if _, err := reconciler.Reconcile(t.Context(), resource.ResourceKey); err != nil {
		t.Fatalf("policy Reconcile() error = %v", err)
	}
	pending, err := reconciler.Reconcile(t.Context(), resource.ResourceKey)
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if pending.Status.Phase != resources.PhasePending || len(authority.planCalls) != 1 ||
		pending.Status.ObservedGeneration != updated.Generation ||
		pending.Status.CatalogGeneration != 1 ||
		!slices.Equal(pending.Status.Tablets[0].TargetVoterNodeIDs, []uint64{2, 3, 4}) ||
		!strings.Contains(pending.Status.Message, "automatic policy repair") {
		t.Fatalf("pending = %+v, plan calls = %+v", pending.Status, authority.planCalls)
	}
	if len(authority.applyCalls) != 2 ||
		authority.applyCalls[0].ExpectedGeneration != 0 ||
		authority.applyCalls[1].ExpectedGeneration != 1 ||
		authority.applyCalls[0].RequestToken == authority.applyCalls[1].RequestToken {
		t.Fatalf("Catalog apply cursors or tokens = %+v", authority.applyCalls)
	}
}

func TestReconcilerDeletesAndRecreatesAcrossSeparatedGenerationClocks(t *testing.T) {
	registry := resources.NewRegistry()
	resource := applyDesired(
		t,
		registry,
		"create-policy-delete",
		regionalKey(resources.KindStream, "policy-delete"),
		1,
		3,
	)
	catalogGeneration := uint64(0)
	catalogLive := false
	authority := &fakeAuthority{
		apply: func(request AuthorityApplyRequest) (AuthorityObservation, error) {
			switch {
			case request.ExpectedGeneration == 0 && !catalogLive:
				catalogGeneration++
				catalogLive = true
			case request.ExpectedGeneration == catalogGeneration && catalogLive:
				// The policy-only desired update is a real Rust Catalog no-op.
			default:
				t.Fatalf(
					"Apply expected Catalog generation = %d with live=%t generation=%d",
					request.ExpectedGeneration,
					catalogLive,
					catalogGeneration,
				)
			}
			return servingObservation(catalogGeneration, 1, 3), nil
		},
		delete: func(request AuthorityDeleteRequest) (AuthorityDeleteObservation, error) {
			if !catalogLive || request.ExpectedGeneration != catalogGeneration {
				t.Fatalf(
					"Delete expected Catalog generation = %d with live=%t generation=%d",
					request.ExpectedGeneration,
					catalogLive,
					catalogGeneration,
				)
			}
			catalogGeneration++
			catalogLive = false
			return AuthorityDeleteObservation{Generation: catalogGeneration, Deleted: true}, nil
		},
	}
	reconciler := NewReconciler(registry, authority)
	if _, err := reconciler.Reconcile(t.Context(), resource.ResourceKey); err != nil {
		t.Fatalf("initial Reconcile() error = %v", err)
	}
	updated := applyDesiredWithPlacement(
		t,
		registry,
		"policy-only-delete",
		resource.ResourceKey,
		1,
		3,
		PlacementPolicy{MinimumZones: 1, MinimumRacks: 1},
	)
	ready, err := reconciler.Reconcile(t.Context(), resource.ResourceKey)
	if err != nil {
		t.Fatalf("policy-only Reconcile() error = %v", err)
	}
	if ready.Status.ObservedGeneration != 2 || ready.Status.CatalogGeneration != 1 {
		t.Fatalf("separated status = %+v", ready.Status)
	}

	expected := updated.Generation
	deleted, err := reconciler.Delete(t.Context(), resources.DeleteRequest{
		RequestToken:       "delete-after-policy-only-generation",
		ExpectedGeneration: &expected,
		Key:                resource.ResourceKey,
	})
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if !deleted.Deleted || deleted.Generation != 3 {
		t.Fatalf("Delete() = %+v", deleted)
	}

	recreated := applyDesired(
		t,
		registry,
		"recreate-after-separated-delete",
		resource.ResourceKey,
		1,
		3,
	)
	if recreated.Generation != 4 {
		t.Fatalf("recreated Go generation = %d, want 4", recreated.Generation)
	}
	reopened, err := reconciler.Reconcile(t.Context(), resource.ResourceKey)
	if err != nil {
		t.Fatalf("recreated Reconcile() error = %v", err)
	}
	if reopened.Status.ObservedGeneration != 4 || reopened.Status.CatalogGeneration != 3 ||
		reopened.Status.Tablets[0].ResourceGeneration != 3 {
		t.Fatalf("recreated separated status = %+v", reopened.Status)
	}
}

func TestReconcilerRejectsZeroCatalogGenerationForObservedResource(t *testing.T) {
	registry := resources.NewRegistry()
	resource := applyDesired(
		t,
		registry,
		"create-zero-catalog-generation",
		regionalKey(resources.KindStream, "zero-catalog-generation"),
		1,
		3,
	)
	authority := &fakeAuthority{
		apply: func(AuthorityApplyRequest) (AuthorityObservation, error) {
			return servingObservation(0, 1, 3), nil
		},
	}
	reconciler := NewReconciler(registry, authority)
	if _, err := reconciler.Reconcile(t.Context(), resource.ResourceKey); err == nil || IsRetryable(err) {
		t.Fatalf("Reconcile() error = %v, want definitive invalid authority response", err)
	}
}

func TestReconcilerDoesNotOverlapMembershipTransitionsForOneResource(t *testing.T) {
	registry := resources.NewRegistry()
	resource := applyDesired(t, registry, "create-serial-repair", regionalKey(resources.KindStream, "serial-repair"), 1, 3)
	initial := servingObservation(resource.Generation, 1, 3)
	planned := initial
	planned.Tablets = append([]resources.TabletStatus(nil), initial.Tablets...)
	planned.Tablets[0].BootstrapVoterNodeIDs = []uint64{1, 2, 3}
	planned.Tablets[0].TargetVoterNodeIDs = []uint64{1, 2, 4}
	inventoryCalls := 0
	currentObservation := initial
	authority := &fakeAuthority{
		inventory: func() (NodeInventory, error) {
			inventoryCalls++
			if inventoryCalls == 1 {
				return regionalInventory(5, 8), nil
			}
			inventory := regionalInventory(5, 8)
			for index := range 3 {
				inventory.Nodes[index].MaxConsensusGroups = 8
				inventory.Nodes[index].UsedConsensusGroups = 7
				inventory.Nodes[index].AvailableConsensusGroups = 1
			}
			return inventory, nil
		},
		apply: func(AuthorityApplyRequest) (AuthorityObservation, error) { return initial, nil },
		observe: func(resources.ResourceKey) (AuthorityObservation, error) {
			return currentObservation, nil
		},
		plan: func(AuthorityMembershipPlanRequest) (AuthorityObservation, error) {
			currentObservation = planned
			return planned, nil
		},
	}
	reconciler := NewReconciler(registry, authority)

	if _, err := reconciler.Reconcile(t.Context(), resource.ResourceKey); err != nil {
		t.Fatalf("initial Reconcile() error = %v", err)
	}
	if _, err := reconciler.Reconcile(t.Context(), resource.ResourceKey); err != nil {
		t.Fatalf("planning Reconcile() error = %v", err)
	}
	if _, err := reconciler.Reconcile(t.Context(), resource.ResourceKey); err != nil {
		t.Fatalf("active-transition Reconcile() error = %v", err)
	}
	if len(authority.planCalls) != 1 {
		t.Fatalf("plan calls = %d, want exactly the initial serialized transition", len(authority.planCalls))
	}
}

func TestReconcilerDoesNotRebalanceADegradedTablet(t *testing.T) {
	registry := resources.NewRegistry()
	resource := applyDesired(t, registry, "create-degraded-rebalance", regionalKey(resources.KindStream, "degraded-rebalance"), 1, 3)
	initial := servingObservation(resource.Generation, 1, 3)
	inventoryCalls := 0
	authority := &fakeAuthority{
		inventory: func() (NodeInventory, error) {
			inventoryCalls++
			inventory := regionalInventory(4, 8)
			if inventoryCalls > 1 {
				for index := range 3 {
					inventory.Nodes[index].MaxConsensusGroups = 8
					inventory.Nodes[index].UsedConsensusGroups = 7
					inventory.Nodes[index].AvailableConsensusGroups = 1
				}
			}
			return inventory, nil
		},
		apply: func(AuthorityApplyRequest) (AuthorityObservation, error) { return initial, nil },
		observe: func(resources.ResourceKey) (AuthorityObservation, error) {
			degraded := initial
			degraded.Tablets = append([]resources.TabletStatus(nil), initial.Tablets...)
			degraded.Tablets[0].ReachableVoterNodeIDs = []uint64{1, 2}
			return degraded, nil
		},
	}
	reconciler := NewReconciler(registry, authority)
	if _, err := reconciler.Reconcile(t.Context(), resource.ResourceKey); err != nil {
		t.Fatal(err)
	}
	degraded, err := reconciler.Reconcile(t.Context(), resource.ResourceKey)
	if err != nil {
		t.Fatal(err)
	}
	if degraded.Status.Phase != resources.PhaseDegraded || len(authority.planCalls) != 0 {
		t.Fatalf("status = %+v, plan calls = %+v", degraded.Status, authority.planCalls)
	}
}

func TestReconcilerReportsUnsafePolicyRepairAsDegraded(t *testing.T) {
	registry := resources.NewRegistry()
	resource := applyDesired(t, registry, "create-unsafe-repair", regionalKey(resources.KindStream, "unsafe-repair"), 1, 3)
	observation := servingObservation(resource.Generation, 1, 3)
	authority := &fakeAuthority{
		inventory: func() (NodeInventory, error) { return regionalInventory(4, 8), nil },
		apply:     func(AuthorityApplyRequest) (AuthorityObservation, error) { return observation, nil },
		observe:   func(resources.ResourceKey) (AuthorityObservation, error) { return observation, nil },
	}
	reconciler := NewReconciler(registry, authority)
	if _, err := reconciler.Reconcile(t.Context(), resource.ResourceKey); err != nil {
		t.Fatal(err)
	}
	updated := applyDesiredWithPlacement(
		t,
		registry,
		"exclude-unsafe-repair-node",
		resource.ResourceKey,
		1,
		3,
		PlacementPolicy{MinimumZones: 3, MinimumRacks: 3, ExcludedNodeIDs: []uint64{1}},
	)
	observation.Generation = updated.Generation
	observation.Tablets = append([]resources.TabletStatus(nil), observation.Tablets...)
	observation.Tablets[0].ResourceGeneration = updated.Generation
	observation.Tablets[0].ReachableVoterNodeIDs = []uint64{1}

	if _, err := reconciler.Reconcile(t.Context(), resource.ResourceKey); err != nil {
		t.Fatal(err)
	}
	degraded, err := reconciler.Reconcile(t.Context(), resource.ResourceKey)
	if err != nil {
		t.Fatal(err)
	}
	if degraded.Status.Phase != resources.PhaseDegraded || len(authority.planCalls) != 0 ||
		!strings.Contains(degraded.Status.Message, "serving placement is incomplete") {
		t.Fatalf("status = %+v, plan calls = %+v", degraded.Status, authority.planCalls)
	}
}

func TestInventoryFromLegacyStatusDoesNotInferRacksFromZones(t *testing.T) {
	inventory := inventoryFromStatus(&resources.PlacementStatus{
		Nodes: []resources.RegionalNodeStatus{
			{NodeID: 1, Zone: "zone-a"},
			{NodeID: 2, Zone: "zone-b"},
		},
	})
	if len(inventory.Nodes) != 2 || inventory.Nodes[0].Rack != "unassigned" ||
		inventory.Nodes[1].Rack != "unassigned" {
		t.Fatalf("inventory = %+v", inventory)
	}
}

func TestReconcilerForwardsExactProfileConfiguration(t *testing.T) {
	registry := resources.NewRegistry()
	key := regionalKey(resources.KindCache, "sessions")
	spec, err := json.Marshal(map[string]any{
		"shard_count":   1,
		"replica_count": 3,
		"configuration": map[string]any{
			"shard_count":    1,
			"max_entries":    12,
			"default_ttl_ms": nil,
			"eviction":       "all_keys_lru",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	applied, err := registry.Apply(resources.ApplyRequest{
		RequestToken: "create-sessions",
		Resource: resources.DesiredResource{
			ResourceKey: key,
			Spec:        spec,
			Governance:  testGovernance(),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	observation := servingObservation(applied.Resource.Generation, 1, 3)
	authority := &fakeAuthority{
		apply: func(request AuthorityApplyRequest) (AuthorityObservation, error) {
			if request.Configuration["eviction"] != "all_keys_lru" {
				t.Fatalf("configuration = %#v", request.Configuration)
			}
			if number, ok := request.Configuration["max_entries"].(json.Number); !ok || number.String() != "12" {
				t.Fatalf("max_entries = %#v", request.Configuration["max_entries"])
			}
			return observation, nil
		},
	}
	reconciler := NewReconciler(registry, authority)
	if _, err := reconciler.Reconcile(t.Context(), key); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
}

func TestReconcilerRejectsCapacityBeforeCatalogApply(t *testing.T) {
	registry := resources.NewRegistry()
	resource := applyDesired(
		t,
		registry,
		"create-over-capacity",
		regionalKey(resources.KindStream, "over-capacity"),
		2,
		3,
	)
	authority := &fakeAuthority{
		inventory: func() (NodeInventory, error) {
			return threeZoneInventory(1), nil
		},
		apply: func(AuthorityApplyRequest) (AuthorityObservation, error) {
			panic("catalog Apply must not run after admission rejection")
		},
	}
	reconciler := NewReconciler(registry, authority)

	_, err := reconciler.Reconcile(t.Context(), resource.ResourceKey)
	if err == nil || IsRetryable(err) {
		t.Fatalf("Reconcile() error = %v, want definitive admission failure", err)
	}
	if len(authority.applyCalls) != 0 || authority.inventoryCalls != 1 {
		t.Fatalf(
			"calls = inventory %d, apply %d",
			authority.inventoryCalls,
			len(authority.applyCalls),
		)
	}
	failed, getErr := registry.Get(resource.ResourceKey)
	if getErr != nil {
		t.Fatalf("Get() error = %v", getErr)
	}
	if failed.Status.Phase != resources.PhaseFailed ||
		!strings.Contains(failed.Status.Message, string(AdmissionConsensusGroupCapacity)) {
		t.Fatalf("failed status = %+v", failed.Status)
	}
}

func TestReconcilerSerializesAdmissionAndCatalogMutationInOneControlProcess(t *testing.T) {
	registry := resources.NewRegistry()
	first := applyDesired(
		t,
		registry,
		"create-serialized-one",
		regionalKey(resources.KindStream, "serialized-one"),
		1,
		3,
	)
	second := applyDesired(
		t,
		registry,
		"create-serialized-two",
		regionalKey(resources.KindStream, "serialized-two"),
		1,
		3,
	)
	entered := make(chan string, 2)
	release := make(chan struct{})
	authority := &fakeAuthority{
		apply: func(request AuthorityApplyRequest) (AuthorityObservation, error) {
			entered <- request.Key.Name
			<-release
			return servingObservation(1, 1, 3), nil
		},
	}
	reconciler := NewReconciler(registry, authority)
	results := make(chan error, 2)
	go func() {
		_, err := reconciler.Reconcile(t.Context(), first.ResourceKey)
		results <- err
	}()
	<-entered
	go func() {
		_, err := reconciler.Reconcile(t.Context(), second.ResourceKey)
		results <- err
	}()

	select {
	case name := <-entered:
		t.Fatalf("second catalog mutation %q entered before the first completed", name)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("Reconcile() error = %v", err)
		}
	}
}

func TestReconcilerRetainsPendingDesiredStateAcrossAuthorityDisconnect(t *testing.T) {
	registry := resources.NewRegistry()
	resource := applyDesired(t, registry, "create-jobs", regionalKey(resources.KindQueue, "jobs"), 1, 3)
	connected := false
	authority := &fakeAuthority{
		apply: func(AuthorityApplyRequest) (AuthorityObservation, error) {
			if !connected {
				return AuthorityObservation{}, availabilityError("regional nodes are unavailable")
			}
			return servingObservation(resource.Generation, 1, 3), nil
		},
		observe: func(resources.ResourceKey) (AuthorityObservation, error) {
			panic("Observe should not run before the desired generation is applied")
		},
	}
	reconciler := NewReconciler(registry, authority)

	if _, err := reconciler.Reconcile(t.Context(), resource.ResourceKey); !IsRetryable(err) {
		t.Fatalf("disconnected Reconcile() error = %v, want retryable", err)
	}
	pending, err := registry.Get(resource.ResourceKey)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if pending.Status.Phase != resources.PhasePending || pending.Status.ObservedGeneration != 0 {
		t.Fatalf("pending status = %+v", pending.Status)
	}

	connected = true
	ready, err := reconciler.Reconcile(t.Context(), resource.ResourceKey)
	if err != nil {
		t.Fatalf("reconnected Reconcile() error = %v", err)
	}
	assertReady(t, ready, 1)
}

func TestReconcilerDoesNotPresentStalePlacementDuringAuthorityDisconnect(t *testing.T) {
	registry := resources.NewRegistry()
	resource := applyDesired(
		t,
		registry,
		"create-live-orders",
		regionalKey(resources.KindStream, "live-orders"),
		1,
		3,
	)
	authority := &fakeAuthority{
		apply: func(AuthorityApplyRequest) (AuthorityObservation, error) {
			return servingObservation(resource.Generation, 1, 3), nil
		},
		observe: func(resources.ResourceKey) (AuthorityObservation, error) {
			return AuthorityObservation{}, availabilityError("regional nodes are unavailable")
		},
	}
	reconciler := NewReconciler(registry, authority)
	ready, err := reconciler.Reconcile(t.Context(), resource.ResourceKey)
	if err != nil {
		t.Fatalf("initial Reconcile() error = %v", err)
	}
	assertReady(t, ready, 1)

	if _, err := reconciler.Reconcile(t.Context(), resource.ResourceKey); !IsRetryable(err) {
		t.Fatalf("disconnected Reconcile() error = %v, want retryable", err)
	}
	pending, err := registry.Get(resource.ResourceKey)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if pending.Status.Phase != resources.PhasePending ||
		pending.Status.ObservedGeneration != resource.Generation ||
		pending.Status.CatalogGeneration != ready.Status.CatalogGeneration ||
		len(pending.Status.Tablets) != 0 {
		t.Fatalf("disconnected status presents stale placement: %+v", pending.Status)
	}
}

func TestReconcilerUsesAdmittedTopologyButCurrentRoutesDuringPartialOutage(t *testing.T) {
	registry := resources.NewRegistry()
	resource := applyDesired(
		t,
		registry,
		"create-zone-aware-orders",
		regionalKey(resources.KindStream, "zone-aware-orders"),
		1,
		3,
	)
	inventoryAvailable := true
	authority := &fakeAuthority{
		inventory: func() (NodeInventory, error) {
			if !inventoryAvailable {
				return NodeInventory{}, availabilityError("one topology endpoint is unavailable")
			}
			return threeZoneInventory(8), nil
		},
		apply: func(AuthorityApplyRequest) (AuthorityObservation, error) {
			return servingObservation(resource.Generation, 1, 3), nil
		},
		observe: func(resources.ResourceKey) (AuthorityObservation, error) {
			observation := servingObservation(resource.Generation, 1, 3)
			observation.Tablets[0].ReachableVoterNodeIDs = []uint64{1, 2}
			observation.Tablets[0].LeaderNodeID = 2
			return observation, nil
		},
	}
	reconciler := NewReconciler(registry, authority)
	ready, err := reconciler.Reconcile(t.Context(), resource.ResourceKey)
	if err != nil || ready.Status.Phase != resources.PhaseReady {
		t.Fatalf("initial Reconcile() = %+v, %v", ready, err)
	}

	inventoryAvailable = false
	degraded, err := reconciler.Reconcile(t.Context(), resource.ResourceKey)
	if err != nil {
		t.Fatalf("partial-outage Reconcile() error = %v", err)
	}
	if degraded.Status.Phase != resources.PhaseDegraded ||
		len(degraded.Status.Tablets[0].VoterNodeIDs) != 3 ||
		len(degraded.Status.Tablets[0].ReachableVoterNodeIDs) != 2 ||
		degraded.Status.Placement == nil ||
		degraded.Status.Placement.AchievedZones != 3 {
		t.Fatalf("partial-outage status = %+v", degraded.Status)
	}
}

func TestReconcilerRecordsNonRetryableAuthorityConflict(t *testing.T) {
	registry := resources.NewRegistry()
	resource := applyDesired(t, registry, "create-sessions", regionalKey(resources.KindCache, "sessions"), 1, 3)
	authority := &fakeAuthority{
		apply: func(AuthorityApplyRequest) (AuthorityObservation, error) {
			return AuthorityObservation{}, conflictError("catalog generation was changed elsewhere")
		},
		observe: func(resources.ResourceKey) (AuthorityObservation, error) {
			panic("Observe should not run")
		},
	}
	reconciler := NewReconciler(registry, authority)

	_, err := reconciler.Reconcile(t.Context(), resource.ResourceKey)
	if err == nil || IsRetryable(err) {
		t.Fatalf("Reconcile() error = %v, want non-retryable conflict", err)
	}
	failed, getErr := registry.Get(resource.ResourceKey)
	if getErr != nil {
		t.Fatalf("Get() error = %v", getErr)
	}
	if failed.Status.Phase != resources.PhaseFailed {
		t.Fatalf("failed status = %+v", failed.Status)
	}
}

func TestLateObservationCannotMarkANewerDesiredGenerationReady(t *testing.T) {
	registry := resources.NewRegistry()
	key := regionalKey(resources.KindEventBus, "events")
	first := applyDesired(t, registry, "create-events", key, 1, 3)
	started := make(chan struct{})
	release := make(chan struct{})
	authority := &fakeAuthority{
		apply: func(AuthorityApplyRequest) (AuthorityObservation, error) {
			close(started)
			<-release
			return servingObservation(first.Generation, 1, 3), nil
		},
		observe: func(resources.ResourceKey) (AuthorityObservation, error) {
			panic("Observe should not run")
		},
	}
	reconciler := NewReconciler(registry, authority)
	result := make(chan error, 1)
	go func() {
		_, err := reconciler.Reconcile(context.Background(), key)
		result <- err
	}()
	<-started

	updated := applyDesiredWithExpected(t, registry, "update-events", key, 2, 3, first.Generation)
	close(release)
	if err := <-result; !IsRetryable(err) {
		t.Fatalf("late Reconcile() error = %v, want retryable stale observation", err)
	}
	current, err := registry.Get(key)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if current.Generation != updated.Generation || current.Status.Phase != resources.PhasePending {
		t.Fatalf("current resource = %+v", current)
	}
}

func applyDesired(
	t *testing.T,
	registry *resources.Registry,
	token string,
	key resources.ResourceKey,
	shards uint32,
	replicas uint16,
) resources.Resource {
	t.Helper()
	return applyDesiredWithExpected(t, registry, token, key, shards, replicas, 0)
}

func applyDesiredWithExpected(
	t *testing.T,
	registry *resources.Registry,
	token string,
	key resources.ResourceKey,
	shards uint32,
	replicas uint16,
	expected uint64,
) resources.Resource {
	t.Helper()
	spec, err := json.Marshal(map[string]any{
		"shard_count":   shards,
		"replica_count": replicas,
	})
	if err != nil {
		t.Fatalf("spec encoding error = %v", err)
	}
	request := resources.ApplyRequest{
		RequestToken: token,
		Resource: resources.DesiredResource{
			ResourceKey: key,
			Spec:        spec,
			Governance:  testGovernance(),
		},
	}
	if expected > 0 {
		request.ExpectedGeneration = &expected
	}
	applied, err := registry.Apply(request)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	return applied.Resource
}

func applyDesiredWithPlacement(
	t *testing.T,
	registry *resources.Registry,
	token string,
	key resources.ResourceKey,
	shards uint32,
	replicas uint16,
	placement PlacementPolicy,
) resources.Resource {
	t.Helper()
	spec, err := json.Marshal(map[string]any{
		"shard_count": shards, "replica_count": replicas, "placement": placement,
	})
	if err != nil {
		t.Fatal(err)
	}
	applied, err := registry.Apply(resources.ApplyRequest{
		RequestToken: token,
		Resource:     resources.DesiredResource{ResourceKey: key, Spec: spec, Governance: testGovernance()},
	})
	if err != nil {
		t.Fatal(err)
	}
	return applied.Resource
}

func testGovernance() *resources.ResourceGovernance {
	return &resources.ResourceGovernance{
		Owner:          "team:platform",
		CostCenter:     "cc-1042",
		Classification: resources.ClassificationInternal,
		Tags:           map[string]string{"service": "epoch"},
	}
}

func regionalKey(kind resources.Kind, name string) resources.ResourceKey {
	return resources.ResourceKey{
		Organization: "acme",
		Project:      "shop",
		Environment:  "dev",
		Namespace:    "core",
		Kind:         kind,
		Name:         name,
	}
}

func servingObservation(generation uint64, shards uint32, replicas uint16) AuthorityObservation {
	tablets := make([]resources.TabletStatus, shards)
	voters := make([]uint64, replicas)
	for index := range voters {
		voters[index] = uint64(index + 1)
	}
	for shard := range shards {
		tablets[shard] = resources.TabletStatus{
			TabletID:              uint64(shard) + 10,
			ConsensusGroupID:      uint64(shard) + 20,
			ShardIndex:            shard,
			TabletEpoch:           1,
			ResourceGeneration:    generation,
			DesiredReplicas:       uint32(replicas),
			AssignedNodeIDs:       append([]uint64(nil), voters...),
			VoterNodeIDs:          append([]uint64(nil), voters...),
			ReachableVoterNodeIDs: append([]uint64(nil), voters...),
			LeaderNodeID:          1,
		}
	}
	return AuthorityObservation{Generation: generation, Tablets: tablets}
}

func assertReady(t *testing.T, resource resources.Resource, tablets int) {
	t.Helper()
	if resource.Status.Phase != resources.PhaseReady ||
		resource.Status.ObservedGeneration != resource.Generation ||
		len(resource.Status.Tablets) != tablets {
		t.Fatalf("resource is not ready: %+v", resource)
	}
}

func TestAuthorityErrorClassification(t *testing.T) {
	if !IsRetryable(availabilityError("offline")) {
		t.Fatal("availability error should be retryable")
	}
	if IsRetryable(conflictError("conflict")) {
		t.Fatal("conflict error should not be retryable")
	}
	if IsRetryable(errors.New("unknown")) {
		t.Fatal("unknown errors should not be retryable")
	}
}
