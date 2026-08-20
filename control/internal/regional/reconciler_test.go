package regional

import (
	"context"
	"encoding/json"
	"errors"
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
	inventoryCalls int
	inventory      func() (NodeInventory, error)
	apply          func(AuthorityApplyRequest) (AuthorityObservation, error)
	observe        func(resources.ResourceKey) (AuthorityObservation, error)
	delete         func(AuthorityDeleteRequest) (AuthorityDeleteObservation, error)
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
			observation.Tablets[0].VoterNodeIDs = []uint64{1, 2}
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
		len(degraded.Status.Tablets[0].VoterNodeIDs) != 2 ||
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
	for shard := range shards {
		tablets[shard] = resources.TabletStatus{
			TabletID:           uint64(shard) + 10,
			ConsensusGroupID:   uint64(shard) + 20,
			ShardIndex:         shard,
			TabletEpoch:        1,
			ResourceGeneration: generation,
			DesiredReplicas:    uint32(replicas),
			VoterNodeIDs:       []uint64{1, 2, 3},
			LeaderNodeID:       1,
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
