package epoch

import (
	"context"
	"encoding/json"
	"math"
	"reflect"
	"testing"
)

func TestRegionalCacheClientRoutesCompleteMutationAndReadContracts(t *testing.T) {
	leader := &regionalFakeTransport{route: Document{
		"resource_generation": "6", "tablet_epoch": "4", "term": "11", "accepts_writes": true,
	}}
	client, err := NewRegionalCacheClientWithTransports(
		[]Transport{leader}, "secret-token",
		RegionalScope{Organization: "acme", Project: "shop", Environment: "dev", Namespace: "core"},
	)
	if err != nil {
		t.Fatal(err)
	}
	setValue, err := NewRegionalCacheSet([]string{"a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	sortedValue, err := NewRegionalCacheSortedSet(map[string]float64{"alice": 1.5})
	if err != nil {
		t.Fatal(err)
	}
	guard := RegionalCacheLockGuard{LockKey: "critical", Owner: "worker-a", OwnerEpoch: 7, LeaseToken: "lease-7"}
	ctx := context.Background()
	cache := "sessions/eu"

	if _, err = client.Set(ctx, cache, 0, "set-1", "profile", NewRegionalCacheString("alice"), RegionalCacheWriteOptions{TTLMS: pointer(uint64(5_000))}); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Delete(ctx, cache, 0, "delete-1", "old", RegionalCacheDeleteOptions{ExpectedVersion: pointer(uint64(4))}); err != nil {
		t.Fatal(err)
	}
	if _, err = client.CompareAndSet(ctx, cache, 0, "cas-1", "profile", RegionalCacheVersion(1), NewRegionalCacheBlob([]byte{0, 255}), RegionalCacheWriteOptions{LockGuard: &guard}); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Increment(ctx, cache, 0, "inc-1", "visits", -3, RegionalCacheIncrementOptions{ExpectedVersion: pointer(uint64(0))}); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Get(ctx, cache, 0, "get-1", "profile"); err != nil {
		t.Fatal(err)
	}
	mutations := []RegionalCacheMutation{
		NewRegionalCacheSetMutation("set", setValue, nil),
		NewRegionalCacheSetMutation("rank", sortedValue, nil),
		NewRegionalCacheCompareAndSetMutation("new", RegionalCacheMissing(4), NewRegionalCacheCounter(-2), nil),
	}
	if _, err = client.AtomicBatch(ctx, cache, 0, "batch-1", 4, mutations, []RegionalCacheLockGuard{guard}); err != nil {
		t.Fatal(err)
	}
	if _, err = client.AcquireLock(ctx, cache, 0, "lock-1", "critical", "worker-a", 7, 3_000); err != nil {
		t.Fatal(err)
	}
	if _, err = client.RenewLock(ctx, cache, 0, "renew-1", "critical", "worker-a", 7, "lease-7", 4_000); err != nil {
		t.Fatal(err)
	}
	if _, err = client.ReleaseLock(ctx, cache, 0, "release-1", "critical", "worker-a", 7, "lease-8"); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Maintain(ctx, cache, 0, "maintain-1", 100); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Mutation(ctx, cache, 0, 12); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Observe(ctx, cache, 0, "profile"); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Status(ctx, cache, 0); err != nil {
		t.Fatal(err)
	}

	base := "/v1/organizations/acme/projects/shop/environments/dev/namespaces/core/caches/sessions%2Feu/shards/0"
	var operations []Request
	for index := 1; index < len(leader.requests); index += 2 {
		operations = append(operations, leader.requests[index])
	}
	if len(operations) != 13 {
		t.Fatalf("expected 13 operations, got %d", len(operations))
	}
	if operations[0].Path != base+"/mutations" || operations[10].Path != base+"/mutations/12" {
		t.Fatalf("unexpected Cache paths: %#v", operations)
	}
	var transaction map[string]any
	getPayload, err := json.Marshal(operations[4].Body)
	if err != nil {
		t.Fatal(err)
	}
	var get map[string]any
	if err = json.Unmarshal(getPayload, &get); err != nil {
		t.Fatal(err)
	}
	if get["operation"].(map[string]any)["kind"] != "get" {
		t.Fatalf("unexpected committed Get: %#v", get)
	}
	payload, err := json.Marshal(operations[5].Body)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(payload, &transaction); err != nil {
		t.Fatal(err)
	}
	operation := transaction["operation"].(map[string]any)
	if operation["expected_revision"] != "4" || operation["kind"] != "transaction" {
		t.Fatalf("unexpected transaction: %#v", operation)
	}
	if operations[11].Query.Get("key") != "profile" {
		t.Fatalf("unexpected observation query: %#v", operations[11].Query)
	}
	for _, request := range operations[10:] {
		if request.Headers[regionalReadHeader] != "linearizable" {
			t.Fatalf("read %q was not linearizable", request.Path)
		}
	}
}

func TestRegionalCacheClientRejectsInvalidValuesAndBoundsBeforeNetwork(t *testing.T) {
	leader := &regionalFakeTransport{route: Document{
		"resource_generation": "6", "tablet_epoch": "4", "term": "11", "accepts_writes": true,
	}}
	client, err := NewRegionalCacheClientWithTransports(
		[]Transport{leader}, "secret-token",
		RegionalScope{Organization: "acme", Project: "shop", Environment: "dev", Namespace: "core"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = NewRegionalCacheSet([]string{"same", "same"}); err == nil {
		t.Fatal("duplicate Cache set should fail")
	}
	if _, err = NewRegionalCacheSortedSet(map[string]float64{"bad": math.Inf(1)}); err == nil {
		t.Fatal("non-finite Cache sorted set should fail")
	}
	if _, err = client.Maintain(context.Background(), "sessions", 0, "maintain", 0); err == nil {
		t.Fatal("zero maintenance bound should fail")
	}
	if _, err = client.Transaction(context.Background(), "sessions", 0, "tx", 0, nil, nil); err == nil {
		t.Fatal("empty transaction should fail")
	}
	if !reflect.DeepEqual(leader.requests, []Request(nil)) {
		t.Fatalf("invalid calls reached network: %#v", leader.requests)
	}
}
