package epoch

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
)

func TestRegionalBusClientRoutesCompleteMutationAndReadContracts(t *testing.T) {
	leader := &regionalFakeTransport{route: Document{
		"resource_generation": "6", "tablet_epoch": "4", "term": "11", "accepts_writes": true,
	}}
	client, err := NewRegionalBusClientWithTransports(
		[]Transport{leader}, "secret-token",
		RegionalScope{Organization: "acme", Project: "shop", Environment: "dev", Namespace: "core"},
	)
	if err != nil {
		t.Fatal(err)
	}
	policy := DefaultDeliveryPolicy()
	policy.Retry.Strategy = FixedDeliveryBackoff
	subscription := Subscription{
		Name:           "orders",
		Filter:         EventFilter{EventTypePatterns: []string{"order.*"}},
		Target:         SignedWebhookTarget("https://example.com/orders", "primary"),
		DeliveryPolicy: &policy,
	}
	event := EventEnvelope{ID: "order-2", Source: "go-regional-sdk", Type: "order.created", TimeMS: 2, Payload: map[string]any{"id": 2}}
	ctx := context.Background()
	bus := "events/eu"

	if _, err = client.UpsertSubscription(ctx, bus, 0, "upsert-1", subscription); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Publish(ctx, bus, 0, "publish-1", event); err != nil {
		t.Fatal(err)
	}
	if _, err = client.AcquireDeliveries(ctx, bus, 0, "acquire-1", RegionalBusAcquireOptions{Subscription: "orders", Dispatcher: "worker-a", DispatcherEpoch: 7, MaxDeliveries: 10}); err != nil {
		t.Fatal(err)
	}
	if _, err = client.AcknowledgeDelivery(ctx, bus, 0, "ack-1", "delivery-1", "worker-a", 7, "lease-1"); err != nil {
		t.Fatal(err)
	}
	if _, err = client.FailDelivery(ctx, bus, 0, "fail-1", "delivery-2", "worker-a", 7, "lease-2", "downstream timeout"); err != nil {
		t.Fatal(err)
	}
	if _, err = client.RejectDelivery(ctx, bus, 0, "reject-1", "delivery-3", "worker-a", 7, "lease-3", "http status 400"); err != nil {
		t.Fatal(err)
	}
	if _, err = client.MaintainDeliveries(ctx, bus, 0, "maintain-1", 100); err != nil {
		t.Fatal(err)
	}
	if _, err = client.RemoveSubscription(ctx, bus, 0, "remove-1", "orders"); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Mutation(ctx, bus, 0, 12); err != nil {
		t.Fatal(err)
	}
	if _, err = client.ReplayArchive(ctx, bus, 0, RegionalBusReplayOptions{FromMS: 1, ToMS: 10, Limit: 100, Filter: &subscription.Filter}); err != nil {
		t.Fatal(err)
	}
	state := InFlightDelivery
	if _, err = client.QueryDeliveries(ctx, bus, 0, RegionalBusDeliveryQuery{Subscription: "orders", State: &state, Limit: 100}); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Status(ctx, bus, 0); err != nil {
		t.Fatal(err)
	}

	base := "/v1/organizations/acme/projects/shop/environments/dev/namespaces/core/buses/events%2Feu/shards/0"
	var operations []Request
	for index := 1; index < len(leader.requests); index += 2 {
		operations = append(operations, leader.requests[index])
	}
	if len(operations) != 12 {
		t.Fatalf("expected 12 operations, got %d", len(operations))
	}
	if operations[0].Path != base+"/mutations" || operations[8].Path != base+"/mutations/12" {
		t.Fatalf("unexpected Bus paths: %#v", operations)
	}
	var upsert map[string]any
	payload, err := json.Marshal(operations[0].Body)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(payload, &upsert); err != nil {
		t.Fatal(err)
	}
	operation := upsert["operation"].(map[string]any)
	if operation["kind"] != "upsert_subscription" || operation["subscription"].(map[string]any)["delivery_policy"] == nil {
		t.Fatalf("unexpected subscription operation: %#v", operation)
	}
	target := operation["subscription"].(map[string]any)["target"].(map[string]any)
	if target["signing_key_id"] != "primary" {
		t.Fatalf("signed target was not serialized: %#v", target)
	}
	var rejection map[string]any
	payload, err = json.Marshal(operations[5].Body)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(payload, &rejection); err != nil {
		t.Fatal(err)
	}
	if rejection["operation"].(map[string]any)["kind"] != "reject_delivery" {
		t.Fatalf("unexpected rejection operation: %#v", rejection)
	}
	if operations[9].Path != base+"/archive/replay" || operations[10].Path != base+"/deliveries/query" {
		t.Fatalf("unexpected Bus read paths: %#v", operations[9:])
	}
	for _, request := range operations[8:] {
		if request.Headers[regionalReadHeader] != "linearizable" {
			t.Fatalf("read %q was not linearizable", request.Path)
		}
	}
}

func TestRegionalBusClientRejectsInvalidBoundsBeforeNetwork(t *testing.T) {
	leader := &regionalFakeTransport{route: Document{
		"resource_generation": "6", "tablet_epoch": "4", "term": "11", "accepts_writes": true,
	}}
	client, err := NewRegionalBusClientWithTransports(
		[]Transport{leader}, "secret-token",
		RegionalScope{Organization: "acme", Project: "shop", Environment: "dev", Namespace: "core"},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err = client.AcquireDeliveries(ctx, "events", 0, "acquire", RegionalBusAcquireOptions{Subscription: "orders", Dispatcher: "worker", DispatcherEpoch: 1, MaxDeliveries: 0}); err == nil {
		t.Fatal("zero acquire bound should fail")
	}
	if _, err = client.ReplayArchive(ctx, "events", 0, RegionalBusReplayOptions{FromMS: 10, ToMS: 1, Limit: 1}); err == nil {
		t.Fatal("reversed replay range should fail")
	}
	badPolicy := DefaultDeliveryPolicy()
	badPolicy.Retry.InitialDelayMS = badPolicy.Retry.MaxDelayMS + 1
	if _, err = client.UpsertSubscription(ctx, "events", 0, "upsert", Subscription{Name: "orders", Target: PullTarget(), DeliveryPolicy: &badPolicy}); err == nil {
		t.Fatal("invalid delivery retry range should fail")
	}
	if _, err = client.UpsertSubscription(ctx, "events", 0, "upsert-signed", Subscription{Name: "orders", Target: SignedWebhookTarget("https://example.com/orders", "bad/key")}); err == nil {
		t.Fatal("invalid signing key ID should fail")
	}
	if _, err = client.UpsertSubscription(ctx, "events", 0, "upsert-empty-key", Subscription{Name: "orders", Target: SignedWebhookTarget("https://example.com/orders", "")}); err == nil {
		t.Fatal("empty signing key ID should fail")
	}
	if !reflect.DeepEqual(leader.requests, []Request(nil)) {
		t.Fatalf("invalid calls reached network: %#v", leader.requests)
	}
}
