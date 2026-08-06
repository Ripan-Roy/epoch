package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"epoch.local/epoch/sdk/go/epoch"
)

func main() {
	client, err := epoch.NewRegionalBusClient(
		strings.Split(environment("EPOCH_REGIONAL_ENDPOINTS", "http://127.0.0.1:18661,http://127.0.0.1:18662,http://127.0.0.1:18663"), ","),
		environment("EPOCH_TOKEN", "epoch-dev-admin-v1"),
		epoch.RegionalScope{Organization: "acme", Project: "shop", Environment: "dev", Namespace: "core"},
		3*time.Second,
	)
	must(err)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	policy := epoch.DefaultDeliveryPolicy()
	policy.Retry.Strategy = epoch.FixedDeliveryBackoff
	subscription := epoch.Subscription{
		Name: "orders", Filter: epoch.EventFilter{EventTypePatterns: []string{"order.*"}},
		Target: epoch.PullTarget(), DeliveryPolicy: &policy,
	}
	upserted, err := client.UpsertSubscription(ctx, "events", 0, "docs-go-bus-upsert-v1", subscription)
	must(err)
	event := epoch.EventEnvelope{ID: "docs-order-1", Source: "docs-go", Type: "order.created", TimeMS: uint64(time.Now().UnixMilli()), Payload: map[string]any{"id": 1}}
	published, err := client.Publish(ctx, "events", 0, "docs-go-bus-publish-v1", event)
	must(err)
	replayed, err := client.Publish(ctx, "events", 0, "docs-go-bus-publish-v1", event)
	must(err)
	acquired, err := client.AcquireDeliveries(ctx, "events", 0, "docs-go-bus-acquire-v1", epoch.RegionalBusAcquireOptions{Subscription: "orders", Dispatcher: "docs-go", DispatcherEpoch: 1, MaxDeliveries: 1})
	must(err)
	delivery := result(acquired)["deliveries"].([]any)[0].(map[string]any)
	acknowledged, err := client.AcknowledgeDelivery(ctx, "events", 0, "docs-go-bus-ack-v1", delivery["delivery_id"].(string), "docs-go", 1, delivery["lease_token"].(string))
	must(err)
	archive, err := client.ReplayArchive(ctx, "events", 0, epoch.RegionalBusReplayOptions{FromMS: 0, ToMS: ^uint64(0), Limit: 100, Filter: &subscription.Filter})
	must(err)
	state := epoch.AcknowledgedDelivery
	deliveries, err := client.QueryDeliveries(ctx, "events", 0, epoch.RegionalBusDeliveryQuery{Subscription: "orders", State: &state, Limit: 100})
	must(err)
	status, err := client.Status(ctx, "events", 0)
	must(err)

	output, err := json.MarshalIndent(map[string]any{
		"upsert": upserted, "publish": published, "exact_retry": replayed,
		"acknowledge": acknowledged, "archive": archive, "deliveries": deliveries, "status": status,
	}, "", "  ")
	must(err)
	fmt.Println(string(output))
}

func result(document epoch.Document) map[string]any {
	return document["receipt"].(map[string]any)["outcome"].(map[string]any)["result"].(map[string]any)
}

func environment(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
