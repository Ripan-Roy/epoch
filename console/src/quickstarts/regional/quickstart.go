package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"epoch.local/epoch/sdk/go/epoch"
)

func main() {
	endpoints := strings.Split(environment("EPOCH_REGIONAL_ENDPOINTS", "http://127.0.0.1:18661,http://127.0.0.1:18662,http://127.0.0.1:18663"), ",")
	client, err := epoch.NewRegionalStreamClient(
		endpoints,
		environment("EPOCH_TOKEN", "epoch-dev-admin-v1"),
		epoch.RegionalScope{Organization: "acme", Project: "shop", Environment: "dev", Namespace: "core"},
		3*time.Second,
	)
	must(err)

	event := epoch.NewEventEnvelope("docs-go", "order.created", map[string]any{"order_id": "go-42"})
	event.ID = "docs-go-order-42"
	event.TimeMS = 42
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	appended, err := client.Append(ctx, "orders", 0, "docs-go-append-v1", event)
	must(err)
	replayed, err := client.Append(ctx, "orders", 0, "docs-go-append-v1", event)
	must(err)
	fetched, err := client.Fetch(ctx, "orders", 0, appendOffset(appended), 10)
	must(err)
	groupRecords, err := client.FetchGroup(ctx, "orders", 0, "docs-go", 100)
	must(err)
	checkpoint, err := client.CommitOffset(ctx, "orders", 0, "docs-go", "docs-go-worker", 1, appendOffset(appended)+1, false, "docs-go-checkpoint-v1")
	must(err)
	lag, err := client.Lag(ctx, "orders", 0, "docs-go")
	must(err)

	output, err := json.MarshalIndent(map[string]any{
		"append": appended, "exact_retry": replayed, "fetch": fetched,
		"group_fetch": groupRecords, "checkpoint": checkpoint, "lag": lag,
	}, "", "  ")
	must(err)
	fmt.Println(string(output))
}

func appendOffset(document epoch.Document) uint64 {
	receipt, ok := document["receipt"].(map[string]any)
	if !ok {
		panic("append response has no receipt")
	}
	offset, err := strconv.ParseUint(fmt.Sprint(receipt["offset"]), 10, 64)
	must(err)
	return offset
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
