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
	event.Key = "customer-0"
	event.TimeMS = 42
	shard, err := epoch.StreamShardFor(event.Key, 3)
	must(err)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	appended, err := client.AppendKeyed(ctx, "orders", "docs-go-keyed-stream-v1", event)
	must(err)
	replayed, err := client.AppendKeyed(ctx, "orders", "docs-go-keyed-stream-v1", event)
	must(err)
	batchFrame, err := epoch.EncodeStreamBatch([]epoch.StreamBatchRecord{
		{ClientSequence: 101, Envelope: event},
		{ClientSequence: 102, Envelope: func() epoch.EventEnvelope {
			copy := event
			copy.ID = "docs-go-order-43"
			return copy
		}()},
	}, epoch.StreamCompressionGzip)
	must(err)
	batch, err := client.AppendBatch(ctx, "orders", shard, "docs-go-gzip-batch-v1", batchFrame)
	must(err)
	fetched, err := client.Fetch(ctx, "orders", shard, appendOffset(appended), 10)
	must(err)
	groupRecords, err := client.FetchGroup(ctx, "orders", shard, "docs-go", 100)
	must(err)
	checkpoint, err := client.CommitOffset(ctx, "orders", shard, "docs-go", "docs-go-worker", 1, appendOffset(appended)+1, false, "docs-go-checkpoint-v1")
	must(err)
	lag, err := client.Lag(ctx, "orders", shard, "docs-go")
	must(err)
	joined, err := client.JoinConsumerSession(ctx, "orders", "docs-go-session", "docs-go-worker", 30*time.Second, "docs-go-session-join-v1")
	must(err)
	heartbeat, err := client.HeartbeatConsumerSession(ctx, "orders", "docs-go-session", "docs-go-worker", 1, "docs-go-session-heartbeat-v1")
	must(err)
	session, err := client.ConsumerSession(ctx, "orders", "docs-go-session")
	must(err)
	left, err := client.LeaveConsumerSession(ctx, "orders", "docs-go-session", "docs-go-worker", 1, "docs-go-session-leave-v1")
	must(err)
	configured, err := client.ConfigureRetention(ctx, "orders", shard, "docs-go-retention-v1", epoch.StreamRetentionPolicy{
		MaxRecordsPerPartition: 10_000,
		MaxBytesPerPartition:   3 * 1024 * 1024,
		MaxAgeMS:               7 * 24 * 60 * 60 * 1_000,
	})
	must(err)
	maintained, err := client.MaintainRetention(ctx, "orders", shard, "docs-go-retention-sweep-v1")
	must(err)
	retention, err := client.Retention(ctx, "orders", shard)
	must(err)

	output, err := json.MarshalIndent(map[string]any{
		"selected_shard": shard, "append": appended, "exact_retry": replayed,
		"gzip_batch": batch, "fetch": fetched,
		"group_fetch": groupRecords, "checkpoint": checkpoint, "lag": lag,
		"session_join": joined, "session_heartbeat": heartbeat, "session": session, "session_leave": left,
		"retention_configure": configured, "retention_maintenance": maintained,
		"retention": retention,
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
