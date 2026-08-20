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
	client, err := epoch.NewRegionalCacheClient(
		strings.Split(environment("EPOCH_REGIONAL_ENDPOINTS", "http://127.0.0.1:18661,http://127.0.0.1:18662,http://127.0.0.1:18663"), ","),
		environment("EPOCH_TOKEN", "epoch-dev-admin-v1"),
		epoch.RegionalScope{Organization: "acme", Project: "shop", Environment: "dev", Namespace: "core"},
		3*time.Second,
	)
	must(err)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	written, err := client.Set(ctx, "sessions", 0, "docs-go-cache-set-v1", "profile", epoch.NewRegionalCacheString("alice"), epoch.RegionalCacheWriteOptions{})
	must(err)
	replayed, err := client.Set(ctx, "sessions", 0, "docs-go-cache-set-v1", "profile", epoch.NewRegionalCacheString("alice"), epoch.RegionalCacheWriteOptions{})
	must(err)
	version := decimal(result(written)["item"].(map[string]any)["version"])
	compared, err := client.CompareAndSet(ctx, "sessions", 0, "docs-go-cache-cas-v1", "profile", epoch.RegionalCacheVersion(version), epoch.NewRegionalCacheHash(map[string]string{"name": "alice", "role": "admin"}), epoch.RegionalCacheWriteOptions{})
	must(err)
	committedGet, err := client.Get(ctx, "sessions", 0, "docs-go-cache-get-v1", "profile")
	must(err)
	observation, err := client.Observe(ctx, "sessions", 0, "profile")
	must(err)
	revision := decimal(observation["observation"].(map[string]any)["shard_revision"])
	roles, err := epoch.NewRegionalCacheSet([]string{"admin", "buyer"})
	must(err)
	rank, err := epoch.NewRegionalCacheSortedSet(map[string]float64{"alice": 9.5})
	must(err)
	batch, err := client.AtomicBatch(ctx, "sessions", 0, "docs-go-cache-batch-v1", revision, []epoch.RegionalCacheMutation{
		epoch.NewRegionalCacheSetMutation("visits", epoch.NewRegionalCacheCounter(1), nil),
		epoch.NewRegionalCacheSetMutation("recent", epoch.NewRegionalCacheList([]string{"home", "checkout"}), nil),
		epoch.NewRegionalCacheSetMutation("roles", roles, nil),
		epoch.NewRegionalCacheSetMutation("rank", rank, nil),
		epoch.NewRegionalCacheSetMutation("avatar", epoch.NewRegionalCacheBlob([]byte("epoch")), nil),
	}, nil)
	must(err)
	acquired, err := client.AcquireLock(ctx, "sessions", 0, "docs-go-cache-lock-v1", "profile-lock", "docs-go", 1, 60_000)
	must(err)
	leaseToken := result(acquired)["lease_token"].(string)
	guard := &epoch.RegionalCacheLockGuard{LockKey: "profile-lock", Owner: "docs-go", OwnerEpoch: 1, LeaseToken: leaseToken}
	guarded, err := client.Increment(ctx, "sessions", 0, "docs-go-cache-guarded-increment-v1", "visits", 1, epoch.RegionalCacheIncrementOptions{LockGuard: guard})
	must(err)
	released, err := client.ReleaseLock(ctx, "sessions", 0, "docs-go-cache-release-v1", "profile-lock", "docs-go", 1, leaseToken)
	must(err)
	ttl := uint64(1)
	ephemeral, err := client.Set(ctx, "sessions", 0, "docs-go-cache-ttl-v1", "flash", epoch.NewRegionalCacheString("short"), epoch.RegionalCacheWriteOptions{TTLMS: &ttl})
	must(err)
	time.Sleep(10 * time.Millisecond)
	maintained, err := client.Maintain(ctx, "sessions", 0, "docs-go-cache-maintain-v1", 100)
	must(err)
	cold, err := client.Set(ctx, "sessions", 0, "docs-go-cache-cold-v1", "profile-archive", epoch.NewRegionalCacheString("alice-archive"), epoch.RegionalCacheWriteOptions{StorageClass: "cold"})
	must(err)
	backup, err := client.Backup(ctx, "sessions", 0)
	must(err)
	capturedRevision := decimal(backup["captured_revision"])
	_, err = client.Set(ctx, "sessions", 0, fmt.Sprintf("docs-go-cache-restore-scratch-%d", capturedRevision), "restore-scratch", epoch.NewRegionalCacheString("remove-me"), epoch.RegionalCacheWriteOptions{})
	must(err)
	restored, err := client.Restore(ctx, "sessions", 0, fmt.Sprintf("docs-go-cache-restore-%d", capturedRevision), backup["artifact_base64"].(string), capturedRevision)
	must(err)
	bitmapTransform, err := epoch.NewRegionalCacheTransform("bitmap_set", map[string]any{"bit": 7, "value": true})
	must(err)
	bitmap, err := client.Transform(ctx, "sessions", 0, "docs-go-cache-bitmap-v1", "feature-flags", bitmapTransform, nil, nil, nil)
	must(err)
	bitmapQuery, err := client.Query(ctx, "sessions", 0, map[string]any{"kind": "bitmap_get", "key": "feature-flags", "bit": 7})
	must(err)
	presence, err := epoch.NewRegionalCacheMultiplexMutation("presence", "docs-go-cache-multiplex-presence-v1", epoch.NewRegionalCacheSetMutation("presence", epoch.NewRegionalCacheString("online"), nil))
	must(err)
	notifications, err := epoch.NewRegionalCacheMultiplexMutation("notifications", "docs-go-cache-multiplex-notifications-v1", epoch.NewRegionalCacheIncrementMutation("notifications", 1, nil, nil))
	must(err)
	multiplexed, err := client.Multiplex(ctx, "sessions", 0, []epoch.RegionalCacheMultiplexMutation{presence, notifications})
	must(err)
	subscription, err := client.CreateSubscription(ctx, "sessions", 0, []string{"session.audit"}, []string{"session.*"})
	must(err)
	subscriptionID := subscription["subscription_id"].(string)
	published, err := client.Publish(ctx, "sessions", 0, "session.audit", map[string]any{"profile": "alice"})
	must(err)
	polled, err := client.PollSubscription(ctx, "sessions", 0, subscriptionID, 10)
	must(err)
	deletedSubscription, err := client.DeleteSubscription(ctx, "sessions", 0, subscriptionID)
	must(err)
	changes, err := client.Changes(ctx, "sessions", 0, 1, 100)
	must(err)
	status, err := client.Status(ctx, "sessions", 0)
	must(err)

	output, err := json.MarshalIndent(map[string]any{
		"set": written, "exact_retry": replayed, "cas": compared, "committed_get": committedGet, "atomic_batch": batch,
		"guarded_increment": guarded, "release": released, "ttl": ephemeral, "maintain": maintained,
		"cold": cold, "backup": backup, "restore": restored, "bitmap": bitmap, "bitmap_query": bitmapQuery,
		"multiplex": multiplexed, "subscription": subscription, "publish": published, "poll": polled,
		"delete_subscription": deletedSubscription, "changes": changes,
		"profile": observation, "status": status,
	}, "", "  ")
	must(err)
	fmt.Println(string(output))
}

func result(document epoch.Document) map[string]any {
	return document["receipt"].(map[string]any)["outcome"].(map[string]any)["result"].(map[string]any)
}

func decimal(value any) uint64 {
	parsed, err := strconv.ParseUint(fmt.Sprint(value), 10, 64)
	must(err)
	return parsed
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
