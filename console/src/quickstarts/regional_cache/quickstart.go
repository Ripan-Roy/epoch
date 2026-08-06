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
	observation, err := client.Observe(ctx, "sessions", 0, "profile")
	must(err)
	revision := decimal(observation["observation"].(map[string]any)["shard_revision"])
	roles, err := epoch.NewRegionalCacheSet([]string{"admin", "buyer"})
	must(err)
	rank, err := epoch.NewRegionalCacheSortedSet(map[string]float64{"alice": 9.5})
	must(err)
	transaction, err := client.Transaction(ctx, "sessions", 0, "docs-go-cache-transaction-v1", revision, []epoch.RegionalCacheMutation{
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
	status, err := client.Status(ctx, "sessions", 0)
	must(err)

	output, err := json.MarshalIndent(map[string]any{
		"set": written, "exact_retry": replayed, "cas": compared, "transaction": transaction,
		"guarded_increment": guarded, "release": released, "ttl": ephemeral, "maintain": maintained,
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
