package epoch

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	maxRegionalCacheTransactionMutations = 128
	maxRegionalCacheExpirations          = 1_000
)

// RegionalCacheValue is one strict Cache scalar or collection value.
// Construct values with the NewRegionalCache* functions.
type RegionalCacheValue struct {
	kind  string
	value any
}

// NewRegionalCacheString constructs a string Cache value.
func NewRegionalCacheString(value string) RegionalCacheValue {
	return RegionalCacheValue{kind: "string", value: value}
}

// NewRegionalCacheBlob constructs a blob Cache value and copies the input.
func NewRegionalCacheBlob(value []byte) RegionalCacheValue {
	bytes := make([]int, len(value))
	for index, item := range value {
		bytes[index] = int(item)
	}
	return RegionalCacheValue{kind: "blob", value: bytes}
}

// NewRegionalCacheCounter constructs a signed 64-bit counter Cache value.
func NewRegionalCacheCounter(value int64) RegionalCacheValue {
	return RegionalCacheValue{kind: "counter", value: value}
}

// NewRegionalCacheHash constructs a string-to-string hash Cache value and copies the input.
func NewRegionalCacheHash(value map[string]string) RegionalCacheValue {
	copy := make(map[string]string, len(value))
	for key, item := range value {
		copy[key] = item
	}
	return RegionalCacheValue{kind: "hash", value: copy}
}

// NewRegionalCacheList constructs an ordered string-list Cache value and copies the input.
func NewRegionalCacheList(value []string) RegionalCacheValue {
	return RegionalCacheValue{kind: "list", value: append([]string(nil), value...)}
}

// NewRegionalCacheSet constructs a unique string-set Cache value and copies the input.
func NewRegionalCacheSet(value []string) (RegionalCacheValue, error) {
	seen := make(map[string]struct{}, len(value))
	for _, member := range value {
		if _, exists := seen[member]; exists {
			return RegionalCacheValue{}, fmt.Errorf("epoch: Cache set value contains duplicate member %q", member)
		}
		seen[member] = struct{}{}
	}
	return RegionalCacheValue{kind: "set", value: append([]string(nil), value...)}, nil
}

// NewRegionalCacheSortedSet constructs a finite-score sorted-set Cache value.
func NewRegionalCacheSortedSet(value map[string]float64) (RegionalCacheValue, error) {
	copy := make(map[string]float64, len(value))
	for member, score := range value {
		if math.IsNaN(score) || math.IsInf(score, 0) {
			return RegionalCacheValue{}, fmt.Errorf("epoch: Cache sorted-set score for %q must be finite", member)
		}
		copy[member] = score
	}
	return RegionalCacheValue{kind: "sorted_set", value: copy}, nil
}

func (value RegionalCacheValue) wire() (map[string]any, error) {
	var encoded any
	switch value.kind {
	case "string":
		item, ok := value.value.(string)
		if !ok {
			return nil, fmt.Errorf("epoch: Cache string value is invalid")
		}
		encoded = item
	case "blob":
		item, ok := value.value.([]int)
		if !ok {
			return nil, fmt.Errorf("epoch: Cache blob value is invalid")
		}
		encoded = append([]int(nil), item...)
	case "counter":
		item, ok := value.value.(int64)
		if !ok {
			return nil, fmt.Errorf("epoch: Cache counter value is invalid")
		}
		encoded = strconv.FormatInt(item, 10)
	case "hash":
		item, ok := value.value.(map[string]string)
		if !ok {
			return nil, fmt.Errorf("epoch: Cache hash value is invalid")
		}
		copy := make(map[string]string, len(item))
		for key, entry := range item {
			copy[key] = entry
		}
		encoded = copy
	case "list", "set":
		item, ok := value.value.([]string)
		if !ok {
			return nil, fmt.Errorf("epoch: Cache %s value is invalid", value.kind)
		}
		encoded = append([]string(nil), item...)
	case "sorted_set":
		item, ok := value.value.(map[string]float64)
		if !ok {
			return nil, fmt.Errorf("epoch: Cache sorted-set value is invalid")
		}
		copy := make(map[string]float64, len(item))
		for member, score := range item {
			if math.IsNaN(score) || math.IsInf(score, 0) {
				return nil, fmt.Errorf("epoch: Cache sorted-set score for %q must be finite", member)
			}
			copy[member] = score
		}
		encoded = copy
	default:
		return nil, fmt.Errorf("epoch: Cache value must be created with a NewRegionalCache* function")
	}
	return map[string]any{"kind": value.kind, "value": encoded}, nil
}

// RegionalCacheExpectation is a version or missing-at-revision CAS precondition.
type RegionalCacheExpectation struct {
	kind  string
	value uint64
}

// RegionalCacheMissing expects a key to be absent at the observed shard revision.
func RegionalCacheMissing(shardRevision uint64) RegionalCacheExpectation {
	return RegionalCacheExpectation{kind: "missing", value: shardRevision}
}

// RegionalCacheVersion expects the exact non-ABA key version.
func RegionalCacheVersion(version uint64) RegionalCacheExpectation {
	return RegionalCacheExpectation{kind: "version", value: version}
}

func (expectation RegionalCacheExpectation) wire() (map[string]any, error) {
	switch expectation.kind {
	case "missing":
		return map[string]any{"kind": "missing", "shard_revision": strconv.FormatUint(expectation.value, 10)}, nil
	case "version":
		return map[string]any{"kind": "version", "version": strconv.FormatUint(expectation.value, 10)}, nil
	default:
		return nil, fmt.Errorf("epoch: Cache expectation must be created with RegionalCacheMissing or RegionalCacheVersion")
	}
}

// RegionalCacheLockGuard proves ownership of the latest opaque lock lease.
type RegionalCacheLockGuard struct {
	LockKey    string
	Owner      string
	OwnerEpoch uint64
	LeaseToken string
}

func (guard RegionalCacheLockGuard) wire() (map[string]any, error) {
	if err := validateCacheLockIdentity(guard.LockKey, guard.Owner, guard.OwnerEpoch); err != nil {
		return nil, err
	}
	if strings.TrimSpace(guard.LeaseToken) == "" {
		return nil, fmt.Errorf("epoch: Cache lease token is required")
	}
	return map[string]any{
		"lock_key": guard.LockKey, "owner": guard.Owner,
		"owner_epoch": strconv.FormatUint(guard.OwnerEpoch, 10), "lease_token": guard.LeaseToken,
	}, nil
}

// RegionalCacheWriteOptions controls TTL and optional lock fencing for set and CAS.
type RegionalCacheWriteOptions struct {
	TTLMS     *uint64
	LockGuard *RegionalCacheLockGuard
}

// RegionalCacheDeleteOptions controls version and optional lock fencing for delete.
type RegionalCacheDeleteOptions struct {
	ExpectedVersion *uint64
	LockGuard       *RegionalCacheLockGuard
}

// RegionalCacheIncrementOptions controls version, TTL, and optional lock fencing for increment.
type RegionalCacheIncrementOptions struct {
	ExpectedVersion *uint64
	TTLMS           *uint64
	LockGuard       *RegionalCacheLockGuard
}

// RegionalCacheMutation is one operation permitted inside an atomic transaction.
type RegionalCacheMutation struct {
	key       string
	operation map[string]any
	err       error
}

// NewRegionalCacheSetMutation constructs a transactional set.
func NewRegionalCacheSetMutation(key string, value RegionalCacheValue, ttlMS *uint64) RegionalCacheMutation {
	operation, err := cacheSetOperation(key, value, RegionalCacheWriteOptions{TTLMS: ttlMS})
	delete(operation, "shard")
	return RegionalCacheMutation{key: key, operation: operation, err: err}
}

// NewRegionalCacheDeleteMutation constructs a transactional conditional delete.
func NewRegionalCacheDeleteMutation(key string, expectedVersion *uint64) RegionalCacheMutation {
	operation, err := cacheDeleteOperation(key, RegionalCacheDeleteOptions{ExpectedVersion: expectedVersion})
	delete(operation, "shard")
	return RegionalCacheMutation{key: key, operation: operation, err: err}
}

// NewRegionalCacheCompareAndSetMutation constructs a transactional CAS.
func NewRegionalCacheCompareAndSetMutation(key string, expected RegionalCacheExpectation, value RegionalCacheValue, ttlMS *uint64) RegionalCacheMutation {
	operation, err := cacheCASOperation(key, expected, value, RegionalCacheWriteOptions{TTLMS: ttlMS})
	delete(operation, "shard")
	return RegionalCacheMutation{key: key, operation: operation, err: err}
}

// NewRegionalCacheIncrementMutation constructs a transactional signed increment.
func NewRegionalCacheIncrementMutation(key string, delta int64, expectedVersion, ttlMS *uint64) RegionalCacheMutation {
	operation, err := cacheIncrementOperation(key, delta, RegionalCacheIncrementOptions{ExpectedVersion: expectedVersion, TTLMS: ttlMS})
	delete(operation, "shard")
	return RegionalCacheMutation{key: key, operation: operation, err: err}
}

// RegionalCacheClient routes authenticated Cache calls across regional nodes.
// Mutation retries preserve the caller's idempotency key across rediscovery.
type RegionalCacheClient struct {
	regional *regionalClient
}

// NewRegionalCacheClient builds a regional Cache client over one or more HTTP endpoints.
func NewRegionalCacheClient(endpoints []string, token string, scope RegionalScope, timeout time.Duration) (*RegionalCacheClient, error) {
	regional, err := newRegionalClient(endpoints, token, scope, timeout)
	if err != nil {
		return nil, err
	}
	return &RegionalCacheClient{regional: regional}, nil
}

// NewRegionalCacheClientWithTransports injects endpoint transports for tests or custom networking.
func NewRegionalCacheClientWithTransports(transports []Transport, token string, scope RegionalScope) (*RegionalCacheClient, error) {
	regional, err := newRegionalClientWithTransports(transports, token, scope)
	if err != nil {
		return nil, err
	}
	return &RegionalCacheClient{regional: regional}, nil
}

// Set writes one strict Cache value.
func (client *RegionalCacheClient) Set(ctx context.Context, cache string, shard uint32, idempotencyKey, key string, value RegionalCacheValue, options RegionalCacheWriteOptions) (Document, error) {
	operation, err := cacheSetOperation(key, value, options)
	if err != nil {
		return nil, err
	}
	return client.mutate(ctx, cache, shard, idempotencyKey, operation)
}

// Delete conditionally removes one Cache key.
func (client *RegionalCacheClient) Delete(ctx context.Context, cache string, shard uint32, idempotencyKey, key string, options RegionalCacheDeleteOptions) (Document, error) {
	operation, err := cacheDeleteOperation(key, options)
	if err != nil {
		return nil, err
	}
	return client.mutate(ctx, cache, shard, idempotencyKey, operation)
}

// CompareAndSet writes only when the version or missing-at-revision expectation holds.
func (client *RegionalCacheClient) CompareAndSet(ctx context.Context, cache string, shard uint32, idempotencyKey, key string, expected RegionalCacheExpectation, value RegionalCacheValue, options RegionalCacheWriteOptions) (Document, error) {
	operation, err := cacheCASOperation(key, expected, value, options)
	if err != nil {
		return nil, err
	}
	return client.mutate(ctx, cache, shard, idempotencyKey, operation)
}

// Increment applies a checked signed 64-bit delta.
func (client *RegionalCacheClient) Increment(ctx context.Context, cache string, shard uint32, idempotencyKey, key string, delta int64, options RegionalCacheIncrementOptions) (Document, error) {
	operation, err := cacheIncrementOperation(key, delta, options)
	if err != nil {
		return nil, err
	}
	return client.mutate(ctx, cache, shard, idempotencyKey, operation)
}

// Get returns one value and commits its access for deterministic LRU/LFU admission.
// Use Observe when the read must remain pure and must not affect eviction order.
func (client *RegionalCacheClient) Get(ctx context.Context, cache string, shard uint32, idempotencyKey, key string) (Document, error) {
	if strings.TrimSpace(key) == "" {
		return nil, fmt.Errorf("epoch: Cache key is required")
	}
	return client.mutate(ctx, cache, shard, idempotencyKey, map[string]any{
		"kind": "get", "shard": uint32(0), "key": key,
	})
}

// Transaction commits one to 128 distinct-key mutations at one shard revision.
func (client *RegionalCacheClient) Transaction(ctx context.Context, cache string, shard uint32, idempotencyKey string, expectedRevision uint64, mutations []RegionalCacheMutation, lockGuards []RegionalCacheLockGuard) (Document, error) {
	if len(mutations) == 0 || len(mutations) > maxRegionalCacheTransactionMutations {
		return nil, fmt.Errorf("epoch: Cache transaction mutations must be between 1 and %d", maxRegionalCacheTransactionMutations)
	}
	seen := make(map[string]struct{}, len(mutations))
	operations := make([]map[string]any, len(mutations))
	for index, mutation := range mutations {
		if mutation.err != nil {
			return nil, mutation.err
		}
		if strings.TrimSpace(mutation.key) == "" || mutation.operation == nil {
			return nil, fmt.Errorf("epoch: Cache transaction mutation %d is invalid", index)
		}
		if _, exists := seen[mutation.key]; exists {
			return nil, fmt.Errorf("epoch: Cache transaction keys must be distinct")
		}
		seen[mutation.key] = struct{}{}
		operations[index] = mutation.operation
	}
	guards := make([]map[string]any, len(lockGuards))
	for index, guard := range lockGuards {
		wire, err := guard.wire()
		if err != nil {
			return nil, err
		}
		guards[index] = wire
	}
	return client.mutate(ctx, cache, shard, idempotencyKey, map[string]any{
		"kind": "transaction", "shard": uint32(0),
		"expected_revision": strconv.FormatUint(expectedRevision, 10),
		"mutations":         operations, "lock_guards": guards,
	})
}

// AtomicBatch sends one ordered, distinct-key batch as one HTTP request and consensus proposal.
// The complete batch commits or rejects together; it does not return partial pipeline results.
func (client *RegionalCacheClient) AtomicBatch(ctx context.Context, cache string, shard uint32, idempotencyKey string, expectedRevision uint64, mutations []RegionalCacheMutation, lockGuards []RegionalCacheLockGuard) (Document, error) {
	return client.Transaction(ctx, cache, shard, idempotencyKey, expectedRevision, mutations, lockGuards)
}

// AcquireLock creates or replaces an expired advisory lock lease.
func (client *RegionalCacheClient) AcquireLock(ctx context.Context, cache string, shard uint32, idempotencyKey, lockKey, owner string, ownerEpoch, leaseMS uint64) (Document, error) {
	if err := validateCacheLockIdentity(lockKey, owner, ownerEpoch); err != nil {
		return nil, err
	}
	if leaseMS == 0 {
		return nil, fmt.Errorf("epoch: Cache lock lease must be non-zero")
	}
	return client.mutate(ctx, cache, shard, idempotencyKey, map[string]any{
		"kind": "acquire_lock", "shard": uint32(0), "lock_key": lockKey, "owner": owner,
		"owner_epoch": strconv.FormatUint(ownerEpoch, 10), "lease_ms": strconv.FormatUint(leaseMS, 10),
	})
}

// RenewLock rotates an opaque lease token while retaining its fencing token.
func (client *RegionalCacheClient) RenewLock(ctx context.Context, cache string, shard uint32, idempotencyKey, lockKey, owner string, ownerEpoch uint64, leaseToken string, extensionMS uint64) (Document, error) {
	operation, err := cacheLockOperation("renew_lock", lockKey, owner, ownerEpoch, leaseToken)
	if err != nil {
		return nil, err
	}
	if extensionMS == 0 {
		return nil, fmt.Errorf("epoch: Cache lock extension must be non-zero")
	}
	operation["extension_ms"] = strconv.FormatUint(extensionMS, 10)
	return client.mutate(ctx, cache, shard, idempotencyKey, operation)
}

// ReleaseLock removes the exact current advisory lock lease.
func (client *RegionalCacheClient) ReleaseLock(ctx context.Context, cache string, shard uint32, idempotencyKey, lockKey, owner string, ownerEpoch uint64, leaseToken string) (Document, error) {
	operation, err := cacheLockOperation("release_lock", lockKey, owner, ownerEpoch, leaseToken)
	if err != nil {
		return nil, err
	}
	return client.mutate(ctx, cache, shard, idempotencyKey, operation)
}

// Maintain deterministically removes a bounded number of expired keys and locks.
func (client *RegionalCacheClient) Maintain(ctx context.Context, cache string, shard uint32, idempotencyKey string, maxExpirations uint16) (Document, error) {
	if maxExpirations == 0 || maxExpirations > maxRegionalCacheExpirations {
		return nil, fmt.Errorf("epoch: Cache max expirations must be between 1 and %d", maxRegionalCacheExpirations)
	}
	return client.mutate(ctx, cache, shard, idempotencyKey, map[string]any{
		"kind": "maintain", "shard": uint32(0), "max_expirations": maxExpirations,
	})
}

// Mutation returns one mutation outcome by proposal ID.
func (client *RegionalCacheClient) Mutation(ctx context.Context, cache string, shard uint32, proposalID uint64) (Document, error) {
	if proposalID == 0 {
		return nil, fmt.Errorf("epoch: Cache proposal ID must be non-zero")
	}
	return client.read(ctx, cache, shard, "/mutations/"+strconv.FormatUint(proposalID, 10), nil)
}

// Observe returns one linearizable Cache key observation.
func (client *RegionalCacheClient) Observe(ctx context.Context, cache string, shard uint32, key string) (Document, error) {
	if strings.TrimSpace(key) == "" {
		return nil, fmt.Errorf("epoch: Cache key is required")
	}
	return client.read(ctx, cache, shard, "/observations", url.Values{"key": {key}})
}

// Status returns the linearizable Cache tablet status and digest.
func (client *RegionalCacheClient) Status(ctx context.Context, cache string, shard uint32) (Document, error) {
	return client.read(ctx, cache, shard, "/status", nil)
}

func (client *RegionalCacheClient) read(ctx context.Context, cache string, shard uint32, path string, query url.Values) (Document, error) {
	return regionalCall[Document](ctx, client.regionalClient(), "caches", "Cache", cache, shard, func(_ regionalRoute) Request {
		return Request{Method: "GET", Path: path, Query: query, Headers: map[string]string{regionalReadHeader: "linearizable"}}
	})
}

func (client *RegionalCacheClient) mutate(ctx context.Context, cache string, shard uint32, idempotencyKey string, operation any) (Document, error) {
	if strings.TrimSpace(idempotencyKey) == "" {
		return nil, fmt.Errorf("epoch: idempotency key is required")
	}
	return regionalCall[Document](ctx, client.regionalClient(), "caches", "Cache", cache, shard, func(route regionalRoute) Request {
		return Request{Method: "POST", Path: "/mutations", Body: struct {
			IdempotencyKey string `json:"idempotency_key"`
			ExpectedTerm   string `json:"expected_term"`
			Operation      any    `json:"operation"`
		}{idempotencyKey, route.Term, operation}}
	})
}

func (client *RegionalCacheClient) regionalClient() *regionalClient {
	if client == nil {
		return nil
	}
	return client.regional
}

func cacheSetOperation(key string, value RegionalCacheValue, options RegionalCacheWriteOptions) (map[string]any, error) {
	if strings.TrimSpace(key) == "" {
		return nil, fmt.Errorf("epoch: Cache key is required")
	}
	wire, err := value.wire()
	if err != nil {
		return nil, err
	}
	operation := map[string]any{"kind": "set", "shard": uint32(0), "key": key, "value": wire}
	if err := addCacheWriteOptions(operation, options); err != nil {
		return nil, err
	}
	return operation, nil
}

func cacheDeleteOperation(key string, options RegionalCacheDeleteOptions) (map[string]any, error) {
	if strings.TrimSpace(key) == "" {
		return nil, fmt.Errorf("epoch: Cache key is required")
	}
	operation := map[string]any{"kind": "delete", "shard": uint32(0), "key": key}
	if options.ExpectedVersion != nil {
		operation["expected_version"] = strconv.FormatUint(*options.ExpectedVersion, 10)
	}
	if options.LockGuard != nil {
		guard, err := options.LockGuard.wire()
		if err != nil {
			return nil, err
		}
		operation["lock_guard"] = guard
	}
	return operation, nil
}

func cacheCASOperation(key string, expected RegionalCacheExpectation, value RegionalCacheValue, options RegionalCacheWriteOptions) (map[string]any, error) {
	operation, err := cacheSetOperation(key, value, options)
	if err != nil {
		return nil, err
	}
	wire, err := expected.wire()
	if err != nil {
		return nil, err
	}
	operation["kind"] = "compare_and_set"
	operation["expected"] = wire
	return operation, nil
}

func cacheIncrementOperation(key string, delta int64, options RegionalCacheIncrementOptions) (map[string]any, error) {
	if strings.TrimSpace(key) == "" {
		return nil, fmt.Errorf("epoch: Cache key is required")
	}
	operation := map[string]any{
		"kind": "increment", "shard": uint32(0), "key": key, "delta": strconv.FormatInt(delta, 10),
	}
	if options.ExpectedVersion != nil {
		operation["expected_version"] = strconv.FormatUint(*options.ExpectedVersion, 10)
	}
	if options.TTLMS != nil {
		if *options.TTLMS == 0 {
			return nil, fmt.Errorf("epoch: Cache TTL must be non-zero when provided")
		}
		operation["ttl_ms"] = strconv.FormatUint(*options.TTLMS, 10)
	}
	if options.LockGuard != nil {
		guard, err := options.LockGuard.wire()
		if err != nil {
			return nil, err
		}
		operation["lock_guard"] = guard
	}
	return operation, nil
}

func addCacheWriteOptions(operation map[string]any, options RegionalCacheWriteOptions) error {
	if options.TTLMS != nil {
		if *options.TTLMS == 0 {
			return fmt.Errorf("epoch: Cache TTL must be non-zero when provided")
		}
		operation["ttl_ms"] = strconv.FormatUint(*options.TTLMS, 10)
	}
	if options.LockGuard != nil {
		guard, err := options.LockGuard.wire()
		if err != nil {
			return err
		}
		operation["lock_guard"] = guard
	}
	return nil
}

func validateCacheLockIdentity(lockKey, owner string, ownerEpoch uint64) error {
	if strings.TrimSpace(lockKey) == "" {
		return fmt.Errorf("epoch: Cache lock key is required")
	}
	if strings.TrimSpace(owner) == "" {
		return fmt.Errorf("epoch: Cache lock owner is required")
	}
	if ownerEpoch == 0 {
		return fmt.Errorf("epoch: Cache lock owner epoch must be non-zero")
	}
	return nil
}

func cacheLockOperation(kind, lockKey, owner string, ownerEpoch uint64, leaseToken string) (map[string]any, error) {
	if err := validateCacheLockIdentity(lockKey, owner, ownerEpoch); err != nil {
		return nil, err
	}
	if strings.TrimSpace(leaseToken) == "" {
		return nil, fmt.Errorf("epoch: Cache lease token is required")
	}
	return map[string]any{
		"kind": kind, "shard": uint32(0), "lock_key": lockKey, "owner": owner,
		"owner_epoch": strconv.FormatUint(ownerEpoch, 10), "lease_token": leaseToken,
	}, nil
}
