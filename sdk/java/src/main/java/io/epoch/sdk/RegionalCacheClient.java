package io.epoch.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Authenticated, leader- and fence-aware client for the complete regional Cache lifecycle. */
public final class RegionalCacheClient {
  private static final int MAX_TRANSACTION_MUTATIONS = 128;
  private static final int MAX_MAINTENANCE_EXPIRATIONS = 1_000;

  private final RegionalClientCore regional;

  /** Constructs a client over one or more regional node endpoints. */
  public RegionalCacheClient(
      List<URI> endpoints, String token, RegionalScope scope, Duration timeout) {
    this.regional = RegionalClientCore.forEndpoints(endpoints, token, scope, timeout);
  }

  private RegionalCacheClient(List<Transport> transports, String token, RegionalScope scope) {
    this.regional = RegionalClientCore.forTransports(transports, token, scope);
  }

  /** Constructs with injected transports for tests or custom networking. */
  public static RegionalCacheClient withTransports(
      List<Transport> transports, String token, RegionalScope scope) {
    return new RegionalCacheClient(transports, token, scope);
  }

  /** Writes one strict Cache value. */
  public JsonNode set(
      String cache,
      int shard,
      String idempotencyKey,
      String key,
      RegionalCacheValue value,
      BigInteger ttlMs,
      RegionalCacheLockGuard lockGuard)
      throws IOException, InterruptedException {
    ObjectNode operation = keyedOperation("set", key);
    operation.set("value", Objects.requireNonNull(value, "value").toJson());
    RegionalCacheMutation.optionalPositive(operation, "ttl_ms", ttlMs);
    addGuard(operation, lockGuard);
    return mutate(cache, shard, idempotencyKey, operation);
  }

  /** Conditionally removes one Cache key. */
  public JsonNode delete(
      String cache,
      int shard,
      String idempotencyKey,
      String key,
      BigInteger expectedVersion,
      RegionalCacheLockGuard lockGuard)
      throws IOException, InterruptedException {
    ObjectNode operation = keyedOperation("delete", key);
    RegionalCacheMutation.optionalNonNegative(operation, "expected_version", expectedVersion);
    addGuard(operation, lockGuard);
    return mutate(cache, shard, idempotencyKey, operation);
  }

  /** Writes only when the version or missing-at-revision expectation holds. */
  public JsonNode compareAndSet(
      String cache,
      int shard,
      String idempotencyKey,
      String key,
      RegionalCacheExpectation expected,
      RegionalCacheValue value,
      BigInteger ttlMs,
      RegionalCacheLockGuard lockGuard)
      throws IOException, InterruptedException {
    ObjectNode operation = keyedOperation("compare_and_set", key);
    operation.set("expected", Objects.requireNonNull(expected, "expected").toJson());
    operation.set("value", Objects.requireNonNull(value, "value").toJson());
    RegionalCacheMutation.optionalPositive(operation, "ttl_ms", ttlMs);
    addGuard(operation, lockGuard);
    return mutate(cache, shard, idempotencyKey, operation);
  }

  /** Applies a checked signed 64-bit increment. */
  public JsonNode increment(
      String cache,
      int shard,
      String idempotencyKey,
      String key,
      long delta,
      BigInteger expectedVersion,
      BigInteger ttlMs,
      RegionalCacheLockGuard lockGuard)
      throws IOException, InterruptedException {
    ObjectNode operation = keyedOperation("increment", key).put("delta", Long.toString(delta));
    RegionalCacheMutation.optionalNonNegative(operation, "expected_version", expectedVersion);
    RegionalCacheMutation.optionalPositive(operation, "ttl_ms", ttlMs);
    addGuard(operation, lockGuard);
    return mutate(cache, shard, idempotencyKey, operation);
  }

  /** Commits one to 128 distinct-key mutations at one shard revision. */
  public JsonNode transaction(
      String cache,
      int shard,
      String idempotencyKey,
      BigInteger expectedRevision,
      List<RegionalCacheMutation> mutations,
      List<RegionalCacheLockGuard> lockGuards)
      throws IOException, InterruptedException {
    RegionalClientCore.nonNegativeU64(expectedRevision, "Cache expected revision");
    Objects.requireNonNull(mutations, "mutations");
    if (mutations.isEmpty() || mutations.size() > MAX_TRANSACTION_MUTATIONS) {
      throw new IllegalArgumentException(
          "Cache transaction mutations must be between 1 and " + MAX_TRANSACTION_MUTATIONS);
    }
    ObjectNode operation = operation("transaction");
    operation.put("expected_revision", expectedRevision.toString());
    ArrayNode mutationArray = operation.putArray("mutations");
    Set<String> keys = new HashSet<>();
    for (RegionalCacheMutation mutation : mutations) {
      Objects.requireNonNull(mutation, "mutation");
      if (!keys.add(mutation.key())) {
        throw new IllegalArgumentException("Cache transaction keys must be distinct");
      }
      mutationArray.add(mutation.toJson());
    }
    ArrayNode guardArray = operation.putArray("lock_guards");
    for (RegionalCacheLockGuard guard : Objects.requireNonNull(lockGuards, "lockGuards")) {
      guardArray.add(Objects.requireNonNull(guard, "lock guard").toJson());
    }
    return mutate(cache, shard, idempotencyKey, operation);
  }

  /** Creates or replaces an expired advisory lock lease. */
  public JsonNode acquireLock(
      String cache,
      int shard,
      String idempotencyKey,
      String lockKey,
      String owner,
      BigInteger ownerEpoch,
      BigInteger leaseMs)
      throws IOException, InterruptedException {
    lockIdentity(lockKey, owner, ownerEpoch);
    RegionalClientCore.positiveU64(leaseMs, "Cache lock lease");
    ObjectNode operation = operation("acquire_lock");
    operation
        .put("lock_key", lockKey)
        .put("owner", owner)
        .put("owner_epoch", ownerEpoch.toString())
        .put("lease_ms", leaseMs.toString());
    return mutate(cache, shard, idempotencyKey, operation);
  }

  /** Rotates the opaque lease token while retaining the lock's fencing token. */
  public JsonNode renewLock(
      String cache,
      int shard,
      String idempotencyKey,
      String lockKey,
      String owner,
      BigInteger ownerEpoch,
      String leaseToken,
      BigInteger extensionMs)
      throws IOException, InterruptedException {
    ObjectNode operation = lockOperation("renew_lock", lockKey, owner, ownerEpoch, leaseToken);
    RegionalClientCore.positiveU64(extensionMs, "Cache lock extension");
    operation.put("extension_ms", extensionMs.toString());
    return mutate(cache, shard, idempotencyKey, operation);
  }

  /** Removes the exact current advisory lock lease. */
  public JsonNode releaseLock(
      String cache,
      int shard,
      String idempotencyKey,
      String lockKey,
      String owner,
      BigInteger ownerEpoch,
      String leaseToken)
      throws IOException, InterruptedException {
    return mutate(
        cache,
        shard,
        idempotencyKey,
        lockOperation("release_lock", lockKey, owner, ownerEpoch, leaseToken));
  }

  /** Deterministically removes a bounded number of expired keys and locks. */
  public JsonNode maintain(String cache, int shard, String idempotencyKey, int maxExpirations)
      throws IOException, InterruptedException {
    if (maxExpirations < 1 || maxExpirations > MAX_MAINTENANCE_EXPIRATIONS) {
      throw new IllegalArgumentException(
          "Cache max expirations must be between 1 and " + MAX_MAINTENANCE_EXPIRATIONS);
    }
    return mutate(
        cache, shard, idempotencyKey, operation("maintain").put("max_expirations", maxExpirations));
  }

  /** Returns one mutation outcome over the complete unsigned 64-bit proposal range. */
  public JsonNode mutation(String cache, int shard, BigInteger proposalId)
      throws IOException, InterruptedException {
    RegionalClientCore.positiveU64(proposalId, "Cache proposal ID");
    return read(cache, shard, "/mutations/" + proposalId, Map.of());
  }

  /** Returns one linearizable Cache key observation. */
  public JsonNode observe(String cache, int shard, String key)
      throws IOException, InterruptedException {
    RegionalClientCore.required(key, "Cache key");
    return read(cache, shard, "/observations", Map.of("key", key));
  }

  /** Returns linearizable Cache tablet status and recovery evidence. */
  public JsonNode status(String cache, int shard) throws IOException, InterruptedException {
    return read(cache, shard, "/status", Map.of());
  }

  private JsonNode read(String cache, int shard, String path, Map<String, ?> query)
      throws IOException, InterruptedException {
    return regional.call(
        "caches",
        "Cache",
        cache,
        shard,
        route ->
            new RegionalClientCore.RequestSpec(
                "GET", path, null, query, Map.of("x-epoch-read-consistency", "linearizable")));
  }

  private JsonNode mutate(String cache, int shard, String idempotencyKey, ObjectNode operation)
      throws IOException, InterruptedException {
    RegionalClientCore.required(idempotencyKey, "idempotency key");
    return regional.call(
        "caches",
        "Cache",
        cache,
        shard,
        route -> {
          ObjectNode body = RegionalClientCore.MAPPER.createObjectNode();
          body.put("idempotency_key", idempotencyKey);
          body.put("expected_term", route.term());
          body.set("operation", operation);
          return new RegionalClientCore.RequestSpec("POST", "/mutations", body, Map.of(), Map.of());
        });
  }

  private static ObjectNode operation(String kind) {
    return RegionalClientCore.MAPPER.createObjectNode().put("kind", kind).put("shard", 0);
  }

  private static ObjectNode keyedOperation(String kind, String key) {
    RegionalClientCore.required(key, "Cache key");
    return operation(kind).put("key", key);
  }

  private static void addGuard(ObjectNode operation, RegionalCacheLockGuard lockGuard) {
    if (lockGuard != null) {
      operation.set("lock_guard", lockGuard.toJson());
    }
  }

  private static void lockIdentity(String lockKey, String owner, BigInteger ownerEpoch) {
    RegionalClientCore.required(lockKey, "Cache lock key");
    RegionalClientCore.required(owner, "Cache lock owner");
    RegionalClientCore.positiveU64(ownerEpoch, "Cache lock owner epoch");
  }

  private static ObjectNode lockOperation(
      String kind, String lockKey, String owner, BigInteger ownerEpoch, String leaseToken) {
    lockIdentity(lockKey, owner, ownerEpoch);
    RegionalClientCore.required(leaseToken, "Cache lease token");
    return operation(kind)
        .put("lock_key", lockKey)
        .put("owner", owner)
        .put("owner_epoch", ownerEpoch.toString())
        .put("lease_token", leaseToken);
  }
}
