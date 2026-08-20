package io.epoch.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
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
    return set(cache, shard, idempotencyKey, key, value, ttlMs, lockGuard, null);
  }

  /** Writes one strict Cache value to memory or the configured cold tier. */
  public JsonNode set(
      String cache,
      int shard,
      String idempotencyKey,
      String key,
      RegionalCacheValue value,
      BigInteger ttlMs,
      RegionalCacheLockGuard lockGuard,
      String storageClass)
      throws IOException, InterruptedException {
    ObjectNode operation = keyedOperation("set", key);
    operation.set("value", Objects.requireNonNull(value, "value").toJson());
    RegionalCacheMutation.optionalPositive(operation, "ttl_ms", ttlMs);
    addGuard(operation, lockGuard);
    if (storageClass != null) {
      if (!"memory".equals(storageClass) && !"cold".equals(storageClass)) {
        throw new IllegalArgumentException("Cache storage class must be memory or cold");
      }
      operation.put("storage_class", storageClass);
    }
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

  /** Applies one atomic advanced bitmap, probabilistic, geo, JSON, or vector transform. */
  public JsonNode transform(
      String cache,
      int shard,
      String idempotencyKey,
      String key,
      String kind,
      Map<String, ?> fields,
      BigInteger expectedVersion,
      BigInteger ttlMs,
      RegionalCacheLockGuard lockGuard)
      throws IOException, InterruptedException {
    ObjectNode operation = keyedOperation("transform", key);
    operation.set("transform", RegionalCacheMutation.transformJson(kind, fields));
    RegionalCacheMutation.optionalNonNegative(operation, "expected_version", expectedVersion);
    RegionalCacheMutation.optionalPositive(operation, "ttl_ms", ttlMs);
    addGuard(operation, lockGuard);
    return mutate(cache, shard, idempotencyKey, operation);
  }

  /** Returns one value and commits its access for deterministic LRU/LFU admission. */
  public JsonNode get(String cache, int shard, String idempotencyKey, String key)
      throws IOException, InterruptedException {
    return mutate(cache, shard, idempotencyKey, keyedOperation("get", key));
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

  /** Sends one ordered atomic batch as one HTTP request and consensus proposal. */
  public JsonNode atomicBatch(
      String cache,
      int shard,
      String idempotencyKey,
      BigInteger expectedRevision,
      List<RegionalCacheMutation> mutations,
      List<RegionalCacheLockGuard> lockGuards)
      throws IOException, InterruptedException {
    return transaction(cache, shard, idempotencyKey, expectedRevision, mutations, lockGuards);
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

  /** Returns retained durable Cache changes from one sequence cursor. */
  public JsonNode changes(String cache, int shard, BigInteger fromSequence, int limit)
      throws IOException, InterruptedException {
    RegionalClientCore.positiveU64(fromSequence, "Cache change sequence");
    if (limit <= 0) {
      throw new IllegalArgumentException("Cache change limit must be positive");
    }
    return read(
        cache,
        shard,
        "/changes",
        Map.of("from_sequence", fromSequence.toString(), "limit", Integer.toString(limit)));
  }

  /** Downloads one canonical bounded backup and PITR window. */
  public JsonNode backup(String cache, int shard) throws IOException, InterruptedException {
    return read(cache, shard, "/backup", Map.of());
  }

  /** Atomically restores one backup at a retained revision. */
  public JsonNode restore(
      String cache,
      int shard,
      String idempotencyKey,
      String artifactBase64,
      BigInteger targetRevision)
      throws IOException, InterruptedException {
    RegionalClientCore.required(artifactBase64, "Cache backup artifact");
    RegionalClientCore.positiveU64(targetRevision, "Cache restore revision");
    return mutate(
        cache,
        shard,
        idempotencyKey,
        operation("restore")
            .put("backup_base64", artifactBase64)
            .put("target_revision", targetRevision.toString()));
  }

  /** Runs one typed read-only bitmap, probabilistic, geo, JSON, or vector query. */
  public JsonNode query(String cache, int shard, String kind, Map<String, ?> fields)
      throws IOException, InterruptedException {
    return postRead(cache, shard, "/query", RegionalCacheMutation.transformJson(kind, fields));
  }

  /** Pipelines independently committed, correlated mutations through one HTTP request. */
  public JsonNode multiplex(String cache, int shard, List<RegionalCacheMultiplexMutation> mutations)
      throws IOException, InterruptedException {
    Objects.requireNonNull(mutations, "mutations");
    if (mutations.isEmpty() || mutations.size() > MAX_TRANSACTION_MUTATIONS) {
      throw new IllegalArgumentException(
          "Cache multiplex mutations must be between 1 and " + MAX_TRANSACTION_MUTATIONS);
    }
    Set<String> correlations = new HashSet<>();
    Set<String> identities = new HashSet<>();
    ArrayNode items = RegionalClientCore.MAPPER.createArrayNode();
    for (RegionalCacheMultiplexMutation mutation : mutations) {
      Objects.requireNonNull(mutation, "mutation");
      if (!correlations.add(mutation.correlationId())) {
        throw new IllegalArgumentException("Cache multiplex correlation IDs must be unique");
      }
      if (!identities.add(mutation.idempotencyKey())) {
        throw new IllegalArgumentException("Cache multiplex idempotency keys must be unique");
      }
      ObjectNode item = items.addObject();
      item.put("correlation_id", mutation.correlationId());
      item.put("idempotency_key", mutation.idempotencyKey());
      item.set("operation", mutation.operation());
    }
    return regional.call(
        "caches",
        "Cache",
        cache,
        shard,
        route -> {
          ObjectNode body = RegionalClientCore.MAPPER.createObjectNode();
          body.put("expected_term", route.term());
          body.set("mutations", items);
          return new RegionalClientCore.RequestSpec("POST", "/multiplex", body, Map.of(), Map.of());
        });
  }

  /** Creates one node-affine, at-most-once channel/pattern subscription. */
  public JsonNode createSubscription(
      String cache, int shard, List<String> channels, List<String> patterns)
      throws IOException, InterruptedException {
    if (channels.isEmpty() && patterns.isEmpty()) {
      throw new IllegalArgumentException("Cache Pub/Sub requires a channel or pattern");
    }
    ObjectNode body = RegionalClientCore.MAPPER.createObjectNode();
    body.set("channels", RegionalClientCore.MAPPER.valueToTree(List.copyOf(channels)));
    body.set("patterns", RegionalClientCore.MAPPER.valueToTree(List.copyOf(patterns)));
    return write(cache, shard, "POST", "/pubsub/subscriptions", body);
  }

  /** Publishes one explicitly lossy, at-most-once Cache message. */
  public JsonNode publish(String cache, int shard, String channel, Object payload)
      throws IOException, InterruptedException {
    RegionalClientCore.required(channel, "Cache Pub/Sub channel");
    ObjectNode body = RegionalClientCore.MAPPER.createObjectNode().put("channel", channel);
    body.set("payload", RegionalClientCore.MAPPER.valueToTree(payload));
    return write(cache, shard, "POST", "/pubsub/messages", body);
  }

  /** Drains up to limit pending at-most-once messages. */
  public JsonNode pollSubscription(String cache, int shard, String subscriptionId, int limit)
      throws IOException, InterruptedException {
    RegionalClientCore.required(subscriptionId, "Cache Pub/Sub subscription");
    if (limit <= 0) {
      throw new IllegalArgumentException("Cache Pub/Sub poll limit must be positive");
    }
    return read(
        cache,
        shard,
        "/pubsub/subscriptions/" + segment(subscriptionId) + "/messages",
        Map.of("limit", Integer.toString(limit)));
  }

  /** Deletes one node-local Cache Pub/Sub subscription. */
  public JsonNode deleteSubscription(String cache, int shard, String subscriptionId)
      throws IOException, InterruptedException {
    RegionalClientCore.required(subscriptionId, "Cache Pub/Sub subscription");
    return write(cache, shard, "DELETE", "/pubsub/subscriptions/" + segment(subscriptionId), null);
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

  private JsonNode postRead(String cache, int shard, String path, JsonNode body)
      throws IOException, InterruptedException {
    return regional.call(
        "caches",
        "Cache",
        cache,
        shard,
        route ->
            new RegionalClientCore.RequestSpec(
                "POST", path, body, Map.of(), Map.of("x-epoch-read-consistency", "linearizable")));
  }

  private JsonNode write(String cache, int shard, String method, String path, JsonNode body)
      throws IOException, InterruptedException {
    return regional.call(
        "caches",
        "Cache",
        cache,
        shard,
        route -> new RegionalClientCore.RequestSpec(method, path, body, Map.of(), Map.of()));
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

  private static String segment(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
  }
}
