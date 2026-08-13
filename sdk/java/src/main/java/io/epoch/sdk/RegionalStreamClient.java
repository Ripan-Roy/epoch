package io.epoch.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Authenticated, leader- and fence-aware client for regional Stream shards. */
public final class RegionalStreamClient {
  private static final int MAX_FETCH_RECORDS = 1_000;
  private static final Duration MIN_SESSION_TIMEOUT = Duration.ofSeconds(1);
  private static final Duration MAX_SESSION_TIMEOUT = Duration.ofMinutes(5);

  private final RegionalClientCore regional;

  /** Constructs a client over one or more regional node endpoints. */
  public RegionalStreamClient(
      List<URI> endpoints, String token, RegionalScope scope, Duration timeout) {
    this.regional = RegionalClientCore.forEndpoints(endpoints, token, scope, timeout);
  }

  private RegionalStreamClient(List<Transport> transports, String token, RegionalScope scope) {
    this.regional = RegionalClientCore.forTransports(transports, token, scope);
  }

  /** Constructs with injected transports for tests or custom networking. */
  public static RegionalStreamClient withTransports(
      List<Transport> transports, String token, RegionalScope scope) {
    return new RegionalStreamClient(transports, token, scope);
  }

  /** Appends one record using an explicit idempotency key. */
  public JsonNode append(String stream, int shard, String idempotencyKey, EventEnvelope event)
      throws IOException, InterruptedException {
    RegionalClientCore.required(idempotencyKey, "idempotency key");
    Objects.requireNonNull(event, "event");
    return call(
        stream,
        shard,
        route -> {
          ObjectNode body = RegionalClientCore.MAPPER.createObjectNode();
          body.put("idempotency_key", idempotencyKey);
          body.put("expected_term", route.term());
          body.put("partition", 0);
          body.set("envelope", event.toJson());
          return new RegionalClientCore.RequestSpec("POST", "/records", body, Map.of(), Map.of());
        });
  }

  /** Atomically appends one caller-framed batch to a single Stream shard. */
  public JsonNode appendBatch(
      String stream, int shard, String idempotencyKey, StreamBatchFrame frame)
      throws IOException, InterruptedException {
    RegionalClientCore.required(idempotencyKey, "idempotency key");
    Objects.requireNonNull(frame, "frame");
    return call(
        stream,
        shard,
        route ->
            new RegionalClientCore.RequestSpec(
                "POST",
                "/records/batches",
                frame.toRequest(idempotencyKey, route.term()),
                Map.of(),
                Map.of()));
  }

  /**
   * Discovers the Stream partitioning contract and appends by event key, falling back to event ID.
   * The target generation is pinned so an expansion race cannot silently remap an uncertain write.
   */
  public JsonNode appendKeyed(String stream, String idempotencyKey, EventEnvelope event)
      throws IOException, InterruptedException {
    RegionalClientCore.required(idempotencyKey, "idempotency key");
    Objects.requireNonNull(event, "event");
    RegionalClientCore.Route routing = regional.discoverRoute("streams", "Stream", stream, 0);
    RegionalClientCore.StreamPartitioning partitioning = routing.streamPartitioning();
    if (partitioning == null
        || !StreamPartitioner.ALGORITHM.equals(partitioning.algorithm())
        || !"utf8".equals(partitioning.keyEncoding())
        || !"event_id".equals(partitioning.missingKeyFallback())
        || partitioning.shardCount() <= 0) {
      throw new IOException("regional Stream partitioning metadata is unsupported or incomplete");
    }
    String partitionValue = event.key();
    if (partitionValue == null || partitionValue.isEmpty()) {
      partitionValue = event.id();
    }
    int shard = StreamPartitioner.shardFor(partitionValue, partitioning.shardCount());
    return regional.callAtGeneration(
        "streams",
        "Stream",
        stream,
        shard,
        routing.resourceGeneration(),
        route -> {
          ObjectNode body = RegionalClientCore.MAPPER.createObjectNode();
          body.put("idempotency_key", idempotencyKey);
          body.put("expected_term", route.term());
          body.put("partition", 0);
          body.set("envelope", event.toJson());
          return new RegionalClientCore.RequestSpec("POST", "/records", body, Map.of(), Map.of());
        });
  }

  /** Performs a linearizable bounded fetch. */
  public JsonNode fetch(String stream, int shard, long offset, int limit)
      throws IOException, InterruptedException {
    return fetch(stream, shard, BigInteger.valueOf(offset), limit);
  }

  /** Performs a linearizable bounded fetch over the complete unsigned 64-bit offset range. */
  public JsonNode fetch(String stream, int shard, BigInteger offset, int limit)
      throws IOException, InterruptedException {
    RegionalClientCore.nonNegativeU64(offset, "offset");
    fetchLimit(limit);
    return call(
        stream,
        shard,
        route ->
            new RegionalClientCore.RequestSpec(
                "GET", "/records", null, Map.of("offset", offset, "limit", limit), linearizable()));
  }

  /** Commits or explicitly resets a generation-fenced next offset. */
  public JsonNode commitOffset(
      String stream,
      int shard,
      String group,
      String memberId,
      long generation,
      long nextOffset,
      boolean reset,
      String idempotencyKey)
      throws IOException, InterruptedException {
    return commitOffset(
        stream,
        shard,
        group,
        memberId,
        BigInteger.valueOf(generation),
        BigInteger.valueOf(nextOffset),
        reset,
        idempotencyKey);
  }

  /** Commits or resets a checkpoint over the complete unsigned 64-bit position range. */
  public JsonNode commitOffset(
      String stream,
      int shard,
      String group,
      String memberId,
      BigInteger generation,
      BigInteger nextOffset,
      boolean reset,
      String idempotencyKey)
      throws IOException, InterruptedException {
    String groupSegment = RegionalClientCore.segment(group, "consumer group");
    RegionalClientCore.required(memberId, "consumer member");
    RegionalClientCore.positiveU64(generation, "consumer group generation");
    RegionalClientCore.nonNegativeU64(nextOffset, "next offset");
    RegionalClientCore.required(idempotencyKey, "idempotency key");
    return call(
        stream,
        shard,
        route -> {
          ObjectNode body = RegionalClientCore.MAPPER.createObjectNode();
          body.put("idempotency_key", idempotencyKey);
          body.put("expected_term", route.term());
          body.put("member_id", memberId);
          body.put("group_generation", generation.toString());
          body.put("partition", 0);
          body.put("next_offset", nextOffset.toString());
          body.put("mode", reset ? "reset" : "commit");
          return new RegionalClientCore.RequestSpec(
              "PUT", "/groups/" + groupSegment + "/offsets", body, Map.of(), Map.of());
        });
  }

  /** Returns a linearizable checkpoint and lag observation. */
  public JsonNode lag(String stream, int shard, String group)
      throws IOException, InterruptedException {
    String groupSegment = RegionalClientCore.segment(group, "consumer group");
    return call(
        stream,
        shard,
        route ->
            new RegionalClientCore.RequestSpec(
                "GET", "/groups/" + groupSegment + "/lag", null, Map.of(), linearizable()));
  }

  /** Fetches records beginning at the durable group checkpoint. */
  public JsonNode fetchGroup(String stream, int shard, String group, int limit)
      throws IOException, InterruptedException {
    fetchLimit(limit);
    String groupSegment = RegionalClientCore.segment(group, "consumer group");
    return call(
        stream,
        shard,
        route ->
            new RegionalClientCore.RequestSpec(
                "GET",
                "/groups/" + groupSegment + "/records",
                null,
                Map.of("limit", limit),
                linearizable()));
  }

  /** Joins or renews a coordinated consumer member and returns its shard assignment. */
  public JsonNode joinConsumerSession(
      String stream, String group, String memberId, Duration sessionTimeout, String idempotencyKey)
      throws IOException, InterruptedException {
    String groupSegment = RegionalClientCore.segment(group, "consumer group");
    RegionalClientCore.required(memberId, "consumer member");
    Objects.requireNonNull(sessionTimeout, "sessionTimeout");
    if (sessionTimeout.compareTo(MIN_SESSION_TIMEOUT) < 0
        || sessionTimeout.compareTo(MAX_SESSION_TIMEOUT) > 0
        || sessionTimeout.getNano() % 1_000_000 != 0) {
      throw new IllegalArgumentException(
          "consumer session timeout must be a whole millisecond between 1 second and 5 minutes");
    }
    RegionalClientCore.required(idempotencyKey, "idempotency key");
    return call(
        stream,
        0,
        route -> {
          ObjectNode body = RegionalClientCore.MAPPER.createObjectNode();
          body.put("idempotency_key", idempotencyKey);
          body.put("expected_term", route.term());
          body.put("member_id", memberId);
          body.put("session_timeout_ms", sessionTimeout.toMillis());
          return new RegionalClientCore.RequestSpec(
              "POST", "/groups/" + groupSegment + "/sessions", body, Map.of(), Map.of());
        });
  }

  /** Renews one coordinated member using the current group-generation fence. */
  public JsonNode heartbeatConsumerSession(
      String stream, String group, String memberId, long generation, String idempotencyKey)
      throws IOException, InterruptedException {
    return heartbeatConsumerSession(
        stream, group, memberId, BigInteger.valueOf(generation), idempotencyKey);
  }

  /** Renews one coordinated member over the complete unsigned generation range. */
  public JsonNode heartbeatConsumerSession(
      String stream, String group, String memberId, BigInteger generation, String idempotencyKey)
      throws IOException, InterruptedException {
    return mutateConsumerSession(
        "PUT", stream, group, memberId, generation, idempotencyKey, "/heartbeat");
  }

  /** Leaves a coordinated group and deterministically reassigns the member's shards. */
  public JsonNode leaveConsumerSession(
      String stream, String group, String memberId, long generation, String idempotencyKey)
      throws IOException, InterruptedException {
    return leaveConsumerSession(
        stream, group, memberId, BigInteger.valueOf(generation), idempotencyKey);
  }

  /** Leaves a coordinated group over the complete unsigned generation range. */
  public JsonNode leaveConsumerSession(
      String stream, String group, String memberId, BigInteger generation, String idempotencyKey)
      throws IOException, InterruptedException {
    return mutateConsumerSession("DELETE", stream, group, memberId, generation, idempotencyKey, "");
  }

  /** Commits an inclusive-deadline expiry sweep on the shard-zero coordinator. */
  public JsonNode maintainConsumerSession(String stream, String group, String idempotencyKey)
      throws IOException, InterruptedException {
    String groupSegment = RegionalClientCore.segment(group, "consumer group");
    RegionalClientCore.required(idempotencyKey, "idempotency key");
    return call(
        stream,
        0,
        route -> {
          ObjectNode body = RegionalClientCore.MAPPER.createObjectNode();
          body.put("idempotency_key", idempotencyKey);
          body.put("expected_term", route.term());
          return new RegionalClientCore.RequestSpec(
              "POST",
              "/groups/" + groupSegment + "/sessions/maintenance",
              body,
              Map.of(),
              Map.of());
        });
  }

  /** Returns linearizable coordinated membership, deadlines, generation, and assignments. */
  public JsonNode consumerSession(String stream, String group)
      throws IOException, InterruptedException {
    String groupSegment = RegionalClientCore.segment(group, "consumer group");
    return call(
        stream,
        0,
        route ->
            new RegionalClientCore.RequestSpec(
                "GET", "/groups/" + groupSegment + "/sessions", null, Map.of(), linearizable()));
  }

  private JsonNode mutateConsumerSession(
      String method,
      String stream,
      String group,
      String memberId,
      BigInteger generation,
      String idempotencyKey,
      String suffix)
      throws IOException, InterruptedException {
    String groupSegment = RegionalClientCore.segment(group, "consumer group");
    String memberSegment = RegionalClientCore.segment(memberId, "consumer member");
    RegionalClientCore.positiveU64(generation, "consumer group generation");
    RegionalClientCore.required(idempotencyKey, "idempotency key");
    return call(
        stream,
        0,
        route -> {
          ObjectNode body = RegionalClientCore.MAPPER.createObjectNode();
          body.put("idempotency_key", idempotencyKey);
          body.put("expected_term", route.term());
          body.put("group_generation", generation.toString());
          return new RegionalClientCore.RequestSpec(
              method,
              "/groups/" + groupSegment + "/sessions/" + memberSegment + suffix,
              body,
              Map.of(),
              Map.of());
        });
  }

  /** Commits a replacement time/size/count policy and immediately applies it. */
  public JsonNode configureRetention(
      String stream, int shard, String idempotencyKey, StreamRetentionPolicy policy)
      throws IOException, InterruptedException {
    RegionalClientCore.required(idempotencyKey, "idempotency key");
    Objects.requireNonNull(policy, "policy");
    return call(
        stream,
        shard,
        route -> {
          ObjectNode body = policy.toJson();
          body.put("idempotency_key", idempotencyKey);
          body.put("expected_term", route.term());
          return new RegionalClientCore.RequestSpec("PUT", "/retention", body, Map.of(), Map.of());
        });
  }

  /** Commits an idle-stream age sweep using the current leader time. */
  public JsonNode maintainRetention(String stream, int shard, String idempotencyKey)
      throws IOException, InterruptedException {
    RegionalClientCore.required(idempotencyKey, "idempotency key");
    return call(
        stream,
        shard,
        route -> {
          ObjectNode body = RegionalClientCore.MAPPER.createObjectNode();
          body.put("idempotency_key", idempotencyKey);
          body.put("expected_term", route.term());
          return new RegionalClientCore.RequestSpec(
              "POST", "/retention/maintenance", body, Map.of(), Map.of());
        });
  }

  /** Returns a linearizable policy, watermark, retained boundary, and byte count. */
  public JsonNode retention(String stream, int shard) throws IOException, InterruptedException {
    return call(
        stream,
        shard,
        route ->
            new RegionalClientCore.RequestSpec(
                "GET", "/retention", null, Map.of(), linearizable()));
  }

  private JsonNode call(String stream, int shard, RegionalClientCore.RequestFactory requestFactory)
      throws IOException, InterruptedException {
    return regional.call("streams", "Stream", stream, shard, requestFactory);
  }

  private static Map<String, String> linearizable() {
    return Map.of("x-epoch-read-consistency", "linearizable");
  }

  private static void fetchLimit(int limit) {
    if (limit < 1 || limit > MAX_FETCH_RECORDS) {
      throw new IllegalArgumentException("fetch limit must be between 1 and " + MAX_FETCH_RECORDS);
    }
  }
}
