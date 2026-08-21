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

/** Authenticated, leader- and fence-aware client for the complete regional Queue lifecycle. */
public final class RegionalQueueClient {
  private static final int MAX_ACQUIRE_BATCH = 100;
  private static final int MAX_IN_FLIGHT = 10_000;
  private static final int MAX_HISTORY = 1_000;

  private final RegionalClientCore regional;

  /** Constructs a client over one or more regional node endpoints. */
  public RegionalQueueClient(
      List<URI> endpoints, String token, RegionalScope scope, Duration timeout) {
    this.regional = RegionalClientCore.forEndpoints(endpoints, token, scope, timeout);
  }

  private RegionalQueueClient(List<Transport> transports, String token, RegionalScope scope) {
    this.regional = RegionalClientCore.forTransports(transports, token, scope);
  }

  /** Constructs with injected transports for tests or custom networking. */
  public static RegionalQueueClient withTransports(
      List<Transport> transports, String token, RegionalScope scope) {
    return new RegionalQueueClient(transports, token, scope);
  }

  /** Enqueues one message using an explicit idempotency key. */
  public JsonNode enqueue(String queue, int shard, String idempotencyKey, EventEnvelope event)
      throws IOException, InterruptedException {
    return enqueueAdvanced(queue, shard, idempotencyKey, event, null, null, null);
  }

  /** Enqueues one message with optional session and request/reply metadata. */
  public JsonNode enqueueAdvanced(
      String queue,
      int shard,
      String idempotencyKey,
      EventEnvelope event,
      String sessionId,
      String correlationId,
      String replyTo)
      throws IOException, InterruptedException {
    Objects.requireNonNull(event, "event");
    ObjectNode operation = operation("enqueue");
    operation.set("envelope", event.toJson());
    putOptionalRequired(operation, "session_id", sessionId, "Queue session ID");
    putOptionalRequired(operation, "correlation_id", correlationId, "Queue correlation ID");
    putOptionalRequired(operation, "reply_to", replyTo, "Queue reply destination");
    return mutate(queue, shard, idempotencyKey, operation);
  }

  /** Acquires a credit-aware batch using signed-long convenience values. */
  public JsonNode acquire(
      String queue,
      int shard,
      String idempotencyKey,
      String consumer,
      long consumerEpoch,
      int maxMessages,
      Integer maxInFlight,
      Long visibilityTimeoutMs)
      throws IOException, InterruptedException {
    return acquire(
        queue,
        shard,
        idempotencyKey,
        consumer,
        BigInteger.valueOf(consumerEpoch),
        maxMessages,
        maxInFlight,
        visibilityTimeoutMs == null ? null : BigInteger.valueOf(visibilityTimeoutMs));
  }

  /** Acquires a credit-aware batch over the complete unsigned 64-bit epoch and timeout range. */
  public JsonNode acquire(
      String queue,
      int shard,
      String idempotencyKey,
      String consumer,
      BigInteger consumerEpoch,
      int maxMessages,
      Integer maxInFlight,
      BigInteger visibilityTimeoutMs)
      throws IOException, InterruptedException {
    consumer(consumer, consumerEpoch);
    bounded(maxMessages, 1, MAX_ACQUIRE_BATCH, "Queue max messages");
    if (maxInFlight != null) {
      bounded(maxInFlight, 1, MAX_IN_FLIGHT, "Queue max in flight");
    }
    if (visibilityTimeoutMs != null) {
      RegionalClientCore.positiveU64(visibilityTimeoutMs, "Queue visibility timeout");
    }
    ObjectNode operation = settlementBase("acquire", consumer, consumerEpoch, null);
    operation.put("max_messages", maxMessages);
    if (maxInFlight != null) {
      operation.put("max_in_flight", maxInFlight);
    }
    if (visibilityTimeoutMs != null) {
      operation.put("visibility_timeout_ms", visibilityTimeoutMs.toString());
    }
    return mutate(queue, shard, idempotencyKey, operation);
  }

  /** Acquires FIFO messages under an exclusive fenced session lock. */
  public JsonNode acquireSession(
      String queue,
      int shard,
      String idempotencyKey,
      String sessionId,
      String consumer,
      BigInteger consumerEpoch,
      int maxMessages,
      int maxInFlight,
      BigInteger visibilityTimeoutMs,
      String sessionLockToken)
      throws IOException, InterruptedException {
    RegionalClientCore.required(sessionId, "Queue session ID");
    consumer(consumer, consumerEpoch);
    bounded(maxMessages, 1, MAX_ACQUIRE_BATCH, "Queue max messages");
    bounded(maxInFlight, 1, MAX_IN_FLIGHT, "Queue max in flight");
    if (visibilityTimeoutMs != null) {
      RegionalClientCore.positiveU64(visibilityTimeoutMs, "Queue visibility timeout");
    }
    ObjectNode operation = settlementBase("acquire", consumer, consumerEpoch, null);
    operation.put("session_id", sessionId);
    operation.put("max_messages", maxMessages);
    operation.put("max_in_flight", maxInFlight);
    if (visibilityTimeoutMs != null) {
      operation.put("visibility_timeout_ms", visibilityTimeoutMs.toString());
    }
    putOptionalRequired(
        operation, "session_lock_token", sessionLockToken, "Queue session lock token");
    return mutate(queue, shard, idempotencyKey, operation);
  }

  /** Renews one exact fenced session lock. */
  public JsonNode renewSessionLock(
      String queue,
      int shard,
      String idempotencyKey,
      String consumer,
      BigInteger consumerEpoch,
      String sessionLockToken,
      BigInteger extensionMs)
      throws IOException, InterruptedException {
    consumer(consumer, consumerEpoch);
    RegionalClientCore.required(sessionLockToken, "Queue session lock token");
    RegionalClientCore.positiveU64(extensionMs, "Queue session lock extension");
    ObjectNode operation = operation("renew_session_lock");
    operation.put("consumer", consumer);
    operation.put("consumer_epoch", consumerEpoch.toString());
    operation.put("session_lock_token", sessionLockToken);
    operation.put("extension_ms", extensionMs.toString());
    return mutate(queue, shard, idempotencyKey, operation);
  }

  /** Releases one exact fenced session lock. */
  public JsonNode releaseSessionLock(
      String queue,
      int shard,
      String idempotencyKey,
      String consumer,
      BigInteger consumerEpoch,
      String sessionLockToken)
      throws IOException, InterruptedException {
    consumer(consumer, consumerEpoch);
    RegionalClientCore.required(sessionLockToken, "Queue session lock token");
    ObjectNode operation = operation("release_session_lock");
    operation.put("consumer", consumer);
    operation.put("consumer_epoch", consumerEpoch.toString());
    operation.put("session_lock_token", sessionLockToken);
    return mutate(queue, shard, idempotencyKey, operation);
  }

  /** Defers a live delivery until exact message-ID retrieval. */
  public JsonNode defer(
      String queue,
      int shard,
      String idempotencyKey,
      String consumer,
      BigInteger consumerEpoch,
      String leaseToken,
      String reason)
      throws IOException, InterruptedException {
    RegionalClientCore.required(reason, "Queue defer reason");
    ObjectNode operation = settlementBase("defer", consumer, consumerEpoch, leaseToken);
    operation.put("reason", reason);
    return mutate(queue, shard, idempotencyKey, operation);
  }

  /** Receives one exact deferred message. */
  public JsonNode receiveDeferred(
      String queue,
      int shard,
      String idempotencyKey,
      String messageId,
      String consumer,
      BigInteger consumerEpoch,
      BigInteger visibilityTimeoutMs)
      throws IOException, InterruptedException {
    RegionalClientCore.required(messageId, "Queue message ID");
    consumer(consumer, consumerEpoch);
    ObjectNode operation = operation("receive_deferred");
    operation.put("message_id", messageId);
    operation.put("consumer", consumer);
    operation.put("consumer_epoch", consumerEpoch.toString());
    if (visibilityTimeoutMs != null) {
      RegionalClientCore.positiveU64(visibilityTimeoutMs, "Queue visibility timeout");
      operation.put("visibility_timeout_ms", visibilityTimeoutMs.toString());
    }
    return mutate(queue, shard, idempotencyKey, operation);
  }

  /** Permanently settles a fenced lease. */
  public JsonNode acknowledge(
      String queue,
      int shard,
      String idempotencyKey,
      String consumer,
      long consumerEpoch,
      String leaseToken)
      throws IOException, InterruptedException {
    return acknowledge(
        queue, shard, idempotencyKey, consumer, BigInteger.valueOf(consumerEpoch), leaseToken);
  }

  /** Permanently settles a fenced lease over the complete unsigned 64-bit epoch range. */
  public JsonNode acknowledge(
      String queue,
      int shard,
      String idempotencyKey,
      String consumer,
      BigInteger consumerEpoch,
      String leaseToken)
      throws IOException, InterruptedException {
    return mutate(
        queue,
        shard,
        idempotencyKey,
        settlementBase("acknowledge", consumer, consumerEpoch, leaseToken));
  }

  /** Extends a fenced lease using signed-long convenience values. */
  public JsonNode extendLease(
      String queue,
      int shard,
      String idempotencyKey,
      String consumer,
      long consumerEpoch,
      String leaseToken,
      long extensionMs)
      throws IOException, InterruptedException {
    return extendLease(
        queue,
        shard,
        idempotencyKey,
        consumer,
        BigInteger.valueOf(consumerEpoch),
        leaseToken,
        BigInteger.valueOf(extensionMs));
  }

  /** Extends a fenced lease over the complete unsigned 64-bit time range. */
  public JsonNode extendLease(
      String queue,
      int shard,
      String idempotencyKey,
      String consumer,
      BigInteger consumerEpoch,
      String leaseToken,
      BigInteger extensionMs)
      throws IOException, InterruptedException {
    RegionalClientCore.positiveU64(extensionMs, "Queue lease extension");
    ObjectNode operation = settlementBase("extend_lease", consumer, consumerEpoch, leaseToken);
    operation.put("extension_ms", extensionMs.toString());
    return mutate(queue, shard, idempotencyKey, operation);
  }

  /** Releases a fenced lease using signed-long convenience values. */
  public JsonNode release(
      String queue,
      int shard,
      String idempotencyKey,
      String consumer,
      long consumerEpoch,
      String leaseToken,
      long delayMs,
      String reason)
      throws IOException, InterruptedException {
    return release(
        queue,
        shard,
        idempotencyKey,
        consumer,
        BigInteger.valueOf(consumerEpoch),
        leaseToken,
        BigInteger.valueOf(delayMs),
        reason);
  }

  /** Releases a fenced lease over the complete unsigned 64-bit delay range. */
  public JsonNode release(
      String queue,
      int shard,
      String idempotencyKey,
      String consumer,
      BigInteger consumerEpoch,
      String leaseToken,
      BigInteger delayMs,
      String reason)
      throws IOException, InterruptedException {
    RegionalClientCore.nonNegativeU64(delayMs, "Queue release delay");
    ObjectNode operation = settlementBase("release", consumer, consumerEpoch, leaseToken);
    operation.put("delay_ms", delayMs.toString());
    if (reason != null && !reason.isBlank()) {
      operation.put("reason", reason);
    }
    return mutate(queue, shard, idempotencyKey, operation);
  }

  /** Records a retryable processing failure. */
  public JsonNode nack(
      String queue,
      int shard,
      String idempotencyKey,
      String consumer,
      long consumerEpoch,
      String leaseToken,
      String reason)
      throws IOException, InterruptedException {
    return disposition(
        queue,
        shard,
        idempotencyKey,
        "nack",
        consumer,
        BigInteger.valueOf(consumerEpoch),
        leaseToken,
        reason);
  }

  /** Records a retryable processing failure over the complete unsigned 64-bit epoch range. */
  public JsonNode nack(
      String queue,
      int shard,
      String idempotencyKey,
      String consumer,
      BigInteger consumerEpoch,
      String leaseToken,
      String reason)
      throws IOException, InterruptedException {
    return disposition(
        queue, shard, idempotencyKey, "nack", consumer, consumerEpoch, leaseToken, reason);
  }

  /** Dead-letters a fenced lease with a terminal reason. */
  public JsonNode reject(
      String queue,
      int shard,
      String idempotencyKey,
      String consumer,
      long consumerEpoch,
      String leaseToken,
      String reason)
      throws IOException, InterruptedException {
    return disposition(
        queue,
        shard,
        idempotencyKey,
        "reject",
        consumer,
        BigInteger.valueOf(consumerEpoch),
        leaseToken,
        reason);
  }

  /** Dead-letters a fenced lease over the complete unsigned 64-bit epoch range. */
  public JsonNode reject(
      String queue,
      int shard,
      String idempotencyKey,
      String consumer,
      BigInteger consumerEpoch,
      String leaseToken,
      String reason)
      throws IOException, InterruptedException {
    return disposition(
        queue, shard, idempotencyKey, "reject", consumer, consumerEpoch, leaseToken, reason);
  }

  private JsonNode disposition(
      String queue,
      int shard,
      String idempotencyKey,
      String kind,
      String consumer,
      BigInteger consumerEpoch,
      String leaseToken,
      String reason)
      throws IOException, InterruptedException {
    RegionalClientCore.required(reason, "Queue disposition reason");
    ObjectNode operation = settlementBase(kind, consumer, consumerEpoch, leaseToken);
    operation.put("reason", reason);
    return mutate(queue, shard, idempotencyKey, operation);
  }

  /** Redrives an exact dead-letter history entry. */
  public JsonNode redrive(
      String queue, int shard, String idempotencyKey, String messageId, long deadLetterHistoryId)
      throws IOException, InterruptedException {
    return redrive(
        queue, shard, idempotencyKey, messageId, BigInteger.valueOf(deadLetterHistoryId));
  }

  /** Redrives an exact dead-letter entry over the complete unsigned 64-bit history range. */
  public JsonNode redrive(
      String queue,
      int shard,
      String idempotencyKey,
      String messageId,
      BigInteger deadLetterHistoryId)
      throws IOException, InterruptedException {
    RegionalClientCore.required(messageId, "Queue message ID");
    RegionalClientCore.positiveU64(deadLetterHistoryId, "Queue dead-letter history ID");
    ObjectNode operation = operation("redrive");
    operation.put("message_id", messageId);
    operation.put("dead_letter_history_id", deadLetterHistoryId.toString());
    return mutate(queue, shard, idempotencyKey, operation);
  }

  /** Applies due-delay and visibility-timeout transitions. */
  public JsonNode maintain(String queue, int shard, String idempotencyKey)
      throws IOException, InterruptedException {
    return mutate(queue, shard, idempotencyKey, operation("maintain"));
  }

  /** Returns one mutation outcome by proposal ID. */
  public JsonNode mutation(String queue, int shard, long proposalId)
      throws IOException, InterruptedException {
    return mutation(queue, shard, BigInteger.valueOf(proposalId));
  }

  /** Returns one mutation outcome over the complete unsigned 64-bit proposal range. */
  public JsonNode mutation(String queue, int shard, BigInteger proposalId)
      throws IOException, InterruptedException {
    RegionalClientCore.positiveU64(proposalId, "Queue proposal ID");
    return read(queue, shard, "/mutations/" + proposalId, Map.of());
  }

  /** Returns a linearizable Queue state summary. */
  public JsonNode counts(String queue, int shard) throws IOException, InterruptedException {
    return read(queue, shard, "/counts", Map.of());
  }

  /** Returns bounded linearizable dead-letter history. */
  public JsonNode deadLetters(String queue, int shard, int limit)
      throws IOException, InterruptedException {
    return history(queue, shard, "/dead-letters", limit);
  }

  /** Returns bounded linearizable redrive history. */
  public JsonNode redrives(String queue, int shard, int limit)
      throws IOException, InterruptedException {
    return history(queue, shard, "/redrives", limit);
  }

  /** Returns linearizable credit and in-flight state for one consumer. */
  public JsonNode consumerFlow(String queue, int shard, String consumer)
      throws IOException, InterruptedException {
    return read(
        queue,
        shard,
        "/consumers/" + RegionalClientCore.segment(consumer, "Queue consumer") + "/flow",
        Map.of());
  }

  /** Returns replicated capacity, expiry, session, defer, and circuit state. */
  public JsonNode advancedStatus(String queue, int shard) throws IOException, InterruptedException {
    return read(queue, shard, "/advanced", Map.of());
  }

  /** Returns active messages matching one request/reply correlation ID. */
  public JsonNode correlation(String queue, int shard, String correlationId)
      throws IOException, InterruptedException {
    return read(
        queue,
        shard,
        "/correlations/" + RegionalClientCore.segment(correlationId, "Queue correlation ID"),
        Map.of());
  }

  /** Returns the bounded pending Queue dead-letter forwarding outbox. */
  public JsonNode deadLetterForwards(String queue, int shard, int limit)
      throws IOException, InterruptedException {
    return history(queue, shard, "/dead-letter-forwards", limit);
  }

  /** Returns linearizable Queue tablet status and digest. */
  public JsonNode status(String queue, int shard) throws IOException, InterruptedException {
    return read(queue, shard, "/status", Map.of());
  }

  private JsonNode history(String queue, int shard, String path, int limit)
      throws IOException, InterruptedException {
    bounded(limit, 1, MAX_HISTORY, "Queue history limit");
    return read(queue, shard, path, Map.of("limit", limit));
  }

  private JsonNode read(String queue, int shard, String path, Map<String, ?> query)
      throws IOException, InterruptedException {
    return regional.call(
        "queues",
        "Queue",
        queue,
        shard,
        route ->
            new RegionalClientCore.RequestSpec(
                "GET", path, null, query, Map.of("x-epoch-read-consistency", "linearizable")));
  }

  private JsonNode mutate(String queue, int shard, String idempotencyKey, ObjectNode operation)
      throws IOException, InterruptedException {
    RegionalClientCore.required(idempotencyKey, "idempotency key");
    return regional.call(
        "queues",
        "Queue",
        queue,
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
    return RegionalClientCore.MAPPER.createObjectNode().put("kind", kind).put("partition", 0);
  }

  private static ObjectNode settlementBase(
      String kind, String consumer, BigInteger consumerEpoch, String leaseToken) {
    consumer(consumer, consumerEpoch);
    ObjectNode operation = operation(kind);
    operation.put("consumer", consumer);
    operation.put("consumer_epoch", consumerEpoch.toString());
    if (leaseToken != null) {
      RegionalClientCore.required(leaseToken, "Queue lease token");
      operation.put("lease_token", leaseToken);
    }
    return operation;
  }

  private static void consumer(String consumer, BigInteger consumerEpoch) {
    RegionalClientCore.required(consumer, "Queue consumer");
    RegionalClientCore.positiveU64(consumerEpoch, "Queue consumer epoch");
  }

  private static void putOptionalRequired(
      ObjectNode operation, String field, String value, String label) {
    if (value != null) {
      RegionalClientCore.required(value, label);
      operation.put(field, value);
    }
  }

  private static void bounded(int value, int minimum, int maximum, String label) {
    if (value < minimum || value > maximum) {
      throw new IllegalArgumentException(label + " must be between " + minimum + " and " + maximum);
    }
  }
}
