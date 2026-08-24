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

/** Authenticated, leader- and fence-aware client for the regional Event Bus lifecycle. */
public final class RegionalBusClient {
  private static final int MAX_DELIVERY_BATCH = 100;
  private static final int MAX_READ_RESULTS = 10_000;
  private static final long MAX_LONG_POLL_MS = 30_000;

  private final RegionalClientCore regional;

  /** Constructs a client over one or more regional node endpoints. */
  public RegionalBusClient(
      List<URI> endpoints, String token, RegionalScope scope, Duration timeout) {
    this.regional = RegionalClientCore.forEndpoints(endpoints, token, scope, timeout);
  }

  public RegionalBusClient(
      List<URI> endpoints, String token, RegionalScope scope, Duration timeout, TlsConfig tls) {
    this.regional = RegionalClientCore.forEndpoints(endpoints, token, scope, timeout, tls);
  }

  private RegionalBusClient(List<Transport> transports, String token, RegionalScope scope) {
    this.regional = RegionalClientCore.forTransports(transports, token, scope);
  }

  /** Constructs with injected transports for tests or custom networking. */
  public static RegionalBusClient withTransports(
      List<Transport> transports, String token, RegionalScope scope) {
    return new RegionalBusClient(transports, token, scope);
  }

  /** Creates or replaces one typed subscription. */
  public JsonNode upsertSubscription(
      String bus, int shard, String idempotencyKey, Subscription subscription)
      throws IOException, InterruptedException {
    Objects.requireNonNull(subscription, "subscription");
    ObjectNode operation = operation("upsert_subscription");
    operation.set("subscription", subscription.toJson(RegionalClientCore.MAPPER));
    return mutate(bus, shard, idempotencyKey, operation);
  }

  /** Removes one exact subscription. */
  public JsonNode removeSubscription(String bus, int shard, String idempotencyKey, String name)
      throws IOException, InterruptedException {
    RegionalClientCore.required(name, "subscription name");
    ObjectNode operation = operation("remove_subscription");
    operation.put("name", name);
    return mutate(bus, shard, idempotencyKey, operation);
  }

  /** Routes and archives one strict event envelope. */
  public JsonNode publish(String bus, int shard, String idempotencyKey, EventEnvelope event)
      throws IOException, InterruptedException {
    Objects.requireNonNull(event, "event");
    ObjectNode operation = operation("publish");
    operation.set("envelope", event.toJson());
    return mutate(bus, shard, idempotencyKey, operation);
  }

  /** Leases a bounded delivery batch to one dispatcher epoch. */
  public JsonNode acquireDeliveries(
      String bus,
      int shard,
      String idempotencyKey,
      String subscription,
      String dispatcher,
      BigInteger dispatcherEpoch,
      int maxDeliveries)
      throws IOException, InterruptedException {
    return acquireDeliveries(
        bus,
        shard,
        idempotencyKey,
        subscription,
        dispatcher,
        dispatcherEpoch,
        maxDeliveries,
        Duration.ZERO);
  }

  /** Long-polls and leases a bounded delivery batch to one dispatcher epoch. */
  public JsonNode acquireDeliveries(
      String bus,
      int shard,
      String idempotencyKey,
      String subscription,
      String dispatcher,
      BigInteger dispatcherEpoch,
      int maxDeliveries,
      Duration wait)
      throws IOException, InterruptedException {
    RegionalClientCore.required(subscription, "subscription name");
    RegionalClientCore.required(dispatcher, "dispatcher");
    RegionalClientCore.positiveU64(dispatcherEpoch, "dispatcher epoch");
    deliveryBatch(maxDeliveries);
    Objects.requireNonNull(wait, "wait");
    if (wait.isNegative()
        || wait.compareTo(Duration.ofMillis(MAX_LONG_POLL_MS)) > 0
        || wait.toNanosPart() % 1_000_000 != 0) {
      throw new IllegalArgumentException(
          "delivery wait must be a whole number of milliseconds between 0 and " + MAX_LONG_POLL_MS);
    }
    ObjectNode operation = operation("acquire_deliveries");
    operation.put("subscription", subscription);
    operation.put("dispatcher", dispatcher);
    operation.put("dispatcher_epoch", dispatcherEpoch.toString());
    operation.put("max_deliveries", maxDeliveries);
    operation.put("wait_ms", wait.toMillis());
    return mutate(bus, shard, idempotencyKey, operation);
  }

  /** Permanently settles one fenced delivery lease. */
  public JsonNode acknowledgeDelivery(
      String bus,
      int shard,
      String idempotencyKey,
      String deliveryId,
      String dispatcher,
      BigInteger dispatcherEpoch,
      String leaseToken)
      throws IOException, InterruptedException {
    return mutate(
        bus,
        shard,
        idempotencyKey,
        settlement("acknowledge_delivery", deliveryId, dispatcher, dispatcherEpoch, leaseToken));
  }

  /** Records a fenced failure and deterministic retry/dead-letter transition. */
  public JsonNode failDelivery(
      String bus,
      int shard,
      String idempotencyKey,
      String deliveryId,
      String dispatcher,
      BigInteger dispatcherEpoch,
      String leaseToken,
      String reason)
      throws IOException, InterruptedException {
    RegionalClientCore.required(reason, "delivery failure reason");
    ObjectNode operation =
        settlement("fail_delivery", deliveryId, dispatcher, dispatcherEpoch, leaseToken);
    operation.put("reason", reason);
    return mutate(bus, shard, idempotencyKey, operation);
  }

  /** Records a terminal failure and dead-letters one fenced delivery lease. */
  public JsonNode rejectDelivery(
      String bus,
      int shard,
      String idempotencyKey,
      String deliveryId,
      String dispatcher,
      BigInteger dispatcherEpoch,
      String leaseToken,
      String reason)
      throws IOException, InterruptedException {
    RegionalClientCore.required(reason, "delivery rejection reason");
    ObjectNode operation =
        settlement("reject_delivery", deliveryId, dispatcher, dispatcherEpoch, leaseToken);
    operation.put("reason", reason);
    return mutate(bus, shard, idempotencyKey, operation);
  }

  /** Returns one dead-lettered delivery to pending while preserving prior attempt history. */
  public JsonNode redriveDelivery(String bus, int shard, String idempotencyKey, String deliveryId)
      throws IOException, InterruptedException {
    RegionalClientCore.required(deliveryId, "delivery ID");
    ObjectNode operation = operation("redrive_delivery");
    operation.put("delivery_id", deliveryId);
    return mutate(bus, shard, idempotencyKey, operation);
  }

  /** Applies due retry and expired-lease transitions explicitly. */
  public JsonNode maintainDeliveries(
      String bus, int shard, String idempotencyKey, int maxDeliveries)
      throws IOException, InterruptedException {
    deliveryBatch(maxDeliveries);
    ObjectNode operation = operation("maintain_deliveries");
    operation.put("max_deliveries", maxDeliveries);
    return mutate(bus, shard, idempotencyKey, operation);
  }

  /** Applies bounded replicated archive age/count retention immediately. */
  public JsonNode maintainArchive(String bus, int shard, String idempotencyKey, int maxEvents)
      throws IOException, InterruptedException {
    readLimit(maxEvents);
    ObjectNode operation = operation("maintain_archive");
    operation.put("max_events", maxEvents);
    return mutate(bus, shard, idempotencyKey, operation);
  }

  /** Commits one schema, connector, MQTT, catalog, enrichment, or endpoint operation. */
  public JsonNode applyIntegration(
      String bus, int shard, String idempotencyKey, JsonNode integrationOperation)
      throws IOException, InterruptedException {
    Objects.requireNonNull(integrationOperation, "integrationOperation");
    JsonNode kind = integrationOperation.get("kind");
    if (!integrationOperation.isObject()
        || kind == null
        || !kind.isTextual()
        || kind.asText().isBlank()) {
      throw new IllegalArgumentException("integration operation kind is required");
    }
    ObjectNode operation = operation("apply_integration");
    operation.set("operation", integrationOperation.deepCopy());
    return mutate(bus, shard, idempotencyKey, operation);
  }

  /** Compiles and commits one immutable schema revision. */
  public JsonNode registerSchema(
      String bus, int shard, String idempotencyKey, SchemaRegistration registration)
      throws IOException, InterruptedException {
    Objects.requireNonNull(registration, "registration");
    ObjectNode integration = operation("register_schema");
    integration.set("registration", registration.toJson(RegionalClientCore.MAPPER));
    return applyIntegration(bus, shard, idempotencyKey, integration);
  }

  /** Creates or replaces one event-type schema-validation policy. */
  public JsonNode upsertSchemaValidationPolicy(
      String bus, int shard, String idempotencyKey, SchemaValidationPolicy policy)
      throws IOException, InterruptedException {
    Objects.requireNonNull(policy, "policy");
    ObjectNode integration = operation("upsert_validation_policy");
    integration.set("policy", policy.toJson(RegionalClientCore.MAPPER));
    return applyIntegration(bus, shard, idempotencyKey, integration);
  }

  /** Removes one exact schema-validation policy. */
  public JsonNode removeSchemaValidationPolicy(
      String bus, int shard, String idempotencyKey, String name)
      throws IOException, InterruptedException {
    ObjectNode integration = operation("remove_validation_policy");
    integration.put("name", SchemaRegistration.resourceName(name, "schema validation policy name"));
    return applyIntegration(bus, shard, idempotencyKey, integration);
  }

  /** Performs a linearizable read-only producer or broker schema validation. */
  public JsonNode validateSchema(
      String bus, int shard, SchemaValidationStage stage, EventEnvelope event)
      throws IOException, InterruptedException {
    Objects.requireNonNull(stage, "stage");
    Objects.requireNonNull(event, "event");
    ObjectNode body = RegionalClientCore.MAPPER.createObjectNode();
    body.put("mode", stage.wireValue());
    body.set("envelope", event.toJson());
    return read(bus, shard, "POST", "/schema/validate", body);
  }

  /** Resolves one proposal from the current leader. */
  public JsonNode mutation(String bus, int shard, BigInteger proposalId)
      throws IOException, InterruptedException {
    RegionalClientCore.positiveU64(proposalId, "Event Bus proposal ID");
    return read(bus, shard, "GET", "/mutations/" + proposalId, null);
  }

  /** Performs a bounded linearizable archive replay. */
  public JsonNode replayArchive(
      String bus, int shard, BigInteger fromMs, BigInteger toMs, int limit, EventFilter filter)
      throws IOException, InterruptedException {
    RegionalClientCore.nonNegativeU64(fromMs, "Event Bus replay from time");
    RegionalClientCore.nonNegativeU64(toMs, "Event Bus replay to time");
    if (fromMs.compareTo(toMs) > 0) {
      throw new IllegalArgumentException("Event Bus replay from time must not exceed to time");
    }
    readLimit(limit);
    ObjectNode body = RegionalClientCore.MAPPER.createObjectNode();
    body.put("from_ms", fromMs.toString());
    body.put("to_ms", toMs.toString());
    body.put("limit", limit);
    if (filter != null) {
      body.set("filter", filter.toJson(RegionalClientCore.MAPPER));
    }
    return read(bus, shard, "POST", "/archive/replay", body);
  }

  /** Queries bounded replicated delivery records. */
  public JsonNode queryDeliveries(
      String bus, int shard, String subscription, RegionalBusDeliveryState state, int limit)
      throws IOException, InterruptedException {
    readLimit(limit);
    ObjectNode body = RegionalClientCore.MAPPER.createObjectNode().put("limit", limit);
    if (subscription != null) {
      RegionalClientCore.required(subscription, "subscription name");
      body.put("subscription", subscription);
    }
    if (state != null) {
      body.put("state", state.wireValue());
    }
    return read(bus, shard, "POST", "/deliveries/query", body);
  }

  /** Returns the linearizable Event Bus status and digest. */
  public JsonNode status(String bus, int shard) throws IOException, InterruptedException {
    return read(bus, shard, "GET", "/status", null);
  }

  /** Returns the complete linearizable Event Bus integration state. */
  public JsonNode integrationState(String bus, int shard) throws IOException, InterruptedException {
    return read(bus, shard, "GET", "/integration/state", null);
  }

  private JsonNode mutate(String bus, int shard, String idempotencyKey, ObjectNode operation)
      throws IOException, InterruptedException {
    RegionalClientCore.required(idempotencyKey, "idempotency key");
    return regional.call(
        "buses",
        "Event Bus",
        bus,
        shard,
        route -> {
          ObjectNode body = RegionalClientCore.MAPPER.createObjectNode();
          body.put("idempotency_key", idempotencyKey);
          body.put("expected_term", route.term());
          body.set("operation", operation);
          return new RegionalClientCore.RequestSpec("POST", "/mutations", body, Map.of(), Map.of());
        });
  }

  private JsonNode read(String bus, int shard, String method, String path, JsonNode body)
      throws IOException, InterruptedException {
    return regional.call(
        "buses",
        "Event Bus",
        bus,
        shard,
        route ->
            new RegionalClientCore.RequestSpec(
                method, path, body, Map.of(), Map.of("x-epoch-read-consistency", "linearizable")));
  }

  private static ObjectNode operation(String kind) {
    return RegionalClientCore.MAPPER.createObjectNode().put("kind", kind);
  }

  private static ObjectNode settlement(
      String kind,
      String deliveryId,
      String dispatcher,
      BigInteger dispatcherEpoch,
      String leaseToken) {
    RegionalClientCore.required(deliveryId, "delivery ID");
    RegionalClientCore.required(dispatcher, "dispatcher");
    RegionalClientCore.positiveU64(dispatcherEpoch, "dispatcher epoch");
    RegionalClientCore.required(leaseToken, "delivery lease token");
    ObjectNode operation = operation(kind);
    operation.put("delivery_id", deliveryId);
    operation.put("dispatcher", dispatcher);
    operation.put("dispatcher_epoch", dispatcherEpoch.toString());
    operation.put("lease_token", leaseToken);
    return operation;
  }

  private static void deliveryBatch(int value) {
    if (value < 1 || value > MAX_DELIVERY_BATCH) {
      throw new IllegalArgumentException("Event Bus max deliveries must be between 1 and 100");
    }
  }

  private static void readLimit(int value) {
    if (value < 1 || value > MAX_READ_RESULTS) {
      throw new IllegalArgumentException("Event Bus read limit must be between 1 and 10000");
    }
  }
}
