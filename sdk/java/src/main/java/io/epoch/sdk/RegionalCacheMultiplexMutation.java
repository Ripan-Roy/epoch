package io.epoch.sdk;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Objects;

/** One independently committed mutation in a correlated, non-atomic Cache multiplex request. */
public final class RegionalCacheMultiplexMutation {
  private final String correlationId;
  private final String idempotencyKey;
  private final ObjectNode operation;

  /** Constructs one correlated multiplex item from a normal transaction mutation. */
  public RegionalCacheMultiplexMutation(
      String correlationId, String idempotencyKey, RegionalCacheMutation mutation) {
    RegionalClientCore.required(correlationId, "Cache multiplex correlation ID");
    RegionalClientCore.required(idempotencyKey, "Cache multiplex idempotency key");
    this.correlationId = correlationId;
    this.idempotencyKey = idempotencyKey;
    this.operation = Objects.requireNonNull(mutation, "mutation").toJson();
  }

  String correlationId() {
    return correlationId;
  }

  String idempotencyKey() {
    return idempotencyKey;
  }

  ObjectNode operation() {
    return operation.deepCopy();
  }
}
