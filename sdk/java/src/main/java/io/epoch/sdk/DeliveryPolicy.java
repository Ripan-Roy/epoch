package io.epoch.sdk;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigInteger;
import java.util.Objects;

/** Timeout, concurrency, and retry bounds for one Event Bus subscription. */
public record DeliveryPolicy(BigInteger timeoutMs, int maxInFlight, DeliveryRetryPolicy retry) {
  private static final BigInteger MAX_TIMEOUT_MS = BigInteger.valueOf(604_800_000);

  public DeliveryPolicy {
    RegionalClientCore.positiveU64(timeoutMs, "delivery timeout");
    if (timeoutMs.compareTo(MAX_TIMEOUT_MS) > 0) {
      throw new IllegalArgumentException("delivery timeout must not exceed 604800000");
    }
    if (maxInFlight < 1 || maxInFlight > 1_000) {
      throw new IllegalArgumentException("delivery max in flight must be between 1 and 1000");
    }
    Objects.requireNonNull(retry, "retry");
  }

  /** Returns the server defaults. */
  public static DeliveryPolicy defaults() {
    return new DeliveryPolicy(BigInteger.valueOf(30_000), 16, DeliveryRetryPolicy.defaults());
  }

  ObjectNode toJson() {
    ObjectNode value = RegionalClientCore.MAPPER.createObjectNode();
    value.put("timeout_ms", timeoutMs);
    value.put("max_in_flight", maxInFlight);
    value.set("retry", retry.toJson());
    return value;
  }
}
