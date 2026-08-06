package io.epoch.sdk;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigInteger;
import java.util.Objects;

/** Bounded retry policy for one Event Bus subscription. */
public record DeliveryRetryPolicy(
    DeliveryBackoffStrategy strategy,
    BigInteger initialDelayMs,
    BigInteger maxDelayMs,
    int jitterPercent,
    int maxAttempts,
    BigInteger maxAgeMs) {
  private static final BigInteger MAX_TIMEOUT_MS = BigInteger.valueOf(604_800_000);

  public DeliveryRetryPolicy {
    Objects.requireNonNull(strategy, "strategy");
    RegionalClientCore.nonNegativeU64(initialDelayMs, "delivery retry initial delay");
    RegionalClientCore.nonNegativeU64(maxDelayMs, "delivery retry max delay");
    if (maxDelayMs.compareTo(MAX_TIMEOUT_MS) > 0) {
      throw new IllegalArgumentException("delivery retry max delay must not exceed 604800000");
    }
    if (initialDelayMs.compareTo(maxDelayMs) > 0) {
      throw new IllegalArgumentException("delivery retry initial delay must not exceed max delay");
    }
    if (jitterPercent < 0 || jitterPercent > 100) {
      throw new IllegalArgumentException("delivery retry jitter percent must be between 0 and 100");
    }
    if (maxAttempts < 1 || maxAttempts > 100) {
      throw new IllegalArgumentException("delivery retry max attempts must be between 1 and 100");
    }
    if (maxAgeMs != null) {
      RegionalClientCore.positiveU64(maxAgeMs, "delivery retry max age");
    }
  }

  /** Returns the server defaults. */
  public static DeliveryRetryPolicy defaults() {
    return new DeliveryRetryPolicy(
        DeliveryBackoffStrategy.EXPONENTIAL,
        BigInteger.valueOf(1_000),
        BigInteger.valueOf(60_000),
        10,
        8,
        null);
  }

  ObjectNode toJson() {
    ObjectNode value = RegionalClientCore.MAPPER.createObjectNode();
    value.put("strategy", strategy.wireValue());
    value.put("initial_delay_ms", initialDelayMs);
    value.put("max_delay_ms", maxDelayMs);
    value.put("jitter_percent", jitterPercent);
    value.put("max_attempts", maxAttempts);
    if (maxAgeMs == null) {
      value.putNull("max_age_ms");
    } else {
      value.put("max_age_ms", maxAgeMs);
    }
    return value;
  }
}
