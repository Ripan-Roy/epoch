package io.epoch.sdk;

import java.math.BigInteger;
import java.util.Objects;

/** Event Bus archive replay bounds. BigInteger preserves the API's unsigned 64-bit range. */
public record BusReplayOptions(BigInteger fromMs, BigInteger toMs, int limit, String eventType) {
  private static final BigInteger MAX_U64 = new BigInteger("18446744073709551615");

  public BusReplayOptions {
    Objects.requireNonNull(fromMs, "fromMs");
    Objects.requireNonNull(toMs, "toMs");
    if (fromMs.signum() < 0 || toMs.signum() < 0 || toMs.compareTo(MAX_U64) > 0) {
      throw new IllegalArgumentException("replay times must fit an unsigned 64-bit integer");
    }
    if (fromMs.compareTo(toMs) > 0) {
      throw new IllegalArgumentException("fromMs cannot be greater than toMs");
    }
    if (limit <= 0 || limit > 10_000) {
      throw new IllegalArgumentException("limit must be between 1 and 10000");
    }
  }

  public static BusReplayOptions defaults() {
    return new BusReplayOptions(BigInteger.ZERO, MAX_U64, 100, null);
  }
}
