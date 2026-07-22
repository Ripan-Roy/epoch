package io.epoch.sdk;

import java.util.Objects;

/** Stream creation options. */
public record StreamConfig(
    int partitions, DurabilityProfile durability, Integer maxRecordsPerPartition) {

  public StreamConfig {
    if (partitions <= 0) {
      throw new IllegalArgumentException("partitions must be greater than zero");
    }
    Objects.requireNonNull(durability, "durability");
    if (maxRecordsPerPartition != null && maxRecordsPerPartition <= 0) {
      throw new IllegalArgumentException("maxRecordsPerPartition must be greater than zero");
    }
  }

  /** Returns the current safe default: one volatile partition. */
  public static StreamConfig defaults() {
    return new StreamConfig(1, DurabilityProfile.VOLATILE, null);
  }
}
