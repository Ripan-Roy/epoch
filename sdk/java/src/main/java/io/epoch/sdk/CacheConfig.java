package io.epoch.sdk;

import java.util.Objects;

/** Cache creation options including byte-tier and durability disclosure. */
public record CacheConfig(
    int maxEntries,
    Long maxMemoryBytes,
    Long maxColdBytes,
    Long defaultTtlMs,
    String eviction,
    DurabilityProfile durability) {

  /** Backward-compatible volatile memory-only constructor. */
  public CacheConfig(int maxEntries, Long defaultTtlMs, String eviction) {
    this(maxEntries, null, null, defaultTtlMs, eviction, DurabilityProfile.VOLATILE);
  }

  public CacheConfig {
    if (maxEntries <= 0) {
      throw new IllegalArgumentException("maxEntries must be greater than zero");
    }
    if (defaultTtlMs != null && defaultTtlMs <= 0) {
      throw new IllegalArgumentException("defaultTtlMs must be greater than zero");
    }
    if (maxMemoryBytes != null && maxMemoryBytes <= 0
        || maxColdBytes != null && maxColdBytes <= 0) {
      throw new IllegalArgumentException("Cache byte capacities must be greater than zero");
    }
    if (Objects.requireNonNull(eviction, "eviction").isBlank()) {
      throw new IllegalArgumentException("eviction is required");
    }
    Objects.requireNonNull(durability, "durability");
  }

  public static CacheConfig defaults() {
    return new CacheConfig(10_000, null, null, null, "no_eviction", DurabilityProfile.VOLATILE);
  }
}
