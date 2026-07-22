package io.epoch.sdk;

import java.util.Objects;

/** Cache creation options for the currently implemented volatile profile. */
public record CacheConfig(int maxEntries, Long defaultTtlMs, String eviction) {

  public CacheConfig {
    if (maxEntries <= 0) {
      throw new IllegalArgumentException("maxEntries must be greater than zero");
    }
    if (defaultTtlMs != null && defaultTtlMs <= 0) {
      throw new IllegalArgumentException("defaultTtlMs must be greater than zero");
    }
    if (Objects.requireNonNull(eviction, "eviction").isBlank()) {
      throw new IllegalArgumentException("eviction is required");
    }
  }

  public static CacheConfig defaults() {
    return new CacheConfig(10_000, null, "no_eviction");
  }
}
