package io.epoch.sdk;

/** Conditional and expiry options for one Cache write. */
public record CacheWriteOptions(
    Long ttlMs, Long expectedVersion, boolean onlyIfAbsent, boolean onlyIfPresent) {

  public CacheWriteOptions {
    if (ttlMs != null && ttlMs <= 0) {
      throw new IllegalArgumentException("ttlMs must be greater than zero");
    }
    if (expectedVersion != null && expectedVersion <= 0) {
      throw new IllegalArgumentException("expectedVersion must be greater than zero");
    }
    if (onlyIfAbsent && onlyIfPresent) {
      throw new IllegalArgumentException("onlyIfAbsent and onlyIfPresent are mutually exclusive");
    }
  }

  public static CacheWriteOptions defaults() {
    return new CacheWriteOptions(null, null, false, false);
  }
}
