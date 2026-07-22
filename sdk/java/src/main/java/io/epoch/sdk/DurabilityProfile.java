package io.epoch.sdk;

/** Acknowledgement boundary requested for an Epoch resource. */
public enum DurabilityProfile {
  VOLATILE("volatile"),
  REPLICATED_MEMORY("replicated_memory"),
  LOCAL_DURABLE("local_durable"),
  QUORUM_DURABLE("quorum_durable"),
  GEO_ASYNC("geo_async"),
  GEO_SYNC("geo_sync");

  private final String wireName;

  DurabilityProfile(String wireName) {
    this.wireName = wireName;
  }

  /** Returns the native JSON API spelling. */
  public String wireName() {
    return wireName;
  }
}
