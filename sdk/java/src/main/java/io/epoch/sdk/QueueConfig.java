package io.epoch.sdk;

import java.util.Objects;

/** Work Queue creation options with an explicit acknowledgement boundary. */
public record QueueConfig(
    DurabilityProfile durability, long visibilityTimeoutMs, int maxMessages, int maxAttempts) {

  public QueueConfig {
    Objects.requireNonNull(durability, "durability");
    if (visibilityTimeoutMs <= 0) {
      throw new IllegalArgumentException("visibilityTimeoutMs must be greater than zero");
    }
    if (maxMessages <= 0) {
      throw new IllegalArgumentException("maxMessages must be greater than zero");
    }
    if (maxAttempts <= 0) {
      throw new IllegalArgumentException("maxAttempts must be greater than zero");
    }
  }

  public static QueueConfig defaults() {
    return new QueueConfig(DurabilityProfile.VOLATILE, 30_000, 100_000, 8);
  }
}
