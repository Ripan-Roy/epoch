package io.epoch.sdk;

/** Work Queue creation options for the currently implemented volatile profile. */
public record QueueConfig(long visibilityTimeoutMs, int maxMessages, int maxAttempts) {

  public QueueConfig {
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
    return new QueueConfig(30_000, 100_000, 8);
  }
}
