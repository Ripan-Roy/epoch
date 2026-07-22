package io.epoch.sdk;

import java.util.Objects;

/** Lease acquisition options for a Work Queue consumer. */
public record QueueReceiveOptions(String consumer, int maxMessages, Long visibilityTimeoutMs) {

  public QueueReceiveOptions {
    if (Objects.requireNonNull(consumer, "consumer").isBlank()) {
      throw new IllegalArgumentException("consumer is required");
    }
    if (maxMessages <= 0) {
      throw new IllegalArgumentException("maxMessages must be greater than zero");
    }
    if (visibilityTimeoutMs != null && visibilityTimeoutMs <= 0) {
      throw new IllegalArgumentException("visibilityTimeoutMs must be greater than zero");
    }
  }
}
