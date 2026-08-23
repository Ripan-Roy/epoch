package io.epoch.sdk;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Deterministic transform CPU, memory, time, and network limits. */
public record TransformLimits(
    int maxOperations,
    long maxOutputBytes,
    long maxValueBytes,
    long timeoutMs,
    boolean networkAccess) {

  public TransformLimits {
    if (maxOperations < 1 || maxOperations > 256) {
      throw new IllegalArgumentException("transform max operations must be between 1 and 256");
    }
    if (maxOutputBytes < 1 || maxOutputBytes > 1024 * 1024) {
      throw new IllegalArgumentException(
          "transform max output bytes must be between 1 and 1048576");
    }
    if (maxValueBytes < 1 || maxValueBytes > 256 * 1024 || maxValueBytes > maxOutputBytes) {
      throw new IllegalArgumentException(
          "transform max value bytes must be positive, bounded, and not exceed output");
    }
    if (timeoutMs < 1 || timeoutMs > 1_000) {
      throw new IllegalArgumentException(
          "transform timeout must be between 1 and 1000 milliseconds");
    }
    if (networkAccess) {
      throw new IllegalArgumentException("deterministic transforms cannot enable network access");
    }
  }

  public static TransformLimits defaults() {
    return new TransformLimits(64, 256 * 1024, 64 * 1024, 100, false);
  }

  ObjectNode toJson() {
    return JsonNodeFactory.instance
        .objectNode()
        .put("max_operations", maxOperations)
        .put("max_output_bytes", maxOutputBytes)
        .put("max_value_bytes", maxValueBytes)
        .put("timeout_ms", timeoutMs)
        .put("network_access", networkAccess);
  }
}
