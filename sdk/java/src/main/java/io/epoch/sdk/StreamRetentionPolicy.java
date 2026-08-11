package io.epoch.sdk;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigInteger;

/** Independent per-partition record, canonical-byte, and age retention bounds. */
public record StreamRetentionPolicy(
    Integer maxRecordsPerPartition, BigInteger maxBytesPerPartition, BigInteger maxAgeMs) {
  private static final int MAX_RECORDS = 100_000;
  private static final BigInteger MAX_BYTES = BigInteger.valueOf(3L * 1024 * 1024);
  private static final BigInteger MAX_AGE_MS = BigInteger.valueOf(10L * 365 * 24 * 60 * 60 * 1_000);

  /** Validates every configured bound; null disables the corresponding bound. */
  public StreamRetentionPolicy {
    if (maxRecordsPerPartition != null
        && (maxRecordsPerPartition < 1 || maxRecordsPerPartition > MAX_RECORDS)) {
      throw new IllegalArgumentException(
          "Stream retention max records must be between 1 and " + MAX_RECORDS + " when set");
    }
    validateBound(maxBytesPerPartition, MAX_BYTES, "Stream retention max bytes");
    validateBound(maxAgeMs, MAX_AGE_MS, "Stream retention max age");
  }

  ObjectNode toJson() {
    ObjectNode document = RegionalClientCore.MAPPER.createObjectNode();
    if (maxRecordsPerPartition != null) {
      document.put("max_records_per_partition", maxRecordsPerPartition);
    }
    if (maxBytesPerPartition != null) {
      document.put("max_bytes_per_partition", maxBytesPerPartition.toString());
    }
    if (maxAgeMs != null) {
      document.put("max_age_ms", maxAgeMs.toString());
    }
    return document;
  }

  private static void validateBound(BigInteger value, BigInteger maximum, String name) {
    if (value != null && (value.signum() <= 0 || value.compareTo(maximum) > 0)) {
      throw new IllegalArgumentException(name + " must be between 1 and " + maximum + " when set");
    }
  }
}
