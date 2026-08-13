package io.epoch.sdk;

import java.util.Objects;

/** Correlates one Stream batch record with its unsigned 32-bit caller sequence. */
public record StreamBatchRecord(long clientSequence, EventEnvelope envelope) {
  public StreamBatchRecord {
    if (clientSequence < 0 || clientSequence > 0xffff_ffffL) {
      throw new IllegalArgumentException("clientSequence must fit an unsigned 32-bit integer");
    }
    Objects.requireNonNull(envelope, "envelope");
  }
}
