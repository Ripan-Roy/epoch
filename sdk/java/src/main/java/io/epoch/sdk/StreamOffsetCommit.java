package io.epoch.sdk;

import java.math.BigInteger;

/** One consumer offset committed atomically with a Stream transaction. */
public record StreamOffsetCommit(String group, int partition, BigInteger nextOffset) {
  public StreamOffsetCommit {
    RegionalClientCore.required(group, "consumer group");
    if (partition < 0) {
      throw new IllegalArgumentException("partition must be non-negative");
    }
    RegionalClientCore.nonNegativeU64(nextOffset, "next offset");
  }

  public StreamOffsetCommit(String group, int partition, long nextOffset) {
    this(group, partition, BigInteger.valueOf(nextOffset));
  }
}
