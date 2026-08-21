package io.epoch.sdk;

import java.math.BigInteger;

/** One named physical Stream shard and starting offset in a logical superstream merge. */
public record StreamSuperstreamMember(String name, String stream, int shard, BigInteger offset) {
  public StreamSuperstreamMember {
    RegionalClientCore.required(name, "superstream member name");
    RegionalClientCore.required(stream, "superstream member Stream");
    if (shard < 0) {
      throw new IllegalArgumentException("superstream member shard must be non-negative");
    }
    RegionalClientCore.nonNegativeU64(offset, "superstream member offset");
  }

  public StreamSuperstreamMember(String name, String stream, int shard, long offset) {
    this(name, stream, shard, BigInteger.valueOf(offset));
  }
}
