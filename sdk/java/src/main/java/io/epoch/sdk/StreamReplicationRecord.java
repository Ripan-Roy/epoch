package io.epoch.sdk;

import java.math.BigInteger;
import java.util.List;
import java.util.Objects;

/** One source-positioned replication record and its loop-prevention path. */
public record StreamReplicationRecord(
    BigInteger sourceOffset, EventEnvelope envelope, List<String> traversedClusters) {
  public StreamReplicationRecord {
    RegionalClientCore.nonNegativeU64(sourceOffset, "source offset");
    Objects.requireNonNull(envelope, "envelope");
    traversedClusters = List.copyOf(traversedClusters);
    traversedClusters.forEach(cluster -> RegionalClientCore.required(cluster, "traversed cluster"));
  }

  public StreamReplicationRecord(
      long sourceOffset, EventEnvelope envelope, List<String> traversedClusters) {
    this(BigInteger.valueOf(sourceOffset), envelope, traversedClusters);
  }
}
