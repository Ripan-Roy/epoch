package io.epoch.sdk;

import java.math.BigInteger;
import java.util.List;

/** One bounded contiguous cross-cluster replication batch. */
public record StreamReplicationBatch(
    String sourceCluster,
    String sourceStream,
    int sourcePartition,
    BigInteger firstSourceOffset,
    List<StreamReplicationRecord> records) {
  public StreamReplicationBatch {
    RegionalClientCore.required(sourceCluster, "source cluster");
    RegionalClientCore.required(sourceStream, "source stream");
    if (sourcePartition < 0) {
      throw new IllegalArgumentException("source partition must be non-negative");
    }
    RegionalClientCore.nonNegativeU64(firstSourceOffset, "first source offset");
    records = List.copyOf(records);
    if (records.isEmpty() || records.size() > 128) {
      throw new IllegalArgumentException(
          "replication batch must contain between 1 and 128 records");
    }
  }

  public StreamReplicationBatch(
      String sourceCluster,
      String sourceStream,
      int sourcePartition,
      long firstSourceOffset,
      List<StreamReplicationRecord> records) {
    this(
        sourceCluster,
        sourceStream,
        sourcePartition,
        BigInteger.valueOf(firstSourceOffset),
        records);
  }
}
