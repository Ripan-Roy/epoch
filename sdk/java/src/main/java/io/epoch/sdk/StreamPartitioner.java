package io.epoch.sdk;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Stable FNV-1a UTF-8 partitioner advertised by the regional Stream API. */
public final class StreamPartitioner {
  public static final String ALGORITHM = "fnv1a64_utf8_mod_n_v1";

  private StreamPartitioner() {}

  /** Maps a key or event ID to one logical shard using unsigned 64-bit arithmetic. */
  public static int shardFor(String partitionValue, int shardCount) {
    Objects.requireNonNull(partitionValue, "partitionValue");
    if (shardCount <= 0) {
      throw new IllegalArgumentException("Stream shard count must be greater than zero");
    }
    long hash = 0xcbf29ce484222325L;
    for (byte value : partitionValue.getBytes(StandardCharsets.UTF_8)) {
      hash = (hash ^ Byte.toUnsignedLong(value)) * 0x100000001b3L;
    }
    return (int) Long.remainderUnsigned(hash, shardCount);
  }
}
