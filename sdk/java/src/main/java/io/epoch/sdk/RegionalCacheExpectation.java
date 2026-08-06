package io.epoch.sdk;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigInteger;

/** A Cache CAS version or missing-at-revision precondition. */
public final class RegionalCacheExpectation {
  private final String kind;
  private final BigInteger value;

  private RegionalCacheExpectation(String kind, BigInteger value) {
    RegionalClientCore.nonNegativeU64(value, "Cache " + kind + " expectation");
    this.kind = kind;
    this.value = value;
  }

  /** Expects the key to be absent at the observed shard revision. */
  public static RegionalCacheExpectation missing(BigInteger shardRevision) {
    return new RegionalCacheExpectation("missing", shardRevision);
  }

  /** Expects the exact non-ABA key version. */
  public static RegionalCacheExpectation version(BigInteger version) {
    return new RegionalCacheExpectation("version", version);
  }

  ObjectNode toJson() {
    ObjectNode result = RegionalClientCore.MAPPER.createObjectNode().put("kind", kind);
    result.put("missing".equals(kind) ? "shard_revision" : "version", value.toString());
    return result;
  }
}
