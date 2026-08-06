package io.epoch.sdk;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigInteger;

/** Opaque lease proof used to fence a guarded Cache mutation. */
public record RegionalCacheLockGuard(
    String lockKey, String owner, BigInteger ownerEpoch, String leaseToken) {
  /** Validates the caller-owned lock identity and opaque lease token. */
  public RegionalCacheLockGuard {
    RegionalClientCore.required(lockKey, "Cache lock key");
    RegionalClientCore.required(owner, "Cache lock owner");
    RegionalClientCore.positiveU64(ownerEpoch, "Cache lock owner epoch");
    RegionalClientCore.required(leaseToken, "Cache lease token");
  }

  ObjectNode toJson() {
    return RegionalClientCore.MAPPER
        .createObjectNode()
        .put("lock_key", lockKey)
        .put("owner", owner)
        .put("owner_epoch", ownerEpoch.toString())
        .put("lease_token", leaseToken);
  }
}
