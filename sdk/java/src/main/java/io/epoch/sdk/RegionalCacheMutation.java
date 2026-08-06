package io.epoch.sdk;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigInteger;
import java.util.Objects;

/** One operation permitted inside an atomic regional Cache transaction. */
public final class RegionalCacheMutation {
  private final String key;
  private final ObjectNode operation;

  private RegionalCacheMutation(String key, ObjectNode operation) {
    RegionalClientCore.required(key, "Cache key");
    this.key = key;
    this.operation = operation;
  }

  /** Constructs a transactional set. */
  public static RegionalCacheMutation set(String key, RegionalCacheValue value, BigInteger ttlMs) {
    ObjectNode operation = base("set", key);
    operation.set("value", Objects.requireNonNull(value, "value").toJson());
    optionalPositive(operation, "ttl_ms", ttlMs);
    return new RegionalCacheMutation(key, operation);
  }

  /** Constructs a transactional conditional delete. */
  public static RegionalCacheMutation delete(String key, BigInteger expectedVersion) {
    ObjectNode operation = base("delete", key);
    optionalNonNegative(operation, "expected_version", expectedVersion);
    return new RegionalCacheMutation(key, operation);
  }

  /** Constructs a transactional compare-and-set. */
  public static RegionalCacheMutation compareAndSet(
      String key, RegionalCacheExpectation expected, RegionalCacheValue value, BigInteger ttlMs) {
    ObjectNode operation = base("compare_and_set", key);
    operation.set("expected", Objects.requireNonNull(expected, "expected").toJson());
    operation.set("value", Objects.requireNonNull(value, "value").toJson());
    optionalPositive(operation, "ttl_ms", ttlMs);
    return new RegionalCacheMutation(key, operation);
  }

  /** Constructs a transactional signed increment. */
  public static RegionalCacheMutation increment(
      String key, long delta, BigInteger expectedVersion, BigInteger ttlMs) {
    ObjectNode operation = base("increment", key).put("delta", Long.toString(delta));
    optionalNonNegative(operation, "expected_version", expectedVersion);
    optionalPositive(operation, "ttl_ms", ttlMs);
    return new RegionalCacheMutation(key, operation);
  }

  String key() {
    return key;
  }

  ObjectNode toJson() {
    return operation.deepCopy();
  }

  static void optionalPositive(ObjectNode operation, String field, BigInteger value) {
    if (value != null) {
      RegionalClientCore.positiveU64(value, "Cache " + field.replace('_', ' '));
      operation.put(field, value.toString());
    }
  }

  static void optionalNonNegative(ObjectNode operation, String field, BigInteger value) {
    if (value != null) {
      RegionalClientCore.nonNegativeU64(value, "Cache " + field.replace('_', ' '));
      operation.put(field, value.toString());
    }
  }

  private static ObjectNode base(String kind, String key) {
    RegionalClientCore.required(key, "Cache key");
    return RegionalClientCore.MAPPER.createObjectNode().put("kind", kind).put("key", key);
  }
}
