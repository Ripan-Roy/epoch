package io.epoch.sdk;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** One strict scalar or collection value accepted by the regional Cache tablet. */
public final class RegionalCacheValue {
  private final ObjectNode value;

  private RegionalCacheValue(ObjectNode value) {
    this.value = value;
  }

  /** Constructs a string Cache value. */
  public static RegionalCacheValue string(String value) {
    return new RegionalCacheValue(
        taggedNode("string").put("value", Objects.requireNonNull(value, "value")));
  }

  /** Constructs a byte-array Cache value without using JSON base64 encoding. */
  public static RegionalCacheValue blob(byte[] value) {
    Objects.requireNonNull(value, "value");
    ObjectNode tagged = taggedNode("blob");
    ArrayNode bytes = tagged.putArray("value");
    for (byte item : value) {
      bytes.add(Byte.toUnsignedInt(item));
    }
    return new RegionalCacheValue(tagged);
  }

  /** Constructs a signed 64-bit counter Cache value. */
  public static RegionalCacheValue counter(long value) {
    return new RegionalCacheValue(taggedNode("counter").put("value", Long.toString(value)));
  }

  /** Constructs a string-to-string hash Cache value. */
  public static RegionalCacheValue hash(Map<String, String> value) {
    Objects.requireNonNull(value, "value");
    ObjectNode tagged = taggedNode("hash");
    ObjectNode entries = tagged.putObject("value");
    value.forEach(
        (key, item) ->
            entries.put(
                Objects.requireNonNull(key, "hash key"),
                Objects.requireNonNull(item, "hash value")));
    return new RegionalCacheValue(tagged);
  }

  /** Constructs an ordered string-list Cache value. */
  public static RegionalCacheValue list(List<String> value) {
    Objects.requireNonNull(value, "value");
    ObjectNode tagged = taggedNode("list");
    ArrayNode entries = tagged.putArray("value");
    value.forEach(item -> entries.add(Objects.requireNonNull(item, "list item")));
    return new RegionalCacheValue(tagged);
  }

  /** Constructs a unique string-set Cache value. */
  public static RegionalCacheValue set(List<String> value) {
    Objects.requireNonNull(value, "value");
    if (new HashSet<>(value).size() != value.size()) {
      throw new IllegalArgumentException("Cache set value contains duplicate members");
    }
    ObjectNode tagged = taggedNode("set");
    ArrayNode entries = tagged.putArray("value");
    value.forEach(item -> entries.add(Objects.requireNonNull(item, "set item")));
    return new RegionalCacheValue(tagged);
  }

  /** Constructs a finite-score sorted-set Cache value. */
  public static RegionalCacheValue sortedSet(Map<String, Double> value) {
    Objects.requireNonNull(value, "value");
    ObjectNode tagged = taggedNode("sorted_set");
    ObjectNode entries = tagged.putObject("value");
    value.forEach(
        (member, score) -> {
          Objects.requireNonNull(member, "sorted-set member");
          if (score == null || !Double.isFinite(score)) {
            throw new IllegalArgumentException("Cache sorted-set scores must be finite");
          }
          entries.put(member, score);
        });
    return new RegionalCacheValue(tagged);
  }

  ObjectNode toJson() {
    return value.deepCopy();
  }

  private static ObjectNode taggedNode(String kind) {
    return RegionalClientCore.MAPPER.createObjectNode().put("kind", kind);
  }
}
