package io.epoch.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.zip.GZIPOutputStream;

/** One bounded, client-framed atomic Stream batch. */
public final class StreamBatchFrame {
  static final int MAX_RECORDS = 1_000;
  static final int MAX_COMPRESSED_BYTES = 360 * 1024;
  static final int MAX_UNCOMPRESSED_BYTES = 4 * 1024 * 1024;

  private final StreamCompression compression;
  private final int recordCount;
  private final int uncompressedBytes;
  private final byte[] compressed;

  private StreamBatchFrame(
      StreamCompression compression, int recordCount, int uncompressedBytes, byte[] compressed) {
    this.compression = Objects.requireNonNull(compression, "compression");
    if (recordCount < 1 || recordCount > MAX_RECORDS) {
      throw new IllegalArgumentException("recordCount must be between 1 and " + MAX_RECORDS);
    }
    if (uncompressedBytes < 1 || uncompressedBytes > MAX_UNCOMPRESSED_BYTES) {
      throw new IllegalArgumentException(
          "uncompressedBytes must be between 1 and " + MAX_UNCOMPRESSED_BYTES);
    }
    Objects.requireNonNull(compressed, "compressed");
    if (compressed.length < 1 || compressed.length > MAX_COMPRESSED_BYTES) {
      throw new IllegalArgumentException(
          "compressed frame must contain between 1 and " + MAX_COMPRESSED_BYTES + " bytes");
    }
    if (compression == StreamCompression.NONE && compressed.length != uncompressedBytes) {
      throw new IllegalArgumentException("uncompressed frame sizes must match");
    }
    this.recordCount = recordCount;
    this.uncompressedBytes = uncompressedBytes;
    this.compressed = compressed.clone();
  }

  /** Wraps exact standard LZ4, Snappy, Zstd, gzip, or uncompressed frame bytes. */
  public static StreamBatchFrame compressed(
      StreamCompression compression, int recordCount, int uncompressedBytes, byte[] compressed) {
    return new StreamBatchFrame(compression, recordCount, uncompressedBytes, compressed);
  }

  /** Encodes canonical record JSON using the built-in none or gzip path. */
  public static StreamBatchFrame encode(
      List<StreamBatchRecord> records, StreamCompression compression) {
    Objects.requireNonNull(records, "records");
    Objects.requireNonNull(compression, "compression");
    if (records.isEmpty() || records.size() > MAX_RECORDS) {
      throw new IllegalArgumentException(
          "Stream batch must contain between 1 and " + MAX_RECORDS + " records");
    }
    Set<Long> sequences = new HashSet<>();
    ArrayNode canonical = RegionalClientCore.MAPPER.createArrayNode();
    for (StreamBatchRecord record : List.copyOf(records)) {
      Objects.requireNonNull(record, "records cannot contain null");
      if (!sequences.add(record.clientSequence())) {
        throw new IllegalArgumentException(
            "duplicate Stream batch client sequence " + record.clientSequence());
      }
      ObjectNode item = canonical.addObject();
      item.put("client_sequence", record.clientSequence());
      item.set("envelope", canonicalEnvelope(record.envelope()));
    }
    byte[] plain;
    try {
      plain = RegionalClientCore.MAPPER.writeValueAsBytes(canonical);
    } catch (IOException error) {
      throw new IllegalArgumentException("cannot encode Stream batch", error);
    }
    if (plain.length > MAX_UNCOMPRESSED_BYTES) {
      throw new IllegalArgumentException(
          "Stream batch uncompressed bytes must not exceed " + MAX_UNCOMPRESSED_BYTES);
    }
    byte[] frame =
        switch (compression) {
          case NONE -> plain;
          case GZIP -> gzip(plain);
          case LZ4, SNAPPY, ZSTD ->
              throw new IllegalArgumentException(
                  compression.wireName()
                      + " Stream batches require a caller-supplied standard frame");
        };
    return compressed(compression, records.size(), plain.length, frame);
  }

  ObjectNode toRequest(String idempotencyKey, String expectedTerm) {
    return RegionalClientCore.MAPPER
        .createObjectNode()
        .put("idempotency_key", idempotencyKey)
        .put("expected_term", expectedTerm)
        .put("partition", 0)
        .put("compression", compression.wireName())
        .put("record_count", recordCount)
        .put("uncompressed_bytes", uncompressedBytes)
        .put("compressed_bytes", compressed.length)
        .put("payload_base64", Base64.getEncoder().encodeToString(compressed));
  }

  private static byte[] gzip(byte[] plain) {
    try {
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      try (GZIPOutputStream gzip = new GZIPOutputStream(output)) {
        gzip.write(plain);
      }
      return output.toByteArray();
    } catch (IOException error) {
      throw new IllegalArgumentException("cannot gzip Stream batch", error);
    }
  }

  private static ObjectNode canonicalEnvelope(EventEnvelope envelope) {
    ObjectNode input = envelope.toJson();
    ObjectNode output = RegionalClientCore.MAPPER.createObjectNode();
    copy(output, input, "id");
    copy(output, input, "source");
    copy(output, input, "type");
    copyIfPresent(output, input, "subject");
    copy(output, input, "time_ms");
    copyIfPresent(output, input, "key");
    output.set("headers", sortObjects(input.get("headers")));
    copy(output, input, "content_type");
    copyIfPresent(output, input, "schema_ref");
    copyIfPresent(output, input, "traceparent");
    output.set("payload", sortObjects(input.get("payload")));
    copyIfPresent(output, input, "deliver_at_ms");
    copyIfPresent(output, input, "ttl_ms");
    copy(output, input, "priority");
    copyIfPresent(output, input, "dedupe_id");
    copyIfPresent(output, input, "transaction_id");
    output.set("extensions", sortObjects(input.get("extensions")));
    return output;
  }

  private static JsonNode sortObjects(JsonNode value) {
    if (value.isObject()) {
      ObjectNode sorted = RegionalClientCore.MAPPER.createObjectNode();
      TreeMap<String, JsonNode> fields = new TreeMap<>();
      value.properties().forEach(entry -> fields.put(entry.getKey(), entry.getValue()));
      fields.forEach((name, child) -> sorted.set(name, sortObjects(child)));
      return sorted;
    }
    if (value.isArray()) {
      ArrayNode sorted = RegionalClientCore.MAPPER.createArrayNode();
      value.forEach(child -> sorted.add(sortObjects(child)));
      return sorted;
    }
    return value.deepCopy();
  }

  private static void copy(ObjectNode output, ObjectNode input, String field) {
    output.set(field, input.get(field).deepCopy());
  }

  private static void copyIfPresent(ObjectNode output, ObjectNode input, String field) {
    if (input.has(field)) {
      copy(output, input, field);
    }
  }
}
