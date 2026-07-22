package io.epoch.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/** Common native record accepted by every Epoch workload profile. */
public final class EventEnvelope {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String id;
  private final String source;
  private final String eventType;
  private final long timeMs;
  private final String subject;
  private final String key;
  private final Map<String, String> headers;
  private final String contentType;
  private final String schemaRef;
  private final String traceparent;
  private final JsonNode payload;
  private final Long deliverAtMs;
  private final Long ttlMs;
  private final int priority;
  private final String dedupeId;
  private final String transactionId;
  private final Map<String, Object> extensions;

  private EventEnvelope(Builder builder) {
    id = required(builder.id, "id");
    source = required(builder.source, "source");
    eventType = required(builder.eventType, "eventType");
    timeMs = builder.timeMs;
    subject = builder.subject;
    key = builder.key;
    headers = Map.copyOf(builder.headers);
    contentType = required(builder.contentType, "contentType");
    schemaRef = builder.schemaRef;
    traceparent = builder.traceparent;
    payload = builder.payload.deepCopy();
    deliverAtMs = builder.deliverAtMs;
    ttlMs = builder.ttlMs;
    priority = builder.priority;
    dedupeId = builder.dedupeId;
    transactionId = builder.transactionId;
    extensions = Map.copyOf(builder.extensions);
    if (priority < 0 || priority > 9) {
      throw new IllegalArgumentException("priority must be between 0 and 9");
    }
    if (ttlMs != null && ttlMs <= 0) {
      throw new IllegalArgumentException("ttlMs must be greater than zero");
    }
  }

  public static Builder builder(String source, String eventType, Object payload) {
    return new Builder(source, eventType, payload);
  }

  /** Returns the native JSON representation, including the wire field named {@code type}. */
  public ObjectNode toJson() {
    ObjectNode value = MAPPER.createObjectNode();
    value.put("id", id);
    value.put("source", source);
    value.put("type", eventType);
    value.put("time_ms", timeMs);
    value.set("headers", MAPPER.valueToTree(headers));
    value.put("content_type", contentType);
    value.set("payload", payload.deepCopy());
    value.put("priority", priority);
    value.set("extensions", MAPPER.valueToTree(extensions));
    putOptional(value, "subject", subject);
    putOptional(value, "key", key);
    putOptional(value, "schema_ref", schemaRef);
    putOptional(value, "traceparent", traceparent);
    putOptional(value, "deliver_at_ms", deliverAtMs);
    putOptional(value, "ttl_ms", ttlMs);
    putOptional(value, "dedupe_id", dedupeId);
    putOptional(value, "transaction_id", transactionId);
    return value;
  }

  private static void putOptional(ObjectNode target, String name, String value) {
    if (value != null) {
      target.put(name, value);
    }
  }

  private static void putOptional(ObjectNode target, String name, Long value) {
    if (value != null) {
      target.put(name, value);
    }
  }

  private static String required(String value, String name) {
    if (Objects.requireNonNull(value, name).isBlank()) {
      throw new IllegalArgumentException(name + " is required");
    }
    return value;
  }

  /** Fluent builder with safe IDs, timestamps, and envelope defaults. */
  public static final class Builder {
    private String id = UUID.randomUUID().toString();
    private final String source;
    private final String eventType;
    private long timeMs = Instant.now().toEpochMilli();
    private String subject;
    private String key;
    private Map<String, String> headers = Map.of();
    private String contentType = "application/json";
    private String schemaRef;
    private String traceparent;
    private final JsonNode payload;
    private Long deliverAtMs;
    private Long ttlMs;
    private int priority;
    private String dedupeId;
    private String transactionId;
    private Map<String, Object> extensions = Map.of();

    private Builder(String source, String eventType, Object payload) {
      this.source = source;
      this.eventType = eventType;
      this.payload = MAPPER.valueToTree(payload);
    }

    public Builder id(String value) {
      id = value;
      return this;
    }

    public Builder timeMs(long value) {
      timeMs = value;
      return this;
    }

    public Builder subject(String value) {
      subject = value;
      return this;
    }

    public Builder key(String value) {
      key = value;
      return this;
    }

    public Builder headers(Map<String, String> value) {
      headers = Map.copyOf(value);
      return this;
    }

    public Builder contentType(String value) {
      contentType = value;
      return this;
    }

    public Builder schemaRef(String value) {
      schemaRef = value;
      return this;
    }

    public Builder traceparent(String value) {
      traceparent = value;
      return this;
    }

    public Builder deliverAtMs(Long value) {
      deliverAtMs = value;
      return this;
    }

    public Builder ttlMs(Long value) {
      ttlMs = value;
      return this;
    }

    public Builder priority(int value) {
      priority = value;
      return this;
    }

    public Builder dedupeId(String value) {
      dedupeId = value;
      return this;
    }

    public Builder transactionId(String value) {
      transactionId = value;
      return this;
    }

    public Builder extensions(Map<String, Object> value) {
      extensions = Map.copyOf(value);
      return this;
    }

    public EventEnvelope build() {
      return new EventEnvelope(this);
    }
  }
}
