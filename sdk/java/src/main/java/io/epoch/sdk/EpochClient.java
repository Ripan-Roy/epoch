package io.epoch.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Synchronous, guarantee-aware client for one Epoch node. */
public final class EpochClient {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(10);
  private static final URI DEFAULT_ENDPOINT = URI.create("http://127.0.0.1:7601");

  private final Transport transport;

  public EpochClient() {
    this(DEFAULT_ENDPOINT, DEFAULT_TIMEOUT);
  }

  public EpochClient(URI baseUri, Duration timeout) {
    this(new HttpTransport(baseUri, timeout));
  }

  public EpochClient(Transport transport) {
    this.transport = Objects.requireNonNull(transport, "transport");
  }

  public JsonNode health() throws IOException, InterruptedException {
    return request("GET", "/healthz");
  }

  public JsonNode resources() throws IOException, InterruptedException {
    return request("GET", "/v1/resources");
  }

  public JsonNode createCache(String name) throws IOException, InterruptedException {
    return createCache(name, CacheConfig.defaults());
  }

  public JsonNode createCache(String name, CacheConfig config)
      throws IOException, InterruptedException {
    Objects.requireNonNull(config, "config");
    ObjectNode body = MAPPER.createObjectNode();
    body.put("max_entries", config.maxEntries());
    putNullable(body, "default_ttl_ms", config.defaultTtlMs());
    body.put("eviction", config.eviction());
    body.put("durability", DurabilityProfile.VOLATILE.wireName());
    return create("caches", name, body);
  }

  public JsonNode createStream(String name) throws IOException, InterruptedException {
    return createStream(name, StreamConfig.defaults());
  }

  public JsonNode createStream(String name, StreamConfig config)
      throws IOException, InterruptedException {
    Objects.requireNonNull(config, "config");
    ObjectNode body = MAPPER.createObjectNode();
    body.put("partitions", config.partitions());
    body.put("durability", config.durability().wireName());
    putNullable(body, "max_records_per_partition", config.maxRecordsPerPartition());
    return create("streams", name, body);
  }

  public JsonNode createQueue(String name) throws IOException, InterruptedException {
    return createQueue(name, QueueConfig.defaults());
  }

  public JsonNode createQueue(String name, QueueConfig config)
      throws IOException, InterruptedException {
    Objects.requireNonNull(config, "config");
    ObjectNode retry = MAPPER.createObjectNode();
    retry.put("strategy", "exponential");
    retry.put("initial_delay_ms", 1_000);
    retry.put("max_delay_ms", 60_000);
    retry.put("jitter_percent", 10);
    retry.put("max_attempts", config.maxAttempts());
    retry.putNull("max_age_ms");
    ObjectNode body = MAPPER.createObjectNode();
    body.put("durability", DurabilityProfile.VOLATILE.wireName());
    body.put("visibility_timeout_ms", config.visibilityTimeoutMs());
    body.put("max_messages", config.maxMessages());
    body.set("retry", retry);
    body.putNull("dedupe_window_ms");
    return create("queues", name, body);
  }

  public JsonNode createBus(String name) throws IOException, InterruptedException {
    return createBus(name, true);
  }

  public JsonNode createBus(String name, boolean archive) throws IOException, InterruptedException {
    ObjectNode body = MAPPER.createObjectNode();
    body.put("durability", DurabilityProfile.VOLATILE.wireName());
    body.put("archive", archive);
    return create("buses", name, body);
  }

  public JsonNode cacheSet(String cache, String key, String value)
      throws IOException, InterruptedException {
    return cacheSet(cache, key, value, CacheWriteOptions.defaults());
  }

  public JsonNode cacheSet(String cache, String key, String value, CacheWriteOptions options)
      throws IOException, InterruptedException {
    Objects.requireNonNull(options, "options");
    ObjectNode cacheValue = MAPPER.createObjectNode();
    cacheValue.put("kind", "string");
    cacheValue.put("value", Objects.requireNonNull(value, "value"));
    ObjectNode body = MAPPER.createObjectNode();
    body.set("value", cacheValue);
    putNullable(body, "ttl_ms", options.ttlMs());
    putNullable(body, "expected_version", options.expectedVersion());
    body.put("only_if_absent", options.onlyIfAbsent());
    body.put("only_if_present", options.onlyIfPresent());
    return request("PUT", "/v1/caches/" + segment(cache) + "/keys/" + segment(key), body);
  }

  public JsonNode cacheGet(String cache, String key) throws IOException, InterruptedException {
    return request("GET", "/v1/caches/" + segment(cache) + "/keys/" + segment(key));
  }

  public JsonNode cacheDelete(String cache, String key) throws IOException, InterruptedException {
    return request("DELETE", "/v1/caches/" + segment(cache) + "/keys/" + segment(key));
  }

  public JsonNode cacheIncrement(String cache, String key, long delta)
      throws IOException, InterruptedException {
    ObjectNode body = MAPPER.createObjectNode().put("delta", delta);
    return request(
        "POST", "/v1/caches/" + segment(cache) + "/keys/" + segment(key) + "/increment", body);
  }

  public JsonNode appendStream(String stream, EventEnvelope event)
      throws IOException, InterruptedException {
    return appendStream(stream, event, null);
  }

  public JsonNode appendStream(String stream, EventEnvelope event, Integer partition)
      throws IOException, InterruptedException {
    ObjectNode body = MAPPER.createObjectNode();
    body.set("envelope", Objects.requireNonNull(event, "event").toJson());
    putNullable(body, "partition", partition);
    return request("POST", "/v1/streams/" + segment(stream) + "/records", body);
  }

  public JsonNode fetchStream(String stream, int partition, long offset, int limit)
      throws IOException, InterruptedException {
    requireNonNegative(partition, "partition");
    requireNonNegative(offset, "offset");
    requireLimit(limit);
    return request(
        "GET",
        "/v1/streams/" + segment(stream) + "/records",
        Map.of("partition", partition, "offset", offset, "limit", limit));
  }

  public JsonNode commitStreamOffset(
      String stream, String group, int partition, long nextOffset, boolean reset)
      throws IOException, InterruptedException {
    requireNonNegative(partition, "partition");
    requireNonNegative(nextOffset, "nextOffset");
    ObjectNode body = MAPPER.createObjectNode();
    body.put("partition", partition);
    body.put("next_offset", nextOffset);
    body.put("reset", reset);
    return request(
        "PUT", "/v1/streams/" + segment(stream) + "/groups/" + segment(group) + "/offsets", body);
  }

  public JsonNode streamLag(String stream, String group, int partition)
      throws IOException, InterruptedException {
    requireNonNegative(partition, "partition");
    return request(
        "GET",
        "/v1/streams/" + segment(stream) + "/groups/" + segment(group) + "/lag",
        Map.of("partition", partition));
  }

  public JsonNode send(String queue, EventEnvelope event) throws IOException, InterruptedException {
    return request(
        "POST",
        "/v1/queues/" + segment(queue) + "/messages",
        Objects.requireNonNull(event, "event").toJson());
  }

  public JsonNode receive(String queue, QueueReceiveOptions options)
      throws IOException, InterruptedException {
    Objects.requireNonNull(options, "options");
    ObjectNode body = MAPPER.createObjectNode();
    body.put("consumer", options.consumer());
    body.put("max_messages", options.maxMessages());
    putNullable(body, "visibility_timeout_ms", options.visibilityTimeoutMs());
    return request("POST", "/v1/queues/" + segment(queue) + "/acquire", body);
  }

  public JsonNode acknowledge(String queue, String leaseToken)
      throws IOException, InterruptedException {
    return settle(
        queue,
        MAPPER
            .createObjectNode()
            .put("action", "ack")
            .put("token", required(leaseToken, "leaseToken")));
  }

  public JsonNode release(String queue, String leaseToken, long delayMs, String reason)
      throws IOException, InterruptedException {
    requireNonNegative(delayMs, "delayMs");
    ObjectNode body = MAPPER.createObjectNode();
    body.put("action", "release");
    body.put("token", required(leaseToken, "leaseToken"));
    body.put("delay_ms", delayMs);
    putNullable(body, "reason", reason);
    return settle(queue, body);
  }

  public JsonNode reject(String queue, String leaseToken, String reason)
      throws IOException, InterruptedException {
    ObjectNode body = MAPPER.createObjectNode();
    body.put("action", "reject");
    body.put("token", required(leaseToken, "leaseToken"));
    body.put("reason", required(reason, "reason"));
    return settle(queue, body);
  }

  public JsonNode extendLease(String queue, String leaseToken, long extensionMs)
      throws IOException, InterruptedException {
    if (extensionMs <= 0) {
      throw new IllegalArgumentException("extensionMs must be greater than zero");
    }
    ObjectNode body = MAPPER.createObjectNode();
    body.put("action", "extend");
    body.put("token", required(leaseToken, "leaseToken"));
    body.put("extension_ms", extensionMs);
    return settle(queue, body);
  }

  public JsonNode queueCounts(String queue) throws IOException, InterruptedException {
    return request("GET", "/v1/queues/" + segment(queue) + "/counts");
  }

  public JsonNode redrive(String queue, String messageId) throws IOException, InterruptedException {
    return request(
        "POST",
        "/v1/queues/" + segment(queue) + "/dead-letters/" + segment(messageId) + "/redrive");
  }

  public JsonNode publish(String bus, EventEnvelope event)
      throws IOException, InterruptedException {
    return request(
        "POST",
        "/v1/buses/" + segment(bus) + "/events",
        Objects.requireNonNull(event, "event").toJson());
  }

  public JsonNode upsertSubscription(String bus, Subscription subscription)
      throws IOException, InterruptedException {
    Objects.requireNonNull(subscription, "subscription");
    return request(
        "PUT",
        "/v1/buses/" + segment(bus) + "/subscriptions/" + segment(subscription.name()),
        subscription.toJson(MAPPER));
  }

  public JsonNode removeSubscription(String bus, String subscription)
      throws IOException, InterruptedException {
    return request(
        "DELETE", "/v1/buses/" + segment(bus) + "/subscriptions/" + segment(subscription));
  }

  public JsonNode replayBus(String bus, BusReplayOptions options)
      throws IOException, InterruptedException {
    Objects.requireNonNull(options, "options");
    Map<String, Object> query = new LinkedHashMap<>();
    query.put("from_ms", options.fromMs());
    query.put("to_ms", options.toMs());
    query.put("limit", options.limit());
    query.put("event_type", options.eventType());
    return request("GET", "/v1/buses/" + segment(bus) + "/replay", query);
  }

  private JsonNode create(String collection, String name, ObjectNode body)
      throws IOException, InterruptedException {
    return request("POST", "/v1/" + collection + "/" + segment(name), body);
  }

  private JsonNode settle(String queue, ObjectNode body) throws IOException, InterruptedException {
    return request("POST", "/v1/queues/" + segment(queue) + "/settle", body);
  }

  private JsonNode request(String method, String path) throws IOException, InterruptedException {
    return transport.request(method, path, null, null);
  }

  private JsonNode request(String method, String path, JsonNode body)
      throws IOException, InterruptedException {
    return transport.request(method, path, body, null);
  }

  private JsonNode request(String method, String path, Map<String, ?> query)
      throws IOException, InterruptedException {
    return transport.request(method, path, null, query);
  }

  private static String segment(String value) {
    return URLEncoder.encode(required(value, "resource name or key"), StandardCharsets.UTF_8)
        .replace("+", "%20");
  }

  private static String required(String value, String name) {
    if (Objects.requireNonNull(value, name).isBlank()) {
      throw new IllegalArgumentException(name + " is required");
    }
    return value;
  }

  private static void requireNonNegative(long value, String name) {
    if (value < 0) {
      throw new IllegalArgumentException(name + " cannot be negative");
    }
  }

  private static void requireLimit(int limit) {
    if (limit <= 0 || limit > 10_000) {
      throw new IllegalArgumentException("limit must be between 1 and 10000");
    }
  }

  private static void putNullable(ObjectNode body, String name, String value) {
    if (value == null) {
      body.putNull(name);
    } else {
      body.put(name, value);
    }
  }

  private static void putNullable(ObjectNode body, String name, Long value) {
    if (value == null) {
      body.putNull(name);
    } else {
      body.put(name, value);
    }
  }

  private static void putNullable(ObjectNode body, String name, Integer value) {
    if (value == null) {
      body.putNull(name);
    } else {
      body.put(name, value);
    }
  }
}
