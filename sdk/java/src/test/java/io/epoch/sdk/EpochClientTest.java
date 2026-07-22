package io.epoch.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class EpochClientTest {
  private FakeTransport transport;
  private EpochClient client;

  @BeforeEach
  void setUp() {
    transport = new FakeTransport();
    client = new EpochClient(transport);
  }

  @Test
  void createsStreamsWithTruthfulDurability() throws Exception {
    client.createStream("orders", new StreamConfig(4, DurabilityProfile.VOLATILE, null));
    client.createStream("audit", new StreamConfig(1, DurabilityProfile.LOCAL_DURABLE, null));

    assertEquals("POST", transport.requests.get(0).method());
    assertEquals("/v1/streams/orders", transport.requests.get(0).path());
    assertEquals("volatile", transport.requests.get(0).body().get("durability").textValue());
    assertEquals("local_durable", transport.requests.get(1).body().get("durability").textValue());
  }

  @Test
  void serializesTheCommonEnvelopeAndEscapesPathSegments() throws Exception {
    EventEnvelope event =
        EventEnvelope.builder("checkout", "order.created", Map.of("id", "1"))
            .id("order-1")
            .timeMs(1_000)
            .build();

    client.appendStream("orders/eu", event, 1);
    client.cacheSet(
        "sessions/eu", "user 42", "active", new CacheWriteOptions(null, null, true, false));

    Request append = transport.requests.get(0);
    assertEquals("/v1/streams/orders%2Feu/records", append.path());
    assertEquals("order.created", append.body().get("envelope").get("type").textValue());
    assertFalse(append.body().get("envelope").has("event_type"));
    assertEquals(1, append.body().get("partition").intValue());
    assertEquals("/v1/caches/sessions%2Feu/keys/user%2042", transport.requests.get(1).path());
    assertTrue(transport.requests.get(1).body().get("only_if_absent").booleanValue());
  }

  @Test
  void rejectsInvalidEventsBeforeUsingTheTransport() {
    assertThrows(
        IllegalArgumentException.class,
        () -> EventEnvelope.builder("", "created", Map.of()).build());
    assertTrue(transport.requests.isEmpty());
  }

  @Test
  void mapsCacheAndConsumerGroupOperations() throws Exception {
    client.cacheIncrement("sessions", "visits", 2);
    client.cacheDelete("sessions", "user-42");
    client.commitStreamOffset("orders", "billing", 2, 7, false);
    client.streamLag("orders", "billing", 2);

    assertEquals(2, transport.requests.get(0).body().get("delta").intValue());
    assertEquals("DELETE", transport.requests.get(1).method());
    assertEquals(7, transport.requests.get(2).body().get("next_offset").longValue());
    assertEquals(Map.of("partition", 2), transport.requests.get(3).query());
  }

  @Test
  void mapsProfileCreationAndStreamReads() throws Exception {
    client.health();
    client.resources();
    client.createCache("sessions", CacheConfig.defaults());
    client.createQueue("jobs", QueueConfig.defaults());
    client.createBus("events", true);
    client.fetchStream("orders", 1, 20, 50);

    assertEquals("/healthz", transport.requests.get(0).path());
    assertEquals("/v1/resources", transport.requests.get(1).path());
    assertEquals("volatile", transport.requests.get(2).body().get("durability").textValue());
    assertEquals(8, transport.requests.get(3).body().get("retry").get("max_attempts").intValue());
    assertTrue(transport.requests.get(4).body().get("archive").booleanValue());
    assertEquals(
        Map.of("partition", 1, "offset", 20L, "limit", 50), transport.requests.get(5).query());
  }

  @Test
  void mapsQueueLifecycleOperations() throws Exception {
    EventEnvelope event = EventEnvelope.builder("tests", "job.created", Map.of()).build();
    client.send("jobs", event);
    client.receive("jobs", new QueueReceiveOptions("worker-1", 2, 5_000L));
    client.acknowledge("jobs", "lease-0");
    client.release("jobs", "lease-release", 100, "retry");
    client.queueCounts("jobs");
    client.extendLease("jobs", "lease-1", 5_000);
    client.reject("jobs", "lease-2", "invalid");
    client.redrive("jobs", "message-1");

    assertEquals("job.created", transport.requests.get(0).body().get("type").textValue());
    assertEquals("worker-1", transport.requests.get(1).body().get("consumer").textValue());
    assertEquals("ack", transport.requests.get(2).body().get("action").textValue());
    assertEquals("release", transport.requests.get(3).body().get("action").textValue());
    assertEquals("/v1/queues/jobs/counts", transport.requests.get(4).path());
    assertEquals("extend", transport.requests.get(5).body().get("action").textValue());
    assertEquals("invalid", transport.requests.get(6).body().get("reason").textValue());
    assertEquals(
        "/v1/queues/jobs/dead-letters/message-1/redrive", transport.requests.get(7).path());
  }

  @Test
  void mapsTypedBusSubscriptionsAndReplay() throws Exception {
    Subscription subscription =
        new Subscription(
            "priority-orders",
            new EventFilter(List.of("order.*"), List.of(), List.of(), Map.of(), Map.of()),
            SubscriptionTarget.queue("jobs"),
            new EventTransform(Map.of("routed-by", "epoch"), Map.of()));

    client.publish("events", EventEnvelope.builder("tests", "order.created", Map.of()).build());
    client.upsertSubscription("events", subscription);
    client.replayBus(
        "events",
        new BusReplayOptions(BigInteger.valueOf(100), BigInteger.valueOf(200), 100, "order.*"));
    client.removeSubscription("events", "priority-orders");

    assertEquals("order.created", transport.requests.get(0).body().get("type").textValue());
    Request upsert = transport.requests.get(1);
    assertEquals("PUT", upsert.method());
    assertEquals("queue", upsert.body().get("target").get("kind").textValue());
    assertEquals("jobs", upsert.body().get("target").get("resource").textValue());
    assertEquals(
        "order.*", upsert.body().get("filter").get("event_type_patterns").get(0).textValue());
    assertEquals(BigInteger.valueOf(100), transport.requests.get(2).query().get("from_ms"));
    assertEquals("DELETE", transport.requests.get(3).method());
  }

  private record Request(String method, String path, JsonNode body, Map<String, ?> query) {}

  private static final class FakeTransport implements Transport {
    private final List<Request> requests = new ArrayList<>();

    @Override
    public JsonNode request(String method, String path, JsonNode body, Map<String, ?> query)
        throws IOException {
      requests.add(new Request(method, path, body, query));
      return JsonNodeFactory.instance.objectNode().put("ok", true);
    }
  }
}
