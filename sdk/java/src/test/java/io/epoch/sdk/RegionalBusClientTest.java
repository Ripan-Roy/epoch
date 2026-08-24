package io.epoch.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class RegionalBusClientTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void completeMutationAndLinearizableReadContract() throws Exception {
    RecordingTransport transport = new RecordingTransport();
    RegionalBusClient client =
        RegionalBusClient.withTransports(
            List.of(transport), "secret-token", new RegionalScope("acme", "shop", "dev", "core"));
    Subscription subscription =
        new Subscription(
            "orders",
            new EventFilter(List.of("order.*"), List.of(), List.of(), Map.of(), Map.of()),
            SubscriptionTarget.signedWebhook("https://example.com/orders", "primary"),
            EventTransform.empty(),
            new DeliveryPolicy(
                BigInteger.valueOf(30_000),
                16,
                new DeliveryRetryPolicy(
                    DeliveryBackoffStrategy.FIXED,
                    BigInteger.valueOf(1_000),
                    BigInteger.valueOf(60_000),
                    10,
                    8,
                    null),
                new DeliveryRateLimit(25, 50),
                BigInteger.valueOf(86_400_000)));
    EventEnvelope event =
        EventEnvelope.builder("java-regional-sdk", "order.created", Map.of("id", 2))
            .id("order-2")
            .timeMs(2)
            .build();
    String bus = "events/eu";

    client.upsertSubscription(bus, 0, "upsert-1", subscription);
    client.publish(bus, 0, "publish-1", event);
    client.acquireDeliveries(
        bus,
        0,
        "acquire-1",
        "orders",
        "worker-a",
        BigInteger.valueOf(7),
        10,
        Duration.ofSeconds(5));
    client.acknowledgeDelivery(
        bus, 0, "ack-1", "delivery-1", "worker-a", BigInteger.valueOf(7), "lease-1");
    client.failDelivery(
        bus,
        0,
        "fail-1",
        "delivery-2",
        "worker-a",
        BigInteger.valueOf(7),
        "lease-2",
        "downstream timeout");
    client.rejectDelivery(
        bus,
        0,
        "reject-1",
        "delivery-3",
        "worker-a",
        BigInteger.valueOf(7),
        "lease-3",
        "http status 400");
    client.redriveDelivery(bus, 0, "redrive-1", "delivery-3");
    client.maintainDeliveries(bus, 0, "maintain-1", 100);
    client.maintainArchive(bus, 0, "archive-retention-1", 100);
    client.applyIntegration(
        bus,
        0,
        "schema-1",
        MAPPER
            .createObjectNode()
            .put("kind", "register_schema")
            .set("registration", MAPPER.createObjectNode().put("name", "orders")));
    client.removeSubscription(bus, 0, "remove-1", "orders");
    client.mutation(bus, 0, BigInteger.valueOf(12));
    client.replayArchive(bus, 0, BigInteger.ONE, BigInteger.TEN, 100, subscription.filter());
    client.queryDeliveries(bus, 0, "orders", RegionalBusDeliveryState.IN_FLIGHT, 100);
    client.status(bus, 0);
    client.integrationState(bus, 0);

    String base =
        "/v1/organizations/acme/projects/shop/environments/dev/namespaces/core"
            + "/buses/events%2Feu/shards/0";
    List<Request> operations = new ArrayList<>();
    for (int index = 1; index < transport.requests.size(); index += 2) {
      operations.add(transport.requests.get(index));
    }
    assertEquals(16, operations.size());
    assertEquals(base + "/mutations", operations.get(0).path());
    assertEquals(
        "fixed",
        operations
            .get(0)
            .body()
            .path("operation")
            .path("subscription")
            .path("delivery_policy")
            .path("retry")
            .path("strategy")
            .asText());
    assertEquals(
        "primary",
        operations
            .get(0)
            .body()
            .path("operation")
            .path("subscription")
            .path("target")
            .path("signing_key_id")
            .asText());
    assertEquals(
        50,
        operations
            .get(0)
            .body()
            .path("operation")
            .path("subscription")
            .path("delivery_policy")
            .path("rate_limit")
            .path("burst")
            .asInt());
    assertEquals(
        "86400000",
        operations
            .get(0)
            .body()
            .path("operation")
            .path("subscription")
            .path("delivery_policy")
            .path("dead_letter_retention_ms")
            .asText());
    assertEquals(5000, operations.get(2).body().path("operation").path("wait_ms").asInt());
    assertEquals(
        "reject_delivery", operations.get(5).body().path("operation").path("kind").asText());
    assertEquals(
        "redrive_delivery", operations.get(6).body().path("operation").path("kind").asText());
    assertEquals(
        "maintain_archive", operations.get(8).body().path("operation").path("kind").asText());
    assertEquals(
        "apply_integration", operations.get(9).body().path("operation").path("kind").asText());
    assertEquals(base + "/mutations/12", operations.get(11).path());
    assertEquals("1", operations.get(12).body().path("from_ms").asText());
    assertEquals("in_flight", operations.get(13).body().path("state").asText());
    assertEquals(base + "/integration/state", operations.get(15).path());
    for (Request request : operations.subList(11, operations.size())) {
      assertEquals("linearizable", request.headers().get("x-epoch-read-consistency"));
    }
  }

  @Test
  void invalidBoundsAndPolicyFailBeforeNetwork() {
    RecordingTransport transport = new RecordingTransport();
    RegionalBusClient client =
        RegionalBusClient.withTransports(
            List.of(transport), "secret-token", new RegionalScope("acme", "shop", "dev", "core"));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            client.acquireDeliveries(
                "events", 0, "acquire", "orders", "worker", BigInteger.ONE, 0));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            client.acquireDeliveries(
                "events",
                0,
                "acquire-wait",
                "orders",
                "worker",
                BigInteger.ONE,
                1,
                Duration.ofMillis(30_001)));
    assertThrows(
        IllegalArgumentException.class,
        () -> client.replayArchive("events", 0, BigInteger.TEN, BigInteger.ONE, 1, null));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new DeliveryRetryPolicy(
                DeliveryBackoffStrategy.FIXED, BigInteger.TEN, BigInteger.ONE, 0, 1, null));
    assertThrows(
        IllegalArgumentException.class,
        () -> SubscriptionTarget.signedWebhook("https://example.com/orders", "bad/key"));
    assertThrows(IllegalArgumentException.class, () -> new DeliveryRateLimit(0, 1));
    assertThrows(
        IllegalArgumentException.class,
        () -> new TransformLimits(64, 256 * 1024, 64 * 1024, 100, true));
    assertThrows(
        IllegalArgumentException.class,
        () -> DestinationAuth.oauth2("oauth", "file:///token", List.of()));
    assertThrows(
        IllegalArgumentException.class, () -> client.redriveDelivery("events", 0, "redrive", ""));
    assertEquals(List.of(), transport.requests);
  }

  @Test
  void typedSchemaLifecycleIsValidatedAndRouted() throws Exception {
    RecordingTransport transport = new RecordingTransport();
    RegionalBusClient client =
        RegionalBusClient.withTransports(
            List.of(transport), "secret-token", new RegionalScope("acme", "shop", "dev", "core"));
    EventEnvelope event =
        EventEnvelope.builder("java-regional-sdk", "order.created", Map.of("id", 2))
            .id("order-2")
            .timeMs(2)
            .build();

    client.registerSchema(
        "events",
        0,
        "schema-1",
        new SchemaRegistration(
            "orders",
            SchemaFormat.PROTOBUF,
            "syntax = \"proto3\"; message Order { string id = 1; }",
            SchemaCompatibility.BACKWARD,
            "Order"));
    client.upsertSchemaValidationPolicy(
        "events",
        0,
        "policy-1",
        new SchemaValidationPolicy(
            "orders", "order.*", "orders@1", SchemaValidationMode.PRODUCER_AND_BROKER));
    client.validateSchema("events", 0, SchemaValidationStage.PRODUCER, event);
    client.removeSchemaValidationPolicy("events", 0, "policy-remove-1", "orders");

    List<Request> operations = new ArrayList<>();
    for (int index = 1; index < transport.requests.size(); index += 2) {
      operations.add(transport.requests.get(index));
    }
    assertEquals(4, operations.size());
    assertEquals(
        "protobuf",
        operations
            .get(0)
            .body()
            .path("operation")
            .path("operation")
            .path("registration")
            .path("format")
            .asText());
    assertEquals(
        "Order",
        operations
            .get(0)
            .body()
            .path("operation")
            .path("operation")
            .path("registration")
            .path("root_message")
            .asText());
    assertEquals(
        "/v1/organizations/acme/projects/shop/environments/dev/namespaces/core"
            + "/buses/events/shards/0/schema/validate",
        operations.get(2).path());
    assertEquals("producer", operations.get(2).body().path("mode").asText());
    assertEquals("linearizable", operations.get(2).headers().get("x-epoch-read-consistency"));
  }

  @Test
  void invalidSchemaLifecycleFailsBeforeNetwork() {
    RecordingTransport transport = new RecordingTransport();
    RegionalBusClient client =
        RegionalBusClient.withTransports(
            List.of(transport), "secret-token", new RegionalScope("acme", "shop", "dev", "core"));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new SchemaRegistration(
                "bad/name", SchemaFormat.JSON_SCHEMA, "{}", SchemaCompatibility.NONE));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new SchemaRegistration(
                "orders", SchemaFormat.JSON_SCHEMA, "", SchemaCompatibility.NONE));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new SchemaRegistration(
                "orders", SchemaFormat.JSON_SCHEMA, "{}", SchemaCompatibility.NONE, "Order"));
    assertThrows(
        IllegalArgumentException.class,
        () -> new SchemaValidationPolicy("orders", "", "orders@1", SchemaValidationMode.BROKER));
    assertThrows(
        IllegalArgumentException.class,
        () -> client.removeSchemaValidationPolicy("events", 0, "remove", "bad/name"));
    assertEquals(List.of(), transport.requests);
  }

  private record Request(
      String method,
      String path,
      JsonNode body,
      Map<String, ?> query,
      Map<String, String> headers) {}

  private static final class RecordingTransport implements Transport {
    private final List<Request> requests = new ArrayList<>();

    @Override
    public JsonNode request(String method, String path, JsonNode body, Map<String, ?> query) {
      throw new AssertionError("regional calls must use the header-aware transport contract");
    }

    @Override
    public JsonNode request(
        String method,
        String path,
        JsonNode body,
        Map<String, ?> query,
        Map<String, String> headers)
        throws IOException {
      requests.add(new Request(method, path, body, query, headers));
      if ("GET".equals(method) && path.endsWith("/shards/0")) {
        return MAPPER
            .createObjectNode()
            .put("resource_generation", "6")
            .put("tablet_epoch", "4")
            .put("term", "11")
            .put("accepts_writes", true);
      }
      return MAPPER
          .createObjectNode()
          .put("state", "committed")
          .put("outcome_certainty", "committed");
    }
  }
}
