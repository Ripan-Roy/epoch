package io.epoch.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.math.BigInteger;
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
                    null)));
    EventEnvelope event =
        EventEnvelope.builder("java-regional-sdk", "order.created", Map.of("id", 2))
            .id("order-2")
            .timeMs(2)
            .build();
    String bus = "events/eu";

    client.upsertSubscription(bus, 0, "upsert-1", subscription);
    client.publish(bus, 0, "publish-1", event);
    client.acquireDeliveries(bus, 0, "acquire-1", "orders", "worker-a", BigInteger.valueOf(7), 10);
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
    client.maintainDeliveries(bus, 0, "maintain-1", 100);
    client.removeSubscription(bus, 0, "remove-1", "orders");
    client.mutation(bus, 0, BigInteger.valueOf(12));
    client.replayArchive(bus, 0, BigInteger.ONE, BigInteger.TEN, 100, subscription.filter());
    client.queryDeliveries(bus, 0, "orders", RegionalBusDeliveryState.IN_FLIGHT, 100);
    client.status(bus, 0);

    String base =
        "/v1/organizations/acme/projects/shop/environments/dev/namespaces/core"
            + "/buses/events%2Feu/shards/0";
    List<Request> operations = new ArrayList<>();
    for (int index = 1; index < transport.requests.size(); index += 2) {
      operations.add(transport.requests.get(index));
    }
    assertEquals(12, operations.size());
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
        "reject_delivery", operations.get(5).body().path("operation").path("kind").asText());
    assertEquals(base + "/mutations/12", operations.get(8).path());
    assertEquals("1", operations.get(9).body().path("from_ms").asText());
    assertEquals("in_flight", operations.get(10).body().path("state").asText());
    for (Request request : operations.subList(8, operations.size())) {
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
        () -> client.replayArchive("events", 0, BigInteger.TEN, BigInteger.ONE, 1, null));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new DeliveryRetryPolicy(
                DeliveryBackoffStrategy.FIXED, BigInteger.TEN, BigInteger.ONE, 0, 1, null));
    assertThrows(
        IllegalArgumentException.class,
        () -> SubscriptionTarget.signedWebhook("https://example.com/orders", "bad/key"));
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
