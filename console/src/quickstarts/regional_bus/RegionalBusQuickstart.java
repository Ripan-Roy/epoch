import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.epoch.sdk.DeliveryBackoffStrategy;
import io.epoch.sdk.DeliveryPolicy;
import io.epoch.sdk.DeliveryRetryPolicy;
import io.epoch.sdk.EventEnvelope;
import io.epoch.sdk.EventFilter;
import io.epoch.sdk.EventTransform;
import io.epoch.sdk.RegionalBusClient;
import io.epoch.sdk.RegionalBusDeliveryState;
import io.epoch.sdk.RegionalScope;
import io.epoch.sdk.Subscription;
import io.epoch.sdk.SubscriptionTarget;
import java.math.BigInteger;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public final class RegionalBusQuickstart {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static void main(String[] args) throws Exception {
    List<URI> endpoints =
        Arrays.stream(
                environment(
                        "EPOCH_REGIONAL_ENDPOINTS",
                        "http://127.0.0.1:18661,http://127.0.0.1:18662,http://127.0.0.1:18663")
                    .split(","))
            .map(String::trim)
            .map(URI::create)
            .toList();
    RegionalBusClient client =
        new RegionalBusClient(
            endpoints,
            environment("EPOCH_TOKEN", "epoch-dev-admin-v1"),
            new RegionalScope("acme", "shop", "dev", "core"),
            Duration.ofSeconds(3));
    DeliveryPolicy policy =
        new DeliveryPolicy(
            BigInteger.valueOf(30_000),
            16,
            new DeliveryRetryPolicy(
                DeliveryBackoffStrategy.FIXED,
                BigInteger.valueOf(1_000),
                BigInteger.valueOf(60_000),
                10,
                8,
                null));
    Subscription subscription =
        new Subscription(
            "orders",
            new EventFilter(List.of("order.*"), List.of(), List.of(), Map.of(), Map.of()),
            SubscriptionTarget.pull(),
            EventTransform.empty(),
            policy);
    JsonNode upserted =
        client.upsertSubscription("events", 0, "docs-java-bus-upsert-v1", subscription);
    Subscription queueSubscription =
        new Subscription(
            "queue-jobs",
            new EventFilter(List.of("target.*"), List.of(), List.of(), Map.of(), Map.of()),
            SubscriptionTarget.queue("jobs"),
            EventTransform.empty(),
            policy);
    JsonNode queueUpserted =
        client.upsertSubscription(
            "events", 0, "docs-java-bus-queue-target-v1", queueSubscription);
    Subscription streamSubscription =
        new Subscription(
            "stream-orders",
            new EventFilter(List.of("target.*"), List.of(), List.of(), Map.of(), Map.of()),
            SubscriptionTarget.stream("orders"),
            EventTransform.empty(),
            policy);
    JsonNode streamUpserted =
        client.upsertSubscription(
            "events", 0, "docs-java-bus-stream-target-v1", streamSubscription);
    EventEnvelope event =
        EventEnvelope.builder("docs-java", "order.created", Map.of("id", 1))
            .id("docs-order-1")
            .build();
    JsonNode published = client.publish("events", 0, "docs-java-bus-publish-v1", event);
    JsonNode replayed = client.publish("events", 0, "docs-java-bus-publish-v1", event);
    JsonNode acquired =
        client.acquireDeliveries(
            "events", 0, "docs-java-bus-acquire-v1", "orders", "docs-java", BigInteger.ONE, 1);
    JsonNode delivery = result(acquired).path("deliveries").get(0);
    JsonNode acknowledged =
        client.acknowledgeDelivery(
            "events",
            0,
            "docs-java-bus-ack-v1",
            delivery.path("delivery_id").asText(),
            "docs-java",
            BigInteger.ONE,
            delivery.path("lease_token").asText());
    EventEnvelope targetEvent =
        EventEnvelope.builder("docs-java", "target.created", Map.of("id", 2))
            .id("docs-target-1")
            .key("customer-42")
            .build();
    JsonNode targetPublished =
        client.publish("events", 0, "docs-java-bus-target-publish-v1", targetEvent);
    JsonNode queueDelivery = waitForTarget(client, "queue-jobs", "queue");
    JsonNode streamDelivery = waitForTarget(client, "stream-orders", "stream");

    ObjectNode output = MAPPER.createObjectNode();
    output.set("upsert", upserted);
    output.set("queue_target_upsert", queueUpserted);
    output.set("stream_target_upsert", streamUpserted);
    output.set("publish", published);
    output.set("exact_retry", replayed);
    output.set("acknowledge", acknowledged);
    output.set("target_publish", targetPublished);
    output.set("queue_delivery", queueDelivery);
    output.set("stream_delivery", streamDelivery);
    output.set(
        "archive",
        client.replayArchive(
            "events", 0, BigInteger.ZERO, BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE), 100, subscription.filter()));
    output.set(
        "deliveries",
        client.queryDeliveries("events", 0, "orders", RegionalBusDeliveryState.ACKNOWLEDGED, 100));
    output.set("status", client.status("events", 0));
    System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(output));
  }

  private static JsonNode result(JsonNode document) {
    return document.path("receipt").path("outcome").path("result");
  }

  private static JsonNode waitForTarget(
      RegionalBusClient client, String subscription, String kind) throws Exception {
    long deadlineNanos = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    while (System.nanoTime() < deadlineNanos) {
      JsonNode document =
          client.queryDeliveries(
              "events", 0, subscription, RegionalBusDeliveryState.ACKNOWLEDGED, 100);
      for (JsonNode record : document.path("records")) {
        if (kind.equals(record.path("destination").path("kind").asText())) {
          return record;
        }
      }
      Thread.sleep(50);
    }
    throw new IllegalStateException("timed out waiting for " + kind + " target delivery");
  }

  private static String environment(String name, String fallback) {
    String value = System.getenv(name);
    return value == null || value.isBlank() ? fallback : value;
  }
}
