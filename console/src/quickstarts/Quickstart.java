import io.epoch.sdk.DurabilityProfile;
import io.epoch.sdk.EpochClient;
import io.epoch.sdk.EventEnvelope;
import io.epoch.sdk.QueueConfig;
import io.epoch.sdk.QueueReceiveOptions;
import io.epoch.sdk.StreamConfig;
import java.net.URI;
import java.time.Duration;
import java.util.Map;

public final class Quickstart {
  private static final String STREAM = "orders";
  private static final String QUEUE = "jobs";

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException("usage: Quickstart [seed|verify]");
    }

    String endpoint = System.getenv().getOrDefault("EPOCH_URL", "http://127.0.0.1:7601");
    EpochClient client = new EpochClient(URI.create(endpoint), Duration.ofSeconds(10));
    switch (args[0]) {
      case "seed" -> seed(client);
      case "verify" -> verify(client);
      default -> throw new IllegalArgumentException("mode must be seed or verify");
    }
  }

  private static void seed(EpochClient client) throws Exception {
    client.createStream(
        STREAM, new StreamConfig(1, DurabilityProfile.LOCAL_DURABLE, null));
    client.createQueue(
        QUEUE, new QueueConfig(DurabilityProfile.LOCAL_DURABLE, 30_000, 100_000, 8));

    EventEnvelope order =
        EventEnvelope.builder("quickstart", "order.created", Map.of("order_id", "o-1001"))
            .id("order-o-1001")
            .build();
    var receipt = client.appendStream(STREAM, order);
    if (!receipt.get("acknowledgement").get("durability").asText().equals("local_durable")) {
      throw new IllegalStateException("unexpected durability receipt: " + receipt);
    }
    System.out.printf(
        "stream records before restart: %d%n", client.fetchStream(STREAM, 0, 0, 10).size());

    for (String id : new String[] {"job-1001", "job-1002"}) {
      client.send(
          QUEUE,
          EventEnvelope.builder("quickstart", "job.requested", Map.of("job_id", id))
              .id(id)
              .build());
    }

    var deliveries = client.receive(QUEUE, new QueueReceiveOptions("worker-a", 1, null));
    if (deliveries.size() != 1) {
      throw new IllegalStateException("expected one delivery, got " + deliveries.size());
    }
    if (!deliveries.get(0).get("message").get("id").asText().equals("job-1001")) {
      throw new IllegalStateException("expected job-1001 first: " + deliveries);
    }
    client.acknowledge(QUEUE, deliveries.get(0).get("lease_token").textValue());
    System.out.println("acked one job; restart the node now");
  }

  private static void verify(EpochClient client) throws Exception {
    var records = client.fetchStream(STREAM, 0, 0, 10);
    if (records.size() != 1
        || !records.get(0).get("envelope").get("id").asText().equals("order-o-1001")) {
      throw new IllegalStateException("unexpected recovered stream records: " + records);
    }

    var counts = client.queueCounts(QUEUE);
    if (counts.get("acknowledged").asInt() != 1) {
      throw new IllegalStateException("expected one recovered acknowledgement: " + counts);
    }

    var deliveries = client.receive(QUEUE, new QueueReceiveOptions("worker-b", 10, null));
    if (deliveries.size() != 1) {
      throw new IllegalStateException(
          "expected only the unacked job after restart, got " + deliveries.size());
    }
    if (!deliveries.get(0).get("message").get("id").asText().equals("job-1002")) {
      throw new IllegalStateException("expected job-1002 after restart: " + deliveries);
    }
    client.acknowledge(QUEUE, deliveries.get(0).get("lease_token").textValue());
    System.out.println("restart verified: stream record and queue settlement survived");
  }
}
