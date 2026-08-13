import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.epoch.sdk.EventEnvelope;
import io.epoch.sdk.RegionalScope;
import io.epoch.sdk.RegionalStreamClient;
import io.epoch.sdk.StreamPartitioner;
import io.epoch.sdk.StreamBatchFrame;
import io.epoch.sdk.StreamBatchRecord;
import io.epoch.sdk.StreamCompression;
import io.epoch.sdk.StreamRetentionPolicy;
import java.math.BigInteger;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public final class RegionalQuickstart {
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
    RegionalStreamClient client =
        new RegionalStreamClient(
            endpoints,
            environment("EPOCH_TOKEN", "epoch-dev-admin-v1"),
            new RegionalScope("acme", "shop", "dev", "core"),
            Duration.ofSeconds(3));
    EventEnvelope event =
        EventEnvelope.builder("docs-java", "order.created", Map.of("order_id", "java-42"))
            .id("docs-java-order-42")
            .key("customer-0")
            .timeMs(42)
            .build();
    int shard = StreamPartitioner.shardFor(event.key(), 3);

    JsonNode appended = client.appendKeyed("orders", "docs-java-keyed-stream-v1", event);
    JsonNode replayed = client.appendKeyed("orders", "docs-java-keyed-stream-v1", event);
    EventEnvelope secondBatchEvent =
        EventEnvelope.builder("docs-java", "order.created", Map.of("order_id", "java-43"))
            .id("docs-java-order-43")
            .key("customer-0")
            .timeMs(43)
            .build();
    StreamBatchFrame batchFrame =
        StreamBatchFrame.encode(
            List.of(new StreamBatchRecord(101, event), new StreamBatchRecord(102, secondBatchEvent)),
            StreamCompression.GZIP);
    JsonNode batch =
        client.appendBatch("orders", shard, "docs-java-gzip-batch-v1", batchFrame);
    long offset = appended.path("receipt").path("offset").asLong();
    JsonNode fetched = client.fetch("orders", shard, offset, 10);
    JsonNode groupRecords = client.fetchGroup("orders", shard, "docs-java", 100);
    JsonNode checkpoint =
        client.commitOffset(
            "orders",
            shard,
            "docs-java",
            "docs-java-worker",
            1,
            offset + 1,
            false,
            "docs-java-checkpoint-v1");
    JsonNode lag = client.lag("orders", shard, "docs-java");
    JsonNode joined =
        client.joinConsumerSession(
            "orders",
            "docs-java-session",
            "docs-java-worker",
            Duration.ofSeconds(30),
            "docs-java-session-join-v1");
    JsonNode heartbeat =
        client.heartbeatConsumerSession(
            "orders",
            "docs-java-session",
            "docs-java-worker",
            1,
            "docs-java-session-heartbeat-v1");
    JsonNode session = client.consumerSession("orders", "docs-java-session");
    JsonNode left =
        client.leaveConsumerSession(
            "orders",
            "docs-java-session",
            "docs-java-worker",
            1,
            "docs-java-session-leave-v1");
    JsonNode configured =
        client.configureRetention(
            "orders",
            shard,
            "docs-java-retention-v1",
            new StreamRetentionPolicy(
                10_000,
                BigInteger.valueOf(3L * 1024 * 1024),
                BigInteger.valueOf(7L * 24 * 60 * 60 * 1_000)));
    JsonNode maintained =
        client.maintainRetention("orders", shard, "docs-java-retention-sweep-v1");
    JsonNode retention = client.retention("orders", shard);

    ObjectNode output = MAPPER.createObjectNode();
    output.put("selected_shard", shard);
    output.set("append", appended);
    output.set("exact_retry", replayed);
    output.set("gzip_batch", batch);
    output.set("fetch", fetched);
    output.set("group_fetch", groupRecords);
    output.set("checkpoint", checkpoint);
    output.set("lag", lag);
    output.set("session_join", joined);
    output.set("session_heartbeat", heartbeat);
    output.set("session", session);
    output.set("session_leave", left);
    output.set("retention_configure", configured);
    output.set("retention_maintenance", maintained);
    output.set("retention", retention);
    System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(output));
  }

  private static String environment(String name, String fallback) {
    String value = System.getenv(name);
    return value == null || value.isBlank() ? fallback : value;
  }
}
