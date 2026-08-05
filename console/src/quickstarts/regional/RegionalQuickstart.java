import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.epoch.sdk.EventEnvelope;
import io.epoch.sdk.RegionalScope;
import io.epoch.sdk.RegionalStreamClient;
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
            .timeMs(42)
            .build();

    JsonNode appended = client.append("orders", 0, "docs-java-append-v1", event);
    JsonNode replayed = client.append("orders", 0, "docs-java-append-v1", event);
    long offset = appended.path("receipt").path("offset").asLong();
    JsonNode fetched = client.fetch("orders", 0, offset, 10);
    JsonNode groupRecords = client.fetchGroup("orders", 0, "docs-java", 100);
    JsonNode checkpoint =
        client.commitOffset(
            "orders",
            0,
            "docs-java",
            "docs-java-worker",
            1,
            offset + 1,
            false,
            "docs-java-checkpoint-v1");
    JsonNode lag = client.lag("orders", 0, "docs-java");

    ObjectNode output = MAPPER.createObjectNode();
    output.set("append", appended);
    output.set("exact_retry", replayed);
    output.set("fetch", fetched);
    output.set("group_fetch", groupRecords);
    output.set("checkpoint", checkpoint);
    output.set("lag", lag);
    System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(output));
  }

  private static String environment(String name, String fallback) {
    String value = System.getenv(name);
    return value == null || value.isBlank() ? fallback : value;
  }
}
