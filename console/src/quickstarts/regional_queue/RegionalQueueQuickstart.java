import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.epoch.sdk.EventEnvelope;
import io.epoch.sdk.RegionalQueueClient;
import io.epoch.sdk.RegionalScope;
import java.math.BigInteger;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public final class RegionalQueueQuickstart {
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
    RegionalQueueClient client =
        new RegionalQueueClient(
            endpoints,
            environment("EPOCH_TOKEN", "epoch-dev-admin-v1"),
            new RegionalScope("acme", "shop", "dev", "core"),
            Duration.ofSeconds(3));
    EventEnvelope event =
        EventEnvelope.builder("docs-java", "job.created", Map.of("job_id", "java-42"))
            .id("docs-java-job-42")
            .timeMs(42)
            .build();

    JsonNode enqueued = client.enqueue("jobs", 0, "docs-java-enqueue-v1", event);
    JsonNode replayed = client.enqueue("jobs", 0, "docs-java-enqueue-v1", event);
    JsonNode acquired =
        client.acquire("jobs", 0, "docs-java-acquire-v1", "docs-java", 1, 1, 1, 5_000L);
    String leaseToken =
        result(acquired).path("deliveries").path(0).path("lease_token").asText();
    JsonNode extended =
        client.extendLease(
            "jobs", 0, "docs-java-extend-v1", "docs-java", 1, leaseToken, 60_000);
    JsonNode released =
        client.release(
            "jobs",
            0,
            "docs-java-release-v1",
            "docs-java",
            1,
            result(extended).path("lease_token").asText(),
            0,
            "demonstrate retry");
    JsonNode maintained = client.maintain("jobs", 0, "docs-java-maintain-v1");
    JsonNode reacquired =
        client.acquire("jobs", 0, "docs-java-reacquire-v1", "docs-java", 1, 1, 1, null);
    String redeliveryToken =
        result(reacquired).path("deliveries").path(0).path("lease_token").asText();
    JsonNode rejected =
        client.reject(
            "jobs",
            0,
            "docs-java-reject-v1",
            "docs-java",
            1,
            redeliveryToken,
            "poison");
    BigInteger historyId =
        new BigInteger(result(rejected).path("dead_letter_history_id").asText());
    JsonNode deadLetters = client.deadLetters("jobs", 0, 10);
    JsonNode redriven =
        client.redrive("jobs", 0, "docs-java-redrive-v1", "docs-java-job-42", historyId);
    JsonNode finalAcquire =
        client.acquire(
            "jobs", 0, "docs-java-final-acquire-v1", "docs-java", 1, 1, 1, null);
    JsonNode acknowledged =
        client.acknowledge(
            "jobs",
            0,
            "docs-java-ack-v1",
            "docs-java",
            1,
            result(finalAcquire).path("deliveries").path(0).path("lease_token").asText());

    ObjectNode output = MAPPER.createObjectNode();
    output.set("enqueue", enqueued);
    output.set("exact_retry", replayed);
    output.set("release", released);
    output.set("maintain", maintained);
    output.set("dead_letters", deadLetters);
    output.set("redrive", redriven);
    output.set("ack", acknowledged);
    output.set("counts", client.counts("jobs", 0));
    output.set("flow", client.consumerFlow("jobs", 0, "docs-java"));
    System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(output));
  }

  private static JsonNode result(JsonNode document) {
    return document.path("receipt").path("outcome").path("result");
  }

  private static String environment(String name, String fallback) {
    String value = System.getenv(name);
    return value == null || value.isBlank() ? fallback : value;
  }
}
