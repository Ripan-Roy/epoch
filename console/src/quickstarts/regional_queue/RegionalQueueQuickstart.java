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

    EventEnvelope sessionEvent =
        EventEnvelope.builder("docs-java", "session.job.created", Map.of("job_id", "java-session-42"))
            .id("docs-java-session-42")
            .timeMs(43)
            .build();
    JsonNode sessionEnqueue =
        client.enqueueAdvanced(
            "jobs",
            0,
            "docs-java-session-enqueue-v1",
            sessionEvent,
            "account-java-7",
            "request-java-7",
            "reply-temporary");
    JsonNode correlated = client.correlation("jobs", 0, "request-java-7");
    JsonNode sessionAcquire =
        client.acquireSession(
            "jobs",
            0,
            "docs-java-session-acquire-v1",
            "account-java-7",
            "docs-java-session",
            BigInteger.ONE,
            1,
            1,
            BigInteger.valueOf(5_000),
            null);
    JsonNode sessionRenew =
        client.renewSessionLock(
            "jobs",
            0,
            "docs-java-session-renew-v1",
            "docs-java-session",
            BigInteger.ONE,
            result(sessionAcquire).path("session_lock_token").asText(),
            BigInteger.valueOf(30_000));
    client.acknowledge(
        "jobs",
        0,
        "docs-java-session-ack-v1",
        "docs-java-session",
        BigInteger.ONE,
        result(sessionAcquire).path("deliveries").path(0).path("lease_token").asText());
    JsonNode sessionRelease =
        client.releaseSessionLock(
            "jobs",
            0,
            "docs-java-session-release-v1",
            "docs-java-session",
            BigInteger.ONE,
            result(sessionRenew).path("session_lock_token").asText());

    EventEnvelope deferredEvent =
        EventEnvelope.builder("docs-java", "job.deferred", Map.of("job_id", "java-deferred-42"))
            .id("docs-java-deferred-42")
            .timeMs(44)
            .build();
    client.enqueue("jobs", 0, "docs-java-deferred-enqueue-v1", deferredEvent);
    JsonNode deferredAcquire =
        client.acquire(
            "jobs", 0, "docs-java-deferred-acquire-v1", "docs-java-deferred", 1, 1, 1, null);
    JsonNode deferred =
        client.defer(
            "jobs",
            0,
            "docs-java-defer-v1",
            "docs-java-deferred",
            BigInteger.ONE,
            result(deferredAcquire).path("deliveries").path(0).path("lease_token").asText(),
            "await dependency");
    JsonNode receivedDeferred =
        client.receiveDeferred(
            "jobs",
            0,
            "docs-java-receive-deferred-v1",
            "docs-java-deferred-42",
            "docs-java-deferred",
            BigInteger.ONE,
            BigInteger.valueOf(5_000));
    client.acknowledge(
        "jobs",
        0,
        "docs-java-deferred-ack-v1",
        "docs-java-deferred",
        BigInteger.ONE,
        result(receivedDeferred).path("delivery").path("lease_token").asText());

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
    output.set("session_enqueue", sessionEnqueue);
    output.set("correlation", correlated);
    output.set("session_release", sessionRelease);
    output.set("defer", deferred);
    output.set("receive_deferred", receivedDeferred);
    output.set("advanced", client.advancedStatus("jobs", 0));
    output.set("dead_letter_forwards", client.deadLetterForwards("jobs", 0, 10));
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
