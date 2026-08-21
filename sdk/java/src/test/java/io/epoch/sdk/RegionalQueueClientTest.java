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

final class RegionalQueueClientTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void completeMutationAndLinearizableReadContract() throws Exception {
    RecordingTransport transport = new RecordingTransport();
    RegionalQueueClient client =
        RegionalQueueClient.withTransports(
            List.of(transport), "secret-token", new RegionalScope("acme", "shop", "dev", "core"));
    EventEnvelope event =
        EventEnvelope.builder("checkout", "job.created", Map.of("id", "42"))
            .id("job-42")
            .timeMs(42)
            .build();

    client.enqueueAdvanced(
        "jobs/eu", 0, "enqueue-42", event, "account-7", "correlation-42", "reply-temp");
    client.acquireSession(
        "jobs/eu",
        0,
        "acquire-42",
        "account-7",
        "worker-a",
        BigInteger.valueOf(7),
        4,
        2,
        BigInteger.valueOf(5_000),
        null);
    client.renewSessionLock(
        "jobs/eu",
        0,
        "renew-session",
        "worker-a",
        BigInteger.valueOf(7),
        "session-42",
        BigInteger.valueOf(1_000));
    client.releaseSessionLock(
        "jobs/eu", 0, "release-session", "worker-a", BigInteger.valueOf(7), "session-42");
    client.defer(
        "jobs/eu", 0, "defer-42", "worker-a", BigInteger.valueOf(7), "lease-42", "dependency");
    client.receiveDeferred(
        "jobs/eu", 0, "receive-deferred", "job-42", "worker-a", BigInteger.valueOf(7), null);
    client.acknowledge("jobs/eu", 0, "ack-42", "worker-a", 7, "lease-42");
    client.extendLease("jobs/eu", 0, "extend-42", "worker-a", 7, "lease-42", 1_000);
    client.release("jobs/eu", 0, "release-42", "worker-a", 7, "lease-42", 50, "retry");
    client.nack(
        "jobs/eu",
        0,
        "nack-42",
        "worker-a",
        new BigInteger("18446744073709551615"),
        "lease-42",
        "retry");
    client.reject(
        "jobs/eu",
        0,
        "reject-42",
        "worker-a",
        new BigInteger("18446744073709551615"),
        "lease-42",
        "invalid");
    client.redrive("jobs/eu", 0, "redrive-42", "job-42", 9);
    client.maintain("jobs/eu", 0, "maintain-42");
    client.mutation("jobs/eu", 0, 12);
    client.counts("jobs/eu", 0);
    client.deadLetters("jobs/eu", 0, 25);
    client.redrives("jobs/eu", 0, 25);
    client.consumerFlow("jobs/eu", 0, "worker/a");
    client.advancedStatus("jobs/eu", 0);
    client.correlation("jobs/eu", 0, "correlation/a");
    client.deadLetterForwards("jobs/eu", 0, 25);
    client.status("jobs/eu", 0);

    String base =
        "/v1/organizations/acme/projects/shop/environments/dev/namespaces/core"
            + "/queues/jobs%2Feu/shards/0";
    List<Request> operations = new ArrayList<>();
    for (int index = 1; index < transport.requests.size(); index += 2) {
      operations.add(transport.requests.get(index));
    }
    assertEquals(22, operations.size());
    assertEquals(base + "/mutations", operations.get(0).path());
    assertEquals("acquire", operations.get(1).body().path("operation").path("kind").asText());
    assertEquals("7", operations.get(1).body().path("operation").path("consumer_epoch").asText());
    assertEquals(
        "account-7", operations.get(1).body().path("operation").path("session_id").asText());
    assertEquals(base + "/mutations/12", operations.get(13).path());
    assertEquals(base + "/consumers/worker%2Fa/flow", operations.get(17).path());
    assertEquals(base + "/correlations/correlation%2Fa", operations.get(19).path());
    for (Request request : operations.subList(13, operations.size())) {
      assertEquals("linearizable", request.headers().get("x-epoch-read-consistency"));
    }
  }

  @Test
  void routeDiscoveryRejectsNoncanonicalUnsignedIntegers() {
    RecordingTransport transport = new RecordingTransport("01");
    RegionalQueueClient client =
        RegionalQueueClient.withTransports(
            List.of(transport), "secret-token", new RegionalScope("acme", "shop", "dev", "core"));

    assertThrows(EpochApiException.class, () -> client.counts("jobs", 0));
  }

  private record Request(
      String method,
      String path,
      JsonNode body,
      Map<String, ?> query,
      Map<String, String> headers) {}

  private static final class RecordingTransport implements Transport {
    private final List<Request> requests = new ArrayList<>();
    private final String resourceGeneration;

    private RecordingTransport() {
      this("6");
    }

    private RecordingTransport(String resourceGeneration) {
      this.resourceGeneration = resourceGeneration;
    }

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
            .put("resource_generation", resourceGeneration)
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
