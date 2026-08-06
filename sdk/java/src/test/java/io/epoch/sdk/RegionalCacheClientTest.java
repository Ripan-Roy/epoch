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

final class RegionalCacheClientTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void completeMutationAndLinearizableReadContract() throws Exception {
    RecordingTransport transport = new RecordingTransport();
    RegionalCacheClient client =
        RegionalCacheClient.withTransports(
            List.of(transport), "secret-token", new RegionalScope("acme", "shop", "dev", "core"));
    RegionalCacheLockGuard guard =
        new RegionalCacheLockGuard("critical", "worker-a", BigInteger.valueOf(7), "lease-7");

    client.set(
        "sessions/eu",
        0,
        "set-1",
        "profile",
        RegionalCacheValue.string("alice"),
        BigInteger.valueOf(5_000),
        null);
    client.delete("sessions/eu", 0, "delete-1", "old", BigInteger.valueOf(4), null);
    client.compareAndSet(
        "sessions/eu",
        0,
        "cas-1",
        "profile",
        RegionalCacheExpectation.version(BigInteger.ONE),
        RegionalCacheValue.blob(new byte[] {0, (byte) 255}),
        null,
        guard);
    client.increment("sessions/eu", 0, "inc-1", "visits", -3, BigInteger.ZERO, null, null);
    client.transaction(
        "sessions/eu",
        0,
        "tx-1",
        BigInteger.valueOf(4),
        List.of(
            RegionalCacheMutation.set(
                "hash", RegionalCacheValue.hash(Map.of("role", "admin")), null),
            RegionalCacheMutation.set("set", RegionalCacheValue.set(List.of("a", "b")), null),
            RegionalCacheMutation.set(
                "rank", RegionalCacheValue.sortedSet(Map.of("alice", 1.5)), null),
            RegionalCacheMutation.compareAndSet(
                "new",
                RegionalCacheExpectation.missing(BigInteger.valueOf(4)),
                RegionalCacheValue.counter(-2),
                null)),
        List.of(guard));
    client.acquireLock(
        "sessions/eu",
        0,
        "lock-1",
        "critical",
        "worker-a",
        BigInteger.valueOf(7),
        BigInteger.valueOf(3_000));
    client.renewLock(
        "sessions/eu",
        0,
        "renew-1",
        "critical",
        "worker-a",
        BigInteger.valueOf(7),
        "lease-7",
        BigInteger.valueOf(4_000));
    client.releaseLock(
        "sessions/eu", 0, "release-1", "critical", "worker-a", BigInteger.valueOf(7), "lease-8");
    client.maintain("sessions/eu", 0, "maintain-1", 100);
    client.mutation("sessions/eu", 0, BigInteger.valueOf(12));
    client.observe("sessions/eu", 0, "profile");
    client.status("sessions/eu", 0);

    String base =
        "/v1/organizations/acme/projects/shop/environments/dev/namespaces/core"
            + "/caches/sessions%2Feu/shards/0";
    List<Request> operations = new ArrayList<>();
    for (int index = 1; index < transport.requests.size(); index += 2) {
      operations.add(transport.requests.get(index));
    }
    assertEquals(12, operations.size());
    assertEquals(base + "/mutations", operations.get(0).path());
    assertEquals("transaction", operations.get(4).body().path("operation").path("kind").asText());
    assertEquals(
        "sorted_set",
        operations
            .get(4)
            .body()
            .path("operation")
            .path("mutations")
            .get(2)
            .path("value")
            .path("kind")
            .asText());
    assertEquals(base + "/mutations/12", operations.get(9).path());
    assertEquals("profile", operations.get(10).query().get("key"));
    for (Request request : operations.subList(9, operations.size())) {
      assertEquals("linearizable", request.headers().get("x-epoch-read-consistency"));
    }
  }

  @Test
  void invalidValuesAndBoundsFailBeforeNetwork() {
    RecordingTransport transport = new RecordingTransport();
    RegionalCacheClient client =
        RegionalCacheClient.withTransports(
            List.of(transport), "secret-token", new RegionalScope("acme", "shop", "dev", "core"));

    assertThrows(
        IllegalArgumentException.class, () -> RegionalCacheValue.set(List.of("same", "same")));
    assertThrows(
        IllegalArgumentException.class,
        () -> RegionalCacheValue.sortedSet(Map.of("bad", Double.POSITIVE_INFINITY)));
    assertThrows(
        IllegalArgumentException.class, () -> client.maintain("sessions", 0, "maintain", 0));
    assertThrows(
        IllegalArgumentException.class,
        () -> client.transaction("sessions", 0, "tx", BigInteger.ZERO, List.of(), List.of()));
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
