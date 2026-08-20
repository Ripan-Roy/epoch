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
    client.get("sessions/eu", 0, "get-1", "profile");
    client.atomicBatch(
        "sessions/eu",
        0,
        "batch-1",
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
    assertEquals(13, operations.size());
    assertEquals(base + "/mutations", operations.get(0).path());
    assertEquals("get", operations.get(4).body().path("operation").path("kind").asText());
    assertEquals("transaction", operations.get(5).body().path("operation").path("kind").asText());
    assertEquals(
        "sorted_set",
        operations
            .get(5)
            .body()
            .path("operation")
            .path("mutations")
            .get(2)
            .path("value")
            .path("kind")
            .asText());
    assertEquals(base + "/mutations/12", operations.get(10).path());
    assertEquals("profile", operations.get(11).query().get("key"));
    for (Request request : operations.subList(10, operations.size())) {
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

  @Test
  void routesAdvancedStateBackupQueryAndPubSub() throws Exception {
    RecordingTransport transport = new RecordingTransport();
    RegionalCacheClient client =
        RegionalCacheClient.withTransports(
            List.of(transport), "secret-token", new RegionalScope("acme", "shop", "dev", "core"));

    client.transform(
        "sessions",
        0,
        "transform-1",
        "flags",
        "bitmap_set",
        Map.of("bit", 7, "value", true),
        null,
        null,
        null);
    client.changes("sessions", 0, BigInteger.ONE, 100);
    client.backup("sessions", 0);
    client.restore("sessions", 0, "restore-1", "artifact", BigInteger.valueOf(7));
    client.query("sessions", 0, "bitmap_get", Map.of("key", "flags", "bit", 7));
    client.createSubscription("sessions", 0, List.of("audit"), List.of("orders.*"));
    client.publish("sessions", 0, "audit", Map.of("id", 1));
    client.pollSubscription("sessions", 0, "cache-7-1", 10);
    client.deleteSubscription("sessions", 0, "cache-7-1");
    client.multiplex(
        "sessions",
        0,
        List.of(
            new RegionalCacheMultiplexMutation(
                "profile",
                "multiplex-profile",
                RegionalCacheMutation.set("profile", RegionalCacheValue.string("ready"), null)),
            new RegionalCacheMultiplexMutation(
                "visits",
                "multiplex-visits",
                RegionalCacheMutation.increment("visits", 1, null, null))));

    List<Request> operations = new ArrayList<>();
    for (int index = 1; index < transport.requests.size(); index += 2) {
      operations.add(transport.requests.get(index));
    }
    assertEquals(10, operations.size());
    assertEquals("transform", operations.get(0).body().path("operation").path("kind").asText());
    assertEquals(
        "bitmap_set",
        operations.get(0).body().path("operation").path("transform").path("kind").asText());
    assertEquals("POST", operations.get(4).method());
    assertEquals("linearizable", operations.get(4).headers().get("x-epoch-read-consistency"));
    assertEquals("DELETE", operations.get(8).method());
    assertEquals(
        List.of(
            "/mutations",
            "/changes",
            "/backup",
            "/mutations",
            "/query",
            "/pubsub/subscriptions",
            "/pubsub/messages",
            "/pubsub/subscriptions/cache-7-1/messages",
            "/pubsub/subscriptions/cache-7-1",
            "/multiplex"),
        operations.stream()
            .map(request -> request.path().substring(request.path().indexOf("/shards/0") + 9))
            .toList());
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
