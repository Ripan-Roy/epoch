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

final class RegionalStreamClientTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void appendDiscoversLeaderAndCarriesAuthenticationFencesAndTerm() throws Exception {
    RecordingRegionalTransport follower =
        new RecordingRegionalTransport(
            MAPPER.readTree(
                """
                {"resource_generation":"5","tablet_epoch":"3","term":"8","accepts_writes":false}
                """));
    RecordingRegionalTransport leader =
        new RecordingRegionalTransport(
            MAPPER.readTree(
                """
                {"resource_generation":"5","tablet_epoch":"3","term":"8","accepts_writes":true}
                """));
    RegionalStreamClient client =
        RegionalStreamClient.withTransports(
            List.of(follower, leader),
            "secret-token",
            new RegionalScope("acme", "shop", "dev", "core"));
    EventEnvelope event =
        EventEnvelope.builder("checkout", "order.created", Map.of("id", "42"))
            .id("order-42")
            .timeMs(42)
            .build();

    JsonNode response = client.append("orders/eu", 0, "append-42", event);

    assertEquals("committed", response.path("state").asText());
    assertEquals(1, follower.requests.size());
    assertEquals(2, leader.requests.size());
    String path =
        "/v1/organizations/acme/projects/shop/environments/dev/namespaces/core"
            + "/streams/orders%2Feu/shards/0";
    assertEquals(path, follower.requests.get(0).path());
    Request write = leader.requests.get(1);
    assertEquals("POST", write.method());
    assertEquals(path + "/records", write.path());
    assertEquals("Bearer secret-token", write.headers().get("authorization"));
    assertEquals("5", write.headers().get("x-epoch-resource-generation"));
    assertEquals("3", write.headers().get("x-epoch-tablet-epoch"));
    assertEquals("append-42", write.body().path("idempotency_key").asText());
    assertEquals("8", write.body().path("expected_term").asText());
  }

  @Test
  void groupAndLinearizableFetchContractsAreExplicit() throws Exception {
    RecordingRegionalTransport leader =
        new RecordingRegionalTransport(
            MAPPER.readTree(
                """
                {"resource_generation":"2","tablet_epoch":"4","term":"9","accepts_writes":true}
                """));
    RegionalStreamClient client =
        RegionalStreamClient.withTransports(
            List.of(leader), "secret-token", new RegionalScope("acme", "shop", "dev", "core"));

    client.commitOffset("orders", 0, "billing/eu", "member-a", 3, 11, false, "commit-11");
    client.fetch("orders", 0, 11, 25);

    Request commit = leader.requests.get(1);
    assertEquals("PUT", commit.method());
    assertEquals(true, commit.path().endsWith("/groups/billing%2Feu/offsets"));
    assertEquals("commit", commit.body().path("mode").asText());
    Request read = leader.requests.get(3);
    assertEquals("linearizable", read.headers().get("x-epoch-read-consistency"));
    assertEquals(BigInteger.valueOf(11), read.query().get("offset"));
    assertEquals(25, read.query().get("limit"));
  }

  @Test
  void retryableLeaderRaceRediscoversWithoutChangingMutationIdentity() throws Exception {
    RecordingRegionalTransport leader =
        new RecordingRegionalTransport(
            MAPPER.readTree(
                """
                {"resource_generation":"5","tablet_epoch":"3","term":"8","accepts_writes":true}
                """),
            new EpochApiException(
                409, "not_leader", "leadership changed", MAPPER.createObjectNode()));
    RegionalStreamClient client =
        RegionalStreamClient.withTransports(
            List.of(leader), "secret-token", new RegionalScope("acme", "shop", "dev", "core"));
    EventEnvelope event =
        EventEnvelope.builder("checkout", "order.created", Map.of("id", "42"))
            .id("order-42")
            .timeMs(42)
            .build();

    client.append("orders", 0, "append-42", event);

    assertEquals(4, leader.requests.size());
    assertEquals("append-42", leader.requests.get(1).body().path("idempotency_key").asText());
    assertEquals("append-42", leader.requests.get(3).body().path("idempotency_key").asText());
  }

  @Test
  void definitiveDiscoveryFailureIsPreserved() {
    DiscoveryErrorTransport denied =
        new DiscoveryErrorTransport(
            new EpochApiException(403, "forbidden", "scope denied", MAPPER.createObjectNode()));
    RecordingRegionalTransport unused =
        new RecordingRegionalTransport(
            MAPPER
                .createObjectNode()
                .put("resource_generation", "5")
                .put("tablet_epoch", "3")
                .put("term", "8")
                .put("accepts_writes", true));
    RegionalStreamClient client =
        RegionalStreamClient.withTransports(
            List.of(denied, unused),
            "secret-token",
            new RegionalScope("acme", "shop", "dev", "core"));

    EpochApiException failure =
        assertThrows(EpochApiException.class, () -> client.fetch("orders", 0, 0, 1));

    assertEquals(403, failure.status());
    assertEquals("forbidden", failure.code());
    assertEquals(1, denied.requests.size());
    assertEquals(0, unused.requests.size());
  }

  @Test
  void offsetsPreserveTheCompleteUnsigned64BitJsonContract() throws Exception {
    RecordingRegionalTransport leader =
        new RecordingRegionalTransport(
            MAPPER.readTree(
                """
                {"resource_generation":"2","tablet_epoch":"4","term":"9","accepts_writes":true}
                """));
    RegionalStreamClient client =
        RegionalStreamClient.withTransports(
            List.of(leader), "secret-token", new RegionalScope("acme", "shop", "dev", "core"));
    BigInteger maximum = new BigInteger("18446744073709551615");

    client.fetch("orders", 0, maximum, 1);
    client.commitOffset(
        "orders", 0, "billing", "member-a", BigInteger.ONE, maximum, false, "commit-max");

    assertEquals(maximum, leader.requests.get(1).query().get("offset"));
    assertEquals(maximum.toString(), leader.requests.get(3).body().path("next_offset").asText());
  }

  @Test
  void scopeAndMutationInputsFailBeforeNetwork() {
    assertThrows(
        IllegalArgumentException.class, () -> new RegionalScope("", "shop", "dev", "core"));
  }

  private record Request(
      String method,
      String path,
      JsonNode body,
      Map<String, ?> query,
      Map<String, String> headers) {}

  private static final class RecordingRegionalTransport implements Transport {
    private final JsonNode route;
    private final List<Request> requests = new ArrayList<>();
    private IOException nextOperationError;

    private RecordingRegionalTransport(JsonNode route) {
      this(route, null);
    }

    private RecordingRegionalTransport(JsonNode route, IOException nextOperationError) {
      this.route = route;
      this.nextOperationError = nextOperationError;
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
        return route;
      }
      if (nextOperationError != null) {
        IOException error = nextOperationError;
        nextOperationError = null;
        throw error;
      }
      return MAPPER
          .createObjectNode()
          .put("state", "committed")
          .put("outcome_certainty", "committed");
    }
  }

  private static final class DiscoveryErrorTransport implements Transport {
    private final EpochApiException error;
    private final List<Request> requests = new ArrayList<>();

    private DiscoveryErrorTransport(EpochApiException error) {
      this.error = error;
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
      throw error;
    }
  }
}
