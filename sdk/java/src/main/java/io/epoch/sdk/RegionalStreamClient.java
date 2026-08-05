package io.epoch.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Authenticated, leader- and fence-aware client for regional Stream shards. */
public final class RegionalStreamClient {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int MAX_FETCH_RECORDS = 1_000;
  private static final BigInteger MAX_U64 = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);

  private final List<Transport> transports;
  private final String token;
  private final String scopePath;

  /** Constructs a client over one or more regional node endpoints. */
  public RegionalStreamClient(
      List<URI> endpoints, String token, RegionalScope scope, Duration timeout) {
    Objects.requireNonNull(endpoints, "endpoints");
    if (endpoints.isEmpty()) {
      throw new IllegalArgumentException("at least one regional endpoint is required");
    }
    List<Transport> clients = new ArrayList<>(endpoints.size());
    for (URI endpoint : endpoints) {
      clients.add(new HttpTransport(endpoint, timeout));
    }
    this.transports = List.copyOf(clients);
    this.token = token(token);
    this.scopePath = scopePath(scope);
  }

  private RegionalStreamClient(List<Transport> transports, String token, RegionalScope scope) {
    Objects.requireNonNull(transports, "transports");
    if (transports.isEmpty() || transports.stream().anyMatch(Objects::isNull)) {
      throw new IllegalArgumentException(
          "regional transports must contain at least one non-null transport");
    }
    this.transports = List.copyOf(transports);
    this.token = token(token);
    this.scopePath = scopePath(scope);
  }

  /** Constructs with injected transports for tests or custom networking. */
  public static RegionalStreamClient withTransports(
      List<Transport> transports, String token, RegionalScope scope) {
    return new RegionalStreamClient(transports, token, scope);
  }

  /** Appends one record using an explicit idempotency key. */
  public JsonNode append(String stream, int shard, String idempotencyKey, EventEnvelope event)
      throws IOException, InterruptedException {
    required(idempotencyKey, "idempotency key");
    Objects.requireNonNull(event, "event");
    return call(
        stream,
        shard,
        route -> {
          ObjectNode body = MAPPER.createObjectNode();
          body.put("idempotency_key", idempotencyKey);
          body.put("expected_term", route.term());
          body.put("partition", 0);
          body.set("envelope", event.toJson());
          return new RequestSpec("POST", "/records", body, Map.of(), Map.of());
        });
  }

  /** Performs a linearizable bounded fetch. */
  public JsonNode fetch(String stream, int shard, long offset, int limit)
      throws IOException, InterruptedException {
    return fetch(stream, shard, BigInteger.valueOf(offset), limit);
  }

  /** Performs a linearizable bounded fetch over the complete unsigned 64-bit offset range. */
  public JsonNode fetch(String stream, int shard, BigInteger offset, int limit)
      throws IOException, InterruptedException {
    nonNegativeU64(offset, "offset");
    fetchLimit(limit);
    return call(
        stream,
        shard,
        route ->
            new RequestSpec(
                "GET",
                "/records",
                null,
                Map.of("offset", offset, "limit", limit),
                Map.of("x-epoch-read-consistency", "linearizable")));
  }

  /** Commits or explicitly resets a generation-fenced next offset. */
  public JsonNode commitOffset(
      String stream,
      int shard,
      String group,
      String memberId,
      long generation,
      long nextOffset,
      boolean reset,
      String idempotencyKey)
      throws IOException, InterruptedException {
    return commitOffset(
        stream,
        shard,
        group,
        memberId,
        BigInteger.valueOf(generation),
        BigInteger.valueOf(nextOffset),
        reset,
        idempotencyKey);
  }

  /** Commits or resets a checkpoint over the complete unsigned 64-bit position range. */
  public JsonNode commitOffset(
      String stream,
      int shard,
      String group,
      String memberId,
      BigInteger generation,
      BigInteger nextOffset,
      boolean reset,
      String idempotencyKey)
      throws IOException, InterruptedException {
    String groupSegment = segment(group, "consumer group");
    required(memberId, "consumer member");
    positiveU64(generation, "consumer group generation");
    nonNegativeU64(nextOffset, "next offset");
    required(idempotencyKey, "idempotency key");
    return call(
        stream,
        shard,
        route -> {
          ObjectNode body = MAPPER.createObjectNode();
          body.put("idempotency_key", idempotencyKey);
          body.put("expected_term", route.term());
          body.put("member_id", memberId);
          body.put("group_generation", generation.toString());
          body.put("partition", 0);
          body.put("next_offset", nextOffset.toString());
          body.put("mode", reset ? "reset" : "commit");
          return new RequestSpec(
              "PUT", "/groups/" + groupSegment + "/offsets", body, Map.of(), Map.of());
        });
  }

  /** Returns a linearizable checkpoint and lag observation. */
  public JsonNode lag(String stream, int shard, String group)
      throws IOException, InterruptedException {
    String groupSegment = segment(group, "consumer group");
    return call(
        stream,
        shard,
        route ->
            new RequestSpec(
                "GET",
                "/groups/" + groupSegment + "/lag",
                null,
                Map.of(),
                Map.of("x-epoch-read-consistency", "linearizable")));
  }

  /** Fetches records beginning at the durable group checkpoint. */
  public JsonNode fetchGroup(String stream, int shard, String group, int limit)
      throws IOException, InterruptedException {
    fetchLimit(limit);
    String groupSegment = segment(group, "consumer group");
    return call(
        stream,
        shard,
        route ->
            new RequestSpec(
                "GET",
                "/groups/" + groupSegment + "/records",
                null,
                Map.of("limit", limit),
                Map.of("x-epoch-read-consistency", "linearizable")));
  }

  private JsonNode call(String stream, int shard, RequestFactory requestFactory)
      throws IOException, InterruptedException {
    String basePath = streamPath(stream, shard);
    IOException lastError = null;
    for (int attempt = 0; attempt < 2; attempt++) {
      Leader leader;
      try {
        leader = discoverLeader(basePath);
      } catch (EpochApiException error) {
        if (!rediscover(error)) {
          throw error;
        }
        lastError = error;
        continue;
      } catch (IOException error) {
        lastError = error;
        continue;
      }
      RequestSpec request = requestFactory.create(leader.route());
      try {
        return leader
            .transport()
            .request(
                request.method(),
                basePath + request.path(),
                request.body(),
                request.query(),
                headers(leader.route(), request.headers()));
      } catch (EpochApiException error) {
        lastError = error;
        if (!rediscover(error)) {
          throw error;
        }
      }
    }
    throw new EpochApiException(
        0,
        "unavailable",
        "regional Stream operation could not reach a current leader: " + lastError,
        MAPPER.nullNode(),
        lastError);
  }

  private Leader discoverLeader(String path) throws IOException, InterruptedException {
    IOException lastError = null;
    for (Transport transport : transports) {
      try {
        JsonNode document =
            transport.request(
                "GET", path, null, Map.of(), Map.of("authorization", "Bearer " + token));
        Route route = route(document);
        if (document.path("accepts_writes").asBoolean(false)) {
          return new Leader(transport, route);
        }
      } catch (EpochApiException error) {
        if (!rediscover(error)) {
          throw error;
        }
        lastError = error;
      } catch (IOException error) {
        lastError = error;
      } catch (IllegalArgumentException error) {
        lastError = new IOException("regional route response is invalid", error);
      }
    }
    String detail = "no configured endpoint reported the current leader";
    if (lastError != null) {
      detail += ": " + lastError.getMessage();
    }
    throw new EpochApiException(0, "unavailable", detail, MAPPER.nullNode(), lastError);
  }

  private Map<String, String> headers(Route route, Map<String, String> extra) {
    Map<String, String> headers = new LinkedHashMap<>(extra);
    headers.put("authorization", "Bearer " + token);
    headers.put("x-epoch-resource-generation", route.resourceGeneration());
    headers.put("x-epoch-tablet-epoch", route.tabletEpoch());
    return Map.copyOf(headers);
  }

  private String streamPath(String stream, int shard) {
    nonNegative(shard, "shard");
    return scopePath + "/streams/" + segment(stream, "stream") + "/shards/" + shard;
  }

  private static Route route(JsonNode document) {
    if (document == null || !document.isObject()) {
      throw new IllegalArgumentException("regional route response must be an object");
    }
    String generation = decimal(document, "resource_generation");
    String epoch = decimal(document, "tablet_epoch");
    String term = decimal(document, "term");
    return new Route(generation, epoch, term);
  }

  private static String decimal(JsonNode document, String field) {
    String value = document.path(field).asText("");
    try {
      BigInteger parsed = new BigInteger(value);
      if (parsed.signum() <= 0 || parsed.bitLength() > 64) {
        throw new NumberFormatException();
      }
    } catch (NumberFormatException error) {
      throw new IllegalArgumentException(
          "regional route " + field + " must be a non-zero decimal u64", error);
    }
    return value;
  }

  private static boolean rediscover(EpochApiException error) {
    return error.retryable()
        || switch (error.code()) {
          case "not_leader",
              "fenced",
              "route_not_found",
              "route_unavailable",
              "read_barrier_timeout" ->
              true;
          default -> false;
        };
  }

  private static String scopePath(RegionalScope scope) {
    Objects.requireNonNull(scope, "scope");
    return "/v1/organizations/"
        + segment(scope.organization(), "organization")
        + "/projects/"
        + segment(scope.project(), "project")
        + "/environments/"
        + segment(scope.environment(), "environment")
        + "/namespaces/"
        + segment(scope.namespace(), "namespace");
  }

  private static String segment(String value, String label) {
    required(value, label);
    return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
  }

  private static String token(String token) {
    required(token, "bearer token");
    if (token.indexOf('\r') >= 0 || token.indexOf('\n') >= 0) {
      throw new IllegalArgumentException("bearer token must fit one HTTP header");
    }
    return token.trim();
  }

  private static void required(String value, String label) {
    if (Objects.requireNonNull(value, label).isBlank()) {
      throw new IllegalArgumentException(label + " is required");
    }
  }

  private static void nonNegative(long value, String label) {
    if (value < 0) {
      throw new IllegalArgumentException(label + " must be non-negative");
    }
  }

  private static void positiveU64(BigInteger value, String label) {
    nonNegativeU64(value, label);
    if (value.signum() == 0) {
      throw new IllegalArgumentException(label + " must be positive");
    }
  }

  private static void nonNegativeU64(BigInteger value, String label) {
    Objects.requireNonNull(value, label);
    if (value.signum() < 0 || value.compareTo(MAX_U64) > 0) {
      throw new IllegalArgumentException(label + " must be an unsigned 64-bit integer");
    }
  }

  private static void fetchLimit(int limit) {
    if (limit < 1 || limit > MAX_FETCH_RECORDS) {
      throw new IllegalArgumentException("fetch limit must be between 1 and " + MAX_FETCH_RECORDS);
    }
  }

  @FunctionalInterface
  private interface RequestFactory {
    RequestSpec create(Route route);
  }

  private record Route(String resourceGeneration, String tabletEpoch, String term) {}

  private record Leader(Transport transport, Route route) {}

  private record RequestSpec(
      String method,
      String path,
      JsonNode body,
      Map<String, ?> query,
      Map<String, String> headers) {}
}
