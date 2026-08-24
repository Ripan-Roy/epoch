package io.epoch.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

/** Shared leader discovery, fencing, and rediscovery for regional clients. */
final class RegionalClientCore {
  static final ObjectMapper MAPPER = new ObjectMapper();
  static final BigInteger MAX_U64 = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);

  private final List<Transport> transports;
  private final String token;
  private final String scopePath;

  private RegionalClientCore(List<Transport> transports, String token, RegionalScope scope) {
    Objects.requireNonNull(transports, "transports");
    if (transports.isEmpty() || transports.stream().anyMatch(Objects::isNull)) {
      throw new IllegalArgumentException(
          "regional transports must contain at least one non-null transport");
    }
    this.transports = List.copyOf(transports);
    this.token = token(token);
    this.scopePath = scopePath(scope);
  }

  static RegionalClientCore forEndpoints(
      List<URI> endpoints, String token, RegionalScope scope, Duration timeout) {
    return forEndpoints(endpoints, token, scope, timeout, null);
  }

  static RegionalClientCore forEndpoints(
      List<URI> endpoints, String token, RegionalScope scope, Duration timeout, TlsConfig tls) {
    Objects.requireNonNull(endpoints, "endpoints");
    if (endpoints.isEmpty()) {
      throw new IllegalArgumentException("at least one regional endpoint is required");
    }
    List<Transport> transports = new ArrayList<>(endpoints.size());
    for (URI endpoint : endpoints) {
      transports.add(
          tls == null
              ? new HttpTransport(endpoint, timeout)
              : new HttpTransport(endpoint, timeout, tls));
    }
    return new RegionalClientCore(transports, token, scope);
  }

  static RegionalClientCore forTransports(
      List<Transport> transports, String token, RegionalScope scope) {
    return new RegionalClientCore(transports, token, scope);
  }

  JsonNode call(
      String collection,
      String resourceLabel,
      String resource,
      int shard,
      RequestFactory requestFactory)
      throws IOException, InterruptedException {
    return callAtGeneration(collection, resourceLabel, resource, shard, null, requestFactory);
  }

  JsonNode callAtGeneration(
      String collection,
      String resourceLabel,
      String resource,
      int shard,
      String expectedGeneration,
      RequestFactory requestFactory)
      throws IOException, InterruptedException {
    String basePath = resourcePath(collection, resourceLabel, resource, shard);
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
      if (expectedGeneration != null
          && !expectedGeneration.equals(leader.route().resourceGeneration())) {
        throw new IOException(
            "Stream routing generation changed from "
                + expectedGeneration
                + " to "
                + leader.route().resourceGeneration()
                + " before the operation; no request was attempted");
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
        "regional " + resourceLabel + " operation could not reach a current leader: " + lastError,
        MAPPER.nullNode(),
        lastError);
  }

  Route discoverRoute(String collection, String resourceLabel, String resource, int shard)
      throws IOException, InterruptedException {
    String path = resourcePath(collection, resourceLabel, resource, shard);
    IOException lastError = null;
    for (Transport transport : transports) {
      try {
        JsonNode document =
            transport.request(
                "GET", path, null, Map.of(), Map.of("authorization", "Bearer " + token));
        return route(document);
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
    throw new EpochApiException(
        0,
        "unavailable",
        "no configured endpoint reported Stream routing metadata",
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

  private String resourcePath(String collection, String resourceLabel, String resource, int shard) {
    nonNegative(shard, "shard");
    return scopePath
        + "/"
        + collection
        + "/"
        + segment(resource, resourceLabel)
        + "/shards/"
        + shard;
  }

  private static Route route(JsonNode document) {
    if (document == null || !document.isObject()) {
      throw new IllegalArgumentException("regional route response must be an object");
    }
    StreamPartitioning partitioning = null;
    JsonNode streamPartitioning = document.path("stream_partitioning");
    if (streamPartitioning.isObject()) {
      int shardCount = streamPartitioning.path("shard_count").asInt(0);
      partitioning =
          new StreamPartitioning(
              streamPartitioning.path("algorithm").asText(""),
              streamPartitioning.path("key_encoding").asText(""),
              streamPartitioning.path("missing_key_fallback").asText(""),
              shardCount);
    }
    return new Route(
        decimal(document, "resource_generation"),
        decimal(document, "tablet_epoch"),
        decimal(document, "term"),
        partitioning);
  }

  private static String decimal(JsonNode document, String field) {
    String value = document.path(field).asText("");
    try {
      BigInteger parsed = new BigInteger(value);
      if (parsed.signum() <= 0 || parsed.bitLength() > 64 || !parsed.toString().equals(value)) {
        throw new NumberFormatException();
      }
    } catch (NumberFormatException error) {
      throw new IllegalArgumentException(
          "regional route " + field + " must be a non-zero decimal u64", error);
    }
    return value;
  }

  private static boolean rediscover(EpochApiException error) {
    if (error.retryable()) {
      return true;
    }
    if ("fenced".equals(error.code())) {
      return error.body().path("retryable").asBoolean(false);
    }
    return switch (error.code()) {
      case "not_leader", "route_not_found", "route_unavailable", "read_barrier_timeout" -> true;
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

  static String segment(String value, String label) {
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

  static void required(String value, String label) {
    if (Objects.requireNonNull(value, label).isBlank()) {
      throw new IllegalArgumentException(label + " is required");
    }
  }

  static void nonNegative(long value, String label) {
    if (value < 0) {
      throw new IllegalArgumentException(label + " must be non-negative");
    }
  }

  static void positiveU64(BigInteger value, String label) {
    nonNegativeU64(value, label);
    if (value.signum() == 0) {
      throw new IllegalArgumentException(label + " must be positive");
    }
  }

  static void nonNegativeU64(BigInteger value, String label) {
    Objects.requireNonNull(value, label);
    if (value.signum() < 0 || value.compareTo(MAX_U64) > 0) {
      throw new IllegalArgumentException(label + " must be an unsigned 64-bit integer");
    }
  }

  @FunctionalInterface
  interface RequestFactory {
    RequestSpec create(Route route);
  }

  record Route(
      String resourceGeneration,
      String tabletEpoch,
      String term,
      StreamPartitioning streamPartitioning) {}

  record StreamPartitioning(
      String algorithm, String keyEncoding, String missingKeyFallback, int shardCount) {}

  private record Leader(Transport transport, Route route) {}

  record RequestSpec(
      String method,
      String path,
      JsonNode body,
      Map<String, ?> query,
      Map<String, String> headers) {}
}
