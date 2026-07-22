package io.epoch.sdk;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/** JDK HTTP client transport with no networking dependency beyond Java itself. */
public final class HttpTransport implements Transport {
  private static final String USER_AGENT = "epoch-java/0.1.0-alpha.1";

  private final String baseUrl;
  private final Duration timeout;
  private final HttpClient client;
  private final ObjectMapper mapper;

  public HttpTransport(URI baseUri, Duration timeout) {
    Objects.requireNonNull(baseUri, "baseUri");
    if (!("http".equals(baseUri.getScheme()) || "https".equals(baseUri.getScheme()))
        || baseUri.getHost() == null
        || baseUri.getQuery() != null
        || baseUri.getFragment() != null) {
      throw new IllegalArgumentException(
          "baseUri must be an absolute HTTP(S) URI without query or fragment");
    }
    if (Objects.requireNonNull(timeout, "timeout").isZero() || timeout.isNegative()) {
      throw new IllegalArgumentException("timeout must be greater than zero");
    }
    baseUrl = stripTrailingSlash(baseUri.toString());
    this.timeout = timeout;
    client =
        HttpClient.newBuilder()
            .connectTimeout(timeout)
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();
    mapper = new ObjectMapper();
  }

  @Override
  public JsonNode request(String method, String path, JsonNode body, Map<String, ?> query)
      throws IOException, InterruptedException {
    URI uri = URI.create(baseUrl + normalizePath(path) + encodeQuery(query));
    HttpRequest.Builder builder =
        HttpRequest.newBuilder(uri)
            .timeout(timeout)
            .header("accept", "application/json")
            .header("user-agent", USER_AGENT);
    if (body == null) {
      builder.method(method.toUpperCase(Locale.ROOT), HttpRequest.BodyPublishers.noBody());
    } else {
      builder.header("content-type", "application/json");
      builder.method(
          method.toUpperCase(Locale.ROOT),
          HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body)));
    }

    HttpResponse<String> response;
    try {
      response = client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    } catch (IOException error) {
      throw new EpochApiException(
          0, "transport_error", error.getMessage(), NullNode.instance, error);
    }
    if (response.statusCode() >= 200 && response.statusCode() < 300) {
      return decodeSuccess(response.body());
    }
    JsonNode responseBody = decodeError(response.body());
    JsonNode error = responseBody.path("error");
    String code = error.path("code").asText("http_error");
    String detail = error.path("detail").asText("HTTP " + response.statusCode());
    throw new EpochApiException(response.statusCode(), code, detail, responseBody);
  }

  private JsonNode decodeSuccess(String body) throws IOException {
    if (body == null || body.isBlank()) {
      return NullNode.instance;
    }
    try {
      return mapper.readTree(body);
    } catch (JsonProcessingException error) {
      throw new IOException("Epoch returned invalid JSON", error);
    }
  }

  private JsonNode decodeError(String body) {
    if (body == null || body.isBlank()) {
      return NullNode.instance;
    }
    try {
      return mapper.readTree(body);
    } catch (JsonProcessingException error) {
      return TextNode.valueOf(body);
    }
  }

  private static String normalizePath(String path) {
    if (path == null || path.isBlank()) {
      throw new IllegalArgumentException("path is required");
    }
    return path.startsWith("/") ? path : "/" + path;
  }

  private static String encodeQuery(Map<String, ?> query) {
    if (query == null || query.isEmpty()) {
      return "";
    }
    StringJoiner encoded = new StringJoiner("&", "?", "");
    query.forEach(
        (key, value) -> {
          if (value != null) {
            encoded.add(urlEncode(key) + "=" + urlEncode(value.toString()));
          }
        });
    return encoded.length() == 1 ? "" : encoded.toString();
  }

  private static String urlEncode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
  }

  private static String stripTrailingSlash(String value) {
    int end = value.length();
    while (end > 0 && value.charAt(end - 1) == '/') {
      end--;
    }
    return value.substring(0, end);
  }
}
