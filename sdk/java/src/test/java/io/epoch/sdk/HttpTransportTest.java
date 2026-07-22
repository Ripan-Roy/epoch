package io.epoch.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class HttpTransportTest {
  private HttpServer server;
  private HttpTransport transport;
  private AtomicReference<String> requestDetails;

  @BeforeEach
  void setUp() throws IOException {
    requestDetails = new AtomicReference<>();
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/ok", this::handleSuccess);
    server.createContext("/empty", exchange -> respond(exchange, 204, ""));
    server.createContext(
        "/error",
        exchange ->
            respond(exchange, 503, "{\"error\":{\"code\":\"unavailable\",\"detail\":\"later\"}}"));
    server.createContext("/plain-error", exchange -> respond(exchange, 502, "upstream failed"));
    server.start();
    transport =
        new HttpTransport(
            URI.create("http://127.0.0.1:" + server.getAddress().getPort()), Duration.ofSeconds(2));
  }

  @AfterEach
  void tearDown() {
    server.stop(0);
  }

  @Test
  void sendsJsonAndEncodesQueries() throws Exception {
    Map<String, Object> query = new LinkedHashMap<>();
    query.put("event_type", "order created");
    query.put("ignored", null);
    JsonNode response =
        transport.request("post", "/ok", JsonNodeFactory.instance.objectNode().put("id", 1), query);

    assertTrue(response.get("ok").booleanValue());
    assertEquals("POST event_type=order%20created {\"id\":1}", requestDetails.get());
  }

  @Test
  void returnsNullForAnEmptySuccessBody() throws Exception {
    JsonNode response = transport.request("DELETE", "/empty", null, null);

    assertTrue(response.isNull());
  }

  @Test
  void exposesTypedRetryableApiErrors() {
    IOException thrown =
        assertThrows(IOException.class, () -> transport.request("GET", "/error", null, null));
    EpochApiException error = assertInstanceOf(EpochApiException.class, thrown);

    assertEquals(503, error.status());
    assertEquals("unavailable", error.code());
    assertEquals("later", error.detail());
    assertTrue(error.retryable());
  }

  @Test
  void preservesStatusForNonJsonProxyErrors() {
    IOException thrown =
        assertThrows(IOException.class, () -> transport.request("GET", "/plain-error", null, null));
    EpochApiException error = assertInstanceOf(EpochApiException.class, thrown);

    assertEquals(502, error.status());
    assertEquals("http_error", error.code());
    assertTrue(error.retryable());
  }

  private void handleSuccess(HttpExchange exchange) throws IOException {
    String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    requestDetails.set(
        exchange.getRequestMethod() + " " + exchange.getRequestURI().getRawQuery() + " " + body);
    respond(exchange, 200, "{\"ok\":true}");
  }

  private static void respond(HttpExchange exchange, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    if (bytes.length == 0) {
      exchange.sendResponseHeaders(status, -1);
    } else {
      exchange.getResponseHeaders().set("content-type", "application/json");
      exchange.sendResponseHeaders(status, bytes.length);
      exchange.getResponseBody().write(bytes);
    }
    exchange.close();
  }
}
