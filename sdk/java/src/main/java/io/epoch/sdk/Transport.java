package io.epoch.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Map;

/** Minimal synchronous transport boundary consumed by {@link EpochClient}. */
public interface Transport {
  JsonNode request(String method, String path, JsonNode body, Map<String, ?> query)
      throws IOException, InterruptedException;

  /** Header-aware request used by authenticated regional clients. */
  default JsonNode request(
      String method, String path, JsonNode body, Map<String, ?> query, Map<String, String> headers)
      throws IOException, InterruptedException {
    if (headers != null && !headers.isEmpty()) {
      throw new IOException("transport does not support per-request headers");
    }
    return request(method, path, body, query);
  }
}
