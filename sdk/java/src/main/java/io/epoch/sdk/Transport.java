package io.epoch.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Map;

/** Minimal synchronous transport boundary consumed by {@link EpochClient}. */
public interface Transport {
  JsonNode request(String method, String path, JsonNode body, Map<String, ?> query)
      throws IOException, InterruptedException;
}
