package io.epoch.sdk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;

/** Deterministic Event Bus header and payload projection transform. */
public record EventTransform(
    Map<String, String> addHeaders, Map<String, String> payloadProjection) {

  public EventTransform {
    addHeaders = Map.copyOf(addHeaders);
    payloadProjection = Map.copyOf(payloadProjection);
  }

  public static EventTransform empty() {
    return new EventTransform(Map.of(), Map.of());
  }

  ObjectNode toJson(ObjectMapper mapper) {
    ObjectNode value = mapper.createObjectNode();
    value.set("add_headers", mapper.valueToTree(addHeaders));
    value.set("payload_projection", mapper.valueToTree(payloadProjection));
    return value;
  }
}
