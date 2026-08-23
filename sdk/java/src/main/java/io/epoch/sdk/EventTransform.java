package io.epoch.sdk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;

/** Deterministic Event Bus header and payload projection transform. */
public record EventTransform(
    Map<String, String> addHeaders,
    Map<String, String> payloadProjection,
    Map<String, String> renameFields,
    Map<String, Object> constants,
    Map<String, String> templates,
    TransformLimits limits,
    String enrichmentRef) {

  public EventTransform {
    addHeaders = Map.copyOf(addHeaders);
    payloadProjection = Map.copyOf(payloadProjection);
    renameFields = Map.copyOf(renameFields);
    constants = Map.copyOf(constants);
    templates = Map.copyOf(templates);
    if (addHeaders.size() > 64
        || payloadProjection.size() > 64
        || renameFields.size() > 64
        || constants.size() > 64
        || templates.size() > 64) {
      throw new IllegalArgumentException("transform mappings cannot exceed 64 entries each");
    }
    TransformLimits effectiveLimits = limits == null ? TransformLimits.defaults() : limits;
    int operationCount =
        addHeaders.size()
            + payloadProjection.size()
            + renameFields.size()
            + constants.size()
            + templates.size();
    if (operationCount > effectiveLimits.maxOperations()) {
      throw new IllegalArgumentException("transform operations exceed the configured limit");
    }
    if (enrichmentRef != null && enrichmentRef.isBlank()) {
      throw new IllegalArgumentException("enrichment reference cannot be blank");
    }
  }

  public EventTransform(Map<String, String> addHeaders, Map<String, String> payloadProjection) {
    this(addHeaders, payloadProjection, Map.of(), Map.of(), Map.of(), null, null);
  }

  public static EventTransform empty() {
    return new EventTransform(Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), null, null);
  }

  ObjectNode toJson(ObjectMapper mapper) {
    ObjectNode value = mapper.createObjectNode();
    value.set("add_headers", mapper.valueToTree(addHeaders));
    value.set("payload_projection", mapper.valueToTree(payloadProjection));
    value.set("rename_fields", mapper.valueToTree(renameFields));
    value.set("constants", mapper.valueToTree(constants));
    value.set("templates", mapper.valueToTree(templates));
    if (limits != null) {
      value.set("limits", limits.toJson());
    }
    if (enrichmentRef != null) {
      value.put("enrichment_ref", enrichmentRef);
    }
    return value;
  }
}
