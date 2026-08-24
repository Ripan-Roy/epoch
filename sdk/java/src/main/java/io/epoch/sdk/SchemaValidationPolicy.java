package io.epoch.sdk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Objects;

/** Binds one event-type pattern to an immutable schema revision. */
public record SchemaValidationPolicy(
    String name, String eventTypePattern, String schemaRef, SchemaValidationMode mode) {

  public SchemaValidationPolicy {
    name = SchemaRegistration.resourceName(name, "schema validation policy name");
    RegionalClientCore.required(eventTypePattern, "schema validation event type pattern");
    RegionalClientCore.required(schemaRef, "schema reference");
    Objects.requireNonNull(mode, "mode");
  }

  ObjectNode toJson(ObjectMapper mapper) {
    return mapper
        .createObjectNode()
        .put("name", name)
        .put("event_type_pattern", eventTypePattern)
        .put("schema_ref", schemaRef)
        .put("mode", mode.wireValue());
  }
}
