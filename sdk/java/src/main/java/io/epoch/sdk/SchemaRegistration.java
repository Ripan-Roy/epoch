package io.epoch.sdk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Objects;

/** One compiler-validated, immutable Event Bus schema revision request. */
public record SchemaRegistration(
    String name,
    SchemaFormat format,
    String definition,
    SchemaCompatibility compatibility,
    String rootMessage) {

  public SchemaRegistration {
    name = resourceName(name, "schema name");
    Objects.requireNonNull(format, "format");
    if (Objects.requireNonNull(definition, "definition").isBlank()) {
      throw new IllegalArgumentException("schema definition is required");
    }
    Objects.requireNonNull(compatibility, "compatibility");
    if (format != SchemaFormat.PROTOBUF && rootMessage != null) {
      throw new IllegalArgumentException("rootMessage is valid only for Protobuf schemas");
    }
    if (rootMessage != null && rootMessage.isBlank()) {
      throw new IllegalArgumentException("rootMessage must not be blank");
    }
  }

  public SchemaRegistration(
      String name, SchemaFormat format, String definition, SchemaCompatibility compatibility) {
    this(name, format, definition, compatibility, null);
  }

  ObjectNode toJson(ObjectMapper mapper) {
    ObjectNode value = mapper.createObjectNode();
    value.put("name", name);
    value.put("format", format.wireValue());
    value.put("definition", definition);
    value.put("compatibility", compatibility.wireValue());
    if (rootMessage != null) {
      value.put("root_message", rootMessage);
    }
    return value;
  }

  static String resourceName(String value, String label) {
    RegionalClientCore.required(value, label);
    if (value.length() > 128
        || value.chars().anyMatch(character -> !isResourceNameCharacter(character))) {
      throw new IllegalArgumentException(label + " must be a 1-128 byte resource name");
    }
    return value;
  }

  private static boolean isResourceNameCharacter(int character) {
    return character >= 'a' && character <= 'z'
        || character >= 'A' && character <= 'Z'
        || character >= '0' && character <= '9'
        || character == '-'
        || character == '_'
        || character == '.';
  }
}
