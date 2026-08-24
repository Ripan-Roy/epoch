package io.epoch.sdk;

/** Compiler-backed schema language accepted by the Event Bus registry. */
public enum SchemaFormat {
  AVRO("avro"),
  JSON_SCHEMA("json_schema"),
  PROTOBUF("protobuf");

  private final String wireValue;

  SchemaFormat(String wireValue) {
    this.wireValue = wireValue;
  }

  String wireValue() {
    return wireValue;
  }
}
