package io.epoch.sdk;

/** Adjacent-revision compatibility rule enforced by the Event Bus registry. */
public enum SchemaCompatibility {
  NONE("none"),
  BACKWARD("backward"),
  FORWARD("forward"),
  FULL("full");

  private final String wireValue;

  SchemaCompatibility(String wireValue) {
    this.wireValue = wireValue;
  }

  String wireValue() {
    return wireValue;
  }
}
