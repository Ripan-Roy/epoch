package io.epoch.sdk;

/** Explicit read-only schema-validation path selected by an SDK caller. */
public enum SchemaValidationStage {
  PRODUCER("producer"),
  BROKER("broker");

  private final String wireValue;

  SchemaValidationStage(String wireValue) {
    this.wireValue = wireValue;
  }

  String wireValue() {
    return wireValue;
  }
}
