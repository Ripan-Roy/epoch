package io.epoch.sdk;

/** Producer advice and broker enforcement mode for one Event Bus schema policy. */
public enum SchemaValidationMode {
  DISABLED("disabled"),
  PRODUCER("producer"),
  BROKER("broker"),
  PRODUCER_AND_BROKER("producer_and_broker");

  private final String wireValue;

  SchemaValidationMode(String wireValue) {
    this.wireValue = wireValue;
  }

  String wireValue() {
    return wireValue;
  }
}
